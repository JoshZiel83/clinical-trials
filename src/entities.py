"""Canonical entity tables (Phase 7B).

Stable surrogate IDs for condition / drug / sponsor concepts, with
external-identifier crosswalks (MeSH descriptor, ChEMBL ID, ROR ID, …).
`ref.*_dictionary` FKs point at these; `norm.*` tables key on the
surrogate IDs; `views.study_summary` joins through them.

Invariant: entity rows come from trusted external vocabularies (MeSH,
ChEMBL) during pipeline runs, or from approved HITL decisions via
`src.hitl.promote_candidates`. Never from unresolved candidates.
"""

import json

from src import reference_sources
from src.logging_config import get_logger

logger = get_logger("entities")


def ensure_schema(duck_conn):
    """Create the entities schema + tables + sequences if they don't exist.

    Safe to call repeatedly. Does not drop or repopulate existing tables.
    """
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS entities")

    duck_conn.execute("CREATE SEQUENCE IF NOT EXISTS entities.condition_id_seq START 1")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS entities.condition (
            condition_id       BIGINT PRIMARY KEY
                               DEFAULT nextval('entities.condition_id_seq'),
            origin             VARCHAR NOT NULL,   -- 'mesh' | 'umls' | 'manual'
            mesh_descriptor_id VARCHAR UNIQUE,
            umls_cui           VARCHAR UNIQUE,
            canonical_term     VARCHAR NOT NULL,
            source_versions    JSON
        )
    """)

    duck_conn.execute("CREATE SEQUENCE IF NOT EXISTS entities.drug_id_seq START 1")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS entities.drug (
            drug_id            BIGINT PRIMARY KEY
                               DEFAULT nextval('entities.drug_id_seq'),
            origin             VARCHAR NOT NULL,   -- 'chembl' | 'mesh' | 'manual'
            canonical_name     VARCHAR NOT NULL,
            chembl_id          VARCHAR UNIQUE,
            mesh_descriptor_id VARCHAR,
            unii               VARCHAR,
            source_versions    JSON
        )
    """)

    duck_conn.execute("CREATE SEQUENCE IF NOT EXISTS entities.sponsor_id_seq START 1")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS entities.sponsor (
            sponsor_id      BIGINT PRIMARY KEY
                            DEFAULT nextval('entities.sponsor_id_seq'),
            origin          VARCHAR NOT NULL,   -- 'aact' | 'ror' | 'manual'
            canonical_name  VARCHAR UNIQUE NOT NULL,
            ror_id          VARCHAR UNIQUE,
            ringgold_id     VARCHAR,
            source_versions JSON
        )
    """)

    # Phase 7D: merge lineage for sponsor entities. NULL for un-merged rows;
    # populated by entities.merge_sponsor when a reviewer collapses a variant
    # into an anchor. Views resolve the effective sponsor via
    # entities.sponsor_resolved.
    duck_conn.execute(
        "ALTER TABLE entities.sponsor ADD COLUMN IF NOT EXISTS merged_into_id BIGINT"
    )
    duck_conn.execute(
        "ALTER TABLE entities.sponsor ADD COLUMN IF NOT EXISTS merged_at TIMESTAMP"
    )
    duck_conn.execute(
        "ALTER TABLE entities.sponsor ADD COLUMN IF NOT EXISTS merge_rationale JSON"
    )

    create_or_replace_sponsor_resolved_view(duck_conn)


def create_or_replace_sponsor_resolved_view(duck_conn):
    """Recursive resolver view: map each sponsor_id to its effective (post-merge) id.

    For un-merged rows effective_sponsor_id == sponsor_id, chain_depth == 0.
    `hop < 10` guards against accidental cycles; merge_sponsor's write-time
    validation also prevents them.
    """
    duck_conn.execute("""
        CREATE OR REPLACE VIEW entities.sponsor_resolved AS
        WITH RECURSIVE chain(sponsor_id, hop, head_id) AS (
            SELECT sponsor_id, 0, sponsor_id FROM entities.sponsor
          UNION ALL
            SELECT c.sponsor_id, c.hop + 1, s.merged_into_id
            FROM chain c
            JOIN entities.sponsor s ON c.head_id = s.sponsor_id
            WHERE s.merged_into_id IS NOT NULL AND c.hop < 10
        )
        SELECT
            sponsor_id,
            LAST_VALUE(head_id) OVER (
                PARTITION BY sponsor_id
                ORDER BY hop
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            ) AS effective_sponsor_id,
            MAX(hop) OVER (PARTITION BY sponsor_id) AS chain_depth
        FROM chain
    """)


def seed_drugs_from_chembl(duck_conn):
    """Bulk-seed entities.drug from the active ChEMBL synonyms parquet.

    One row per distinct `chembl_id` with origin='chembl'. Idempotent:
    skips chembl_ids already present. Returns rows inserted.
    """
    import pandas as pd

    try:
        path = reference_sources.get_active_path(duck_conn, "chembl")
        version = reference_sources.get_active_version(duck_conn, "chembl")
    except LookupError:
        logger.warning("ChEMBL not registered in meta.reference_sources; skipping drug seed")
        return 0

    from pathlib import Path
    p = Path(path)
    if not p.exists():
        logger.warning(f"ChEMBL synonyms file not found: {p}")
        return 0

    ensure_schema(duck_conn)

    df = pd.read_parquet(p).dropna(subset=["chembl_id", "pref_name"])
    df = df.drop_duplicates(subset=["chembl_id"])[["chembl_id", "pref_name"]]

    existing = {
        r[0] for r in duck_conn.execute(
            "SELECT chembl_id FROM entities.drug WHERE chembl_id IS NOT NULL"
        ).fetchall()
    }
    if existing:
        df = df[~df["chembl_id"].isin(existing)]
    if df.empty:
        logger.info("entities.drug: no new ChEMBL entities to seed")
        return 0

    source_versions = json.dumps({"chembl": version})
    insert_df = pd.DataFrame({
        "origin": "chembl",
        "canonical_name": df["pref_name"].values,
        "chembl_id": df["chembl_id"].values,
        "source_versions": source_versions,
    })

    duck_conn.register("_drug_seed_df", insert_df)
    try:
        duck_conn.execute("""
            INSERT INTO entities.drug (origin, canonical_name, chembl_id, source_versions)
            SELECT origin, canonical_name, chembl_id, source_versions
            FROM _drug_seed_df
        """)
    finally:
        duck_conn.unregister("_drug_seed_df")

    logger.info(f"entities.drug: seeded {len(insert_df):,} rows from ChEMBL {version}")
    return len(insert_df)


def upsert_condition(duck_conn, canonical_term, origin,
                     mesh_descriptor_id=None, umls_cui=None,
                     source_versions=None):
    """Return condition_id for a canonical_term, inserting if absent.

    Resolution order:
    1. If mesh_descriptor_id is provided, lookup by mesh_descriptor_id.
    2. Else lookup by canonical_term.
    3. Else insert a new row with the given origin.
    """
    if mesh_descriptor_id:
        row = duck_conn.execute(
            "SELECT condition_id FROM entities.condition WHERE mesh_descriptor_id = ?",
            [mesh_descriptor_id],
        ).fetchone()
        if row:
            return row[0]
    row = duck_conn.execute(
        "SELECT condition_id FROM entities.condition WHERE canonical_term = ?",
        [canonical_term],
    ).fetchone()
    if row:
        return row[0]

    sv = json.dumps(source_versions) if source_versions else None
    condition_id = duck_conn.execute(
        """
        INSERT INTO entities.condition
            (origin, mesh_descriptor_id, umls_cui, canonical_term, source_versions)
        VALUES (?, ?, ?, ?, ?)
        RETURNING condition_id
        """,
        [origin, mesh_descriptor_id, umls_cui, canonical_term, sv],
    ).fetchone()[0]
    return condition_id


def upsert_sponsor(duck_conn, canonical_name, origin, ror_id=None,
                   ringgold_id=None, source_versions=None):
    """Return sponsor_id for a canonical_name, inserting if absent.

    Resolution order:
    1. If ror_id is provided, lookup by ror_id. Found → return.
    2. Else lookup by canonical_name (UNIQUE). Found → return.
    3. Else insert a new row with the given origin.

    Phase 7D flatten guard: if a resolved row has merged_into_id set, return
    the parent's sponsor_id (one hop) so ETL seeders never re-point
    dictionary entries at merged-away children.
    """
    if ror_id:
        row = duck_conn.execute(
            "SELECT sponsor_id, merged_into_id FROM entities.sponsor WHERE ror_id = ?",
            [ror_id],
        ).fetchone()
        if row:
            return row[1] if row[1] is not None else row[0]
    row = duck_conn.execute(
        "SELECT sponsor_id, merged_into_id FROM entities.sponsor WHERE canonical_name = ?",
        [canonical_name],
    ).fetchone()
    if row:
        return row[1] if row[1] is not None else row[0]

    sv = json.dumps(source_versions) if source_versions else None
    sponsor_id = duck_conn.execute(
        """
        INSERT INTO entities.sponsor
            (origin, canonical_name, ror_id, ringgold_id, source_versions)
        VALUES (?, ?, ?, ?, ?)
        RETURNING sponsor_id
        """,
        [origin, canonical_name, ror_id, ringgold_id, sv],
    ).fetchone()[0]
    return sponsor_id


def merge_sponsor(duck_conn, child_id, parent_id, rationale=None):
    """Collapse child sponsor into parent. Idempotent. (Phase 7D.)

    Writes merged_into_id, merged_at, merge_rationale on the child row and
    re-points any ref.sponsor_dictionary entries at the parent so the next
    create_study_sponsors rebuild lands fresh rows on the parent.

    Raises ValueError on invalid inputs:
      - either id missing
      - child_id == parent_id
      - child already merged into a DIFFERENT parent (prior merge must be
        undone first)
      - parent itself merged away (force reviewer to re-anchor)
    """
    if child_id == parent_id:
        raise ValueError(f"merge_sponsor: cannot merge sponsor {child_id} into itself")

    rows = duck_conn.execute(
        "SELECT sponsor_id, merged_into_id FROM entities.sponsor "
        "WHERE sponsor_id IN (?, ?)",
        [child_id, parent_id],
    ).fetchall()
    by_id = {r[0]: r[1] for r in rows}
    if child_id not in by_id:
        raise ValueError(f"merge_sponsor: child sponsor_id {child_id} not found")
    if parent_id not in by_id:
        raise ValueError(f"merge_sponsor: parent sponsor_id {parent_id} not found")

    child_merged_into = by_id[child_id]
    parent_merged_into = by_id[parent_id]

    if child_merged_into is not None:
        if child_merged_into == parent_id:
            # Idempotent no-op: already merged the same way.
            return parent_id
        raise ValueError(
            f"merge_sponsor: child {child_id} already merged into "
            f"{child_merged_into}; cannot re-merge into {parent_id} without "
            f"explicit un-merge"
        )
    if parent_merged_into is not None:
        raise ValueError(
            f"merge_sponsor: parent {parent_id} is itself merged into "
            f"{parent_merged_into}; re-anchor to the effective parent first"
        )

    rationale_json = json.dumps(rationale) if rationale is not None else None
    duck_conn.execute(
        """
        UPDATE entities.sponsor
        SET merged_into_id = ?,
            merged_at = current_timestamp,
            merge_rationale = ?
        WHERE sponsor_id = ?
        """,
        [parent_id, rationale_json, child_id],
    )

    # Re-point dictionary entries so subsequent create_study_sponsors writes
    # land on the parent directly. Existing norm.study_sponsors rows stay
    # pointing at the child (audit trail); resolution happens at view layer.
    try:
        duck_conn.execute(
            "UPDATE ref.sponsor_dictionary SET sponsor_id = ? WHERE sponsor_id = ?",
            [parent_id, child_id],
        )
    except Exception as exc:
        # Dictionary table may not exist in some test fixtures — merge
        # itself is done either way; warn but don't fail.
        logger.warning(
            f"merge_sponsor: could not update ref.sponsor_dictionary "
            f"({type(exc).__name__}: {exc})"
        )

    logger.info(
        f"merged entities.sponsor {child_id} → {parent_id}"
    )
    return parent_id


def upsert_drug(duck_conn, canonical_name, origin, chembl_id=None,
                mesh_descriptor_id=None, source_versions=None):
    """Return drug_id for a (canonical_name, chembl_id) pair, inserting if absent.

    Resolution order:
    1. If chembl_id is provided, lookup by chembl_id. Found → return its drug_id.
    2. Else lookup by canonical_name. Found → return its drug_id.
    3. Else insert a new row with the given origin.

    Callers: dictionary layer resolve step, HITL promote_candidates.
    """
    # Normalize chembl_id: pandas may hand us NaN (float) or a non-str value
    # when loaded from parquet with mixed NULLs.
    if chembl_id is not None:
        try:
            import math
            if isinstance(chembl_id, float) and math.isnan(chembl_id):
                chembl_id = None
        except Exception:
            pass
    if chembl_id:
        chembl_id = str(chembl_id)
        row = duck_conn.execute(
            "SELECT drug_id FROM entities.drug WHERE chembl_id = ?",
            [chembl_id],
        ).fetchone()
        if row:
            return row[0]
    # canonical_name fallback lookup
    row = duck_conn.execute(
        "SELECT drug_id FROM entities.drug WHERE canonical_name = ? AND chembl_id IS NULL",
        [canonical_name],
    ).fetchone()
    if row and chembl_id is None:
        return row[0]

    sv = json.dumps(source_versions) if source_versions else None
    drug_id = duck_conn.execute(
        """
        INSERT INTO entities.drug
            (origin, canonical_name, chembl_id, mesh_descriptor_id, source_versions)
        VALUES (?, ?, ?, ?, ?)
        RETURNING drug_id
        """,
        [origin, canonical_name, chembl_id, mesh_descriptor_id, sv],
    ).fetchone()[0]
    return drug_id
