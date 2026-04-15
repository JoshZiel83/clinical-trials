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
    """
    if ror_id:
        row = duck_conn.execute(
            "SELECT sponsor_id FROM entities.sponsor WHERE ror_id = ?",
            [ror_id],
        ).fetchone()
        if row:
            return row[0]
    row = duck_conn.execute(
        "SELECT sponsor_id FROM entities.sponsor WHERE canonical_name = ?",
        [canonical_name],
    ).fetchone()
    if row:
        return row[0]

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
