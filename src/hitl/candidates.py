"""Generic human-in-the-loop (HITL) enrichment plumbing.

Shared candidate table and promotion helpers used across domains
(condition, drug, sponsor). Per-domain specifics (which dictionary to
promote into, what the key column is called) live in DOMAIN_TARGETS.
"""

import os

import pandas as pd

from src.logging_config import get_logger

logger = get_logger("hitl")


DOMAINS = ("condition", "drug", "sponsor")

# Phase 7A: after this many distinct rejected canonicals for a given
# (domain, source_value, source), skip that source_value in future
# candidate generation. A `hidden` decision suppresses it immediately.
REJECT_THROTTLE = 2


# Per-domain configuration for HITL promotion. Each dictionary is keyed
# on `source_name`/`condition_name` and FKs to an entities.* surrogate id.
# `entity_upsert` takes (duck_conn, canonical_term, origin, external_id) and
# returns the entity id. `origin` is stamped from the approval context.
DOMAIN_TARGETS = {
    "condition": {
        "dict_table": "ref.condition_dictionary",
        "key_col": "condition_name",
        "entity_fk_col": "condition_id",
    },
    "drug": {
        "dict_table": "ref.drug_dictionary",
        "key_col": "source_name",
        "entity_fk_col": "drug_id",
    },
    "sponsor": {
        "dict_table": "ref.sponsor_dictionary",
        "key_col": "source_name",
        "entity_fk_col": "sponsor_id",
    },
}


def _upsert_entity(duck_conn, domain, canonical_term, canonical_id):
    """Resolve (or create) the entity row for a promoted candidate and return its id.

    Promotions are human-confirmed canonicals → origin='manual'. If the
    caller supplied an external id (ChEMBL), entity-layer upsert prefers it.
    """
    from src import entities

    if domain == "condition":
        return entities.upsert_condition(
            duck_conn, canonical_term=canonical_term, origin="manual",
        )
    if domain == "drug":
        return entities.upsert_drug(
            duck_conn, canonical_name=canonical_term, origin="manual",
            chembl_id=canonical_id,
        )
    if domain == "sponsor":
        return entities.upsert_sponsor(
            duck_conn, canonical_name=canonical_term, origin="manual",
        )
    raise ValueError(f"Unknown HITL domain: {domain!r}")


def _target(domain):
    if domain not in DOMAIN_TARGETS:
        raise ValueError(f"Unknown HITL domain: {domain!r}")
    return DOMAIN_TARGETS[domain]


def ensure_candidates_table(duck_conn):
    """Create ref.mapping_candidates if it doesn't exist."""
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS ref")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS ref.mapping_candidates (
            domain          VARCHAR   NOT NULL,
            source_value    VARCHAR   NOT NULL,
            canonical_term  VARCHAR   NOT NULL,
            canonical_id    VARCHAR,
            score           FLOAT     NOT NULL,
            study_count     INTEGER   NOT NULL,
            source          VARCHAR   NOT NULL,
            rationale       VARCHAR,
            tool_trace      JSON,
            status          VARCHAR   NOT NULL DEFAULT 'pending',
            created_at      TIMESTAMP DEFAULT current_timestamp,
            PRIMARY KEY (domain, source_value, canonical_term, source)
        )
    """)


def insert_candidates(duck_conn, domain, df, source):
    """Insert candidate rows for a domain.

    Clears only `pending` rows for (domain, source) before inserting, so
    approved/rejected decisions persist across regenerations.

    Expected df columns: source_value, canonical_term, score, study_count.
    Optional: canonical_id, rationale, tool_trace.
    """
    _target(domain)
    ensure_candidates_table(duck_conn)

    duck_conn.execute(
        """
        DELETE FROM ref.mapping_candidates
        WHERE domain = ? AND source = ? AND status = 'pending'
        """,
        [domain, source],
    )

    if df is None or len(df) == 0:
        logger.info(f"[{domain}/{source}] no candidates to insert")
        return 0

    required = {"source_value", "canonical_term", "score", "study_count"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"insert_candidates: df missing columns {missing}")

    insert_df = df.copy()
    insert_df["domain"] = domain
    insert_df["source"] = source
    insert_df["status"] = insert_df.get("status", "pending")
    for col in ("canonical_id", "rationale", "tool_trace"):
        if col not in insert_df.columns:
            insert_df[col] = None

    # Skip rows that collide with existing non-pending decisions (approved/rejected/hidden)
    # for the same PK — those are authoritative history. Also apply source-level
    # throttles: a `hidden` decision suppresses the source_value entirely, and
    # `REJECT_THROTTLE` distinct rejected canonicals for the same source_value
    # exhaust it (Phase 7A).
    existing = duck_conn.execute(
        """
        SELECT source_value, canonical_term, status
        FROM ref.mapping_candidates
        WHERE domain = ? AND source = ? AND status != 'pending'
        """,
        [domain, source],
    ).fetchdf()
    if not existing.empty:
        existing_keys = set(
            zip(existing["source_value"], existing["canonical_term"])
        )
        hidden_sources = set(
            existing.loc[existing["status"] == "hidden", "source_value"]
        )
        rejected = existing[existing["status"] == "rejected"]
        reject_counts = (
            rejected.groupby("source_value")["canonical_term"].nunique().to_dict()
        )
        exhausted_sources = {
            sv for sv, n in reject_counts.items() if n >= REJECT_THROTTLE
        }
        banned_sources = hidden_sources | exhausted_sources
        mask = [
            (sv, ct) not in existing_keys and sv not in banned_sources
            for sv, ct in zip(insert_df["source_value"], insert_df["canonical_term"])
        ]
        insert_df = insert_df[mask]

    if insert_df.empty:
        logger.info(f"[{domain}/{source}] all candidates already reviewed; nothing to insert")
        return 0

    cols = [
        "domain", "source_value", "canonical_term", "canonical_id",
        "score", "study_count", "source", "rationale", "tool_trace", "status",
    ]
    insert_df = insert_df[cols]

    duck_conn.register("_hitl_insert_df", insert_df)
    try:
        duck_conn.execute(f"""
            INSERT INTO ref.mapping_candidates ({", ".join(cols)})
            SELECT {", ".join(cols)} FROM _hitl_insert_df
        """)
    finally:
        duck_conn.unregister("_hitl_insert_df")

    logger.info(f"[{domain}/{source}] inserted {len(insert_df):,} candidates")
    return len(insert_df)


def promote_candidates(duck_conn, domain, approved_df):
    """Promote approved candidates into the domain's dictionary (Phase 7B).

    For each approved row:
      1. Resolve/create an entity row via entities.upsert_* (origin='manual').
      2. Insert ref.<domain>_dictionary(source_name/condition_name, *_id,
         mapping_method='manual', confidence='high').
    Rows whose source_value is already in the dictionary are skipped.
    approved_df columns: source_value, canonical_term. Optional: canonical_id.
    Returns the number of dictionary entries added.
    """
    from src import entities  # noqa: F401  (ensures module importable)

    cfg = _target(domain)
    if approved_df is None or approved_df.empty:
        return 0

    key_col = cfg["key_col"]
    entity_fk_col = cfg["entity_fk_col"]
    dict_table = cfg["dict_table"]

    existing = duck_conn.execute(f"SELECT {key_col} FROM {dict_table}").fetchdf()
    existing_keys = set(existing[key_col]) if not existing.empty else set()

    to_promote = approved_df[~approved_df["source_value"].isin(existing_keys)].copy()
    if to_promote.empty:
        logger.info(
            f"[{domain}] no new candidates to promote (all already in {dict_table})"
        )
        return 0

    # Resolve each row to an entity id.
    has_canonical_id = "canonical_id" in to_promote.columns
    inserts = []
    for _, row in to_promote.iterrows():
        canonical_id = row["canonical_id"] if has_canonical_id else None
        entity_id = _upsert_entity(
            duck_conn, domain,
            canonical_term=row["canonical_term"],
            canonical_id=canonical_id,
        )
        inserts.append((row["source_value"], entity_id, "manual", "high"))

    duck_conn.executemany(
        f"INSERT INTO {dict_table} ({key_col}, {entity_fk_col}, mapping_method, confidence) "
        f"VALUES (?, ?, ?, ?)",
        inserts,
    )

    promoted_values = list(to_promote["source_value"])
    duck_conn.execute(
        """
        UPDATE ref.mapping_candidates
        SET status = 'approved'
        WHERE domain = ? AND source_value IN (SELECT unnest(?))
        """,
        [domain, promoted_values],
    )

    logger.info(
        f"[{domain}] promoted {len(inserts):,} candidates to {dict_table}"
    )
    return len(inserts)


def export_candidates_csv(duck_conn, domain, output_path=None):
    """Export pending candidates for a domain to CSV for offline review."""
    _target(domain)
    if output_path is None:
        output_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "data", "reference", f"{domain}_candidates.csv",
        )

    df = duck_conn.execute(
        """
        SELECT source_value, canonical_term, canonical_id, score,
               study_count, source, status
        FROM ref.mapping_candidates
        WHERE domain = ? AND status = 'pending'
        ORDER BY study_count DESC, score DESC
        """,
        [domain],
    ).fetchdf()

    df.to_csv(output_path, index=False)
    logger.info(f"[{domain}] exported {len(df):,} pending candidates to {output_path}")
    return output_path


def import_reviewed_csv(duck_conn, domain, csv_path):
    """Import a reviewed CSV. Promotes rows with status='approved',
    marks status='rejected' rows as rejected in the candidates table.
    Returns the number promoted.
    """
    _target(domain)
    reviewed = pd.read_csv(csv_path)

    approved = reviewed[reviewed["status"] == "approved"]
    promoted = promote_candidates(duck_conn, domain, approved)

    rejected = reviewed[reviewed["status"] == "rejected"]
    if not rejected.empty:
        rejected_values = list(rejected["source_value"])
        duck_conn.execute(
            """
            UPDATE ref.mapping_candidates
            SET status = 'rejected'
            WHERE domain = ? AND source_value IN (SELECT unnest(?))
            """,
            [domain, rejected_values],
        )
        logger.info(f"[{domain}] marked {len(rejected):,} candidates as rejected")

    return promoted


def import_decision_log(duck_conn, parquet_path):
    """Apply a Shiny-written decision log (Parquet) to ref.mapping_candidates
    and promote approvals into their target dictionaries. Idempotent.

    Expected columns: domain, source_value, canonical_term, source, decision
    (approved|rejected|hidden), optional reviewer, decided_at.

    A `hidden` decision suppresses the (source_value, source) entirely from
    future candidate generation (Phase 7A).

    Returns dict: {'approved': N, 'rejected': M, 'hidden': H, 'promoted': P}.
    """
    ensure_candidates_table(duck_conn)

    df = pd.read_parquet(parquet_path)
    required = {"domain", "source_value", "canonical_term", "source", "decision"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"import_decision_log: missing columns {missing}")

    n_approved = int((df["decision"] == "approved").sum())
    n_rejected = int((df["decision"] == "rejected").sum())
    n_hidden = int((df["decision"] == "hidden").sum())
    n_promoted = 0

    # Apply status updates
    duck_conn.register("_hitl_decisions", df)
    try:
        duck_conn.execute("""
            UPDATE ref.mapping_candidates AS c
            SET status = d.decision
            FROM _hitl_decisions AS d
            WHERE c.domain = d.domain
              AND c.source_value = d.source_value
              AND c.canonical_term = d.canonical_term
              AND c.source = d.source
              AND d.decision IN ('approved', 'rejected', 'hidden')
        """)
    finally:
        duck_conn.unregister("_hitl_decisions")

    # Promote approvals per domain
    approved = df[df["decision"] == "approved"]
    for domain in approved["domain"].unique():
        sub = approved[approved["domain"] == domain][
            ["source_value", "canonical_term"]
        ].copy()
        if "canonical_id" in approved.columns:
            sub["canonical_id"] = approved[approved["domain"] == domain]["canonical_id"].values
        n_promoted += promote_candidates(duck_conn, domain, sub)

    logger.info(
        f"decision log: approved={n_approved} rejected={n_rejected} "
        f"hidden={n_hidden} promoted={n_promoted} (path={parquet_path})"
    )
    return {
        "approved": n_approved,
        "rejected": n_rejected,
        "hidden": n_hidden,
        "promoted": n_promoted,
    }
