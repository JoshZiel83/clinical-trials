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


# Per-domain configuration: which dictionary table to promote into,
# what the key/canonical columns in that dictionary are called, and
# whether the dictionary carries a canonical_id column.
DOMAIN_TARGETS = {
    "condition": {
        "dict_table": "ref.condition_dictionary",
        "key_col": "condition_name",
        "canonical_col": "canonical_term",
        "has_canonical_id": False,
    },
    "drug": {
        "dict_table": "ref.drug_dictionary",
        "key_col": "source_name",
        "canonical_col": "canonical_name",
        "has_canonical_id": True,
    },
    "sponsor": {
        "dict_table": "ref.sponsor_dictionary",
        "key_col": "source_name",
        "canonical_col": "canonical_name",
        "has_canonical_id": True,
    },
}


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

    # Skip rows that collide with existing non-pending decisions (approved/rejected)
    # for the same PK — those are authoritative history.
    existing = duck_conn.execute(
        """
        SELECT source_value, canonical_term
        FROM ref.mapping_candidates
        WHERE domain = ? AND source = ? AND status != 'pending'
        """,
        [domain, source],
    ).fetchdf()
    if not existing.empty:
        existing_keys = set(
            zip(existing["source_value"], existing["canonical_term"])
        )
        mask = [
            (sv, ct) not in existing_keys
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
    """Promote approved candidates into the domain's dictionary as manual entries.

    approved_df columns: source_value, canonical_term. Optional: canonical_id.
    Rows whose source_value is already in the dictionary are skipped.
    Candidate status is marked 'approved' for promoted rows.
    Returns the number of dictionary entries added.
    """
    cfg = _target(domain)
    if approved_df is None or approved_df.empty:
        return 0

    key_col = cfg["key_col"]
    canonical_col = cfg["canonical_col"]
    has_id = cfg["has_canonical_id"]
    dict_table = cfg["dict_table"]

    existing = duck_conn.execute(f"SELECT {key_col} FROM {dict_table}").fetchdf()
    existing_keys = set(existing[key_col]) if not existing.empty else set()

    to_promote = approved_df[~approved_df["source_value"].isin(existing_keys)].copy()
    if to_promote.empty:
        logger.info(
            f"[{domain}] no new candidates to promote (all already in {dict_table})"
        )
        return 0

    insert_cols = [key_col, canonical_col]
    insert_df = pd.DataFrame({
        key_col: to_promote["source_value"].values,
        canonical_col: to_promote["canonical_term"].values,
    })
    if has_id:
        insert_df["canonical_id"] = (
            to_promote["canonical_id"].values
            if "canonical_id" in to_promote.columns
            else None
        )
        insert_cols.append("canonical_id")
    insert_df["mapping_method"] = "manual"
    insert_df["confidence"] = "high"
    insert_cols += ["mapping_method", "confidence"]
    insert_df = insert_df[insert_cols]

    duck_conn.register("_hitl_promote_df", insert_df)
    try:
        duck_conn.execute(
            f"INSERT INTO {dict_table} ({', '.join(insert_cols)}) "
            f"SELECT {', '.join(insert_cols)} FROM _hitl_promote_df"
        )
    finally:
        duck_conn.unregister("_hitl_promote_df")

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
        f"[{domain}] promoted {len(insert_df):,} candidates to {dict_table}"
    )
    return len(insert_df)


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
    (approved|rejected), optional reviewer, decided_at.

    Returns dict: {'approved': N, 'rejected': M, 'promoted': P}.
    """
    ensure_candidates_table(duck_conn)

    df = pd.read_parquet(parquet_path)
    required = {"domain", "source_value", "canonical_term", "source", "decision"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"import_decision_log: missing columns {missing}")

    n_approved = int((df["decision"] == "approved").sum())
    n_rejected = int((df["decision"] == "rejected").sum())
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
              AND d.decision IN ('approved', 'rejected')
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
        f"promoted={n_promoted} (path={parquet_path})"
    )
    return {"approved": n_approved, "rejected": n_rejected, "promoted": n_promoted}
