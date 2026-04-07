"""Detect innovative trial design features via regex on free-text fields (Level 3)."""

from config.settings import get_duckdb_connection
from src.logging_config import get_logger

logger = get_logger("innovative_features")

# Each pattern dict defines one feature type to detect.
# - pattern: regex to search for (DuckDB regexp syntax)
# - exclusion: optional regex — if matched in the same text, discard the hit
# - case_sensitive: if True, match case-sensitively
INNOVATIVE_PATTERNS = [
    {
        "feature_type": "adaptive",
        "pattern": r"\badaptive\b|\badaptation\b",
        "exclusion": r"\badaptive\s+(behav|immun|function|sport|servo|optic|coping|capacit)",
        "case_sensitive": False,
    },
    {
        "feature_type": "basket",
        "pattern": r"\bbasket\b",
        "exclusion": None,
        "case_sensitive": False,
    },
    {
        "feature_type": "umbrella",
        "pattern": r"\bumbrella\b",
        "exclusion": None,
        "case_sensitive": False,
    },
    {
        "feature_type": "platform",
        "pattern": r"\bplatform\s+(trial|study|design|protocol)",
        "exclusion": None,
        "case_sensitive": False,
    },
    {
        "feature_type": "bayesian",
        "pattern": r"\bbayesian\b",
        "exclusion": None,
        "case_sensitive": False,
    },
    {
        "feature_type": "SMART",
        "pattern": r"\bSMART\b",
        "exclusion": None,
        "case_sensitive": True,
    },
    {
        "feature_type": "SMART",
        "pattern": r"sequential\s+multiple\s+assignment\s+randomized",
        "exclusion": None,
        "case_sensitive": False,
    },
    {
        "feature_type": "N-of-1",
        "pattern": r"\bn[- ]of[- ]1\b",
        "exclusion": None,
        "case_sensitive": False,
    },
    {
        "feature_type": "pragmatic",
        "pattern": r"\bpragmatic\b",
        "exclusion": None,
        "case_sensitive": False,
    },
    {
        "feature_type": "enrichment",
        "pattern": r"\benrichment\s+(design|strategy|trial|study)",
        "exclusion": None,
        "case_sensitive": False,
    },
    {
        "feature_type": "seamless",
        "pattern": r"\bseamless\b",
        "exclusion": None,
        "case_sensitive": False,
    },
    {
        "feature_type": "master protocol",
        "pattern": r"\bmaster\s+protocol\b",
        "exclusion": None,
        "case_sensitive": False,
    },
]


def _build_source_queries(pattern_def):
    """Build SQL fragments to search each source field for one pattern.

    Returns a list of SQL SELECT strings (one per source field).
    """
    feature_type = pattern_def["feature_type"]
    pattern = pattern_def["pattern"]
    exclusion = pattern_def["exclusion"]
    case_sensitive = pattern_def["case_sensitive"]

    # DuckDB regexp_matches is case-sensitive by default.
    # For case-insensitive, prepend (?i) to the pattern.
    if not case_sensitive:
        match_pattern = f"(?i){pattern}"
        excl_pattern = f"(?i){exclusion}" if exclusion else None
    else:
        match_pattern = pattern
        excl_pattern = exclusion

    # Escape single quotes in patterns for SQL
    match_sql = match_pattern.replace("'", "''")
    excl_sql = excl_pattern.replace("'", "''") if excl_pattern else None

    queries = []

    # Source 1: brief_title
    q = f"""
        SELECT nct_id, '{feature_type}' AS feature_type,
               'brief_title' AS source_field,
               regexp_extract(brief_title, '{match_sql}') AS matched_text
        FROM raw.studies
        WHERE brief_title IS NOT NULL
        AND regexp_matches(brief_title, '{match_sql}')
    """
    if excl_sql:
        q += f"AND NOT regexp_matches(brief_title, '{excl_sql}')\n"
    queries.append(q)

    # Source 2: official_title
    q = f"""
        SELECT nct_id, '{feature_type}' AS feature_type,
               'official_title' AS source_field,
               regexp_extract(official_title, '{match_sql}') AS matched_text
        FROM raw.studies
        WHERE official_title IS NOT NULL
        AND regexp_matches(official_title, '{match_sql}')
    """
    if excl_sql:
        q += f"AND NOT regexp_matches(official_title, '{excl_sql}')\n"
    queries.append(q)

    # Source 3: detailed_descriptions
    q = f"""
        SELECT dd.nct_id, '{feature_type}' AS feature_type,
               'description' AS source_field,
               regexp_extract(dd.description, '{match_sql}') AS matched_text
        FROM raw.detailed_descriptions dd
        WHERE dd.description IS NOT NULL
        AND regexp_matches(dd.description, '{match_sql}')
    """
    if excl_sql:
        q += f"AND NOT regexp_matches(dd.description, '{excl_sql}')\n"
    queries.append(q)

    # Source 4: keywords
    q = f"""
        SELECT k.nct_id, '{feature_type}' AS feature_type,
               'keyword' AS source_field,
               regexp_extract(k.name, '{match_sql}') AS matched_text
        FROM raw.keywords k
        WHERE k.name IS NOT NULL
        AND regexp_matches(k.name, '{match_sql}')
    """
    if excl_sql:
        q += f"AND NOT regexp_matches(k.name, '{excl_sql}')\n"
    queries.append(q)

    return queries


def detect_innovative_features(duck_conn):
    """Detect innovative design features via regex on free-text fields.

    Creates class.innovative_features with one row per (nct_id, feature_type,
    source_field) combination. A study can have multiple features and a feature
    can be detected in multiple source fields.

    Returns the number of rows created.
    """
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS class")
    duck_conn.execute("DROP TABLE IF EXISTS class.innovative_features")

    # Build one big UNION ALL query across all patterns and source fields
    all_queries = []
    for pattern_def in INNOVATIVE_PATTERNS:
        all_queries.extend(_build_source_queries(pattern_def))

    union_sql = "\nUNION ALL\n".join(all_queries)

    # Deduplicate: same nct_id + feature_type + source_field should appear once
    duck_conn.execute(f"""
        CREATE TABLE class.innovative_features AS
        SELECT DISTINCT nct_id, feature_type, source_field, matched_text
        FROM (
            {union_sql}
        )
    """)

    row_count = duck_conn.execute(
        "SELECT COUNT(*) FROM class.innovative_features"
    ).fetchone()[0]
    study_count = duck_conn.execute(
        "SELECT COUNT(DISTINCT nct_id) FROM class.innovative_features"
    ).fetchone()[0]

    # Log feature distribution
    dist = duck_conn.execute("""
        SELECT feature_type, COUNT(DISTINCT nct_id) AS study_count
        FROM class.innovative_features
        GROUP BY feature_type
        ORDER BY study_count DESC
    """).fetchall()

    logger.info(
        f"Created class.innovative_features: {row_count:,} rows, "
        f"{study_count:,} studies with at least one innovative feature"
    )
    logger.info("Feature distribution (studies):")
    for feat, cnt in dist:
        logger.info(f"  {feat}: {cnt:,}")

    return row_count


def run_innovative_features_pipeline(duck_conn=None):
    """Run the innovative features detection pipeline.

    Returns the connection for chaining.
    """
    close_conn = duck_conn is None
    duck_conn = duck_conn or get_duckdb_connection()

    try:
        logger.info("Detecting innovative design features...")
        detect_innovative_features(duck_conn)
        return duck_conn
    finally:
        if close_conn:
            duck_conn.close()


if __name__ == "__main__":
    from src.logging_config import setup_logging

    setup_logging()
    run_innovative_features_pipeline()
