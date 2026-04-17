"""Promote raw.* → enriched.* (Phase 7C).

Creates stable analytical inputs for the mart. `views.study_summary` reads
only from `enriched.*` + `norm.*` + `class.*` + `entities.*` — never from
`raw.*` directly. Each enriched table is stamped in `meta.enriched_tables`
with when it was rebuilt and which raw extract it reflects.

Promotion discipline: only tables with real cross-consumer utility or with
transformations worth centralizing are promoted here. Rote mirrors are not.
"""

from config.settings import get_duckdb_connection
from src.logging_config import get_logger

logger = get_logger("promote")


_REGISTRY_DDL = """
    CREATE TABLE IF NOT EXISTS meta.enriched_tables (
        table_name VARCHAR PRIMARY KEY,
        last_built_at TIMESTAMP NOT NULL,
        extraction_date DATE,
        source_expression VARCHAR,
        row_count INTEGER,
        notes VARCHAR
    )
"""


def promote_to_enriched(duck_conn) -> dict[str, int]:
    """Project raw.* → enriched.* and stamp meta.enriched_tables.

    Returns {table_name: row_count} for the three promoted tables.
    """
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS enriched")
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    duck_conn.execute(_REGISTRY_DDL)

    extraction_date = duck_conn.execute(
        "SELECT MAX(extract_date) FROM meta.extraction_log"
    ).fetchone()[0]

    counts = {
        "enriched.studies": _promote_studies(duck_conn, extraction_date),
        "enriched.interventions": _promote_interventions(duck_conn, extraction_date),
        "enriched.countries": _promote_countries(duck_conn, extraction_date),
    }

    logger.info(f"Promoted to enriched (extract {extraction_date}):")
    for name, n in counts.items():
        logger.info(f"  {name}: {n:,} rows")

    return counts


def _promote_studies(duck_conn, extraction_date) -> int:
    duck_conn.execute("DROP TABLE IF EXISTS enriched.studies")
    duck_conn.execute("""
        CREATE TABLE enriched.studies AS
        SELECT
            nct_id,
            overall_status,
            study_type,
            phase,
            brief_title,
            official_title,
            enrollment,
            start_date,
            completion_date,
            source,
            YEAR(start_date) AS start_year
        FROM raw.studies
    """)
    row_count = _row_count(duck_conn, "enriched.studies")
    _register(
        duck_conn,
        table_name="enriched.studies",
        extraction_date=extraction_date,
        source_expression="raw.studies (+ start_year = YEAR(start_date))",
        row_count=row_count,
        notes="Anchor study row + derived start_year; only columns the mart consumes are promoted.",
    )
    return row_count


def _promote_interventions(duck_conn, extraction_date) -> int:
    duck_conn.execute("DROP TABLE IF EXISTS enriched.interventions")
    duck_conn.execute("""
        CREATE TABLE enriched.interventions AS
        SELECT nct_id, intervention_type
        FROM raw.interventions
    """)
    row_count = _row_count(duck_conn, "enriched.interventions")
    _register(
        duck_conn,
        table_name="enriched.interventions",
        extraction_date=extraction_date,
        source_expression="raw.interventions",
        row_count=row_count,
        notes="Row-level projection; aggregation stays in views.",
    )
    return row_count


def _promote_countries(duck_conn, extraction_date) -> int:
    duck_conn.execute("DROP TABLE IF EXISTS enriched.countries")
    duck_conn.execute("""
        CREATE TABLE enriched.countries AS
        SELECT nct_id, name
        FROM raw.countries
        WHERE removed = FALSE OR removed IS NULL
    """)
    row_count = _row_count(duck_conn, "enriched.countries")
    _register(
        duck_conn,
        table_name="enriched.countries",
        extraction_date=extraction_date,
        source_expression="raw.countries WHERE removed != TRUE",
        row_count=row_count,
        notes="Removed-country filter applied once upstream.",
    )
    return row_count


def _row_count(duck_conn, qualified_table: str) -> int:
    return duck_conn.execute(f"SELECT COUNT(*) FROM {qualified_table}").fetchone()[0]


def _register(
    duck_conn,
    *,
    table_name: str,
    extraction_date,
    source_expression: str,
    row_count: int,
    notes: str,
) -> None:
    duck_conn.execute(
        "DELETE FROM meta.enriched_tables WHERE table_name = ?",
        [table_name],
    )
    duck_conn.execute(
        """
        INSERT INTO meta.enriched_tables
            (table_name, last_built_at, extraction_date, source_expression, row_count, notes)
        VALUES (?, CURRENT_TIMESTAMP, ?, ?, ?, ?)
        """,
        [table_name, extraction_date, source_expression, row_count, notes],
    )


def run_promote_pipeline(duck_conn=None):
    """Run the raw → enriched projection."""
    close_conn = duck_conn is None
    duck_conn = duck_conn or get_duckdb_connection()

    try:
        logger.info("Promoting raw.* to enriched.*...")
        promote_to_enriched(duck_conn)
        return duck_conn
    finally:
        if close_conn:
            duck_conn.close()


if __name__ == "__main__":
    from src.logging_config import setup_logging

    setup_logging()
    run_promote_pipeline()
