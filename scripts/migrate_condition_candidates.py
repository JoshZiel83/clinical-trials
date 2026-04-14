"""One-shot migration: ref.condition_candidates → ref.mapping_candidates.

Copies any existing rows into the shared candidate table with
domain='condition' and source='fuzzy', preserves status + created_at,
then drops the legacy table. Idempotent: a no-op if the legacy table
does not exist.
"""

from config.settings import get_duckdb_connection
from src.hitl import ensure_candidates_table
from src.logging_config import get_logger, setup_logging

logger = get_logger("migrate_condition_candidates")


def migrate(duck_conn):
    exists = duck_conn.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'ref' AND table_name = 'condition_candidates'
        """
    ).fetchone()[0]
    if not exists:
        logger.info("ref.condition_candidates does not exist; nothing to migrate")
        return 0

    ensure_candidates_table(duck_conn)

    before = duck_conn.execute(
        "SELECT COUNT(*) FROM ref.condition_candidates"
    ).fetchone()[0]

    duck_conn.execute("""
        INSERT INTO ref.mapping_candidates
            (domain, source_value, canonical_term, canonical_id,
             score, study_count, source, rationale, tool_trace,
             status, created_at)
        SELECT 'condition', condition_name, canonical_term, NULL,
               score, study_count, 'fuzzy', NULL, NULL,
               status, created_at
        FROM ref.condition_candidates
        ON CONFLICT DO NOTHING
    """)

    duck_conn.execute("DROP TABLE ref.condition_candidates")

    logger.info(f"migrated {before:,} rows from ref.condition_candidates → ref.mapping_candidates")
    return before


if __name__ == "__main__":
    setup_logging()
    conn = get_duckdb_connection()
    try:
        migrate(conn)
    finally:
        conn.close()
