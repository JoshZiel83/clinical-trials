"""One-shot migration: hide pending fuzzy sponsor candidates (Phase 7D).

The Phase 7D anchor-driven agent replaces `generate_sponsor_fuzzy_candidates`.
Any pending rows that were produced by the legacy fuzzy proposer should be
suppressed from the reviewer queue — the agent will re-propose the ones
that survive its stronger grounding requirements, and the rest should not
waste reviewer time.

Only touches rows with `status = 'pending'`. Previously approved / rejected /
hidden fuzzy rows are preserved as history.

Idempotent via `meta.migration_log` — re-running this script is a no-op.
"""

from config.settings import get_duckdb_connection
from src.logging_config import get_logger, setup_logging

logger = get_logger("migrate_sponsor_fuzzy_hidden")


MIGRATION_NAME = "phase_7d_hide_fuzzy_sponsor"


def _ensure_migration_log(duck_conn) -> None:
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.migration_log (
            name        VARCHAR PRIMARY KEY,
            applied_at  TIMESTAMP DEFAULT current_timestamp,
            rows_affected INTEGER
        )
    """)


def _already_applied(duck_conn) -> bool:
    row = duck_conn.execute(
        "SELECT 1 FROM meta.migration_log WHERE name = ?",
        [MIGRATION_NAME],
    ).fetchone()
    return row is not None


def migrate(duck_conn) -> int:
    """Hide pending sponsor/fuzzy rows. Returns count of rows updated."""
    _ensure_migration_log(duck_conn)
    if _already_applied(duck_conn):
        logger.info(
            f"migration {MIGRATION_NAME!r} already applied; skipping"
        )
        return 0

    # Table may not yet exist on a brand-new install.
    exists = duck_conn.execute(
        """
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'ref' AND table_name = 'mapping_candidates'
        """
    ).fetchone()[0]
    if not exists:
        logger.info("ref.mapping_candidates does not exist; nothing to hide")
        # Still record the migration so it doesn't re-run.
        duck_conn.execute(
            "INSERT INTO meta.migration_log (name, rows_affected) VALUES (?, 0)",
            [MIGRATION_NAME],
        )
        return 0

    before = duck_conn.execute(
        """
        SELECT COUNT(*) FROM ref.mapping_candidates
        WHERE domain = 'sponsor' AND source = 'fuzzy' AND status = 'pending'
        """
    ).fetchone()[0]

    duck_conn.execute(
        """
        UPDATE ref.mapping_candidates
        SET status = 'hidden'
        WHERE domain = 'sponsor' AND source = 'fuzzy' AND status = 'pending'
        """
    )

    duck_conn.execute(
        "INSERT INTO meta.migration_log (name, rows_affected) VALUES (?, ?)",
        [MIGRATION_NAME, before],
    )

    logger.info(
        f"hid {before:,} pending sponsor/fuzzy candidates "
        f"(marked migration {MIGRATION_NAME!r} applied)"
    )
    return before


if __name__ == "__main__":
    setup_logging()
    conn = get_duckdb_connection()
    try:
        migrate(conn)
    finally:
        conn.close()
