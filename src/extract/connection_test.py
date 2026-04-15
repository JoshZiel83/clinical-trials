"""Test AACT connectivity and initialize DuckDB schemas."""

from src.logging_config import get_logger
from config.settings import get_aact_connection, get_duckdb_connection
from config.tables import STATUS_WHERE_CLAUSE

logger = get_logger("connection_test")


def test_aact_connection():
    """Connect to AACT and return the count of active studies."""
    logger.info("Connecting to AACT database...")
    conn = get_aact_connection()
    try:
        with conn.cursor() as cur:
            query = f"SELECT COUNT(*) FROM ctgov.studies WHERE {STATUS_WHERE_CLAUSE}"
            cur.execute(query)
            count = cur.fetchone()[0]
            logger.info(f"Connected to AACT. Active/planned study count: {count:,}")
            return count
    finally:
        conn.close()


def initialize_duckdb():
    """Create raw and meta schemas in DuckDB."""
    logger.info("Initializing DuckDB...")
    conn = get_duckdb_connection()
    try:
        conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta.extraction_log (
                extraction_id VARCHAR,
                extract_date DATE,
                table_name VARCHAR,
                row_count INTEGER,
                duration_seconds DOUBLE,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                parquet_path VARCHAR
            )
        """)
        logger.info(f"DuckDB initialized with raw and meta schemas")
    finally:
        conn.close()


if __name__ == "__main__":
    count = test_aact_connection()
    initialize_duckdb()
    logger.info("All connectivity checks passed.")
