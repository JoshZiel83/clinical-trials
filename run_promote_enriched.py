"""Entry point for Phase 7C: raw.* → enriched.* projection."""

from config.settings import get_duckdb_connection
from src.logging_config import setup_logging
from src.transform.promote import promote_to_enriched

if __name__ == "__main__":
    setup_logging()
    conn = get_duckdb_connection()
    try:
        promote_to_enriched(conn)
    finally:
        conn.close()
