"""Entry point for Phase 4: Analytical Views."""

from config.settings import get_duckdb_connection
from src.logging_config import setup_logging
from src.transform.views import build_study_summary

if __name__ == "__main__":
    setup_logging()
    conn = get_duckdb_connection()
    try:
        build_study_summary(conn)
    finally:
        conn.close()
