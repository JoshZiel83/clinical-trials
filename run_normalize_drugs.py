"""Convenience entry point for Phase 2D: drug normalization pipeline."""

from config.settings import get_duckdb_connection
from src.logging_config import setup_logging
from src.normalize_drugs import run_normalization_pipeline

if __name__ == "__main__":
    setup_logging()
    conn = get_duckdb_connection()
    try:
        run_normalization_pipeline(conn)
    finally:
        conn.close()
