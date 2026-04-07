"""Entry point for Phase 2B: Study Design Classification."""

from config.settings import get_duckdb_connection
from src.logging_config import setup_logging
from src.classify_design import classify_study_design
from src.innovative_features import detect_innovative_features

if __name__ == "__main__":
    setup_logging()
    conn = get_duckdb_connection()
    try:
        classify_study_design(conn)
        detect_innovative_features(conn)
    finally:
        conn.close()
