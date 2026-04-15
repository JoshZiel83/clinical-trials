"""Convenience entry point for condition normalization + TA mapping pipeline."""

from config.settings import get_duckdb_connection
from src.logging_config import setup_logging
from src.transform.normalize_conditions import get_coverage_stats, run_normalization_pipeline
from src.transform.therapeutic_areas import run_ta_pipeline

if __name__ == "__main__":
    setup_logging()
    conn = get_duckdb_connection()
    try:
        # Step 1: Build condition dictionary + study_conditions
        run_normalization_pipeline(conn)
        # Step 2: Load TA ref + derive study TAs
        run_ta_pipeline(conn)
        # Step 3: Report coverage
        get_coverage_stats(conn)
    finally:
        conn.close()
