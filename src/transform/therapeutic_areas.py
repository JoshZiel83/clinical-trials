"""Load therapeutic area mapping and derive study-level TA assignments."""

import json

import pandas as pd

from config.settings import get_duckdb_connection
from src import entities, reference_sources
from src.logging_config import get_logger

logger = get_logger("therapeutic_areas")


def _resolve_mapping_path(duck_conn=None):
    """Look up the active MeSH→TA mapping path via meta.reference_sources."""
    if duck_conn is not None:
        return reference_sources.get_active_path(duck_conn, "mesh_ta_mapping")
    conn = get_duckdb_connection(read_only=True)
    try:
        return reference_sources.get_active_path(conn, "mesh_ta_mapping")
    finally:
        conn.close()


def load_ta_mapping(json_path=None, duck_conn=None):
    """Load therapeutic area mapping from JSON file.

    Returns a list of dicts with keys: mesh_ancestor, therapeutic_area.
    """
    path = json_path or _resolve_mapping_path(duck_conn)
    with open(path) as f:
        mapping = json.load(f)
    logger.info(f"Loaded {len(mapping)} TA mapping entries from {path}")
    return mapping


def create_ta_reference_table(duck_conn, mapping):
    """Create or replace ref.therapeutic_areas in DuckDB.

    Returns the number of rows loaded.
    """
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS ref")
    duck_conn.execute("DROP TABLE IF EXISTS ref.therapeutic_areas")

    df = pd.DataFrame(mapping)
    duck_conn.execute(
        "CREATE TABLE ref.therapeutic_areas AS SELECT * FROM df"
    )

    row_count = duck_conn.execute(
        "SELECT COUNT(*) FROM ref.therapeutic_areas"
    ).fetchone()[0]
    logger.info(f"Created ref.therapeutic_areas with {row_count} rows")
    return row_count


def create_study_therapeutic_areas(duck_conn):
    """Derive norm.study_therapeutic_areas from browse_conditions + ref.therapeutic_areas.

    Matches both mesh-list and mesh-ancestor terms against the TA mapping
    in a single pass.

    Returns the number of rows created.
    """
    entities.ensure_schema(duck_conn)
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS norm")
    duck_conn.execute("DROP TABLE IF EXISTS norm.study_therapeutic_areas")

    duck_conn.execute("""
        CREATE TABLE norm.study_therapeutic_areas AS
        SELECT DISTINCT
            bc.nct_id,
            ec.condition_id,
            ta.therapeutic_area,
            bc.mesh_type   AS match_source
        FROM raw.browse_conditions bc
        INNER JOIN ref.therapeutic_areas ta
            ON bc.mesh_term = ta.mesh_ancestor
        LEFT JOIN entities.condition ec
            ON bc.mesh_term = ec.canonical_term
    """)

    row_count = duck_conn.execute(
        "SELECT COUNT(*) FROM norm.study_therapeutic_areas"
    ).fetchone()[0]
    logger.info(f"Created norm.study_therapeutic_areas with {row_count:,} rows")
    return row_count


def get_ta_distribution(duck_conn):
    """Return study counts per therapeutic area as a DataFrame."""
    return duck_conn.execute("""
        SELECT therapeutic_area,
               COUNT(DISTINCT nct_id) AS study_count
        FROM norm.study_therapeutic_areas
        GROUP BY therapeutic_area
        ORDER BY study_count DESC
    """).fetchdf()


def run_ta_pipeline(duck_conn=None):
    """Load TA ref table and derive study TAs. Returns row count."""
    close_conn = duck_conn is None
    duck_conn = duck_conn or get_duckdb_connection()

    try:
        mapping = load_ta_mapping(duck_conn=duck_conn)
        create_ta_reference_table(duck_conn, mapping)
        row_count = create_study_therapeutic_areas(duck_conn)

        distribution = get_ta_distribution(duck_conn)
        logger.info("TA distribution:")
        for _, row in distribution.iterrows():
            logger.info(f"  {row['therapeutic_area']}: {row['study_count']:,} studies")

        return row_count
    finally:
        if close_conn:
            duck_conn.close()


if __name__ == "__main__":
    from src.logging_config import setup_logging

    setup_logging()
    run_ta_pipeline()
