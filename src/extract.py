"""Extract active/planned clinical trial data from AACT into Parquet + DuckDB."""

import datetime
import time
import uuid
import warnings

import pandas as pd

warnings.filterwarnings(
    "ignore",
    message="pandas only supports SQLAlchemy",
    category=UserWarning,
)

from config.settings import (
    AACT_SCHEMA,
    RAW_DATA_DIR,
    get_aact_connection,
    get_duckdb_connection,
)
from config.tables import ANCHOR_TABLE, EXTRACT_TABLES, STATUS_WHERE_CLAUSE
from src.logging_config import get_logger

logger = get_logger("extract")


def get_extract_query(table_name):
    """Build the SQL query for extracting a table from AACT.

    The studies (anchor) table gets a direct WHERE filter.
    All other tables are filtered via INNER JOIN to studies.
    """
    schema = AACT_SCHEMA

    if table_name == ANCHOR_TABLE:
        return f"SELECT * FROM {schema}.{table_name} WHERE {STATUS_WHERE_CLAUSE}"

    return (
        f"SELECT t.* FROM {schema}.{table_name} t "
        f"INNER JOIN {schema}.studies s ON t.nct_id = s.nct_id "
        f"WHERE s.{STATUS_WHERE_CLAUSE}"
    )


def extract_table(table_name, pg_conn, duck_conn, extract_date):
    """Extract one table from AACT to Parquet and load into DuckDB.

    Returns a metadata dict with extraction details.
    """
    logger.info(f"Extracting {table_name}...")
    started_at = datetime.datetime.now()
    start_time = time.time()

    query = get_extract_query(table_name)
    df = pd.read_sql(query, pg_conn)

    # Write Parquet
    output_dir = RAW_DATA_DIR / extract_date
    output_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = output_dir / f"{table_name}.parquet"
    df.to_parquet(parquet_path, engine="pyarrow", index=False)

    # Load into DuckDB
    duck_conn.execute(f"DROP TABLE IF EXISTS raw.{table_name}")
    duck_conn.execute(
        f"CREATE TABLE raw.{table_name} AS "
        f"SELECT * FROM read_parquet('{parquet_path}')"
    )

    duration = time.time() - start_time
    completed_at = datetime.datetime.now()
    row_count = len(df)

    logger.info(
        f"  {table_name}: {row_count:,} rows in {duration:.1f}s -> {parquet_path}"
    )

    return {
        "table_name": table_name,
        "row_count": row_count,
        "duration_seconds": round(duration, 2),
        "started_at": started_at,
        "completed_at": completed_at,
        "parquet_path": str(parquet_path),
    }


def write_extraction_metadata(duck_conn, extraction_id, extract_date, metadata_list):
    """Write extraction metadata to meta.extraction_log."""
    for meta in metadata_list:
        duck_conn.execute(
            """
            INSERT INTO meta.extraction_log
            (extraction_id, extract_date, table_name, row_count,
             duration_seconds, started_at, completed_at, parquet_path)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            [
                extraction_id,
                extract_date,
                meta["table_name"],
                meta["row_count"],
                meta["duration_seconds"],
                meta["started_at"],
                meta["completed_at"],
                meta["parquet_path"],
            ],
        )
    logger.info(f"Wrote {len(metadata_list)} metadata records to extraction_log")


def run_extraction(tables=None, extract_date=None):
    """Run the full extraction pipeline.

    Args:
        tables: List of table names to extract. Defaults to all EXTRACT_TABLES.
        extract_date: Date string (YYYY-MM-DD) for stamping output. Defaults to today.

    Returns:
        List of metadata dicts, one per table.
    """
    tables = tables or EXTRACT_TABLES
    extract_date = extract_date or datetime.date.today().isoformat()
    extraction_id = str(uuid.uuid4())

    logger.info(f"Starting extraction run {extraction_id}")
    logger.info(f"Extract date: {extract_date}")
    logger.info(f"Tables: {len(tables)}")

    # Ensure studies is extracted first
    ordered_tables = []
    if ANCHOR_TABLE in tables:
        ordered_tables.append(ANCHOR_TABLE)
        ordered_tables.extend(t for t in tables if t != ANCHOR_TABLE)
    else:
        ordered_tables = list(tables)

    pg_conn = get_aact_connection()
    duck_conn = get_duckdb_connection()

    try:
        # Ensure schemas exist
        duck_conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
        duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
        duck_conn.execute("""
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

        metadata_list = []
        total_start = time.time()

        for table_name in ordered_tables:
            meta = extract_table(table_name, pg_conn, duck_conn, extract_date)
            metadata_list.append(meta)

        write_extraction_metadata(duck_conn, extraction_id, extract_date, metadata_list)

        total_duration = time.time() - total_start
        total_rows = sum(m["row_count"] for m in metadata_list)
        logger.info(
            f"Extraction complete: {total_rows:,} total rows "
            f"across {len(metadata_list)} tables in {total_duration:.1f}s"
        )

        return metadata_list

    finally:
        pg_conn.close()
        duck_conn.close()


if __name__ == "__main__":
    from src.logging_config import setup_logging

    setup_logging()
    run_extraction()
