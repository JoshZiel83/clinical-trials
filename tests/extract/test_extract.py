"""Tests for src/extract.py."""

import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd

from config.tables import ANCHOR_TABLE, EXTRACT_TABLES
from src.extract.aact import (
    extract_table,
    get_extract_query,
    write_extraction_metadata,
)


class TestGetExtractQuery:
    def test_studies_uses_where_clause(self):
        query = get_extract_query("studies")
        assert "WHERE" in query
        assert "INNER JOIN" not in query
        assert "overall_status IN" in query

    def test_non_studies_uses_join(self):
        query = get_extract_query("conditions")
        assert "INNER JOIN" in query
        assert "ctgov.studies s ON t.nct_id = s.nct_id" in query
        assert "overall_status IN" in query

    def test_all_tables_produce_valid_queries(self):
        for table in EXTRACT_TABLES:
            query = get_extract_query(table)
            assert "SELECT" in query
            assert f"ctgov.{table}" in query
            assert "overall_status IN" in query

    def test_studies_selects_star(self):
        query = get_extract_query("studies")
        assert "SELECT *" in query

    def test_non_studies_selects_t_star(self):
        query = get_extract_query("conditions")
        assert "SELECT t.*" in query


class TestExtractTable:
    def test_writes_parquet_and_loads_duckdb(self):
        with tempfile.TemporaryDirectory() as tmpdir:
            raw_dir = Path(tmpdir) / "raw"
            raw_dir.mkdir()

            # Mock the pg connection / pandas.read_sql
            mock_df = pd.DataFrame({
                "nct_id": ["NCT001", "NCT002"],
                "name": ["test1", "test2"],
            })

            duck_conn = duckdb.connect(":memory:")
            duck_conn.execute("CREATE SCHEMA IF NOT EXISTS raw")

            with patch("src.extract.aact.RAW_DATA_DIR", raw_dir):
                with patch("src.extract.aact.pd.read_sql", return_value=mock_df):
                    meta = extract_table(
                        "conditions",
                        MagicMock(),  # pg_conn (not used since read_sql is mocked)
                        duck_conn,
                        "2026-03-21",
                    )

            # Check metadata
            assert meta["table_name"] == "conditions"
            assert meta["row_count"] == 2
            assert meta["duration_seconds"] >= 0
            assert "parquet_path" in meta

            # Check Parquet file exists
            parquet_path = Path(meta["parquet_path"])
            assert parquet_path.exists()

            # Check DuckDB table loaded
            result = duck_conn.execute(
                "SELECT COUNT(*) FROM raw.conditions"
            ).fetchone()
            assert result[0] == 2

            duck_conn.close()


class TestWriteExtractionMetadata:
    def test_writes_correct_rows(self):
        duck_conn = duckdb.connect(":memory:")
        duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
        duck_conn.execute("""
            CREATE TABLE meta.extraction_log (
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

        import datetime

        metadata_list = [
            {
                "table_name": "studies",
                "row_count": 100,
                "duration_seconds": 1.5,
                "started_at": datetime.datetime(2026, 3, 21, 10, 0, 0),
                "completed_at": datetime.datetime(2026, 3, 21, 10, 0, 1),
                "parquet_path": "/tmp/studies.parquet",
            },
            {
                "table_name": "conditions",
                "row_count": 200,
                "duration_seconds": 2.0,
                "started_at": datetime.datetime(2026, 3, 21, 10, 0, 2),
                "completed_at": datetime.datetime(2026, 3, 21, 10, 0, 4),
                "parquet_path": "/tmp/conditions.parquet",
            },
        ]

        write_extraction_metadata(
            duck_conn, "test-run-id", "2026-03-21", metadata_list
        )

        rows = duck_conn.execute(
            "SELECT * FROM meta.extraction_log ORDER BY table_name"
        ).fetchall()
        assert len(rows) == 2
        assert rows[0][2] == "conditions"  # table_name
        assert rows[0][3] == 200  # row_count
        assert rows[1][2] == "studies"
        assert rows[1][3] == 100

        duck_conn.close()
