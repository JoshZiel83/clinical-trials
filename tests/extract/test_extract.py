"""Tests for src/extract/aact.py (scanner + atomic swap + provenance)."""

import datetime
import tempfile
from pathlib import Path
from unittest.mock import patch

import duckdb

from config.tables import EXTRACT_TABLES
from src import reference_sources
from src.extract import aact
from src.extract.aact import (
    AACT_CATALOG,
    check_schema_drift,
    current_build_timestamp,
    diff_columns,
    ensure_extract_schema,
    get_extract_query,
    last_pinned_build,
    register_aact_build,
    run_extraction,
    swap_into_place,
    write_extraction_log_row,
)


class TestGetExtractQuery:
    def test_studies_uses_where_clause_no_join(self):
        query = get_extract_query("studies")
        assert "WHERE" in query
        assert "INNER JOIN" not in query
        assert "overall_status IN" in query

    def test_non_studies_uses_join(self):
        query = get_extract_query("conditions")
        assert "INNER JOIN" in query
        assert f"{AACT_CATALOG}.ctgov.studies s ON t.nct_id = s.nct_id" in query
        assert "overall_status IN" in query

    def test_all_tables_are_scanner_qualified(self):
        for table in EXTRACT_TABLES:
            query = get_extract_query(table)
            assert f"{AACT_CATALOG}.ctgov.{table}" in query
            assert "overall_status IN" in query

    def test_since_filter_on_anchor(self):
        query = get_extract_query("studies", since="2026-06-01")
        assert "last_update_posted_date >= DATE '2026-06-01'" in query

    def test_since_filter_on_child_is_alias_qualified(self):
        query = get_extract_query("conditions", since="2026-06-01")
        assert "s.last_update_posted_date >= DATE '2026-06-01'" in query

    def test_no_since_means_full_pull(self):
        assert "last_update_posted_date" not in get_extract_query("studies")
        assert "last_update_posted_date" not in get_extract_query("conditions")


class TestSchemaDrift:
    def test_diff_columns_is_order_insensitive(self):
        missing, added = diff_columns(["a", "b", "c"], ["c", "b", "a"])
        assert missing == [] and added == []

    def test_diff_columns_reports_both(self):
        missing, added = diff_columns(["a", "b"], ["b", "z"])
        assert missing == ["a"] and added == ["z"]

    def test_new_column_warns_does_not_raise(self):
        # Should not raise; just warns.
        check_schema_drift("studies", ["a", "b"], ["a", "b", "new_col"])

    def test_dropped_column_raises(self):
        try:
            check_schema_drift("studies", ["a", "b", "c"], ["a", "b"])
        except RuntimeError as e:
            assert "c" in str(e)
        else:
            raise AssertionError("expected RuntimeError on dropped column")


class TestEnsureExtractSchema:
    def test_creates_schemas_and_log(self):
        con = duckdb.connect(":memory:")
        ensure_extract_schema(con)
        # extraction_log exists and is insertable
        cols = [r[0] for r in con.execute("DESCRIBE meta.extraction_log").fetchall()]
        assert "extraction_id" in cols and "parquet_path" in cols
        con.close()


class TestExtractionLog:
    def test_writes_one_row_per_table(self):
        con = duckdb.connect(":memory:")
        ensure_extract_schema(con)
        for name, n in (("studies", 100), ("conditions", 200)):
            write_extraction_log_row(
                con,
                "run-1",
                "2026-06-20",
                {
                    "table_name": name,
                    "row_count": n,
                    "duration_seconds": 1.0,
                    "started_at": datetime.datetime(2026, 6, 20, 10, 0, 0),
                    "completed_at": datetime.datetime(2026, 6, 20, 10, 0, 1),
                    "parquet_path": f"/tmp/{name}.parquet",
                },
            )
        rows = con.execute(
            "SELECT table_name, row_count FROM meta.extraction_log ORDER BY table_name"
        ).fetchall()
        assert rows == [("conditions", 200), ("studies", 100)]
        con.close()


class TestSwapIntoPlace:
    def test_swaps_staging_into_raw_atomically(self):
        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE TABLE raw.studies AS SELECT 1 AS old_val")
        con.execute("CREATE TABLE raw.studies__staging AS SELECT 42 AS nct_id")
        con.execute("CREATE TABLE raw.conditions__staging AS SELECT 7 AS nct_id")

        swap_into_place(con, ["studies", "conditions"])

        assert con.execute("SELECT nct_id FROM raw.studies").fetchone() == (42,)
        assert con.execute("SELECT nct_id FROM raw.conditions").fetchone() == (7,)
        # staging tables consumed by the rename
        staging = con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema='raw' AND table_name LIKE '%__staging'"
        ).fetchone()[0]
        assert staging == 0
        con.close()

    def test_failure_rolls_back_leaving_raw_untouched(self):
        con = duckdb.connect(":memory:")
        con.execute("CREATE SCHEMA raw")
        con.execute("CREATE TABLE raw.studies AS SELECT 1 AS old_val")
        con.execute("CREATE TABLE raw.studies__staging AS SELECT 42 AS nct_id")
        # 'conditions' has no staging table -> the rename fails mid-swap.
        try:
            swap_into_place(con, ["studies", "conditions"])
        except Exception:
            pass
        else:
            raise AssertionError("expected swap to fail")
        # old raw.studies must survive the rollback
        assert con.execute("SELECT old_val FROM raw.studies").fetchone() == (1,)
        con.close()


class TestRegisterAactBuild:
    def test_pins_build_date_from_updated_at(self):
        with tempfile.TemporaryDirectory() as tmp:
            raw_dir = Path(tmp) / "2026-06-20"
            raw_dir.mkdir()
            (raw_dir / "studies.parquet").write_bytes(b"stub")  # checksum target

            con = duckdb.connect(":memory:")
            con.execute("CREATE SCHEMA raw")
            con.execute(
                "CREATE TABLE raw.studies AS "
                "SELECT TIMESTAMP '2026-06-20 04:21:23' AS updated_at, "
                "DATE '2026-06-18' AS last_update_posted_date"
            )
            version = register_aact_build(con, raw_dir, table_count=14)
            assert version == "2026-06-20"

            row = con.execute(
                "SELECT version, acquired_at, is_active FROM meta.reference_sources "
                "WHERE source_name='aact'"
            ).fetchone()
            assert row[0] == "2026-06-20"
            assert row[1] == datetime.datetime(2026, 6, 20, 4, 21, 23)
            assert row[2] is True
            con.close()


class TestPinGate:
    def test_last_pinned_build_returns_acquired_at(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "snap"
            p.mkdir()
            (p / "x.parquet").write_bytes(b"stub")
            con = duckdb.connect(":memory:")
            reference_sources.register_source(
                con, "aact", "2026-06-20", str(p),
                acquired_at=datetime.datetime(2026, 6, 20, 4, 21, 23),
            )
            assert last_pinned_build(con) == datetime.datetime(2026, 6, 20, 4, 21, 23)
            con.close()

    def test_last_pinned_build_none_when_unregistered(self):
        con = duckdb.connect(":memory:")
        assert last_pinned_build(con) is None
        con.close()

    def test_run_extraction_skips_when_build_unchanged(self):
        """Pin already at the current build + no force -> short-circuit."""
        build_ts = datetime.datetime(2026, 6, 20, 4, 21, 23)
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "t.duckdb"
            snap = Path(tmp) / "snap"
            snap.mkdir()
            (snap / "x.parquet").write_bytes(b"stub")
            # Pre-register the pin at build_ts.
            setup = duckdb.connect(str(db_path))
            ensure_extract_schema(setup)
            reference_sources.register_source(
                setup, "aact", "2026-06-20", str(snap), acquired_at=build_ts
            )
            setup.close()

            with patch.object(aact, "get_duckdb_connection",
                              lambda *a, **k: duckdb.connect(str(db_path))), \
                 patch.object(aact, "attach_aact", lambda *a, **k: None), \
                 patch.object(aact, "current_build_timestamp",
                              lambda *a, **k: build_ts):
                result = run_extraction(force=False)
            assert result["status"] == "skipped"

    def test_run_extraction_proceeds_with_force(self):
        """force=True bypasses the pin-gate (we stop it before any real work)."""
        build_ts = datetime.datetime(2026, 6, 20, 4, 21, 23)
        with tempfile.TemporaryDirectory() as tmp:
            db_path = Path(tmp) / "t.duckdb"
            snap = Path(tmp) / "snap"
            snap.mkdir()
            (snap / "x.parquet").write_bytes(b"stub")
            setup = duckdb.connect(str(db_path))
            ensure_extract_schema(setup)
            reference_sources.register_source(
                setup, "aact", "2026-06-20", str(snap), acquired_at=build_ts
            )
            setup.close()

            # Make staging blow up immediately so we don't hit a real Postgres,
            # but prove we got PAST the pin-gate (skipped would not call stage).
            sentinel = RuntimeError("reached staging")

            def boom(*a, **k):
                raise sentinel

            with patch.object(aact, "get_duckdb_connection",
                              lambda *a, **k: duckdb.connect(str(db_path))), \
                 patch.object(aact, "attach_aact", lambda *a, **k: None), \
                 patch.object(aact, "current_build_timestamp",
                              lambda *a, **k: build_ts), \
                 patch.object(aact, "stage_table", boom):
                try:
                    run_extraction(force=True)
                except RuntimeError as e:
                    assert e is sentinel
                else:
                    raise AssertionError("expected to reach staging past the gate")
