"""Tests for config/settings.py."""

import tempfile
from pathlib import Path

from config.settings import ACTIVE_STATUSES, DUCKDB_PATH, RAW_DATA_DIR, get_duckdb_connection


def test_active_statuses_count():
    assert len(ACTIVE_STATUSES) == 5


def test_active_statuses_values():
    expected = {
        "RECRUITING",
        "NOT_YET_RECRUITING",
        "ACTIVE_NOT_RECRUITING",
        "ENROLLING_BY_INVITATION",
        "AVAILABLE",
    }
    assert set(ACTIVE_STATUSES) == expected


def test_duckdb_path_is_in_data_dir():
    assert "data" in DUCKDB_PATH.parts
    assert DUCKDB_PATH.name == "clinical_trials.duckdb"


def test_raw_data_dir_is_in_data_dir():
    assert "data" in RAW_DATA_DIR.parts
    assert RAW_DATA_DIR.name == "raw"


def test_get_duckdb_connection_opens():
    with tempfile.TemporaryDirectory() as tmpdir:
        db_path = Path(tmpdir) / "test.duckdb"
        conn = get_duckdb_connection(path=db_path)
        try:
            result = conn.execute("SELECT 1").fetchone()
            assert result == (1,)
        finally:
            conn.close()
