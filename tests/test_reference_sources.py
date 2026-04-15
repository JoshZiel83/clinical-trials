"""Tests for src/reference_sources.py — Phase 7E reference provenance."""

import duckdb
import pytest

from src import reference_sources as rs


def _fresh_conn():
    return duckdb.connect(":memory:")


def test_ensure_table_idempotent():
    conn = _fresh_conn()
    rs.ensure_table(conn)
    rs.ensure_table(conn)
    n = conn.execute("SELECT COUNT(*) FROM meta.reference_sources").fetchone()[0]
    assert n == 0


def test_register_and_lookup(tmp_path):
    conn = _fresh_conn()
    f = tmp_path / "synonyms.parquet"
    f.write_bytes(b"fake-chembl-data")

    rs.register_source(conn, "chembl", "36", str(f), notes="test")
    assert rs.get_active_path(conn, "chembl") == str(f)
    assert rs.get_active_version(conn, "chembl") == "36"


def test_only_one_active_per_source(tmp_path):
    conn = _fresh_conn()
    a = tmp_path / "v1.bin"
    a.write_bytes(b"a")
    b = tmp_path / "v2.bin"
    b.write_bytes(b"b")

    rs.register_source(conn, "chembl", "35", str(a))
    rs.register_source(conn, "chembl", "36", str(b))

    active = conn.execute("""
        SELECT version FROM meta.reference_sources
        WHERE source_name = 'chembl' AND is_active = TRUE
    """).fetchall()
    assert active == [("36",)]
    assert rs.get_active_version(conn, "chembl") == "36"


def test_register_non_active_preserves_existing_active(tmp_path):
    conn = _fresh_conn()
    a = tmp_path / "v35.bin"
    a.write_bytes(b"a")
    b = tmp_path / "v36.bin"
    b.write_bytes(b"b")

    rs.register_source(conn, "chembl", "36", str(b))  # active
    rs.register_source(conn, "chembl", "35", str(a), make_active=False)  # archival

    assert rs.get_active_version(conn, "chembl") == "36"
    n = conn.execute(
        "SELECT COUNT(*) FROM meta.reference_sources WHERE source_name = 'chembl'"
    ).fetchone()[0]
    assert n == 2


def test_get_active_path_raises_when_unregistered():
    conn = _fresh_conn()
    with pytest.raises(LookupError):
        rs.get_active_path(conn, "chembl")


def test_register_missing_path_raises():
    conn = _fresh_conn()
    with pytest.raises(FileNotFoundError):
        rs.register_source(conn, "chembl", "36", "/nonexistent/path.parquet")


def test_checksum_file(tmp_path):
    f = tmp_path / "x.bin"
    f.write_bytes(b"hello world")
    cs = rs.compute_checksum(str(f))
    assert len(cs) == 64
    # stable
    assert cs == rs.compute_checksum(str(f))


def test_checksum_directory(tmp_path):
    d = tmp_path / "idx"
    d.mkdir()
    (d / "a.txt").write_bytes(b"a")
    (d / "sub").mkdir()
    (d / "sub" / "b.txt").write_bytes(b"b")
    cs = rs.compute_checksum(str(d))
    assert len(cs) == 64


def test_active_versions_snapshot(tmp_path):
    conn = _fresh_conn()
    f1 = tmp_path / "c.bin"; f1.write_bytes(b"c")
    f2 = tmp_path / "m.bin"; f2.write_bytes(b"m")
    rs.register_source(conn, "chembl", "36", str(f1))
    rs.register_source(conn, "mesh", "2026", str(f2))

    snap = rs.active_versions_snapshot(conn)
    assert snap == {"chembl": "36", "mesh": "2026"}

    subset = rs.active_versions_snapshot(conn, ["chembl"])
    assert subset == {"chembl": "36"}
