"""Tests for Phase 7D sponsor anchor set builder."""

import json
from pathlib import Path

import duckdb
import pytest

from src import entities
from src.transform import sponsor_anchors


def _fresh_conn():
    conn = duckdb.connect(":memory:")
    entities.ensure_schema(conn)
    conn.execute("CREATE SCHEMA IF NOT EXISTS norm")
    conn.execute("""
        CREATE TABLE norm.study_sponsors (
            nct_id              VARCHAR,
            original_name       VARCHAR,
            sponsor_id          BIGINT,
            agency_class        VARCHAR,
            lead_or_collaborator VARCHAR
        )
    """)
    return conn


def _seed_sponsors(conn, specs):
    """specs = [(canonical_name, nct_ids=[...]), ...]. Inserts entities +
    one norm.study_sponsors row per (sponsor, nct_id)."""
    ids = {}
    for name, nct_ids in specs:
        sid = entities.upsert_sponsor(conn, canonical_name=name, origin="aact")
        ids[name] = sid
        for nct in nct_ids:
            conn.execute(
                "INSERT INTO norm.study_sponsors "
                "(nct_id, original_name, sponsor_id, agency_class, lead_or_collaborator) "
                "VALUES (?, ?, ?, 'Industry', 'Lead')",
                [nct, name, sid],
            )
    return ids


def _write_curation(tmp_path: Path, top_n=200, include=None, exclude=None) -> Path:
    data = {
        "top_n": top_n,
        "include": include or [],
        "exclude": exclude or [],
    }
    p = tmp_path / "sponsor_anchors.json"
    p.write_text(json.dumps(data))
    return p


def test_build_anchor_set_auto_picks_top_n():
    conn = _fresh_conn()
    _seed_sponsors(conn, [
        ("A", [f"NCT00{i}" for i in range(5)]),   # 5 studies — rank 1
        ("B", [f"NCT01{i}" for i in range(3)]),   # 3 studies — rank 2
        ("C", [f"NCT02{i}" for i in range(2)]),   # 2 studies — rank 3
        ("D", [f"NCT03{i}" for i in range(1)]),   # 1 study  — excluded
    ])
    n = sponsor_anchors.build_anchor_set(conn, top_n=3,
                                          curation_path=Path("/nonexistent"))
    assert n == 3
    rows = conn.execute(
        "SELECT canonical_name, study_count, origin "
        "FROM meta.sponsor_anchor_set ORDER BY study_count DESC"
    ).fetchall()
    assert rows == [
        ("A", 5, "auto"),
        ("B", 3, "auto"),
        ("C", 2, "auto"),
    ]


def test_build_anchor_set_respects_exclude(tmp_path):
    conn = _fresh_conn()
    _seed_sponsors(conn, [
        ("NRG Oncology", [f"NCT0{i}" for i in range(10)]),  # would be top
        ("Novartis",     [f"NCT1{i}" for i in range(5)]),
        ("Pfizer",       [f"NCT2{i}" for i in range(3)]),
    ])
    curation = _write_curation(tmp_path, top_n=3, exclude=["NRG Oncology"])
    n = sponsor_anchors.build_anchor_set(conn, curation_path=curation)
    names = [r[0] for r in conn.execute(
        "SELECT canonical_name FROM meta.sponsor_anchor_set ORDER BY canonical_name"
    ).fetchall()]
    assert "NRG Oncology" not in names
    assert set(names) == {"Novartis", "Pfizer"}
    assert n == 2


def test_build_anchor_set_respects_include_existing(tmp_path):
    conn = _fresh_conn()
    _seed_sponsors(conn, [
        ("Big Pharma",   [f"NCT0{i}" for i in range(10)]),
        ("Mid Pharma",   [f"NCT1{i}" for i in range(5)]),
        ("Tiny Biotech", ["NCT99"]),  # wouldn't make top_n=1 on its own
    ])
    curation = _write_curation(tmp_path, top_n=1, include=["Tiny Biotech"])
    n = sponsor_anchors.build_anchor_set(conn, curation_path=curation)
    rows = conn.execute(
        "SELECT canonical_name, origin FROM meta.sponsor_anchor_set "
        "ORDER BY canonical_name"
    ).fetchall()
    assert ("Big Pharma", "auto") in rows
    assert ("Tiny Biotech", "curated_include") in rows
    assert n == 2


def test_build_anchor_set_skips_missing_include(tmp_path, caplog):
    conn = _fresh_conn()
    _seed_sponsors(conn, [
        ("Novartis", [f"NCT{i}" for i in range(5)]),
    ])
    curation = _write_curation(
        tmp_path, top_n=5,
        include=["Nonexistent Co — not in entities.sponsor"],
    )
    n = sponsor_anchors.build_anchor_set(conn, curation_path=curation)
    # Auto-picks still happen; missing include is logged + skipped.
    assert n == 1
    names = [r[0] for r in conn.execute(
        "SELECT canonical_name FROM meta.sponsor_anchor_set"
    ).fetchall()]
    assert names == ["Novartis"]


def test_build_anchor_set_skips_merged_children(tmp_path):
    """Already-merged children must not occupy anchor slots; only the
    resolved parent counts."""
    conn = _fresh_conn()
    ids = _seed_sponsors(conn, [
        ("Parent Inc",  [f"NCT0{i}" for i in range(5)]),
        ("Parent Inc Pharmaceuticals", [f"NCT1{i}" for i in range(3)]),
    ])
    # Merge child into parent — child's study count should accrue to parent
    # via sponsor_resolved.
    entities.merge_sponsor(
        conn,
        child_id=ids["Parent Inc Pharmaceuticals"],
        parent_id=ids["Parent Inc"],
    )
    n = sponsor_anchors.build_anchor_set(conn, top_n=5,
                                          curation_path=Path("/nonexistent"))
    rows = conn.execute(
        "SELECT canonical_name, study_count FROM meta.sponsor_anchor_set"
    ).fetchall()
    # Only one row — the parent — with both NCTs counted.
    assert len(rows) == 1
    assert rows[0][0] == "Parent Inc"
    assert rows[0][1] == 8


def test_register_anchor_set_writes_reference_source(tmp_path):
    conn = _fresh_conn()
    curation = _write_curation(tmp_path, top_n=3)
    version = sponsor_anchors.register_anchor_set(conn, curation_path=curation)
    row = conn.execute(
        "SELECT version, is_active FROM meta.reference_sources "
        "WHERE source_name = 'sponsor_anchors'"
    ).fetchone()
    assert row is not None
    assert row[0] == version
    assert row[1] is True


def test_build_anchor_set_is_idempotent(tmp_path):
    conn = _fresh_conn()
    _seed_sponsors(conn, [
        ("A", ["NCT1", "NCT2"]),
        ("B", ["NCT3"]),
    ])
    sponsor_anchors.build_anchor_set(conn, top_n=2,
                                      curation_path=Path("/nonexistent"))
    first = conn.execute(
        "SELECT COUNT(*) FROM meta.sponsor_anchor_set"
    ).fetchone()[0]
    sponsor_anchors.build_anchor_set(conn, top_n=2,
                                      curation_path=Path("/nonexistent"))
    second = conn.execute(
        "SELECT COUNT(*) FROM meta.sponsor_anchor_set"
    ).fetchone()[0]
    assert first == second == 2
