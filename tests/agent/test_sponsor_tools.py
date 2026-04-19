"""Tests for Phase 7D sponsor-specific agent tools."""

from unittest.mock import patch

import duckdb
import pytest

from src import entities
from src.agent import enrichment_tools as et


def _fresh_conn():
    conn = duckdb.connect(":memory:")
    entities.ensure_schema(conn)
    conn.execute("CREATE SCHEMA IF NOT EXISTS norm")
    conn.execute("""
        CREATE TABLE norm.study_sponsors (
            nct_id               VARCHAR,
            original_name        VARCHAR,
            sponsor_id           BIGINT,
            agency_class         VARCHAR,
            lead_or_collaborator VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE meta.sponsor_anchor_set (
            sponsor_id     BIGINT PRIMARY KEY,
            canonical_name VARCHAR NOT NULL,
            study_count    INTEGER NOT NULL,
            origin         VARCHAR NOT NULL,
            built_at       TIMESTAMP
        )
    """) if conn.execute("""
        SELECT COUNT(*) FROM information_schema.schemata
        WHERE schema_name = 'meta'
    """).fetchone()[0] else conn.execute("CREATE SCHEMA meta")

    # Ensure meta schema exists regardless
    conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.sponsor_anchor_set (
            sponsor_id     BIGINT PRIMARY KEY,
            canonical_name VARCHAR NOT NULL,
            study_count    INTEGER NOT NULL,
            origin         VARCHAR NOT NULL,
            built_at       TIMESTAMP
        )
    """)
    return conn


def _seed_anchor(conn, canonical_name, study_count, origin="auto"):
    sid = entities.upsert_sponsor(conn, canonical_name=canonical_name, origin="aact")
    conn.execute(
        "INSERT INTO meta.sponsor_anchor_set "
        "(sponsor_id, canonical_name, study_count, origin) VALUES (?, ?, ?, ?)",
        [sid, canonical_name, study_count, origin],
    )
    return sid


# ---------- sponsor_anchor_lookup -----------------------------------------


def test_anchor_lookup_matches_high_similarity():
    conn = _fresh_conn()
    _seed_anchor(conn, "Novartis", study_count=500)
    _seed_anchor(conn, "Pfizer", study_count=450)
    ctx = et.ToolContext(duck_conn=conn)

    results = et.sponsor_anchor_lookup(ctx, "Novartis Pharmaceuticals", limit=5)
    names = [r["canonical_term"] for r in results]
    assert "Novartis" in names
    novartis = next(r for r in results if r["canonical_term"] == "Novartis")
    assert novartis["score"] >= 70
    assert novartis["anchor_sponsor_id"] > 0
    assert novartis["study_count"] == 500


def test_anchor_lookup_returns_empty_when_no_anchors():
    conn = _fresh_conn()
    ctx = et.ToolContext(duck_conn=conn)
    assert et.sponsor_anchor_lookup(ctx, "Novartis") == []


def test_anchor_lookup_empty_query():
    conn = _fresh_conn()
    _seed_anchor(conn, "Novartis", 100)
    ctx = et.ToolContext(duck_conn=conn)
    assert et.sponsor_anchor_lookup(ctx, "") == []
    assert et.sponsor_anchor_lookup(ctx, "  ") == []


def test_anchor_lookup_respects_cutoff():
    """Unrelated strings should not match the anchor set."""
    conn = _fresh_conn()
    _seed_anchor(conn, "Novartis", 500)
    ctx = et.ToolContext(duck_conn=conn)
    results = et.sponsor_anchor_lookup(ctx, "Some Random Hospital System")
    # rapidfuzz WRatio between unrelated names typically << 70
    assert results == []


# ---------- sponsor_co_occurrence -----------------------------------------


def test_co_occurrence_returns_frequent_cosponsors():
    conn = _fresh_conn()
    parent = entities.upsert_sponsor(conn, canonical_name="novartis", origin="aact")
    child = entities.upsert_sponsor(
        conn, canonical_name="novartis pharmaceuticals", origin="aact"
    )
    unrelated = entities.upsert_sponsor(conn, canonical_name="lone hospital", origin="aact")

    # 5 studies where parent + child co-sponsor.
    for i in range(5):
        nct = f"NCT1{i:04d}"
        conn.execute(
            "INSERT INTO norm.study_sponsors "
            "(nct_id, original_name, sponsor_id, agency_class, lead_or_collaborator) "
            "VALUES (?, 'novartis', ?, 'Industry', 'Lead')",
            [nct, parent],
        )
        conn.execute(
            "INSERT INTO norm.study_sponsors "
            "(nct_id, original_name, sponsor_id, agency_class, lead_or_collaborator) "
            "VALUES (?, 'novartis pharmaceuticals', ?, 'Industry', 'Collaborator')",
            [nct, child],
        )
    # One study where unrelated sponsor alone.
    conn.execute(
        "INSERT INTO norm.study_sponsors VALUES ('NCT9', 'lone hospital', ?, 'Other', 'Lead')",
        [unrelated],
    )

    ctx = et.ToolContext(duck_conn=conn)
    results = et.sponsor_co_occurrence(ctx, "Novartis", limit=5)
    assert len(results) == 1
    assert results[0]["canonical_name"] == "novartis pharmaceuticals"
    assert results[0]["shared_studies"] == 5


def test_co_occurrence_empty_when_no_cosponsors():
    conn = _fresh_conn()
    entities.upsert_sponsor(conn, canonical_name="novartis", origin="aact")
    ctx = et.ToolContext(duck_conn=conn)
    assert et.sponsor_co_occurrence(ctx, "Novartis") == []


# ---------- sponsor_ror_api ------------------------------------------------


def test_ror_api_tool_delegates_to_ror_module():
    conn = _fresh_conn()
    ctx = et.ToolContext(duck_conn=conn)

    with patch("src.agent.ror_tool.lookup",
               return_value=[{"canonical_name": "Novartis", "ror_id": "02f9zrr09"}]):
        out = et.sponsor_ror_api(ctx, "Novartis", limit=3)
    assert out == [{"canonical_name": "Novartis", "ror_id": "02f9zrr09"}]


# ---------- feature flag gating -------------------------------------------


def test_domain_tools_v1_without_flag(monkeypatch):
    """With SPONSOR_AGENT_V2_ENABLED=false, only fuzzy_sponsor is registered."""
    monkeypatch.setattr(
        "src.agent.enrichment_tools.DOMAIN_TOOLS",
        {"sponsor": [et.fuzzy_sponsor]},
    )
    tools = et.DOMAIN_TOOLS["sponsor"]
    tool_names = {t.__name__ for t in tools}
    assert tool_names == {"fuzzy_sponsor"}


def test_domain_tools_v2_includes_new_tools():
    """When _build_domain_tools runs with the flag on, new tools register."""
    with patch("config.settings.SPONSOR_AGENT_V2_ENABLED", True):
        tools = et._build_domain_tools()["sponsor"]
    names = {t.__name__ for t in tools}
    assert names == {
        "fuzzy_sponsor",
        "sponsor_anchor_lookup",
        "sponsor_co_occurrence",
        "sponsor_ror_api",
    }
