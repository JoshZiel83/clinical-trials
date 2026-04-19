"""Tests for Phase 7D ROR API tool (src/agent/ror_tool.py)."""

import json
from unittest.mock import MagicMock, patch

import duckdb
import pytest

from src.agent import ror_tool


def _fresh_conn():
    conn = duckdb.connect(":memory:")
    ror_tool.ensure_cache_table(conn)
    return conn


# ---------- canned response helpers ---------------------------------------


def _fake_response(status_code=200, payload=None, text=""):
    r = MagicMock()
    r.status_code = status_code
    r.json.return_value = payload if payload is not None else {"items": []}
    r.text = text
    if status_code >= 400 and status_code not in (429,) and not (500 <= status_code < 600):
        r.raise_for_status.side_effect = Exception(f"HTTP {status_code}")
    else:
        r.raise_for_status.return_value = None
    return r


NOVARTIS_ITEMS = {
    "items": [
        {
            "id": "https://ror.org/02f9zrr09",
            "name": "Novartis (Switzerland)",
            "country": {"country_name": "Switzerland"},
            "aliases": ["Novartis AG", "Novartis International"],
            "relationships": [],
        },
        {
            "id": "https://ror.org/04y0zvm94",
            "name": "Novartis Institutes for BioMedical Research",
            "country": {"country_name": "United States"},
            "aliases": ["NIBR"],
            "relationships": [
                {
                    "type": "Parent",
                    "label": "Novartis (Switzerland)",
                    "id": "https://ror.org/02f9zrr09",
                }
            ],
        },
    ]
}


# ---------- tests ----------------------------------------------------------


def test_parse_response_extracts_hierarchy():
    parsed = ror_tool._parse_ror_response(NOVARTIS_ITEMS, limit=5)
    assert len(parsed) == 2
    assert parsed[0]["canonical_name"] == "Novartis (Switzerland)"
    assert parsed[0]["ror_id"] == "02f9zrr09"
    assert parsed[0]["country"] == "Switzerland"
    assert parsed[0]["score"] == 1.0
    assert parsed[1]["parent"] == {
        "name": "Novartis (Switzerland)",
        "ror_id": "02f9zrr09",
    }


def test_lookup_empty_query_returns_empty():
    conn = _fresh_conn()
    assert ror_tool.lookup(conn, "") == []
    assert ror_tool.lookup(conn, "   ") == []


def test_lookup_success_writes_cache():
    conn = _fresh_conn()
    with patch("src.agent.ror_tool.requests.get",
               return_value=_fake_response(payload=NOVARTIS_ITEMS)) as mock_get:
        first = ror_tool.lookup(conn, "Novartis", limit=5)
    assert mock_get.call_count == 1
    assert first[0]["ror_id"] == "02f9zrr09"

    cache_row = conn.execute(
        "SELECT COUNT(*) FROM meta.ror_cache"
    ).fetchone()[0]
    assert cache_row == 1


def test_lookup_cache_hit_avoids_http_call():
    conn = _fresh_conn()
    with patch("src.agent.ror_tool.requests.get",
               return_value=_fake_response(payload=NOVARTIS_ITEMS)) as mock_get:
        ror_tool.lookup(conn, "Novartis")
        # Second call: cache should serve it, no new HTTP request.
        second = ror_tool.lookup(conn, "Novartis")
    assert mock_get.call_count == 1
    assert len(second) == 2


def test_lookup_cache_respects_ttl():
    """Expired cache rows should trigger a fresh HTTP fetch."""
    conn = _fresh_conn()
    with patch("src.agent.ror_tool.requests.get",
               return_value=_fake_response(payload=NOVARTIS_ITEMS)) as mock_get:
        ror_tool.lookup(conn, "Novartis")
        # Backdate the fetched_at so ttl_days=0 treats it as expired.
        conn.execute(
            "UPDATE meta.ror_cache SET fetched_at = "
            "CAST('2020-01-01 00:00:00' AS TIMESTAMP)"
        )
        ror_tool.lookup(conn, "Novartis", ttl_days=1)
    assert mock_get.call_count == 2


def test_lookup_backs_off_on_429_then_succeeds(monkeypatch):
    """429 → retry → eventual success; assert the success payload is returned."""
    conn = _fresh_conn()
    responses = iter([
        _fake_response(status_code=429, text="rate limited"),
        _fake_response(status_code=429, text="still limited"),
        _fake_response(status_code=200, payload=NOVARTIS_ITEMS),
    ])
    monkeypatch.setattr(ror_tool.time, "sleep", lambda s: None)  # no real waiting
    with patch("src.agent.ror_tool.requests.get",
               side_effect=lambda *a, **kw: next(responses)) as mock_get:
        out = ror_tool.lookup(conn, "Novartis")
    assert mock_get.call_count == 3
    assert out[0]["ror_id"] == "02f9zrr09"


def test_lookup_returns_error_sentinel_on_total_failure(monkeypatch):
    """After exhausting retries, return error sentinel (not raise)."""
    conn = _fresh_conn()
    monkeypatch.setattr(ror_tool.time, "sleep", lambda s: None)
    with patch("src.agent.ror_tool.requests.get",
               return_value=_fake_response(status_code=500, text="boom")):
        out = ror_tool.lookup(conn, "Some Org")
    assert len(out) == 1
    assert "error" in out[0]
    assert out[0]["results"] == []

    # Failures must NOT be cached.
    cache_count = conn.execute(
        "SELECT COUNT(*) FROM meta.ror_cache"
    ).fetchone()[0]
    assert cache_count == 0
