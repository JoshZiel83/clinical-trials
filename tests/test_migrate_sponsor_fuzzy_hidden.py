"""Tests for scripts/migrate_sponsor_fuzzy_hidden.py (Phase 7D)."""

import duckdb
import pandas as pd

from src import hitl
from scripts import migrate_sponsor_fuzzy_hidden as migration


def _fresh_conn_with_candidates():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA ref")
    hitl.ensure_candidates_table(conn)
    return conn


def _seed_rows(conn, rows):
    df = pd.DataFrame(rows)
    conn.register("_seed", df)
    try:
        conn.execute("""
            INSERT INTO ref.mapping_candidates
                (domain, source_value, canonical_term, canonical_id,
                 score, study_count, source, rationale, tool_trace,
                 anchor_sponsor_id, status)
            SELECT domain, source_value, canonical_term, canonical_id,
                   score, study_count, source, rationale, tool_trace,
                   anchor_sponsor_id, status
            FROM _seed
        """)
    finally:
        conn.unregister("_seed")


def test_migrate_hides_pending_sponsor_fuzzy_rows():
    conn = _fresh_conn_with_candidates()
    _seed_rows(conn, [
        {"domain": "sponsor", "source_value": "novartis pharmaceuticals",
         "canonical_term": "Novartis", "canonical_id": None,
         "score": 92.0, "study_count": 10, "source": "fuzzy",
         "rationale": None, "tool_trace": None, "anchor_sponsor_id": None,
         "status": "pending"},
        {"domain": "sponsor", "source_value": "pfizer pharma",
         "canonical_term": "Pfizer", "canonical_id": None,
         "score": 90.0, "study_count": 7, "source": "fuzzy",
         "rationale": None, "tool_trace": None, "anchor_sponsor_id": None,
         "status": "pending"},
    ])
    n = migration.migrate(conn)
    assert n == 2
    statuses = [r[0] for r in conn.execute(
        "SELECT status FROM ref.mapping_candidates "
        "WHERE domain = 'sponsor' AND source = 'fuzzy'"
    ).fetchall()]
    assert statuses == ["hidden", "hidden"]


def test_migrate_preserves_non_pending_fuzzy_rows():
    """Already approved/rejected fuzzy rows stay untouched — that's history."""
    conn = _fresh_conn_with_candidates()
    _seed_rows(conn, [
        {"domain": "sponsor", "source_value": "approved variant",
         "canonical_term": "Canonical", "canonical_id": None,
         "score": 95.0, "study_count": 5, "source": "fuzzy",
         "rationale": None, "tool_trace": None, "anchor_sponsor_id": None,
         "status": "approved"},
        {"domain": "sponsor", "source_value": "pending variant",
         "canonical_term": "Other", "canonical_id": None,
         "score": 89.0, "study_count": 3, "source": "fuzzy",
         "rationale": None, "tool_trace": None, "anchor_sponsor_id": None,
         "status": "pending"},
    ])
    migration.migrate(conn)

    # Only 1 row changed — the pending one.
    rows = conn.execute(
        "SELECT source_value, status FROM ref.mapping_candidates "
        "WHERE domain = 'sponsor' ORDER BY source_value"
    ).fetchall()
    assert rows == [
        ("approved variant", "approved"),
        ("pending variant", "hidden"),
    ]


def test_migrate_leaves_other_domains_alone():
    """Only sponsor/fuzzy pending rows are affected."""
    conn = _fresh_conn_with_candidates()
    _seed_rows(conn, [
        {"domain": "condition", "source_value": "breast ca",
         "canonical_term": "Breast Neoplasms", "canonical_id": None,
         "score": 95.0, "study_count": 10, "source": "fuzzy",
         "rationale": None, "tool_trace": None, "anchor_sponsor_id": None,
         "status": "pending"},
        {"domain": "sponsor", "source_value": "variant co",
         "canonical_term": "Canonical", "canonical_id": None,
         "score": 89.0, "study_count": 3, "source": "agent",
         "rationale": None, "tool_trace": None, "anchor_sponsor_id": None,
         "status": "pending"},
    ])
    migration.migrate(conn)
    rows = conn.execute(
        "SELECT domain, source, status FROM ref.mapping_candidates "
        "ORDER BY domain, source_value"
    ).fetchall()
    # condition/fuzzy and sponsor/agent both stay 'pending'.
    assert ("condition", "fuzzy", "pending") in rows
    assert ("sponsor", "agent", "pending") in rows


def test_migrate_is_idempotent():
    """Second run should return 0 and not re-hide anything."""
    conn = _fresh_conn_with_candidates()
    _seed_rows(conn, [
        {"domain": "sponsor", "source_value": "x",
         "canonical_term": "X", "canonical_id": None,
         "score": 85.0, "study_count": 2, "source": "fuzzy",
         "rationale": None, "tool_trace": None, "anchor_sponsor_id": None,
         "status": "pending"},
    ])
    first = migration.migrate(conn)
    second = migration.migrate(conn)
    assert first == 1
    assert second == 0

    # Seed a fresh pending row after migration; it should NOT be re-hidden
    # (the migration marker prevents re-running).
    _seed_rows(conn, [
        {"domain": "sponsor", "source_value": "y",
         "canonical_term": "Y", "canonical_id": None,
         "score": 85.0, "study_count": 2, "source": "fuzzy",
         "rationale": None, "tool_trace": None, "anchor_sponsor_id": None,
         "status": "pending"},
    ])
    third = migration.migrate(conn)
    assert third == 0
    status = conn.execute(
        "SELECT status FROM ref.mapping_candidates WHERE source_value = 'y'"
    ).fetchone()[0]
    assert status == "pending"


def test_migrate_handles_missing_table():
    """Running against a DB without ref.mapping_candidates is safe."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA ref")  # schema but no table
    n = migration.migrate(conn)
    assert n == 0
    # Marker written so it won't try again.
    marked = conn.execute(
        "SELECT COUNT(*) FROM meta.migration_log WHERE name = ?",
        [migration.MIGRATION_NAME],
    ).fetchone()[0]
    assert marked == 1
