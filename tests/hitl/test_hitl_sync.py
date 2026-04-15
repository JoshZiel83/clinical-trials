"""Tests for run_hitl_sync.py — decision-log application & idempotency."""

from pathlib import Path

import duckdb
import pandas as pd

import run_hitl_sync as rhs
from src import hitl


def _fresh_conn():
    from src import entities

    conn = duckdb.connect(":memory:")
    for s in ("ref", "meta"):
        conn.execute(f"CREATE SCHEMA {s}")
    entities.ensure_schema(conn)
    conn.execute("""
        CREATE TABLE ref.condition_dictionary (
            condition_name VARCHAR PRIMARY KEY, condition_id BIGINT NOT NULL,
            mapping_method VARCHAR, confidence VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE ref.drug_dictionary (
            source_name VARCHAR PRIMARY KEY, drug_id BIGINT NOT NULL,
            mapping_method VARCHAR, confidence VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE ref.sponsor_dictionary (
            source_name VARCHAR PRIMARY KEY, sponsor_id BIGINT NOT NULL,
            mapping_method VARCHAR, confidence VARCHAR
        )
    """)
    hitl.ensure_candidates_table(conn)
    return conn


def _seed_candidates(conn):
    hitl.insert_candidates(conn, "condition", pd.DataFrame([
        {"source_value": "bc", "canonical_term": "Breast Neoplasms",
         "score": 0.9, "study_count": 10},
        {"source_value": "nope", "canonical_term": "Wrong",
         "score": 0.5, "study_count": 2},
    ]), source="fuzzy")


def _make_decision_log(tmp_path: Path, rows: list[dict]) -> Path:
    df = pd.DataFrame(rows)
    path = tmp_path / f"decisions_2026-04-14_120000_condition.parquet"
    df.to_parquet(path)
    return path


def test_apply_promotes_and_marks(tmp_path, monkeypatch):
    conn = _fresh_conn()
    _seed_candidates(conn)

    # Patch the rebuild call — we don't have raw.* tables in this test DB
    monkeypatch.setattr(rhs, "_rebuild_downstream", lambda conn, dom: None)

    log = _make_decision_log(tmp_path, [
        {"domain": "condition", "source_value": "bc",
         "canonical_term": "Breast Neoplasms", "source": "fuzzy",
         "decision": "approved", "reviewer": "u", "decided_at": "2026-04-14T12:00:00Z"},
        {"domain": "condition", "source_value": "nope",
         "canonical_term": "Wrong", "source": "fuzzy",
         "decision": "rejected", "reviewer": "u", "decided_at": "2026-04-14T12:00:00Z"},
    ])
    total, domains = rhs.apply_logs(conn, [log])
    assert total["approved"] == 1 and total["rejected"] == 1 and total["promoted"] == 1
    assert domains == {"condition"}

    # Dictionary entry appeared, FK resolves to the canonical
    row = conn.execute(
        "SELECT e.canonical_term, d.mapping_method "
        "FROM ref.condition_dictionary d "
        "JOIN entities.condition e ON d.condition_id = e.condition_id "
        "WHERE d.condition_name = 'bc'"
    ).fetchone()
    assert row == ("Breast Neoplasms", "manual")

    # Candidates flipped
    statuses = dict(conn.execute(
        "SELECT source_value, status FROM ref.mapping_candidates "
        "WHERE domain = 'condition' ORDER BY source_value"
    ).fetchall())
    assert statuses == {"bc": "approved", "nope": "rejected"}

    # Applied log registered
    n = conn.execute(
        "SELECT COUNT(*) FROM meta.decision_log_applied"
    ).fetchone()[0]
    assert n == 1
    conn.close()


def test_apply_is_idempotent(tmp_path, monkeypatch):
    conn = _fresh_conn()
    _seed_candidates(conn)
    monkeypatch.setattr(rhs, "_rebuild_downstream", lambda conn, dom: None)

    log = _make_decision_log(tmp_path, [
        {"domain": "condition", "source_value": "bc",
         "canonical_term": "Breast Neoplasms", "source": "fuzzy",
         "decision": "approved", "reviewer": "u", "decided_at": "2026-04-14T12:00:00Z"},
    ])

    rhs.apply_logs(conn, [log])
    # Re-apply the same log — should be a no-op for dict size
    rhs.apply_logs(conn, [log])

    n_dict = conn.execute(
        "SELECT COUNT(*) FROM ref.condition_dictionary"
    ).fetchone()[0]
    assert n_dict == 1  # not 2
    conn.close()


def test_unapplied_logs_filters_already_applied(tmp_path, monkeypatch):
    conn = _fresh_conn()
    _seed_candidates(conn)
    monkeypatch.setattr(rhs, "_rebuild_downstream", lambda conn, dom: None)
    monkeypatch.setattr(rhs, "REVIEWS_DIR", tmp_path)

    log_a = _make_decision_log(tmp_path, [
        {"domain": "condition", "source_value": "bc",
         "canonical_term": "Breast Neoplasms", "source": "fuzzy",
         "decision": "approved", "reviewer": "u", "decided_at": "x"},
    ])
    # Apply log_a
    rhs.apply_logs(conn, [log_a])

    # A second log is added later
    log_b = tmp_path / "decisions_2026-04-15_090000_condition.parquet"
    pd.DataFrame([
        {"domain": "condition", "source_value": "nope",
         "canonical_term": "Wrong", "source": "fuzzy",
         "decision": "rejected", "reviewer": "u", "decided_at": "y"},
    ]).to_parquet(log_b)

    unapplied = rhs._unapplied_logs(conn)
    assert len(unapplied) == 1
    assert unapplied[0].name == log_b.name
    conn.close()
