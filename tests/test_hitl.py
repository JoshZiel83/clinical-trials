"""Tests for src/hitl.py — generic HITL plumbing."""

import duckdb
import pandas as pd

from src import hitl


def _fresh_conn():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA ref")
    # Minimal dictionary tables for each domain
    conn.execute("""
        CREATE TABLE ref.condition_dictionary (
            condition_name  VARCHAR NOT NULL,
            canonical_term  VARCHAR NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE ref.drug_dictionary (
            source_name     VARCHAR NOT NULL,
            canonical_name  VARCHAR NOT NULL,
            canonical_id    VARCHAR,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE ref.sponsor_dictionary (
            source_name     VARCHAR NOT NULL,
            canonical_name  VARCHAR NOT NULL,
            canonical_id    VARCHAR,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)
    return conn


def _cand_df(rows):
    return pd.DataFrame(rows)


def test_ensure_candidates_table_idempotent():
    conn = _fresh_conn()
    hitl.ensure_candidates_table(conn)
    hitl.ensure_candidates_table(conn)
    count = conn.execute("SELECT COUNT(*) FROM ref.mapping_candidates").fetchone()[0]
    assert count == 0


def test_insert_and_promote_condition():
    conn = _fresh_conn()
    df = _cand_df([
        {"source_value": "breast cancer", "canonical_term": "Breast Neoplasms",
         "score": 92.0, "study_count": 10},
        {"source_value": "lung ca", "canonical_term": "Lung Neoplasms",
         "score": 88.0, "study_count": 5},
    ])
    n = hitl.insert_candidates(conn, "condition", df, source="fuzzy")
    assert n == 2

    approved = _cand_df([
        {"source_value": "breast cancer", "canonical_term": "Breast Neoplasms"},
    ])
    promoted = hitl.promote_candidates(conn, "condition", approved)
    assert promoted == 1

    row = conn.execute("""
        SELECT canonical_term, mapping_method, confidence
        FROM ref.condition_dictionary
        WHERE condition_name = 'breast cancer'
    """).fetchone()
    assert row == ("Breast Neoplasms", "manual", "high")

    status = conn.execute("""
        SELECT status FROM ref.mapping_candidates
        WHERE domain = 'condition' AND source_value = 'breast cancer'
    """).fetchone()[0]
    assert status == "approved"


def test_insert_and_promote_drug_with_canonical_id():
    conn = _fresh_conn()
    df = _cand_df([
        {"source_value": "asa", "canonical_term": "Aspirin",
         "canonical_id": "CHEMBL25", "score": 95.0, "study_count": 3},
    ])
    hitl.insert_candidates(conn, "drug", df, source="fuzzy")

    approved = _cand_df([
        {"source_value": "asa", "canonical_term": "Aspirin",
         "canonical_id": "CHEMBL25"},
    ])
    promoted = hitl.promote_candidates(conn, "drug", approved)
    assert promoted == 1

    row = conn.execute("""
        SELECT canonical_name, canonical_id, mapping_method
        FROM ref.drug_dictionary
        WHERE source_name = 'asa'
    """).fetchone()
    assert row == ("Aspirin", "CHEMBL25", "manual")


def test_insert_preserves_non_pending_decisions():
    conn = _fresh_conn()
    hitl.insert_candidates(conn, "condition", _cand_df([
        {"source_value": "foo", "canonical_term": "Foo", "score": 80.0, "study_count": 1},
    ]), source="fuzzy")

    conn.execute("""
        UPDATE ref.mapping_candidates SET status = 'rejected'
        WHERE domain = 'condition' AND source_value = 'foo'
    """)

    # Re-insert the same candidate — the rejected row should be preserved
    # and no new pending row should appear for it.
    hitl.insert_candidates(conn, "condition", _cand_df([
        {"source_value": "foo", "canonical_term": "Foo", "score": 80.0, "study_count": 1},
        {"source_value": "bar", "canonical_term": "Bar", "score": 81.0, "study_count": 2},
    ]), source="fuzzy")

    rows = conn.execute("""
        SELECT source_value, status FROM ref.mapping_candidates
        WHERE domain = 'condition' ORDER BY source_value
    """).fetchall()
    assert rows == [("bar", "pending"), ("foo", "rejected")]


def test_promote_skips_existing_dictionary_entries():
    conn = _fresh_conn()
    conn.execute("""
        INSERT INTO ref.condition_dictionary VALUES
        ('diabetes', 'Diabetes Mellitus', 'exact', 'high')
    """)
    hitl.insert_candidates(conn, "condition", _cand_df([
        {"source_value": "diabetes", "canonical_term": "Diabetes Mellitus",
         "score": 100.0, "study_count": 1},
    ]), source="fuzzy")

    promoted = hitl.promote_candidates(conn, "condition", _cand_df([
        {"source_value": "diabetes", "canonical_term": "Diabetes Mellitus"},
    ]))
    assert promoted == 0


def test_export_and_import_reviewed_csv(tmp_path):
    conn = _fresh_conn()
    hitl.insert_candidates(conn, "condition", _cand_df([
        {"source_value": "a", "canonical_term": "A", "score": 90.0, "study_count": 1},
        {"source_value": "b", "canonical_term": "B", "score": 91.0, "study_count": 2},
    ]), source="fuzzy")

    path = tmp_path / "cands.csv"
    hitl.export_candidates_csv(conn, "condition", str(path))
    csv = pd.read_csv(path)
    assert set(csv["source_value"]) == {"a", "b"}

    # Mark decisions in-place and import
    csv.loc[csv["source_value"] == "a", "status"] = "approved"
    csv.loc[csv["source_value"] == "b", "status"] = "rejected"
    csv.to_csv(path, index=False)

    promoted = hitl.import_reviewed_csv(conn, "condition", str(path))
    assert promoted == 1

    rows = conn.execute("""
        SELECT source_value, status FROM ref.mapping_candidates
        WHERE domain = 'condition' ORDER BY source_value
    """).fetchall()
    assert rows == [("a", "approved"), ("b", "rejected")]


def test_import_decision_log(tmp_path):
    conn = _fresh_conn()
    hitl.insert_candidates(conn, "condition", _cand_df([
        {"source_value": "x", "canonical_term": "X", "score": 90.0, "study_count": 1},
    ]), source="fuzzy")
    hitl.insert_candidates(conn, "drug", _cand_df([
        {"source_value": "y", "canonical_term": "Y",
         "canonical_id": "CHEMBL1", "score": 92.0, "study_count": 1},
    ]), source="fuzzy")

    decisions = pd.DataFrame([
        {"domain": "condition", "source_value": "x", "canonical_term": "X",
         "source": "fuzzy", "decision": "approved", "canonical_id": None},
        {"domain": "drug", "source_value": "y", "canonical_term": "Y",
         "source": "fuzzy", "decision": "rejected", "canonical_id": "CHEMBL1"},
    ])
    path = tmp_path / "decisions.parquet"
    decisions.to_parquet(path)

    result = hitl.import_decision_log(conn, str(path))
    assert result == {"approved": 1, "rejected": 1, "promoted": 1}

    # X promoted to condition dictionary
    assert conn.execute("""
        SELECT COUNT(*) FROM ref.condition_dictionary
        WHERE condition_name = 'x'
    """).fetchone()[0] == 1

    # Idempotent second application
    result2 = hitl.import_decision_log(conn, str(path))
    assert result2["promoted"] == 0


def test_unknown_domain_raises():
    conn = _fresh_conn()
    try:
        hitl.insert_candidates(conn, "bogus", _cand_df([]), source="fuzzy")
    except ValueError as e:
        assert "bogus" in str(e)
    else:
        raise AssertionError("expected ValueError")
