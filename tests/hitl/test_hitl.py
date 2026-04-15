"""Tests for src/hitl.py — generic HITL plumbing."""

import duckdb
import pandas as pd

from src import hitl


def _fresh_conn():
    from src import entities

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA ref")
    entities.ensure_schema(conn)
    # New (Phase 7B) dictionaries: source_name/condition_name → entity FK.
    conn.execute("""
        CREATE TABLE ref.condition_dictionary (
            condition_name  VARCHAR PRIMARY KEY,
            condition_id    BIGINT NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE ref.drug_dictionary (
            source_name     VARCHAR PRIMARY KEY,
            drug_id         BIGINT NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE ref.sponsor_dictionary (
            source_name     VARCHAR PRIMARY KEY,
            sponsor_id      BIGINT NOT NULL,
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
        SELECT e.canonical_term, d.mapping_method, d.confidence, e.origin
        FROM ref.condition_dictionary d
        JOIN entities.condition e ON d.condition_id = e.condition_id
        WHERE d.condition_name = 'breast cancer'
    """).fetchone()
    assert row == ("Breast Neoplasms", "manual", "high", "manual")

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
        SELECT e.canonical_name, e.chembl_id, d.mapping_method, e.origin
        FROM ref.drug_dictionary d
        JOIN entities.drug e ON d.drug_id = e.drug_id
        WHERE d.source_name = 'asa'
    """).fetchone()
    assert row == ("Aspirin", "CHEMBL25", "manual", "manual")


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
    from src import entities

    conn = _fresh_conn()
    dm_id = entities.upsert_condition(
        conn, canonical_term="Diabetes Mellitus", origin="mesh",
    )
    conn.execute(
        "INSERT INTO ref.condition_dictionary VALUES (?, ?, ?, ?)",
        ["diabetes", dm_id, "exact", "high"],
    )
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
    assert result == {"approved": 1, "rejected": 1, "hidden": 0, "promoted": 1}

    # X promoted to condition dictionary
    assert conn.execute("""
        SELECT COUNT(*) FROM ref.condition_dictionary
        WHERE condition_name = 'x'
    """).fetchone()[0] == 1

    # Idempotent second application
    result2 = hitl.import_decision_log(conn, str(path))
    assert result2["promoted"] == 0


def test_reject_throttle_blocks_after_n_distinct_rejects():
    """Phase 7A: after REJECT_THROTTLE distinct canonicals rejected for the
    same (source_value, source), further candidates for that source are skipped."""
    conn = _fresh_conn()
    # Seed two prior rejections against distinct canonicals for same source_value.
    hitl.insert_candidates(conn, "condition", _cand_df([
        {"source_value": "foo", "canonical_term": "Foo A", "score": 80.0, "study_count": 1},
        {"source_value": "foo", "canonical_term": "Foo B", "score": 81.0, "study_count": 1},
    ]), source="fuzzy")
    conn.execute("""
        UPDATE ref.mapping_candidates SET status = 'rejected'
        WHERE domain = 'condition' AND source_value = 'foo'
    """)

    # A third canonical for the same source should now be skipped.
    n = hitl.insert_candidates(conn, "condition", _cand_df([
        {"source_value": "foo", "canonical_term": "Foo C", "score": 82.0, "study_count": 1},
        {"source_value": "bar", "canonical_term": "Bar", "score": 83.0, "study_count": 1},
    ]), source="fuzzy")
    assert n == 1  # only bar inserted; foo exhausted

    rows = conn.execute("""
        SELECT source_value, status FROM ref.mapping_candidates
        WHERE domain = 'condition' AND status = 'pending'
    """).fetchall()
    assert rows == [("bar", "pending")]


def test_reject_throttle_allows_single_prior_rejection():
    """One rejection leaves the source_value open for alternate canonicals."""
    conn = _fresh_conn()
    hitl.insert_candidates(conn, "condition", _cand_df([
        {"source_value": "foo", "canonical_term": "Foo A", "score": 80.0, "study_count": 1},
    ]), source="fuzzy")
    conn.execute("""
        UPDATE ref.mapping_candidates SET status = 'rejected'
        WHERE domain = 'condition' AND source_value = 'foo'
    """)

    n = hitl.insert_candidates(conn, "condition", _cand_df([
        {"source_value": "foo", "canonical_term": "Foo B", "score": 81.0, "study_count": 1},
    ]), source="fuzzy")
    assert n == 1

    status = conn.execute("""
        SELECT status FROM ref.mapping_candidates
        WHERE source_value = 'foo' AND canonical_term = 'Foo B'
    """).fetchone()[0]
    assert status == "pending"


def test_hidden_source_blocks_future_candidates():
    """A `hidden` decision suppresses the source_value entirely."""
    conn = _fresh_conn()
    hitl.insert_candidates(conn, "sponsor", _cand_df([
        {"source_value": "acme inc", "canonical_term": "Acme",
         "canonical_id": None, "score": 90.0, "study_count": 1},
    ]), source="fuzzy")
    conn.execute("""
        UPDATE ref.mapping_candidates SET status = 'hidden'
        WHERE domain = 'sponsor' AND source_value = 'acme inc'
    """)

    n = hitl.insert_candidates(conn, "sponsor", _cand_df([
        {"source_value": "acme inc", "canonical_term": "Acme Corp",
         "canonical_id": None, "score": 85.0, "study_count": 1},
        {"source_value": "other co", "canonical_term": "Other",
         "canonical_id": None, "score": 88.0, "study_count": 1},
    ]), source="fuzzy")
    assert n == 1  # only other co inserted


def test_promote_creates_entity_row(tmp_path):
    """Promoting a drug candidate with a chembl_id should create an entities.drug row."""
    conn = _fresh_conn()
    hitl.insert_candidates(conn, "drug", _cand_df([
        {"source_value": "asa", "canonical_term": "Aspirin",
         "canonical_id": "CHEMBL25", "score": 95.0, "study_count": 3},
    ]), source="fuzzy")
    approved = _cand_df([
        {"source_value": "asa", "canonical_term": "Aspirin",
         "canonical_id": "CHEMBL25"},
    ])
    hitl.promote_candidates(conn, "drug", approved)

    row = conn.execute("""
        SELECT origin, canonical_name, chembl_id FROM entities.drug
        WHERE chembl_id = 'CHEMBL25'
    """).fetchone()
    assert row == ("manual", "Aspirin", "CHEMBL25")


def test_import_decision_log_hidden(tmp_path):
    """Decision log with decision='hidden' flips status to 'hidden'."""
    conn = _fresh_conn()
    hitl.insert_candidates(conn, "sponsor", _cand_df([
        {"source_value": "acme", "canonical_term": "Acme",
         "canonical_id": None, "score": 90.0, "study_count": 1},
    ]), source="fuzzy")

    decisions = pd.DataFrame([
        {"domain": "sponsor", "source_value": "acme", "canonical_term": "Acme",
         "source": "fuzzy", "decision": "hidden", "canonical_id": None},
    ])
    path = tmp_path / "decisions.parquet"
    decisions.to_parquet(path)

    result = hitl.import_decision_log(conn, str(path))
    assert result == {"approved": 0, "rejected": 0, "hidden": 1, "promoted": 0}

    status = conn.execute("""
        SELECT status FROM ref.mapping_candidates
        WHERE domain = 'sponsor' AND source_value = 'acme'
    """).fetchone()[0]
    assert status == "hidden"


def test_unknown_domain_raises():
    conn = _fresh_conn()
    try:
        hitl.insert_candidates(conn, "bogus", _cand_df([]), source="fuzzy")
    except ValueError as e:
        assert "bogus" in str(e)
    else:
        raise AssertionError("expected ValueError")
