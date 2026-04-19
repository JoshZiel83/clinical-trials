"""Tests for Phase 7D sponsor merge plumbing — schema, merge_sponsor, and
the sponsor_resolved recursive view.

Covers:
  - new merged_into_id / merged_at / merge_rationale columns
  - entities.sponsor_resolved view identity behavior (no merges)
  - merge_sponsor happy path + idempotency + rejection cases
  - upsert_sponsor one-hop flatten after a merge
"""

import duckdb
import pytest

from src import entities


def _fresh_conn():
    conn = duckdb.connect(":memory:")
    entities.ensure_schema(conn)
    # ref.sponsor_dictionary is created by normalize_sponsors in production;
    # in these unit tests we set up a minimal version so merge_sponsor can
    # re-point rows when requested.
    conn.execute("CREATE SCHEMA IF NOT EXISTS ref")
    conn.execute("""
        CREATE TABLE ref.sponsor_dictionary (
            source_name     VARCHAR PRIMARY KEY,
            sponsor_id      BIGINT NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)
    return conn


def _insert_sponsor(conn, name, origin="aact"):
    return conn.execute(
        "INSERT INTO entities.sponsor (origin, canonical_name) VALUES (?, ?) "
        "RETURNING sponsor_id",
        [origin, name],
    ).fetchone()[0]


# ---------- Step 1: schema + sponsor_resolved view ------------------------


def test_ensure_schema_adds_merge_columns():
    conn = _fresh_conn()
    cols = {
        r[0]
        for r in conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_schema = 'entities' AND table_name = 'sponsor'"
        ).fetchall()
    }
    assert {"merged_into_id", "merged_at", "merge_rationale"}.issubset(cols)


def test_ensure_schema_is_idempotent_with_merge_columns():
    conn = _fresh_conn()
    # Seed a row with a NULL merged_into_id.
    _insert_sponsor(conn, "Novartis")
    # Re-running ensure_schema must not drop or alter existing rows.
    entities.ensure_schema(conn)
    rows = conn.execute(
        "SELECT canonical_name, merged_into_id FROM entities.sponsor"
    ).fetchall()
    assert rows == [("Novartis", None)]


def test_sponsor_resolved_identity_when_no_merges():
    conn = _fresh_conn()
    a = _insert_sponsor(conn, "Novartis")
    b = _insert_sponsor(conn, "Pfizer")
    rows = conn.execute(
        "SELECT sponsor_id, effective_sponsor_id, chain_depth "
        "FROM entities.sponsor_resolved ORDER BY sponsor_id"
    ).fetchall()
    assert rows == [(a, a, 0), (b, b, 0)]


def test_sponsor_resolved_follows_merge_chain():
    conn = _fresh_conn()
    parent = _insert_sponsor(conn, "Novartis")
    child = _insert_sponsor(conn, "Novartis Pharmaceuticals")
    entities.merge_sponsor(conn, child_id=child, parent_id=parent,
                           rationale="subsidiary")
    row = conn.execute(
        "SELECT effective_sponsor_id, chain_depth "
        "FROM entities.sponsor_resolved WHERE sponsor_id = ?",
        [child],
    ).fetchone()
    assert row[0] == parent
    assert row[1] == 1


# ---------- Step 2: merge_sponsor happy path + rejections -----------------


def test_merge_sponsor_sets_columns_and_repoints_dictionary():
    conn = _fresh_conn()
    parent = _insert_sponsor(conn, "Novartis")
    child = _insert_sponsor(conn, "Novartis Pharmaceuticals")
    conn.execute(
        "INSERT INTO ref.sponsor_dictionary "
        "(source_name, sponsor_id, mapping_method, confidence) "
        "VALUES ('novartis pharmaceuticals', ?, 'exact-after-normalize', 'high')",
        [child],
    )

    result = entities.merge_sponsor(
        conn, child_id=child, parent_id=parent,
        rationale={"source": "ROR", "parent_ror_id": "03x7k8a12"},
    )
    assert result == parent

    merged_row = conn.execute(
        "SELECT merged_into_id, merged_at, merge_rationale "
        "FROM entities.sponsor WHERE sponsor_id = ?",
        [child],
    ).fetchone()
    assert merged_row[0] == parent
    assert merged_row[1] is not None
    assert "ROR" in merged_row[2]

    dict_row = conn.execute(
        "SELECT sponsor_id FROM ref.sponsor_dictionary "
        "WHERE source_name = 'novartis pharmaceuticals'"
    ).fetchone()
    assert dict_row[0] == parent


def test_merge_sponsor_idempotent_on_same_pair():
    conn = _fresh_conn()
    parent = _insert_sponsor(conn, "Pfizer")
    child = _insert_sponsor(conn, "Pfizer Pharmaceuticals")
    entities.merge_sponsor(conn, child_id=child, parent_id=parent,
                           rationale="first call")
    first_merged_at = conn.execute(
        "SELECT merged_at FROM entities.sponsor WHERE sponsor_id = ?",
        [child],
    ).fetchone()[0]

    result = entities.merge_sponsor(conn, child_id=child, parent_id=parent,
                                    rationale="second call (should no-op)")
    assert result == parent

    # merged_at should NOT be bumped on the idempotent call.
    second_merged_at = conn.execute(
        "SELECT merged_at FROM entities.sponsor WHERE sponsor_id = ?",
        [child],
    ).fetchone()[0]
    assert first_merged_at == second_merged_at


def test_merge_sponsor_rejects_self_merge():
    conn = _fresh_conn()
    a = _insert_sponsor(conn, "Novartis")
    with pytest.raises(ValueError, match="itself"):
        entities.merge_sponsor(conn, child_id=a, parent_id=a)


def test_merge_sponsor_rejects_missing_ids():
    conn = _fresh_conn()
    a = _insert_sponsor(conn, "Novartis")
    with pytest.raises(ValueError, match="child sponsor_id 999999 not found"):
        entities.merge_sponsor(conn, child_id=999999, parent_id=a)
    with pytest.raises(ValueError, match="parent sponsor_id 999999 not found"):
        entities.merge_sponsor(conn, child_id=a, parent_id=999999)


def test_merge_sponsor_rejects_double_merge_to_different_parent():
    conn = _fresh_conn()
    p1 = _insert_sponsor(conn, "Novartis")
    p2 = _insert_sponsor(conn, "Pfizer")
    child = _insert_sponsor(conn, "Variant Co")
    entities.merge_sponsor(conn, child_id=child, parent_id=p1)
    with pytest.raises(ValueError, match="already merged"):
        entities.merge_sponsor(conn, child_id=child, parent_id=p2)


def test_merge_sponsor_rejects_chain_into_merged_parent():
    conn = _fresh_conn()
    grandparent = _insert_sponsor(conn, "Novartis")
    parent = _insert_sponsor(conn, "Novartis Pharmaceuticals")
    new_child = _insert_sponsor(conn, "Novartis Pharma AG")
    # Merge the would-be parent first.
    entities.merge_sponsor(conn, child_id=parent, parent_id=grandparent)
    # Now rejecting the two-hop chain.
    with pytest.raises(ValueError, match="is itself merged"):
        entities.merge_sponsor(conn, child_id=new_child, parent_id=parent)


def test_upsert_sponsor_follows_merge():
    """After merge, upsert_sponsor by child's canonical_name returns the parent id."""
    conn = _fresh_conn()
    parent = _insert_sponsor(conn, "Novartis")
    child = _insert_sponsor(conn, "Novartis Pharmaceuticals")
    entities.merge_sponsor(conn, child_id=child, parent_id=parent)

    # Upsert by child's canonical_name finds the (now-merged) row and
    # returns the parent's sponsor_id — the flatten guard.
    resolved = entities.upsert_sponsor(
        conn, canonical_name="Novartis Pharmaceuticals", origin="aact",
    )
    assert resolved == parent
