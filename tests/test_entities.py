"""Tests for src/entities.py — canonical entity schema (Phase 7B)."""

import duckdb

from src import entities


def _fresh_conn():
    return duckdb.connect(":memory:")


def test_ensure_schema_idempotent():
    conn = _fresh_conn()
    entities.ensure_schema(conn)
    entities.ensure_schema(conn)
    for t in ("condition", "drug", "sponsor"):
        n = conn.execute(f"SELECT COUNT(*) FROM entities.{t}").fetchone()[0]
        assert n == 0


def test_condition_auto_id():
    conn = _fresh_conn()
    entities.ensure_schema(conn)
    conn.execute("""
        INSERT INTO entities.condition (origin, mesh_descriptor_id, canonical_term)
        VALUES ('mesh', 'D000001', 'Calcimycin'), ('mesh', 'D000002', 'Aspirin')
    """)
    ids = conn.execute(
        "SELECT condition_id FROM entities.condition ORDER BY mesh_descriptor_id"
    ).fetchall()
    assert len(ids) == 2
    assert ids[0][0] != ids[1][0]
    assert all(i[0] is not None for i in ids)


def test_condition_descriptor_id_unique():
    conn = _fresh_conn()
    entities.ensure_schema(conn)
    conn.execute("""
        INSERT INTO entities.condition (origin, mesh_descriptor_id, canonical_term)
        VALUES ('mesh', 'D001', 'Foo')
    """)
    try:
        conn.execute("""
            INSERT INTO entities.condition (origin, mesh_descriptor_id, canonical_term)
            VALUES ('mesh', 'D001', 'Bar')
        """)
    except duckdb.ConstraintException:
        pass
    else:
        raise AssertionError("expected UNIQUE constraint violation on mesh_descriptor_id")


def test_condition_allows_non_mesh_origin():
    """Phase 7B: entity identity is decoupled from MeSH — UMLS or manual
    canonicals can exist without a MeSH descriptor."""
    conn = _fresh_conn()
    entities.ensure_schema(conn)
    conn.execute("""
        INSERT INTO entities.condition (origin, umls_cui, canonical_term)
        VALUES ('umls', 'C0006142', 'Breast Neoplasms UMLS-only')
    """)
    conn.execute("""
        INSERT INTO entities.condition (origin, canonical_term)
        VALUES ('manual', 'Long COVID cognitive sequelae')
    """)
    rows = conn.execute("""
        SELECT origin, mesh_descriptor_id, umls_cui FROM entities.condition
        ORDER BY canonical_term
    """).fetchall()
    assert rows == [
        ('umls', None, 'C0006142'),
        ('manual', None, None),
    ]


def test_drug_chembl_id_unique_nullable():
    conn = _fresh_conn()
    entities.ensure_schema(conn)
    # Two rows with NULL chembl_id should be allowed
    conn.execute("""
        INSERT INTO entities.drug (origin, canonical_name, chembl_id)
        VALUES ('manual', 'Saline', NULL),
               ('manual', 'Vehicle', NULL),
               ('chembl', 'Aspirin', 'CHEMBL25')
    """)
    n = conn.execute("SELECT COUNT(*) FROM entities.drug").fetchone()[0]
    assert n == 3


def test_sponsor_canonical_name_unique():
    conn = _fresh_conn()
    entities.ensure_schema(conn)
    conn.execute(
        "INSERT INTO entities.sponsor (origin, canonical_name) VALUES ('aact', 'Pfizer')"
    )
    try:
        conn.execute(
            "INSERT INTO entities.sponsor (origin, canonical_name) VALUES ('aact', 'Pfizer')"
        )
    except duckdb.ConstraintException:
        pass
    else:
        raise AssertionError("expected UNIQUE violation on sponsor canonical_name")


def test_origin_is_required():
    conn = _fresh_conn()
    entities.ensure_schema(conn)
    try:
        conn.execute("""
            INSERT INTO entities.condition (mesh_descriptor_id, canonical_term)
            VALUES ('D001', 'Foo')
        """)
    except duckdb.ConstraintException:
        pass
    else:
        raise AssertionError("expected NOT NULL violation on origin")
