"""Tests for src/normalize_sponsors.py."""

import duckdb
import pytest

from src.transform.normalize_sponsors import (
    build_sponsor_dictionary,
    create_study_sponsors,
    generate_sponsor_fuzzy_candidates,
    normalize_sponsor_name,
)


@pytest.mark.parametrize("raw,expected", [
    ("Pfizer Inc.", "pfizer"),
    ("Pfizer, Inc", "pfizer"),
    ("PFIZER INC", "pfizer"),
    ("Hoffmann-La Roche", "hoffmann-la roche"),
    ("Bayer AG", "bayer"),
    ("Novartis Pharmaceuticals Corporation", "novartis pharmaceuticals"),
    ("The University of Melbourne", "university of melbourne"),
    ("Genentech, Inc.", "genentech"),
    ("AstraZeneca plc", "astrazeneca"),
    ("Merck Sharp & Dohme LLC", "merck sharp & dohme"),
    ("Sanofi S.A.", "sanofi"),
    ("Roche Co., Ltd.", "roche"),   # iterative stripping
    ("", ""),
    (None, ""),
    ("   ", ""),
])
def test_normalize_sponsor_name(raw, expected):
    assert normalize_sponsor_name(raw) == expected


def _setup_sponsor_db():
    from src import entities

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE SCHEMA ref")
    conn.execute("CREATE SCHEMA norm")
    entities.ensure_schema(conn)
    conn.execute("""
        CREATE TABLE raw.sponsors AS SELECT * FROM (VALUES
            (1, 'NCT001', 'Industry', 'lead',         'Pfizer Inc.'),
            (2, 'NCT002', 'Industry', 'lead',         'Pfizer, Inc'),
            (3, 'NCT003', 'Industry', 'lead',         'Pfizer Inc.'),
            (4, 'NCT004', 'Industry', 'collaborator', 'PFIZER INC'),
            (5, 'NCT005', 'Industry', 'lead',         'Novartis Pharmaceuticals Corporation'),
            (6, 'NCT006', 'NIH',      'lead',         'National Cancer Institute'),
            (7, 'NCT007', 'OTHER',    'lead',         'The University of Melbourne'),
            (8, 'NCT008', 'Industry', 'lead',         'Bayer AG')
        ) AS t(id, nct_id, agency_class, lead_or_collaborator, name)
    """)
    return conn


def test_build_dictionary_groups_variants():
    conn = _setup_sponsor_db()
    build_sponsor_dictionary(conn)
    rows = conn.execute("""
        SELECT d.source_name, e.canonical_name
        FROM ref.sponsor_dictionary d
        JOIN entities.sponsor e ON d.sponsor_id = e.sponsor_id
        WHERE d.source_name LIKE 'pfizer%'
        ORDER BY d.source_name
    """).fetchall()
    source_names = [r[0] for r in rows]
    canonicals = {r[1] for r in rows}
    assert set(source_names) == {"pfizer inc.", "pfizer, inc", "pfizer inc"}
    assert len(canonicals) == 1
    assert canonicals == {"Pfizer Inc."}
    conn.close()


def test_build_dictionary_preserves_manual():
    from src import entities

    conn = _setup_sponsor_db()
    build_sponsor_dictionary(conn)
    # Replace the auto-generated bayer ag row with a manual one pointing to a
    # 'Bayer' entity (short canonical).
    conn.execute("DELETE FROM ref.sponsor_dictionary WHERE source_name = 'bayer ag'")
    bayer_id = entities.upsert_sponsor(conn, canonical_name="Bayer", origin="manual")
    conn.execute(
        "INSERT INTO ref.sponsor_dictionary VALUES (?, ?, ?, ?)",
        ["bayer ag", bayer_id, "manual", "high"],
    )
    build_sponsor_dictionary(conn)
    row = conn.execute("""
        SELECT e.canonical_name, d.mapping_method
        FROM ref.sponsor_dictionary d
        JOIN entities.sponsor e ON d.sponsor_id = e.sponsor_id
        WHERE d.source_name = 'bayer ag'
    """).fetchone()
    assert row == ("Bayer", "manual")
    conn.close()


def test_build_dictionary_rebuild_idempotent():
    conn = _setup_sponsor_db()
    build_sponsor_dictionary(conn)
    count1 = conn.execute("SELECT COUNT(*) FROM ref.sponsor_dictionary").fetchone()[0]
    build_sponsor_dictionary(conn)
    count2 = conn.execute("SELECT COUNT(*) FROM ref.sponsor_dictionary").fetchone()[0]
    assert count1 == count2
    conn.close()


def test_create_study_sponsors():
    conn = _setup_sponsor_db()
    build_sponsor_dictionary(conn)
    n = create_study_sponsors(conn)
    assert n == 8
    # All Pfizer variants unified under one sponsor_id
    ids = conn.execute("""
        SELECT DISTINCT sponsor_id FROM norm.study_sponsors
        WHERE LOWER(original_name) LIKE 'pfizer%'
    """).fetchall()
    assert len(ids) == 1
    # Collaborator preserved; sponsor_id still resolvable
    row = conn.execute("""
        SELECT s.lead_or_collaborator, e.canonical_name
        FROM norm.study_sponsors s
        JOIN entities.sponsor e ON s.sponsor_id = e.sponsor_id
        WHERE s.nct_id = 'NCT004'
    """).fetchone()
    assert row[0] == "collaborator"
    conn.close()


def test_generate_sponsor_fuzzy_candidates_inserts_for_near_duplicates():
    from src import entities

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE SCHEMA ref")
    entities.ensure_schema(conn)
    # Two near-duplicate canonical names that survived deterministic grouping
    conn.execute("""
        CREATE TABLE raw.sponsors AS SELECT * FROM (VALUES
            (1, 'NCT001', 'Industry', 'lead', 'Acme Pharma'),
            (2, 'NCT002', 'Industry', 'lead', 'Acme Pharmaceuticals'),
            (3, 'NCT003', 'Industry', 'lead', 'Acme Pharma')
        ) AS t(id, nct_id, agency_class, lead_or_collaborator, name)
    """)
    build_sponsor_dictionary(conn)
    n = generate_sponsor_fuzzy_candidates(conn, score_cutoff=80)
    assert n >= 1
    rows = conn.execute("""
        SELECT source_value, canonical_term, status
        FROM ref.mapping_candidates WHERE domain = 'sponsor'
    """).fetchall()
    assert rows
    assert all(r[2] == "pending" for r in rows)
    conn.close()
