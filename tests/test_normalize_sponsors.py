"""Tests for src/normalize_sponsors.py."""

import duckdb
import pytest

from src.normalize_sponsors import (
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
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE SCHEMA ref")
    conn.execute("CREATE SCHEMA norm")
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
        SELECT source_name, canonical_name
        FROM ref.sponsor_dictionary
        WHERE source_name LIKE 'pfizer%'
        ORDER BY source_name
    """).fetchall()
    source_names = [r[0] for r in rows]
    canonicals = {r[1] for r in rows}
    # all three pfizer variants present, all pointing to the same canonical
    assert set(source_names) == {"pfizer inc.", "pfizer, inc", "pfizer inc"}
    assert len(canonicals) == 1
    # Canonical should be "Pfizer Inc." — the most frequent original (appears twice)
    assert canonicals == {"Pfizer Inc."}
    conn.close()


def test_build_dictionary_preserves_manual():
    conn = _setup_sponsor_db()
    build_sponsor_dictionary(conn)
    # Delete the auto-generated row for bayer ag, replace with a manual one
    conn.execute("DELETE FROM ref.sponsor_dictionary WHERE source_name = 'bayer ag'")
    conn.execute("""
        INSERT INTO ref.sponsor_dictionary
        VALUES ('bayer ag', 'Bayer', 'MANUAL-ID', 'manual', 'high')
    """)
    build_sponsor_dictionary(conn)
    row = conn.execute("""
        SELECT canonical_name, mapping_method FROM ref.sponsor_dictionary
        WHERE source_name = 'bayer ag'
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
    # All Pfizer variants unified under one canonical_name
    canonicals = conn.execute("""
        SELECT DISTINCT canonical_name FROM norm.study_sponsors
        WHERE LOWER(original_name) LIKE 'pfizer%'
    """).fetchall()
    assert len(canonicals) == 1
    # Collaborator preserved
    row = conn.execute("""
        SELECT lead_or_collaborator, canonical_name
        FROM norm.study_sponsors WHERE nct_id = 'NCT004'
    """).fetchone()
    assert row[0] == "collaborator"
    conn.close()


def test_generate_sponsor_fuzzy_candidates_inserts_for_near_duplicates():
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE SCHEMA ref")
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
