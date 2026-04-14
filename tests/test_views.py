"""Tests for src/views.py — denormalized analytical views."""

import duckdb

from src.views import build_study_summary


def _setup_views_test_db():
    """In-memory DuckDB with minimal raw/class/norm mocks for 5 studies.

    Edge cases covered:
      NCT001: full-featured interventional — TAs, drugs, features, AI, Lead+Collab
      NCT002: observational, no TAs, no drugs, no features, Lead only
      NCT003: no class.study_design row (missing design)
      NCT004: only a Collaborator sponsor (no Lead)
      NCT005: multiple innovative features, multiple TAs (to test primary pick)
    """
    conn = duckdb.connect(":memory:")
    for schema in ("raw", "class", "norm", "views"):
        conn.execute(f"CREATE SCHEMA {schema}")

    conn.execute("""
        CREATE TABLE raw.studies AS SELECT * FROM (VALUES
            ('NCT001', 'RECRUITING', 'INTERVENTIONAL', 'PHASE3',
             'Trial 1', 'Full Trial 1', 100.0, DATE '2024-01-15',
             DATE '2026-01-15', 'Sponsor A'),
            ('NCT002', 'RECRUITING', 'OBSERVATIONAL', NULL,
             'Trial 2', 'Full Trial 2', 50.0, DATE '2023-06-01',
             DATE '2025-06-01', 'Sponsor B'),
            ('NCT003', 'ACTIVE_NOT_RECRUITING', 'INTERVENTIONAL', 'PHASE2',
             'Trial 3', 'Full Trial 3', NULL, DATE '2022-01-01',
             NULL, 'Sponsor C'),
            ('NCT004', 'RECRUITING', 'INTERVENTIONAL', 'PHASE1',
             'Trial 4', 'Full Trial 4', 20.0, DATE '2025-03-01',
             DATE '2027-03-01', 'Sponsor D'),
            ('NCT005', 'RECRUITING', 'INTERVENTIONAL', 'PHASE3',
             'Trial 5', 'Full Trial 5', 300.0, DATE '2024-09-01',
             DATE '2028-09-01', 'Sponsor E')
        ) AS t(nct_id, overall_status, study_type, phase, brief_title,
               official_title, enrollment, start_date, completion_date, source)
    """)

    conn.execute("""
        CREATE TABLE class.study_design AS SELECT * FROM (VALUES
            ('NCT001', 'INTERVENTIONAL', 'Parallel RCT', 'Double Blind', 'TREATMENT'),
            ('NCT002', 'OBSERVATIONAL', 'Cohort', NULL, NULL),
            ('NCT004', 'INTERVENTIONAL', 'Single-Arm', 'Open Label', 'TREATMENT'),
            ('NCT005', 'INTERVENTIONAL', 'Parallel RCT', 'Open Label', 'TREATMENT')
        ) AS t(nct_id, study_type, design_architecture, blinding_level, purpose)
    """)

    conn.execute("""
        CREATE TABLE class.innovative_features AS SELECT * FROM (VALUES
            ('NCT001', 'adaptive', 'brief_title', 'adaptive'),
            ('NCT001', 'adaptive', 'description', 'adaptive design'),
            ('NCT005', 'adaptive', 'brief_title', 'adaptive'),
            ('NCT005', 'basket', 'keyword', 'basket'),
            ('NCT005', 'platform', 'official_title', 'platform trial')
        ) AS t(nct_id, feature_type, source_field, matched_text)
    """)

    conn.execute("""
        CREATE TABLE class.ai_mentions AS SELECT * FROM (VALUES
            ('NCT001', 'machine learning', 'brief_title'),
            ('NCT001', 'artificial intelligence', 'keyword'),
            ('NCT005', 'deep learning', 'description')
        ) AS t(nct_id, ai_term, source_field)
    """)

    conn.execute("""
        CREATE TABLE norm.study_therapeutic_areas AS SELECT * FROM (VALUES
            ('NCT001', 'Neoplasms', 'Oncology', 'mesh-ancestor'),
            ('NCT001', 'Breast Neoplasms', 'Oncology', 'mesh-list'),
            ('NCT005', 'Neoplasms', 'Oncology', 'mesh-ancestor'),
            ('NCT005', 'Cardiovascular Diseases', 'Cardiovascular', 'mesh-ancestor'),
            ('NCT005', 'Heart Diseases', 'Cardiovascular', 'mesh-ancestor'),
            ('NCT004', 'Nervous System Diseases', 'Neurology', 'mesh-ancestor')
        ) AS t(nct_id, matched_ancestor, therapeutic_area, match_source)
    """)

    conn.execute("""
        CREATE TABLE norm.study_conditions AS SELECT * FROM (VALUES
            ('NCT001', 'breast cancer', 'Breast Neoplasms', 'exact', 'high'),
            ('NCT001', 'metastatic breast cancer', 'Breast Neoplasms', 'manual', 'high'),
            ('NCT002', 'diabetes', NULL, NULL, NULL),
            ('NCT005', 'lung cancer', 'Lung Neoplasms', 'exact', 'high')
        ) AS t(nct_id, condition_name, canonical_term, mapping_method, confidence)
    """)

    conn.execute("""
        CREATE TABLE norm.study_drugs AS SELECT * FROM (VALUES
            ('NCT001', 'DRUG', 'aspirin 100mg', 'Aspirin', 'CHEMBL25',
             'chembl-synonym', 'high'),
            ('NCT001', 'DRUG', 'placebo', 'placebo', NULL, 'control-map', 'high'),
            ('NCT005', 'BIOLOGICAL', 'mystery biologic', NULL, NULL, 'unmatched', NULL)
        ) AS t(nct_id, intervention_type, intervention_name, canonical_name,
               canonical_id, mapping_method, confidence)
    """)

    conn.execute("""
        CREATE TABLE raw.interventions AS SELECT * FROM (VALUES
            (1, 'NCT001', 'DRUG', 'aspirin 100mg'),
            (2, 'NCT001', 'DRUG', 'placebo'),
            (3, 'NCT001', 'BEHAVIORAL', 'diet counseling'),
            (4, 'NCT002', 'OBSERVATIONAL', 'blood draw'),
            (5, 'NCT004', 'DEVICE', 'sensor'),
            (6, 'NCT005', 'BIOLOGICAL', 'mystery biologic'),
            (7, 'NCT005', 'PROCEDURE', 'surgery')
        ) AS t(id, nct_id, intervention_type, name)
    """)

    conn.execute("""
        CREATE TABLE raw.countries AS SELECT * FROM (VALUES
            (1, 'NCT001', 'United States', FALSE),
            (2, 'NCT001', 'Canada', FALSE),
            (3, 'NCT002', 'Germany', FALSE),
            (4, 'NCT005', 'France', FALSE),
            (5, 'NCT005', 'Japan', TRUE)
        ) AS t(id, nct_id, name, removed)
    """)

    conn.execute("""
        CREATE TABLE raw.sponsors AS SELECT * FROM (VALUES
            (1, 'NCT001', 'Industry', 'Lead', 'Pharma Inc'),
            (2, 'NCT001', 'OTHER', 'Collaborator', 'University X'),
            (3, 'NCT002', 'NIH', 'Lead', 'NCI'),
            (4, 'NCT003', 'Industry', 'Lead', 'Biotech Co'),
            (5, 'NCT004', 'OTHER', 'Collaborator', 'University Y'),
            (6, 'NCT005', 'Industry', 'Lead', 'Big Pharma')
        ) AS t(id, nct_id, agency_class, lead_or_collaborator, name)
    """)

    # norm.study_sponsors now drives the view's sponsor columns (Phase 6B).
    # Use canonical_name = raw name for the test (normalization tested separately).
    conn.execute("""
        CREATE TABLE norm.study_sponsors AS
        SELECT nct_id, name AS original_name, name AS canonical_name,
               agency_class, lead_or_collaborator
        FROM raw.sponsors
    """)

    return conn


def test_row_count_matches_studies():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    study_count = conn.execute("SELECT COUNT(*) FROM raw.studies").fetchone()[0]
    view_count = conn.execute("SELECT COUNT(*) FROM views.study_summary").fetchone()[0]
    assert view_count == study_count == 5
    conn.close()


def test_core_study_fields():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT overall_status, study_type, phase, start_year
        FROM views.study_summary WHERE nct_id = 'NCT001'
    """).fetchone()
    assert row == ('RECRUITING', 'INTERVENTIONAL', 'PHASE3', 2024)
    conn.close()


def test_design_fields_null_when_missing():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT design_architecture, blinding_level, purpose
        FROM views.study_summary WHERE nct_id = 'NCT003'
    """).fetchone()
    assert row == (None, None, None)
    conn.close()


def test_innovative_feature_flags_and_list():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT has_innovative_feature, innovative_feature_count,
               innovative_feature_types, is_adaptive, is_basket, is_platform,
               is_bayesian
        FROM views.study_summary WHERE nct_id = 'NCT005'
    """).fetchone()
    assert row[0] is True
    assert row[1] == 3
    assert set(row[2]) == {'adaptive', 'basket', 'platform'}
    assert row[3] is True
    assert row[4] is True
    assert row[5] is True
    assert row[6] is False
    conn.close()


def test_no_feature_study_has_false_flags_empty_list():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT has_innovative_feature, innovative_feature_count,
               innovative_feature_types, is_adaptive
        FROM views.study_summary WHERE nct_id = 'NCT002'
    """).fetchone()
    assert row[0] is False
    assert row[1] == 0
    assert row[2] == []
    assert row[3] is False
    conn.close()


def test_ai_mention_aggregation():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT has_ai_mention, ai_mention_terms
        FROM views.study_summary WHERE nct_id = 'NCT001'
    """).fetchone()
    assert row[0] is True
    assert set(row[1]) == {'machine learning', 'artificial intelligence'}

    row2 = conn.execute("""
        SELECT has_ai_mention, ai_mention_terms
        FROM views.study_summary WHERE nct_id = 'NCT002'
    """).fetchone()
    assert row2[0] is False
    assert row2[1] == []
    conn.close()


def test_therapeutic_area_aggregation_and_primary():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    # NCT005: Cardiovascular has 2 ancestor hits, Oncology has 1 → Cardiovascular wins
    row = conn.execute("""
        SELECT therapeutic_areas, therapeutic_area_count, primary_therapeutic_area
        FROM views.study_summary WHERE nct_id = 'NCT005'
    """).fetchone()
    assert set(row[0]) == {'Oncology', 'Cardiovascular'}
    assert row[1] == 2
    assert row[2] == 'Cardiovascular'
    conn.close()


def test_no_ta_study():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT therapeutic_areas, therapeutic_area_count, primary_therapeutic_area
        FROM views.study_summary WHERE nct_id = 'NCT002'
    """).fetchone()
    assert row[0] == []
    assert row[1] == 0
    assert row[2] is None
    conn.close()


def test_conditions_aggregation_filters_nulls():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT canonical_conditions, mapped_condition_count, condition_count
        FROM views.study_summary WHERE nct_id = 'NCT002'
    """).fetchone()
    # NCT002's only condition was unmapped (NULL canonical) → empty list, zero mapped
    assert row[0] == []
    assert row[1] == 0
    assert row[2] == 1
    conn.close()


def test_drugs_aggregation():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT canonical_drugs, chembl_ids, drug_intervention_count, mapped_drug_count
        FROM views.study_summary WHERE nct_id = 'NCT001'
    """).fetchone()
    assert set(row[0]) == {'Aspirin', 'placebo'}
    assert row[1] == ['CHEMBL25']  # placebo has no ChEMBL ID
    assert row[2] == 2
    assert row[3] == 2
    conn.close()


def test_countries_excludes_removed():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT countries, country_count
        FROM views.study_summary WHERE nct_id = 'NCT005'
    """).fetchone()
    # Japan is removed=TRUE; only France should remain
    assert row[0] == ['France']
    assert row[1] == 1
    conn.close()


def test_lead_sponsor_and_collaborators():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT lead_sponsor_name, lead_sponsor_agency_class, collaborator_names
        FROM views.study_summary WHERE nct_id = 'NCT001'
    """).fetchone()
    assert row[0] == 'Pharma Inc'
    assert row[1] == 'Industry'
    assert row[2] == ['University X']
    conn.close()


def test_no_lead_sponsor_study():
    """NCT004 has only a Collaborator — lead fields should be NULL."""
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT lead_sponsor_name, lead_sponsor_agency_class, collaborator_names
        FROM views.study_summary WHERE nct_id = 'NCT004'
    """).fetchone()
    assert row[0] is None
    assert row[1] is None
    assert row[2] == ['University Y']
    conn.close()


def test_intervention_types_aggregation():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    row = conn.execute("""
        SELECT intervention_types, intervention_count
        FROM views.study_summary WHERE nct_id = 'NCT001'
    """).fetchone()
    assert set(row[0]) == {'DRUG', 'BEHAVIORAL'}
    assert row[1] == 3  # 2 drug rows + 1 behavioral

    # NCT003 has no interventions row → empty list
    row3 = conn.execute("""
        SELECT intervention_types, intervention_count
        FROM views.study_summary WHERE nct_id = 'NCT003'
    """).fetchone()
    assert row3[0] == []
    assert row3[1] == 0
    conn.close()


def test_idempotent_rebuild():
    conn = _setup_views_test_db()
    build_study_summary(conn)
    build_study_summary(conn)
    count = conn.execute("SELECT COUNT(*) FROM views.study_summary").fetchone()[0]
    assert count == 5
    conn.close()
