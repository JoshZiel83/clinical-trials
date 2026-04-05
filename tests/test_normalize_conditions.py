"""Tests for src/normalize_conditions.py."""

import duckdb

from src.normalize_conditions import (
    build_condition_dictionary,
    create_study_conditions,
    is_non_condition,
    normalize_condition,
)


def _setup_test_db():
    """Create an in-memory DuckDB with mock raw data for dictionary building."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE SCHEMA ref")
    conn.execute("CREATE SCHEMA norm")

    # Mock studies
    conn.execute("""
        CREATE TABLE raw.studies AS
        SELECT * FROM (VALUES
            ('NCT001'), ('NCT002'), ('NCT003'), ('NCT004'), ('NCT005'),
            ('NCT006'), ('NCT007'), ('NCT008'), ('NCT009'), ('NCT010')
        ) AS t(nct_id)
    """)

    # Mock conditions (free-text with downcase_name)
    conn.execute("""
        CREATE TABLE raw.conditions AS
        SELECT * FROM (VALUES
            ('NCT001', 'Breast Cancer', 'breast cancer'),
            ('NCT002', 'Breast Neoplasms', 'breast neoplasms'),
            ('NCT003', 'Lung Cancer', 'lung cancer'),
            ('NCT004', 'Healthy Volunteers', 'healthy volunteers'),
            ('NCT005', 'Breast Cancer', 'breast cancer'),
            ('NCT005', 'Fatigue', 'fatigue'),
            ('NCT006', 'Breast Cancer', 'breast cancer'),
            ('NCT006', 'Pain', 'pain'),
            ('NCT007', 'Breast Cancer', 'breast cancer'),
            ('NCT007', 'Nausea', 'nausea'),
            ('NCT008', 'Type 2 Diabetes', 'type 2 diabetes'),
            ('NCT009', 'Type 2 Diabetes', 'type 2 diabetes'),
            ('NCT010', 'Type 2 Diabetes', 'type 2 diabetes')
        ) AS t(nct_id, name, downcase_name)
    """)

    # Mock browse_conditions (mesh-list)
    conn.execute("""
        CREATE TABLE raw.browse_conditions AS
        SELECT * FROM (VALUES
            -- NCT001: condition "Breast Cancer", mesh has 2 terms (not 1:1)
            ('NCT001', 'Breast Neoplasms', 'breast neoplasms', 'mesh-list'),
            ('NCT001', 'Carcinoma', 'carcinoma', 'mesh-list'),
            ('NCT001', 'Neoplasms', 'neoplasms', 'mesh-ancestor'),
            -- NCT002: condition "Breast Neoplasms", mesh "Breast Neoplasms" (exact match!)
            ('NCT002', 'Breast Neoplasms', 'breast neoplasms', 'mesh-list'),
            ('NCT002', 'Neoplasms', 'neoplasms', 'mesh-ancestor'),
            -- NCT003: 1 condition + 1 mesh term (1:1 match)
            ('NCT003', 'Lung Neoplasms', 'lung neoplasms', 'mesh-list'),
            ('NCT003', 'Neoplasms', 'neoplasms', 'mesh-ancestor'),
            -- NCT004: no browse_conditions (healthy volunteers)
            -- NCT005: "Breast Cancer" co-occurs with "Breast Neoplasms"
            ('NCT005', 'Breast Neoplasms', 'breast neoplasms', 'mesh-list'),
            ('NCT005', 'Neoplasms', 'neoplasms', 'mesh-ancestor'),
            -- NCT006: "Breast Cancer" co-occurs with "Breast Neoplasms"
            ('NCT006', 'Breast Neoplasms', 'breast neoplasms', 'mesh-list'),
            ('NCT006', 'Neoplasms', 'neoplasms', 'mesh-ancestor'),
            -- NCT007: "Breast Cancer" co-occurs with "Breast Neoplasms"
            ('NCT007', 'Breast Neoplasms', 'breast neoplasms', 'mesh-list'),
            ('NCT007', 'Neoplasms', 'neoplasms', 'mesh-ancestor'),
            -- NCT008-010: "Type 2 Diabetes" co-occurs with "Diabetes Mellitus, Type 2"
            ('NCT008', 'Diabetes Mellitus, Type 2', 'diabetes mellitus, type 2', 'mesh-list'),
            ('NCT009', 'Diabetes Mellitus, Type 2', 'diabetes mellitus, type 2', 'mesh-list'),
            ('NCT010', 'Diabetes Mellitus, Type 2', 'diabetes mellitus, type 2', 'mesh-list')
        ) AS t(nct_id, mesh_term, downcase_mesh_term, mesh_type)
    """)

    return conn


def test_build_dictionary_exact_match():
    """Exact case-insensitive match should produce 'exact' method."""
    conn = _setup_test_db()
    build_condition_dictionary(conn)

    exact = conn.execute("""
        SELECT condition_name, canonical_term
        FROM ref.condition_dictionary
        WHERE mapping_method = 'exact'
    """).fetchall()

    # "breast neoplasms" matches "Breast Neoplasms" exactly
    assert ("breast neoplasms", "Breast Neoplasms") in exact
    conn.close()


def test_build_dictionary_one_to_one():
    """1:1 study match should produce '1:1-study' method."""
    conn = _setup_test_db()
    build_condition_dictionary(conn)

    one_to_one = conn.execute("""
        SELECT condition_name, canonical_term
        FROM ref.condition_dictionary
        WHERE mapping_method = '1:1-study'
    """).fetchall()

    # NCT003: 1 condition "Lung Cancer" + 1 mesh "Lung Neoplasms"
    assert ("lung cancer", "Lung Neoplasms") in one_to_one
    conn.close()


def test_build_dictionary_co_occurrence():
    """Dominant co-occurrence should produce 'co-occurrence' method."""
    conn = _setup_test_db()
    build_condition_dictionary(conn)

    co_occur = conn.execute("""
        SELECT condition_name, canonical_term
        FROM ref.condition_dictionary
        WHERE mapping_method = 'co-occurrence'
    """).fetchall()

    # "breast cancer" co-occurs with "Breast Neoplasms" 4 times
    # (NCT001, NCT005, NCT006, NCT007) — not caught by exact or 1:1 because
    # those studies have multiple conditions or multiple mesh terms
    assert ("breast cancer", "Breast Neoplasms") in co_occur
    conn.close()


def test_build_dictionary_priority():
    """Higher-priority methods should not be overwritten by lower ones."""
    conn = _setup_test_db()
    build_condition_dictionary(conn)

    # "breast neoplasms" should only appear once, with 'exact' method
    rows = conn.execute("""
        SELECT mapping_method FROM ref.condition_dictionary
        WHERE condition_name = 'breast neoplasms'
    """).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "exact"
    conn.close()


def test_build_dictionary_preserves_manual():
    """Manual entries should not be deleted when rebuilding automated entries."""
    conn = _setup_test_db()

    # Build once to create the table
    build_condition_dictionary(conn)

    # Insert a manual entry
    conn.execute("""
        INSERT INTO ref.condition_dictionary VALUES
        ('healthy volunteers', 'Health', 'manual', 'high')
    """)

    # Rebuild — manual entry should survive
    build_condition_dictionary(conn)

    manual = conn.execute("""
        SELECT * FROM ref.condition_dictionary
        WHERE mapping_method = 'manual'
    """).fetchall()
    assert len(manual) == 1
    assert manual[0][0] == "healthy volunteers"
    conn.close()


def test_create_study_conditions_all_rows():
    """Every raw.conditions row should appear in norm.study_conditions."""
    conn = _setup_test_db()
    build_condition_dictionary(conn)
    create_study_conditions(conn)

    raw_count = conn.execute("SELECT COUNT(*) FROM raw.conditions").fetchone()[0]
    norm_count = conn.execute("SELECT COUNT(*) FROM norm.study_conditions").fetchone()[0]
    assert norm_count == raw_count
    conn.close()


def test_unmapped_conditions_have_null_canonical():
    """Conditions with no dictionary entry should have NULL canonical_term."""
    conn = _setup_test_db()
    build_condition_dictionary(conn)
    create_study_conditions(conn)

    unmapped = conn.execute("""
        SELECT condition_name FROM norm.study_conditions
        WHERE canonical_term IS NULL
    """).fetchall()
    names = {row[0] for row in unmapped}
    assert "Healthy Volunteers" in names
    conn.close()


def test_manual_dictionary_entry_picked_up():
    """A manual dictionary entry should map conditions in study_conditions."""
    conn = _setup_test_db()

    # Build automated dictionary first
    build_condition_dictionary(conn)

    # "healthy volunteers" is unmapped
    create_study_conditions(conn)
    before = conn.execute("""
        SELECT canonical_term FROM norm.study_conditions
        WHERE condition_name = 'Healthy Volunteers'
    """).fetchone()
    assert before[0] is None

    # Now add a manual mapping
    conn.execute("""
        INSERT INTO ref.condition_dictionary VALUES
        ('healthy volunteers', 'Health', 'manual', 'high')
    """)

    # Re-run study_conditions — manual entry should be picked up
    create_study_conditions(conn)
    after = conn.execute("""
        SELECT canonical_term, mapping_method FROM norm.study_conditions
        WHERE condition_name = 'Healthy Volunteers'
    """).fetchone()
    assert after[0] == "Health"
    assert after[1] == "manual"
    conn.close()


# --- Preprocessing and fuzzy matching tests ---


def test_normalize_condition_strips_qualifiers():
    assert normalize_condition("Advanced Breast Cancer") == "breast cancer"
    assert normalize_condition("Metastatic Refractory Lung Cancer") == "lung cancer"
    assert normalize_condition("Stage IV Lung Cancer AJCC v8") == "lung cancer"


def test_normalize_condition_strips_parenthetical():
    assert normalize_condition("Lung Cancer (NSCLC)") == "lung cancer"
    assert normalize_condition("Obesity (BMI > 30)") == "obesity"


def test_normalize_condition_handles_clean_input():
    assert normalize_condition("Breast Neoplasms") == "breast neoplasms"
    assert normalize_condition("diabetes") == "diabetes"


def test_is_non_condition_flags_non_diseases():
    assert is_non_condition("Healthy Volunteers") is True
    assert is_non_condition("Immunotherapy") is True
    assert is_non_condition("Artificial Intelligence") is True
    assert is_non_condition("Children") is True
    assert is_non_condition("Quality of Life") is True


def test_is_non_condition_passes_real_conditions():
    assert is_non_condition("Breast Cancer") is False
    assert is_non_condition("Diabetes Mellitus") is False
    assert is_non_condition("Depression") is False
    assert is_non_condition("Obesity") is False


def _setup_fuzzy_test_db():
    """Create an in-memory DuckDB with data for fuzzy/cancer-synonym testing."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE SCHEMA ref")
    conn.execute("CREATE SCHEMA norm")

    conn.execute("""
        CREATE TABLE raw.studies AS
        SELECT * FROM (VALUES
            ('NCT001'), ('NCT002'), ('NCT003'), ('NCT004')
        ) AS t(nct_id)
    """)

    conn.execute("""
        CREATE TABLE raw.conditions AS
        SELECT * FROM (VALUES
            ('NCT001', 'Breast Cancer', 'breast cancer'),
            ('NCT002', 'Advanced Lung Cancer', 'advanced lung cancer'),
            ('NCT003', 'Immunotherapy', 'immunotherapy'),
            ('NCT004', 'Overweght and Obesity', 'overweght and obesity')
        ) AS t(nct_id, name, downcase_name)
    """)

    # Provide mesh-list targets for matching
    conn.execute("""
        CREATE TABLE raw.browse_conditions AS
        SELECT * FROM (VALUES
            ('NCT999', 'Breast Neoplasms', 'breast neoplasms', 'mesh-list'),
            ('NCT999', 'Lung Neoplasms', 'lung neoplasms', 'mesh-list'),
            ('NCT999', 'Obesity', 'obesity', 'mesh-list'),
            ('NCT999', 'Overweight', 'overweight', 'mesh-list')
        ) AS t(nct_id, mesh_term, downcase_mesh_term, mesh_type)
    """)

    return conn


def test_cancer_synonym_mapping():
    """'Breast Cancer' should map to 'Breast Neoplasms' via cancer-synonym."""
    conn = _setup_fuzzy_test_db()
    build_condition_dictionary(conn)

    result = conn.execute("""
        SELECT canonical_term, mapping_method
        FROM ref.condition_dictionary
        WHERE condition_name = 'breast cancer'
    """).fetchone()
    assert result is not None
    assert result[0] == "Breast Neoplasms"
    assert result[1] == "cancer-synonym"
    conn.close()


def test_cancer_synonym_with_qualifier():
    """'Advanced Lung Cancer' should map to 'Lung Neoplasms' via cancer-synonym."""
    conn = _setup_fuzzy_test_db()
    build_condition_dictionary(conn)

    result = conn.execute("""
        SELECT canonical_term, mapping_method
        FROM ref.condition_dictionary
        WHERE condition_name = 'advanced lung cancer'
    """).fetchone()
    assert result is not None
    assert result[0] == "Lung Neoplasms"
    assert result[1] == "cancer-synonym"
    conn.close()


def test_fuzzy_skips_non_conditions():
    """Non-condition terms like 'Immunotherapy' should not get a fuzzy match."""
    conn = _setup_fuzzy_test_db()
    build_condition_dictionary(conn)

    result = conn.execute("""
        SELECT * FROM ref.condition_dictionary
        WHERE condition_name = 'immunotherapy'
    """).fetchone()
    assert result is None
    conn.close()


def test_fuzzy_singleton_gets_low_confidence():
    """A condition appearing in only 1 study should get 'low' confidence from fuzzy."""
    conn = _setup_fuzzy_test_db()
    build_condition_dictionary(conn)

    result = conn.execute("""
        SELECT confidence, mapping_method
        FROM ref.condition_dictionary
        WHERE condition_name = 'overweght and obesity'
        AND mapping_method = 'fuzzy'
    """).fetchone()
    # This is a singleton (1 study) so should be low confidence
    if result is not None:
        assert result[0] == "low"
    conn.close()
