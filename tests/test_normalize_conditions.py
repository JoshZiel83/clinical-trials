"""Tests for src/normalize_conditions.py."""

import duckdb

from src.normalize_conditions import (
    build_condition_dictionary,
    create_study_conditions,
    generate_fuzzy_candidates,
    is_non_condition,
    normalize_condition,
    promote_candidates,
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
            ('NCT001'), ('NCT002'), ('NCT003'), ('NCT004'), ('NCT005')
        ) AS t(nct_id)
    """)

    conn.execute("""
        CREATE TABLE raw.conditions AS
        SELECT * FROM (VALUES
            ('NCT001', 'Breast Cancer', 'breast cancer'),
            ('NCT002', 'Advanced Lung Cancer', 'advanced lung cancer'),
            ('NCT003', 'Immunotherapy', 'immunotherapy'),
            ('NCT004', 'Diabetic Nephropathy', 'diabetic nephropathy'),
            ('NCT005', 'Diabetic Nephropathy', 'diabetic nephropathy')
        ) AS t(nct_id, name, downcase_name)
    """)

    # Provide mesh-list targets for matching
    conn.execute("""
        CREATE TABLE raw.browse_conditions AS
        SELECT * FROM (VALUES
            ('NCT999', 'Breast Neoplasms', 'breast neoplasms', 'mesh-list'),
            ('NCT999', 'Lung Neoplasms', 'lung neoplasms', 'mesh-list'),
            ('NCT999', 'Obesity', 'obesity', 'mesh-list'),
            ('NCT999', 'Overweight', 'overweight', 'mesh-list'),
            ('NCT999', 'Diabetic Nephropathies', 'diabetic nephropathies', 'mesh-list')
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
    """Non-condition terms like 'Immunotherapy' should not appear in candidates."""
    conn = _setup_fuzzy_test_db()
    build_condition_dictionary(conn)
    generate_fuzzy_candidates(conn)

    result = conn.execute("""
        SELECT * FROM ref.mapping_candidates
        WHERE domain = 'condition' AND source_value = 'immunotherapy'
    """).fetchone()
    assert result is None
    conn.close()


def test_fuzzy_candidate_has_correct_study_count():
    """Fuzzy candidates should reflect the correct study count."""
    conn = _setup_fuzzy_test_db()
    build_condition_dictionary(conn)
    generate_fuzzy_candidates(conn)

    result = conn.execute("""
        SELECT study_count, status
        FROM ref.mapping_candidates
        WHERE domain = 'condition' AND source_value = 'diabetic nephropathy'
    """).fetchone()
    # "diabetic nephropathy" appears in NCT004 and NCT005 → study_count=2
    assert result is not None
    assert result[0] == 2
    assert result[1] == "pending"
    conn.close()


def test_build_dictionary_no_fuzzy_entries():
    """After building dictionary, no rows should have mapping_method='fuzzy'."""
    conn = _setup_fuzzy_test_db()
    build_condition_dictionary(conn)

    fuzzy_count = conn.execute("""
        SELECT COUNT(*) FROM ref.condition_dictionary
        WHERE mapping_method = 'fuzzy'
    """).fetchone()[0]
    assert fuzzy_count == 0
    conn.close()


def test_generate_fuzzy_candidates_creates_table():
    """generate_fuzzy_candidates should populate ref.mapping_candidates for domain='condition'."""
    conn = _setup_fuzzy_test_db()
    build_condition_dictionary(conn)
    df = generate_fuzzy_candidates(conn)

    count = conn.execute(
        "SELECT COUNT(*) FROM ref.mapping_candidates WHERE domain = 'condition'"
    ).fetchone()[0]
    assert count > 0

    non_pending = conn.execute("""
        SELECT COUNT(*) FROM ref.mapping_candidates
        WHERE domain = 'condition' AND status != 'pending'
    """).fetchone()[0]
    assert non_pending == 0

    assert len(df) == count
    assert set(df.columns) >= {"condition_name", "canonical_term", "score", "study_count", "status"}
    conn.close()


def test_promote_candidates():
    """Promoting a candidate should add it to the dictionary as manual/high."""
    import pandas as pd

    conn = _setup_fuzzy_test_db()
    build_condition_dictionary(conn)
    generate_fuzzy_candidates(conn)

    # Promote one candidate
    approved = pd.DataFrame([{
        "condition_name": "diabetic nephropathy",
        "canonical_term": "Diabetic Nephropathies",
    }])
    promoted = promote_candidates(conn, approved)

    # Should appear in dictionary
    result = conn.execute("""
        SELECT mapping_method, confidence
        FROM ref.condition_dictionary
        WHERE condition_name = 'diabetic nephropathy'
    """).fetchone()
    assert result is not None
    assert result[0] == "manual"
    assert result[1] == "high"
    assert promoted == 1

    # Candidate status should be updated
    status = conn.execute("""
        SELECT status FROM ref.mapping_candidates
        WHERE domain = 'condition' AND source_value = 'diabetic nephropathy'
    """).fetchone()
    assert status[0] == "approved"
    conn.close()


def test_regenerate_preserves_reviewed_candidates():
    """Regenerating candidates should preserve approved/rejected, only refresh pending."""
    conn = _setup_fuzzy_test_db()
    build_condition_dictionary(conn)
    generate_fuzzy_candidates(conn)

    conn.execute("""
        UPDATE ref.mapping_candidates
        SET status = 'rejected'
        WHERE domain = 'condition' AND source_value = 'diabetic nephropathy'
    """)

    generate_fuzzy_candidates(conn)

    rejected = conn.execute("""
        SELECT status FROM ref.mapping_candidates
        WHERE domain = 'condition' AND source_value = 'diabetic nephropathy'
          AND status = 'rejected'
    """).fetchone()
    assert rejected is not None

    pending_dup = conn.execute("""
        SELECT COUNT(*) FROM ref.mapping_candidates
        WHERE domain = 'condition' AND source_value = 'diabetic nephropathy'
          AND status = 'pending'
    """).fetchone()[0]
    assert pending_dup == 0
    conn.close()
