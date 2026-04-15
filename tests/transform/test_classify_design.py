"""Tests for src/classify_design.py."""

import duckdb

from src.transform.classify_design import classify_study_design


def _setup_design_test_db():
    """Create an in-memory DuckDB with mock data for design classification."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE SCHEMA class")

    conn.execute("""
        CREATE TABLE raw.studies AS
        SELECT * FROM (VALUES
            ('NCT001', 'INTERVENTIONAL'),
            ('NCT002', 'INTERVENTIONAL'),
            ('NCT003', 'INTERVENTIONAL'),
            ('NCT004', 'OBSERVATIONAL'),
            ('NCT005', 'EXPANDED_ACCESS'),
            ('NCT006', 'INTERVENTIONAL'),
            ('NCT007', 'INTERVENTIONAL'),
            ('NCT008', 'INTERVENTIONAL')
        ) AS t(nct_id, study_type)
    """)

    conn.execute("""
        CREATE TABLE raw.designs AS
        SELECT * FROM (VALUES
            -- NCT001: Randomized Parallel → Parallel RCT
            ('NCT001', 'RANDOMIZED', 'PARALLEL', 'None', 'TREATMENT', 'DOUBLE'),
            -- NCT002: Non-Randomized Single Group → Single-Arm
            ('NCT002', 'NON_RANDOMIZED', 'SINGLE_GROUP', 'None', 'TREATMENT', 'NONE'),
            -- NCT003: Randomized Crossover → Crossover RCT
            ('NCT003', 'RANDOMIZED', 'CROSSOVER', 'None', 'PREVENTION', 'SINGLE'),
            -- NCT004: Observational Cohort
            ('NCT004', 'None', 'None', 'COHORT', 'None', 'None'),
            -- NCT005: Expanded Access (no meaningful design fields)
            ('NCT005', 'None', 'None', 'None', 'None', 'None'),
            -- NCT006: NA allocation + Single Group → Single-Arm
            ('NCT006', 'NA', 'SINGLE_GROUP', 'None', 'DIAGNOSTIC', 'NONE'),
            -- NCT007: Randomized Factorial → Factorial RCT
            ('NCT007', 'RANDOMIZED', 'FACTORIAL', 'None', 'TREATMENT', 'QUADRUPLE')
            -- NCT008: no design record (missing from designs table)
        ) AS t(nct_id, allocation, intervention_model, observational_model,
               primary_purpose, masking)
    """)

    return conn


def test_study_type_classification():
    """L1: study_type should pass through."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    rows = conn.execute("""
        SELECT nct_id, study_type FROM class.study_design
        ORDER BY nct_id
    """).fetchall()
    types = {r[0]: r[1] for r in rows}
    assert types["NCT001"] == "INTERVENTIONAL"
    assert types["NCT004"] == "OBSERVATIONAL"
    assert types["NCT005"] == "EXPANDED_ACCESS"
    conn.close()


def test_design_architecture_rct():
    """L2: RANDOMIZED + PARALLEL → Parallel RCT."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    result = conn.execute("""
        SELECT design_architecture FROM class.study_design
        WHERE nct_id = 'NCT001'
    """).fetchone()
    assert result[0] == "Parallel RCT"
    conn.close()


def test_design_architecture_crossover_rct():
    """L2: RANDOMIZED + CROSSOVER → Crossover RCT."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    result = conn.execute("""
        SELECT design_architecture FROM class.study_design
        WHERE nct_id = 'NCT003'
    """).fetchone()
    assert result[0] == "Crossover RCT"
    conn.close()


def test_design_architecture_factorial_rct():
    """L2: RANDOMIZED + FACTORIAL → Factorial RCT."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    result = conn.execute("""
        SELECT design_architecture FROM class.study_design
        WHERE nct_id = 'NCT007'
    """).fetchone()
    assert result[0] == "Factorial RCT"
    conn.close()


def test_design_architecture_single_arm():
    """L2: NON_RANDOMIZED + SINGLE_GROUP → Single-Arm."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    result = conn.execute("""
        SELECT design_architecture FROM class.study_design
        WHERE nct_id = 'NCT002'
    """).fetchone()
    assert result[0] == "Single-Arm"
    conn.close()


def test_design_architecture_single_arm_na_allocation():
    """L2: NA allocation + SINGLE_GROUP → Single-Arm."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    result = conn.execute("""
        SELECT design_architecture FROM class.study_design
        WHERE nct_id = 'NCT006'
    """).fetchone()
    assert result[0] == "Single-Arm"
    conn.close()


def test_design_architecture_observational():
    """L2: OBSERVATIONAL → uses observational_model."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    result = conn.execute("""
        SELECT design_architecture FROM class.study_design
        WHERE nct_id = 'NCT004'
    """).fetchone()
    assert result[0] == "Cohort"
    conn.close()


def test_design_architecture_expanded_access():
    """L2: EXPANDED_ACCESS → 'Expanded Access'."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    result = conn.execute("""
        SELECT design_architecture FROM class.study_design
        WHERE nct_id = 'NCT005'
    """).fetchone()
    assert result[0] == "Expanded Access"
    conn.close()


def test_blinding_level_mapping():
    """L4: masking values should map to readable blinding levels."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    rows = conn.execute("""
        SELECT nct_id, blinding_level FROM class.study_design
        ORDER BY nct_id
    """).fetchall()
    blinding = {r[0]: r[1] for r in rows}
    assert blinding["NCT001"] == "Double Blind"
    assert blinding["NCT002"] == "Open Label"
    assert blinding["NCT003"] == "Single Blind"
    assert blinding["NCT007"] == "Quadruple Blind"
    conn.close()


def test_purpose_passthrough():
    """L5: primary_purpose should pass through."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    rows = conn.execute("""
        SELECT nct_id, purpose FROM class.study_design
        ORDER BY nct_id
    """).fetchall()
    purposes = {r[0]: r[1] for r in rows}
    assert purposes["NCT001"] == "TREATMENT"
    assert purposes["NCT003"] == "PREVENTION"
    assert purposes["NCT006"] == "DIAGNOSTIC"
    conn.close()


def test_none_string_treated_as_null():
    """'None' string values should become NULL in output."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    # NCT004 is observational — masking and purpose should be NULL
    result = conn.execute("""
        SELECT blinding_level, purpose FROM class.study_design
        WHERE nct_id = 'NCT004'
    """).fetchone()
    assert result[0] is None  # masking was 'None'
    assert result[1] is None  # primary_purpose was 'None'
    conn.close()


def test_study_without_design_record():
    """Study with no row in raw.designs should have NULLs for L2/L4/L5."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    result = conn.execute("""
        SELECT study_type, design_architecture, blinding_level, purpose
        FROM class.study_design
        WHERE nct_id = 'NCT008'
    """).fetchone()
    assert result[0] == "INTERVENTIONAL"  # L1 from studies table
    assert result[1] is None  # L2 no design record
    assert result[2] is None  # L4 no design record
    assert result[3] is None  # L5 no design record
    conn.close()


def test_all_studies_classified():
    """Every study should have a row in class.study_design."""
    conn = _setup_design_test_db()
    classify_study_design(conn)

    study_count = conn.execute("SELECT COUNT(*) FROM raw.studies").fetchone()[0]
    class_count = conn.execute("SELECT COUNT(*) FROM class.study_design").fetchone()[0]
    assert class_count == study_count
    conn.close()
