"""Tests for src/normalize_drugs.py."""

from unittest.mock import patch

import duckdb

from src.transform.normalize_drugs import (
    build_drug_dictionary,
    classify_control,
    create_study_drugs,
    generate_drug_fuzzy_candidates,
    get_coverage_stats,
    is_non_drug,
    normalize_drug_name,
    _load_chembl_synonyms,
)


def _setup_test_db():
    """Create an in-memory DuckDB with mock raw data for drug normalization."""
    from src import entities

    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE SCHEMA ref")
    conn.execute("CREATE SCHEMA norm")
    entities.ensure_schema(conn)

    # Mock studies
    conn.execute("""
        CREATE TABLE raw.studies AS
        SELECT * FROM (VALUES
            ('NCT001'), ('NCT002'), ('NCT003'), ('NCT004'), ('NCT005')
        ) AS t(nct_id)
    """)

    # Mock interventions
    conn.execute("""
        CREATE TABLE raw.interventions AS
        SELECT * FROM (VALUES
            (1, 'NCT001', 'DRUG', 'Metformin', 'desc'),
            (2, 'NCT001', 'DRUG', 'Metformin 500mg tablets', 'desc'),
            (3, 'NCT002', 'DRUG', 'Pembrolizumab', 'desc'),
            (4, 'NCT003', 'BIOLOGICAL', 'Nivolumab IV infusion', 'desc'),
            (5, 'NCT003', 'DEVICE', 'Blood pressure monitor', 'desc'),
            (6, 'NCT004', 'DRUG', 'Placebo', 'desc'),
            (7, 'NCT004', 'DRUG', 'Aspirin', 'desc'),
            (8, 'NCT005', 'BEHAVIORAL', 'Exercise program', 'desc')
        ) AS t(id, nct_id, intervention_type, name, description)
    """)

    # Mock browse_interventions
    conn.execute("""
        CREATE TABLE raw.browse_interventions AS
        SELECT * FROM (VALUES
            (1, 'NCT001', 'Metformin', 'metformin', 'mesh-list'),
            (2, 'NCT001', 'Hypoglycemic Agents', 'hypoglycemic agents', 'mesh-ancestor'),
            (3, 'NCT002', 'Pembrolizumab', 'pembrolizumab', 'mesh-list'),
            (4, 'NCT003', 'Nivolumab', 'nivolumab', 'mesh-list'),
            (5, 'NCT004', 'Aspirin', 'aspirin', 'mesh-list'),
            (6, 'NCT004', 'Placebos', 'placebos', 'mesh-list')
        ) AS t(id, nct_id, mesh_term, downcase_mesh_term, mesh_type)
    """)

    return conn


# --- normalize_drug_name tests ---


def test_normalize_basic_lowercase():
    assert normalize_drug_name("Metformin") == "metformin"


def test_normalize_strips_dosage():
    assert normalize_drug_name("Metformin 500mg") == "metformin"


def test_normalize_strips_complex_dosage():
    assert normalize_drug_name("Paclitaxel 100 mg/m2") == "paclitaxel"


def test_normalize_strips_route():
    assert normalize_drug_name("Nivolumab IV infusion") == "nivolumab"


def test_normalize_strips_formulation():
    assert normalize_drug_name("Metformin 500mg tablets") == "metformin"


def test_normalize_strips_parenthetical():
    assert normalize_drug_name("Aspirin (acetylsalicylic acid)") == "aspirin"


def test_normalize_empty_string():
    assert normalize_drug_name("") == ""


def test_normalize_none():
    assert normalize_drug_name(None) == ""


def test_normalize_preserves_dev_code():
    assert normalize_drug_name("BI 3000202") == "bi 3000202"


def test_normalize_collapses_whitespace():
    assert normalize_drug_name("Drug   Name   Here") == "drug name here"


# --- is_non_drug tests ---


def test_is_non_drug_placebo():
    assert is_non_drug("Placebo") is True


def test_is_non_drug_matching_placebo():
    assert is_non_drug("Matching placebo") is True


def test_is_non_drug_standard_of_care():
    assert is_non_drug("Standard of Care") is True


def test_is_non_drug_real_drug():
    assert is_non_drug("Metformin") is False


def test_is_non_drug_saline():
    assert is_non_drug("Normal Saline") is True


# --- classify_control tests ---


def test_classify_control_placebo():
    assert classify_control("placebo") == "Placebo"


def test_classify_control_matching_placebo():
    assert classify_control("matching placebo") == "Placebo"


def test_classify_control_placebo_for_drug():
    assert classify_control("placebo for atogepant") == "Placebo"


def test_classify_control_vehicle():
    assert classify_control("vehicle") == "Vehicle"


def test_classify_control_vehicle_cream():
    assert classify_control("vehicle cream") == "Vehicle Cream"


def test_classify_control_saline():
    assert classify_control("normal saline") == "Saline"


def test_classify_control_standard_of_care():
    assert classify_control("standard of care") == "Standard of Care"


def test_classify_control_control_group():
    assert classify_control("control group") == "Control"


def test_classify_control_real_drug():
    assert classify_control("metformin") is None


# --- build_drug_dictionary tests ---


def test_build_dictionary_mesh_exact():
    """Exact MeSH match: normalized name matches downcase_mesh_term on same study."""
    conn = _setup_test_db()
    build_drug_dictionary(conn, skip_chembl=True)

    exact = conn.execute("""
        SELECT d.source_name, e.canonical_name
        FROM ref.drug_dictionary d
        JOIN entities.drug e ON d.drug_id = e.drug_id
        WHERE d.mapping_method = 'mesh-exact'
    """).fetchall()

    assert ("metformin", "Metformin") in exact
    assert ("pembrolizumab", "Pembrolizumab") in exact
    conn.close()


def test_build_dictionary_nivolumab_via_exact():
    """Nivolumab IV infusion should match via mesh-exact after normalization."""
    conn = _setup_test_db()
    build_drug_dictionary(conn, skip_chembl=True)

    # "Nivolumab IV infusion" normalizes to "nivolumab" → matches mesh "nivolumab"
    row = conn.execute("""
        SELECT source_name, mapping_method
        FROM ref.drug_dictionary
        WHERE source_name = 'nivolumab'
    """).fetchone()
    assert row is not None
    assert row[1] == "mesh-exact"
    conn.close()


def test_build_dictionary_preserves_manual():
    """Manual entries survive dictionary rebuild."""
    from src import entities

    conn = _setup_test_db()
    build_drug_dictionary(conn, skip_chembl=True)

    # Insert a manual entity + dictionary row
    drug_id = entities.upsert_drug(
        conn, canonical_name="Custom Drug Canonical", origin="manual",
        chembl_id="CHEMBL999",
    )
    conn.execute(
        "INSERT INTO ref.drug_dictionary VALUES (?, ?, ?, ?)",
        ["custom drug", drug_id, "manual", "high"],
    )

    # Rebuild
    build_drug_dictionary(conn, skip_chembl=True)

    manual = conn.execute("""
        SELECT source_name, mapping_method
        FROM ref.drug_dictionary WHERE mapping_method = 'manual'
    """).fetchall()
    assert len(manual) == 1
    assert manual[0][0] == "custom drug"
    conn.close()


def test_build_dictionary_no_duplicates():
    """A name matched in layer 1 should not appear again in layer 2."""
    conn = _setup_test_db()
    build_drug_dictionary(conn, skip_chembl=True)

    # Check metformin only appears once
    rows = conn.execute("""
        SELECT mapping_method FROM ref.drug_dictionary
        WHERE source_name = 'metformin'
    """).fetchall()
    assert len(rows) == 1
    assert rows[0][0] == "mesh-exact"
    conn.close()


# --- ChEMBL layer tests (mocked) ---


def test_build_dictionary_chembl_local_lookup():
    """ChEMBL layer should match against local synonym lookup."""
    conn = _setup_test_db()

    # Remove browse_interventions so nothing matches MeSH
    conn.execute("DELETE FROM raw.browse_interventions")

    mock_synonyms = {
        "metformin": ("METFORMIN", "CHEMBL1431"),
        "pembrolizumab": ("PEMBROLIZUMAB", "CHEMBL3137343"),
    }

    with patch("src.transform.normalize_drugs._load_chembl_synonyms", return_value=mock_synonyms):
        build_drug_dictionary(conn, skip_chembl=False)

    chembl = conn.execute("""
        SELECT d.source_name, e.canonical_name, e.chembl_id
        FROM ref.drug_dictionary d
        JOIN entities.drug e ON d.drug_id = e.drug_id
        WHERE d.mapping_method = 'chembl-synonym'
    """).fetchall()

    assert ("metformin", "METFORMIN", "CHEMBL1431") in chembl
    assert ("pembrolizumab", "PEMBROLIZUMAB", "CHEMBL3137343") in chembl
    conn.close()


def test_build_dictionary_chembl_skips_non_drugs():
    """ChEMBL layer should not match placebo/control terms."""
    conn = _setup_test_db()
    conn.execute("DELETE FROM raw.browse_interventions")

    # Even if "placebo" is in the synonym dict, it should be skipped
    mock_synonyms = {
        "placebo": ("PLACEBO_COMPOUND", "CHEMBL9999"),
        "metformin": ("METFORMIN", "CHEMBL1431"),
    }

    with patch("src.transform.normalize_drugs._load_chembl_synonyms", return_value=mock_synonyms):
        build_drug_dictionary(conn, skip_chembl=False)

    chembl = conn.execute("""
        SELECT source_name FROM ref.drug_dictionary
        WHERE mapping_method = 'chembl-synonym'
    """).fetchall()
    names = [r[0] for r in chembl]

    assert "metformin" in names
    assert "placebo" not in names
    conn.close()


# --- create_study_drugs tests ---


def test_create_study_drugs_row_count():
    """Every Drug/Biological intervention should get a row in study_drugs."""
    conn = _setup_test_db()
    build_drug_dictionary(conn, skip_chembl=True)
    create_study_drugs(conn)

    drug_bio_count = conn.execute("""
        SELECT COUNT(*) FROM raw.interventions
        WHERE intervention_type IN ('DRUG', 'BIOLOGICAL')
    """).fetchone()[0]
    study_drugs_count = conn.execute(
        "SELECT COUNT(*) FROM norm.study_drugs"
    ).fetchone()[0]

    assert study_drugs_count == drug_bio_count
    conn.close()


def test_create_study_drugs_excludes_non_drugs():
    """Devices and behavioral interventions should not appear in study_drugs."""
    conn = _setup_test_db()
    build_drug_dictionary(conn, skip_chembl=True)
    create_study_drugs(conn)

    types = conn.execute("""
        SELECT DISTINCT intervention_type FROM norm.study_drugs
    """).fetchall()
    type_set = {row[0] for row in types}

    assert "DEVICE" not in type_set
    assert "BEHAVIORAL" not in type_set
    conn.close()


def test_create_study_drugs_matched_have_drug_id():
    """Matched drugs should have non-null drug_id."""
    conn = _setup_test_db()
    build_drug_dictionary(conn, skip_chembl=True)
    create_study_drugs(conn)

    row = conn.execute("""
        SELECT drug_id, mapping_method
        FROM norm.study_drugs
        WHERE intervention_name = 'Metformin'
    """).fetchone()
    assert row[0] is not None
    assert row[1] != "unmatched"
    conn.close()


def test_create_study_drugs_unmatched_have_null_drug_id():
    """Unmatched drugs should have null drug_id and 'unmatched' method."""
    conn = _setup_test_db()
    conn.execute("DELETE FROM raw.browse_interventions")
    build_drug_dictionary(conn, skip_chembl=True)
    create_study_drugs(conn)

    row = conn.execute("""
        SELECT drug_id, mapping_method
        FROM norm.study_drugs
        WHERE intervention_name = 'Metformin'
    """).fetchone()
    assert row[0] is None
    assert row[1] == "unmatched"
    conn.close()


# --- get_coverage_stats tests ---


def test_coverage_stats_keys():
    """Coverage stats should contain expected keys."""
    conn = _setup_test_db()
    build_drug_dictionary(conn, skip_chembl=True)
    create_study_drugs(conn)
    stats = get_coverage_stats(conn)

    assert "total_interventions" in stats
    assert "matched_interventions" in stats
    assert "intervention_coverage_pct" in stats
    assert "total_studies" in stats
    assert "matched_studies" in stats
    assert "study_coverage_pct" in stats
    assert "method_breakdown" in stats
    assert "dictionary_stats" in stats
    conn.close()


def test_coverage_stats_values():
    """Coverage percentages should be between 0 and 100."""
    conn = _setup_test_db()
    build_drug_dictionary(conn, skip_chembl=True)
    create_study_drugs(conn)
    stats = get_coverage_stats(conn)

    assert 0 <= stats["intervention_coverage_pct"] <= 100
    assert 0 <= stats["study_coverage_pct"] <= 100
    assert stats["total_interventions"] > 0
    conn.close()


def test_generate_drug_fuzzy_candidates_inserts_into_mapping_candidates():
    """Unmatched interventions should surface as sponsor=... err, drug fuzzy candidates."""
    conn = _setup_test_db()
    # Add an unmatched intervention similar to a known MeSH term
    conn.execute("""
        INSERT INTO raw.interventions VALUES
        (99, 'NCT005', 'DRUG', 'Metphormin', 'typo')
    """)
    build_drug_dictionary(conn, skip_chembl=True)
    create_study_drugs(conn)
    n = generate_drug_fuzzy_candidates(conn, score_cutoff=80, top_n=100)
    assert n >= 1

    rows = conn.execute("""
        SELECT source_value, canonical_term, score, status
        FROM ref.mapping_candidates WHERE domain = 'drug'
    """).fetchall()
    assert rows
    # Our typo should be among them
    assert any("metphormin" in r[0] for r in rows)
    assert all(r[3] == "pending" for r in rows)
    conn.close()
