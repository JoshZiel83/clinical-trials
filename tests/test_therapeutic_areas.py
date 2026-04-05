"""Tests for src/therapeutic_areas.py."""

import json
import tempfile
from pathlib import Path

import duckdb

from src.therapeutic_areas import (
    create_ta_reference_table,
    create_study_therapeutic_areas,
    get_ta_distribution,
    load_ta_mapping,
)


SAMPLE_MAPPING = [
    {"mesh_ancestor": "Neoplasms", "therapeutic_area": "Oncology"},
    {"mesh_ancestor": "Cardiovascular Diseases", "therapeutic_area": "Cardiovascular"},
    {"mesh_ancestor": "Mental Disorders", "therapeutic_area": "Psychiatry"},
    {"mesh_ancestor": "Endocrine System Diseases", "therapeutic_area": "Metabolic/Endocrine"},
    {"mesh_ancestor": "Nutritional and Metabolic Diseases", "therapeutic_area": "Metabolic/Endocrine"},
    {"mesh_ancestor": "Behavior", "therapeutic_area": "Behavioral/Lifestyle"},
    {"mesh_ancestor": "Stomatognathic Diseases", "therapeutic_area": "Dentistry/Oral Health"},
]


def _write_temp_json(mapping):
    """Write mapping to a temp JSON file and return the path."""
    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False)
    json.dump(mapping, tmp)
    tmp.close()
    return Path(tmp.name)


def _setup_ta_test_db():
    """Create an in-memory DuckDB with mock browse_conditions + ref.therapeutic_areas."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA raw")
    conn.execute("CREATE SCHEMA ref")
    conn.execute("CREATE SCHEMA norm")

    conn.execute("""
        CREATE TABLE raw.browse_conditions AS
        SELECT * FROM (VALUES
            ('NCT001', 'Breast Neoplasms', 'breast neoplasms', 'mesh-list'),
            ('NCT001', 'Neoplasms', 'neoplasms', 'mesh-ancestor'),
            ('NCT002', 'Heart Failure', 'heart failure', 'mesh-list'),
            ('NCT002', 'Cardiovascular Diseases', 'cardiovascular diseases', 'mesh-ancestor'),
            ('NCT003', 'Motor Activity', 'motor activity', 'mesh-list'),
            ('NCT003', 'Behavior', 'behavior', 'mesh-ancestor'),
            ('NCT004', 'Dental Caries', 'dental caries', 'mesh-list'),
            ('NCT004', 'Stomatognathic Diseases', 'stomatognathic diseases', 'mesh-ancestor'),
            ('NCT005', 'Neoplasms', 'neoplasms', 'mesh-list')
        ) AS t(nct_id, mesh_term, downcase_mesh_term, mesh_type)
    """)

    create_ta_reference_table(conn, SAMPLE_MAPPING)
    return conn


def test_load_ta_mapping_returns_expected_structure():
    path = _write_temp_json(SAMPLE_MAPPING)
    mapping = load_ta_mapping(json_path=path)
    assert len(mapping) == 7
    assert all("mesh_ancestor" in entry for entry in mapping)
    assert all("therapeutic_area" in entry for entry in mapping)


def test_load_ta_mapping_default_file_has_required_keys():
    mapping = load_ta_mapping()
    assert len(mapping) >= 19
    for entry in mapping:
        assert "mesh_ancestor" in entry
        assert "therapeutic_area" in entry


def test_load_ta_mapping_covers_major_categories():
    mapping = load_ta_mapping()
    ancestors = {entry["mesh_ancestor"] for entry in mapping}
    assert "Neoplasms" in ancestors
    assert "Cardiovascular Diseases" in ancestors
    assert "Nervous System Diseases" in ancestors
    assert "Mental Disorders" in ancestors
    assert "Infections" in ancestors


def test_load_ta_mapping_includes_gap_categories():
    """Behavior and Stomatognathic Diseases should be in the mapping."""
    mapping = load_ta_mapping()
    ancestors = {entry["mesh_ancestor"] for entry in mapping}
    assert "Behavior" in ancestors
    assert "Stomatognathic Diseases" in ancestors


def test_create_ta_reference_table_loads_into_duckdb():
    conn = duckdb.connect(":memory:")
    row_count = create_ta_reference_table(conn, SAMPLE_MAPPING)
    assert row_count == 7

    rows = conn.execute(
        "SELECT mesh_ancestor, therapeutic_area FROM ref.therapeutic_areas ORDER BY mesh_ancestor"
    ).fetchall()
    assert ("Neoplasms", "Oncology") in rows
    assert ("Behavior", "Behavioral/Lifestyle") in rows
    conn.close()


def test_create_ta_reference_table_is_idempotent():
    conn = duckdb.connect(":memory:")
    create_ta_reference_table(conn, SAMPLE_MAPPING)
    row_count = create_ta_reference_table(conn, SAMPLE_MAPPING)
    assert row_count == 7
    conn.close()


def test_multiple_ancestors_map_to_same_ta():
    mapping = load_ta_mapping()
    ta_counts = {}
    for entry in mapping:
        ta = entry["therapeutic_area"]
        ta_counts[ta] = ta_counts.get(ta, 0) + 1
    assert ta_counts.get("Metabolic/Endocrine", 0) >= 2


def test_create_study_therapeutic_areas():
    """TA join should produce correct assignments from browse_conditions ancestors."""
    conn = _setup_ta_test_db()
    row_count = create_study_therapeutic_areas(conn)
    assert row_count > 0

    results = conn.execute(
        "SELECT nct_id, therapeutic_area FROM norm.study_therapeutic_areas ORDER BY nct_id"
    ).fetchall()

    nct_tas = {}
    for nct_id, ta in results:
        nct_tas.setdefault(nct_id, set()).add(ta)

    assert "Oncology" in nct_tas["NCT001"]
    assert "Cardiovascular" in nct_tas["NCT002"]
    conn.close()


def test_ta_gap_categories():
    """Behavior and Stomatognathic Diseases should now produce TAs."""
    conn = _setup_ta_test_db()
    create_study_therapeutic_areas(conn)

    results = conn.execute(
        "SELECT nct_id, therapeutic_area FROM norm.study_therapeutic_areas"
    ).fetchall()
    nct_tas = {}
    for nct_id, ta in results:
        nct_tas.setdefault(nct_id, set()).add(ta)

    assert "Behavioral/Lifestyle" in nct_tas["NCT003"]
    assert "Dentistry/Oral Health" in nct_tas["NCT004"]
    conn.close()


def test_mesh_list_direct_match():
    """A mesh-list term that directly matches a TA ancestor should also map."""
    conn = _setup_ta_test_db()
    create_study_therapeutic_areas(conn)

    # NCT005 has "Neoplasms" as mesh-list (not ancestor)
    tas = conn.execute(
        "SELECT therapeutic_area FROM norm.study_therapeutic_areas WHERE nct_id = 'NCT005'"
    ).fetchall()
    assert ("Oncology",) in tas
    conn.close()


def test_get_ta_distribution():
    conn = _setup_ta_test_db()
    create_study_therapeutic_areas(conn)
    dist = get_ta_distribution(conn)
    assert "therapeutic_area" in dist.columns
    assert "study_count" in dist.columns
    assert len(dist) >= 4
    conn.close()
