"""Tests for scripts/load_mesh_descriptors.py — MeSH → entities.condition."""

import duckdb

from scripts import load_mesh_descriptors as lmd
from src import entities, reference_sources


MINI_MESH_XML = """<?xml version="1.0"?>
<DescriptorRecordSet LanguageCode="eng">
  <DescriptorRecord DescriptorClass="1">
    <DescriptorUI>D000001</DescriptorUI>
    <DescriptorName><String>Calcimycin</String></DescriptorName>
  </DescriptorRecord>
  <DescriptorRecord DescriptorClass="1">
    <DescriptorUI>D000002</DescriptorUI>
    <DescriptorName><String>Aspirin</String></DescriptorName>
  </DescriptorRecord>
  <DescriptorRecord DescriptorClass="1">
    <DescriptorUI>D001943</DescriptorUI>
    <DescriptorName><String>Breast Neoplasms</String></DescriptorName>
  </DescriptorRecord>
</DescriptorRecordSet>
"""


def _fresh_conn():
    return duckdb.connect(":memory:")


def test_iter_descriptors(tmp_path):
    p = tmp_path / "mini.xml"
    p.write_text(MINI_MESH_XML)
    rows = list(lmd.iter_descriptors(str(p)))
    assert rows == [
        ("D000001", "Calcimycin"),
        ("D000002", "Aspirin"),
        ("D001943", "Breast Neoplasms"),
    ]


def test_load_into_entities(tmp_path):
    p = tmp_path / "mini.xml"
    p.write_text(MINI_MESH_XML)
    conn = _fresh_conn()
    entities.ensure_schema(conn)
    reference_sources.ensure_table(conn)
    reference_sources.register_source(conn, "mesh", "test", str(p))

    n = lmd.load(conn)
    assert n == 3

    rows = conn.execute("""
        SELECT origin, mesh_descriptor_id, canonical_term, source_versions
        FROM entities.condition
        ORDER BY mesh_descriptor_id
    """).fetchall()
    assert [(r[0], r[1], r[2]) for r in rows] == [
        ("mesh", "D000001", "Calcimycin"),
        ("mesh", "D000002", "Aspirin"),
        ("mesh", "D001943", "Breast Neoplasms"),
    ]
    # Provenance stamped
    assert '"mesh"' in rows[0][3] and '"test"' in rows[0][3]


def test_load_is_idempotent(tmp_path):
    p = tmp_path / "mini.xml"
    p.write_text(MINI_MESH_XML)
    conn = _fresh_conn()
    entities.ensure_schema(conn)
    reference_sources.ensure_table(conn)
    reference_sources.register_source(conn, "mesh", "test", str(p))

    assert lmd.load(conn) == 3
    assert lmd.load(conn) == 0  # second run inserts nothing
    total = conn.execute("SELECT COUNT(*) FROM entities.condition").fetchone()[0]
    assert total == 3


def test_load_with_override_path(tmp_path):
    """xml_path override bypasses reference_sources lookup."""
    p = tmp_path / "mini.xml"
    p.write_text(MINI_MESH_XML)
    conn = _fresh_conn()
    entities.ensure_schema(conn)
    # Note: no reference_sources registration; using explicit overrides
    n = lmd.load(conn, xml_path=str(p), mesh_version="override-v1")
    assert n == 3
