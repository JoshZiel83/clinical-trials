"""Parse MeSH DescriptorRecordSet XML → entities.condition.

Seeds the condition entity table with all ~30k MeSH descriptors. Uses
streaming iterparse to keep memory bounded on the 313MB XML file.
Idempotent by descriptor UI — re-running the same version is a no-op;
running a new MeSH version only inserts newly-introduced descriptors
(no retirements or renames applied — out of scope for Phase 7B).

Reads the active MeSH path from meta.reference_sources.
"""

import argparse
import json
import sys
from pathlib import Path
from xml.etree import ElementTree as ET

import pandas as pd

from config.settings import get_duckdb_connection
from src import entities, reference_sources
from src.logging_config import get_logger, setup_logging

logger = get_logger("load_mesh_descriptors")


def iter_descriptors(xml_path):
    """Stream (descriptor_ui, canonical_term) pairs from the MeSH XML.

    Uses iterparse + elem.clear() to avoid loading the whole tree.
    """
    for _event, elem in ET.iterparse(str(xml_path), events=("end",)):
        if elem.tag != "DescriptorRecord":
            continue
        ui = elem.findtext("DescriptorUI")
        name = elem.findtext("DescriptorName/String")
        if ui and name:
            yield ui, name.strip()
        elem.clear()


def load(duck_conn, xml_path=None, mesh_version=None):
    """Load MeSH descriptors into entities.condition.

    Args:
        duck_conn: DuckDB connection (read-write).
        xml_path: override path; default resolved via meta.reference_sources.
        mesh_version: override version string for provenance stamping.

    Returns the number of rows inserted.
    """
    entities.ensure_schema(duck_conn)

    if xml_path is None:
        xml_path = Path(reference_sources.get_active_path(duck_conn, "mesh"))
    else:
        xml_path = Path(xml_path)
    if mesh_version is None:
        mesh_version = reference_sources.get_active_version(duck_conn, "mesh")

    logger.info(f"parsing MeSH descriptors from {xml_path}")
    descriptors = list(iter_descriptors(xml_path))
    logger.info(f"parsed {len(descriptors):,} descriptors")

    source_versions = json.dumps({"mesh": mesh_version})
    df = pd.DataFrame(descriptors, columns=["mesh_descriptor_id", "canonical_term"])
    df["origin"] = "mesh"
    df["umls_cui"] = None
    df["source_versions"] = source_versions

    # Idempotent: skip descriptors already present (keyed by UI).
    existing = {
        row[0] for row in duck_conn.execute(
            "SELECT mesh_descriptor_id FROM entities.condition"
        ).fetchall()
    }
    if existing:
        before = len(df)
        df = df[~df["mesh_descriptor_id"].isin(existing)]
        logger.info(f"skipping {before - len(df):,} already-loaded descriptors")

    if df.empty:
        logger.info("no new descriptors to load")
        return 0

    duck_conn.register("_mesh_df", df)
    try:
        duck_conn.execute("""
            INSERT INTO entities.condition
                (origin, mesh_descriptor_id, canonical_term, umls_cui, source_versions)
            SELECT origin, mesh_descriptor_id, canonical_term, umls_cui, source_versions
            FROM _mesh_df
        """)
    finally:
        duck_conn.unregister("_mesh_df")

    logger.info(f"inserted {len(df):,} rows into entities.condition")
    return len(df)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--xml-path", default=None,
                        help="Override MeSH XML path; default resolved from meta.reference_sources")
    parser.add_argument("--mesh-version", default=None,
                        help="Override version for source_versions stamp; default from meta.reference_sources")
    args = parser.parse_args()

    setup_logging()
    conn = get_duckdb_connection()
    try:
        load(conn, xml_path=args.xml_path, mesh_version=args.mesh_version)
        total = conn.execute("SELECT COUNT(*) FROM entities.condition").fetchone()[0]
        logger.info(f"entities.condition total rows: {total:,}")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
