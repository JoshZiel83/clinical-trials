"""One-shot: move existing reference files into versioned directory layout
and populate meta.reference_sources (Phase 7E).

Expected before:
    data/reference/chembl_synonyms.parquet
    data/reference/umls/quickumls_index/
    data/reference/therapeutic_area_mapping.json
    data/reference/mesh/2026/desc.xml     (already placed)

After:
    data/reference/chembl/36/synonyms.parquet
    data/reference/umls/2025AB/quickumls_index/
    data/reference/mesh_ta_mapping/v1/mapping.json
    data/reference/mesh/2026/desc.xml

Versions passed via flags default to the values current as of 2026-04-15.
Idempotent: if the target path already exists and the source is gone, just
(re)registers the row in meta.reference_sources.
"""

import argparse
import shutil
import sys
from pathlib import Path

from config.settings import REFERENCE_DATA_DIR, get_duckdb_connection
from src.logging_config import get_logger, setup_logging
from src.reference_sources import ensure_table, register_source

logger = get_logger("bootstrap_reference_sources")


MOVES = [
    # (source_name, version, old_relpath, new_relpath, notes)
    (
        "chembl", "36",
        "chembl_synonyms.parquet",
        "chembl/36/synonyms.parquet",
        "ChEMBL 36 drug synonyms: synonym, pref_name, chembl_id",
    ),
    (
        "umls", "2025AB",
        "umls/quickumls_index",
        "umls/2025AB/quickumls_index",
        "UMLS 2025AB Metathesaurus — QuickUMLS-built index",
    ),
    (
        "mesh_ta_mapping", "v1",
        "therapeutic_area_mapping.json",
        "mesh_ta_mapping/v1/mapping.json",
        "Curated MeSH ancestor → therapeutic area mapping (NBK611886-seeded)",
    ),
]


def move_if_needed(old_path: Path, new_path: Path) -> bool:
    """Move old_path → new_path if old exists and new doesn't. Returns True
    if a move occurred, False if the target already existed or source missing.
    """
    if new_path.exists():
        if old_path.exists():
            logger.warning(
                f"both old and new exist; leaving as-is: {old_path} and {new_path}"
            )
        else:
            logger.info(f"already at target: {new_path}")
        return False
    if not old_path.exists():
        logger.info(f"source missing, nothing to move: {old_path}")
        return False
    new_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.move(str(old_path), str(new_path))
    logger.info(f"moved {old_path} → {new_path}")
    return True


def bootstrap(duck_conn, mesh_version="2026"):
    ensure_table(duck_conn)

    for source_name, version, old_rel, new_rel, notes in MOVES:
        old_path = REFERENCE_DATA_DIR / old_rel
        new_path = REFERENCE_DATA_DIR / new_rel
        move_if_needed(old_path, new_path)
        if not new_path.exists():
            logger.warning(
                f"{source_name}@{version}: target path missing, skipping registration: {new_path}"
            )
            continue
        register_source(duck_conn, source_name, version, str(new_path), notes=notes)

    # MeSH descriptor file — already placed at data/reference/mesh/<ver>/desc.xml
    mesh_path = REFERENCE_DATA_DIR / "mesh" / mesh_version / "desc.xml"
    if mesh_path.exists():
        register_source(
            duck_conn, "mesh", mesh_version, str(mesh_path),
            notes=f"NLM MeSH {mesh_version} DescriptorRecordSet XML",
        )
    else:
        logger.warning(f"MeSH descriptor XML not found at {mesh_path}; skipping")

    rows = duck_conn.execute("""
        SELECT source_name, version, is_active, path
        FROM meta.reference_sources
        ORDER BY source_name, version
    """).fetchall()
    logger.info("meta.reference_sources contents:")
    for r in rows:
        logger.info(f"  {r[0]}@{r[1]} active={r[2]} path={r[3]}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mesh-version", default="2026",
                        help="MeSH version (directory under data/reference/mesh/); default 2026")
    args = parser.parse_args()

    setup_logging()
    conn = get_duckdb_connection()
    try:
        bootstrap(conn, mesh_version=args.mesh_version)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
