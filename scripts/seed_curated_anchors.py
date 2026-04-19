"""Seed curated parent-entity sponsors referenced by sponsor_anchors.json.

Phase 7D's anchor-set builder requires that any canonical_name in the
`include` list already exists in entities.sponsor. Most curated parents
(e.g. 'Novartis', 'Johnson & Johnson') already exist as AACT-derived
canonicals. A handful do NOT — specifically, 'Merck & Co., Inc.', the
true US Merck parent, never appears cleanly in AACT: it's referenced only
indirectly via subsidiary suffixes like 'a subsidiary of Merck & Co., Inc.
(Rahway, New Jersey USA)'.

This script idempotently INSERTs those "missing parent" entity rows with
origin='manual' so the curation file can reference them. It never updates
or deletes existing rows. Safe to re-run.

Intended as a one-shot before the first Phase 7D anchor build. Tracked in
meta.migration_log so re-runs are fast no-ops.
"""

from config.settings import get_duckdb_connection
from src import entities
from src.logging_config import get_logger, setup_logging

logger = get_logger("seed_curated_anchors")


MIGRATION_NAME = "phase_7d_seed_curated_anchors"


# Parents that must exist in entities.sponsor for sponsor_anchors.json
# `include` to resolve. origin='manual' since these are human-curated
# rather than AACT-derived.
CURATED_PARENTS = [
    {
        "canonical_name": "Merck & Co., Inc.",
        "source_versions": {
            "notes": "curated Phase 7D parent for US Merck; see subsidiary "
                     "notation in raw sponsor names like 'Peloton "
                     "Therapeutics, Inc., a subsidiary of Merck & Co., Inc. "
                     "(Rahway, New Jersey USA)'",
        },
    },
    # Add more curated parents here as the curation file evolves. Existing
    # AACT-derived canonicals (Novartis, Johnson & Johnson, Merck KGaA,
    # Darmstadt, Germany) need no seeding.
]


def _ensure_migration_log(duck_conn) -> None:
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.migration_log (
            name          VARCHAR PRIMARY KEY,
            applied_at    TIMESTAMP DEFAULT current_timestamp,
            rows_affected INTEGER
        )
    """)


def seed(duck_conn) -> int:
    """Insert any missing curated parent entities. Returns rows created."""
    entities.ensure_schema(duck_conn)
    _ensure_migration_log(duck_conn)

    created = 0
    for spec in CURATED_PARENTS:
        name = spec["canonical_name"]
        existing = duck_conn.execute(
            "SELECT sponsor_id FROM entities.sponsor WHERE canonical_name = ?",
            [name],
        ).fetchone()
        if existing:
            logger.info(f"entities.sponsor already has {name!r} (id={existing[0]})")
            continue
        sid = entities.upsert_sponsor(
            duck_conn,
            canonical_name=name,
            origin="manual",
            source_versions=spec.get("source_versions"),
        )
        logger.info(f"inserted entities.sponsor {name!r} as sponsor_id={sid}")
        created += 1

    duck_conn.execute(
        """
        INSERT OR REPLACE INTO meta.migration_log (name, rows_affected)
        VALUES (?, ?)
        """,
        [MIGRATION_NAME, created],
    )
    return created


if __name__ == "__main__":
    setup_logging()
    conn = get_duckdb_connection()
    try:
        n = seed(conn)
        print(f"seeded {n} curated parent entities")
    finally:
        conn.close()
