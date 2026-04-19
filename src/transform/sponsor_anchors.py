"""Sponsor anchor set (Phase 7D).

The anchor set is the curated collection of high-frequency canonical sponsors
that Phase 7D's enrichment agent treats as merge targets — "is this lower-
frequency canonical a variant of one of these anchors?"

Composition: top-N by distinct `study_count` (via `norm.study_sponsors`) plus
any `canonical_name`s forced in via `data/reference/sponsor_anchors.json`
(`include`), minus any `exclude`d canonicals. Regenerated on each sponsor
pipeline run; registered in `meta.reference_sources` as `sponsor_anchors`.
"""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Optional

from config.settings import PROJECT_ROOT
from src import reference_sources
from src.logging_config import get_logger

logger = get_logger("sponsor_anchors")


ANCHORS_JSON_PATH = PROJECT_ROOT / "data" / "reference" / "sponsor_anchors.json"
DEFAULT_TOP_N = 200


def _load_curation(path: Path = ANCHORS_JSON_PATH) -> dict:
    """Load the curation JSON. Returns defaults if the file is missing."""
    if not path.exists():
        logger.warning(
            f"sponsor_anchors.json not found at {path}; using defaults "
            f"(top_n={DEFAULT_TOP_N}, no overrides)"
        )
        return {"top_n": DEFAULT_TOP_N, "include": [], "exclude": []}
    with open(path) as f:
        data = json.load(f)
    data.setdefault("top_n", DEFAULT_TOP_N)
    data.setdefault("include", [])
    data.setdefault("exclude", [])
    return data


def _ensure_anchor_table(duck_conn) -> None:
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.sponsor_anchor_set (
            sponsor_id      BIGINT  PRIMARY KEY,
            canonical_name  VARCHAR NOT NULL,
            study_count     INTEGER NOT NULL,
            origin          VARCHAR NOT NULL,   -- 'auto' | 'curated_include'
            built_at        TIMESTAMP DEFAULT current_timestamp
        )
    """)


def build_anchor_set(
    duck_conn,
    top_n: Optional[int] = None,
    curation_path: Path = ANCHORS_JSON_PATH,
) -> int:
    """Build (or rebuild) meta.sponsor_anchor_set. Returns row count.

    Auto-picks: top `top_n` distinct sponsors by study_count from
    `norm.study_sponsors`, joining through `entities.sponsor_resolved` so
    already-merged children do not occupy anchor slots.

    Curation overrides from the JSON file at `curation_path`:
      * `exclude`: canonical_names dropped from auto-picks
      * `include`: canonical_names forced into the set (must already exist
        in entities.sponsor — otherwise logged + skipped)
    """
    _ensure_anchor_table(duck_conn)
    curation = _load_curation(curation_path)
    top_n = top_n if top_n is not None else int(curation.get("top_n", DEFAULT_TOP_N))
    exclude = {str(x) for x in curation.get("exclude", [])}
    include = [str(x) for x in curation.get("include", [])]

    auto_rows = duck_conn.execute("""
        SELECT
            r.effective_sponsor_id AS sponsor_id,
            e.canonical_name,
            COUNT(DISTINCT ss.nct_id) AS study_count
        FROM norm.study_sponsors ss
        JOIN entities.sponsor_resolved r ON ss.sponsor_id = r.sponsor_id
        JOIN entities.sponsor e ON r.effective_sponsor_id = e.sponsor_id
        GROUP BY r.effective_sponsor_id, e.canonical_name
        ORDER BY study_count DESC
        LIMIT ?
    """, [top_n]).fetchall()

    final: dict[int, tuple[str, int, str]] = {}
    for sponsor_id, canonical_name, study_count in auto_rows:
        if canonical_name in exclude:
            continue
        final[sponsor_id] = (canonical_name, int(study_count), "auto")

    for name in include:
        row = duck_conn.execute(
            "SELECT sponsor_id, canonical_name FROM entities.sponsor "
            "WHERE canonical_name = ? AND merged_into_id IS NULL",
            [name],
        ).fetchone()
        if row is None:
            logger.warning(
                f"sponsor_anchors.include: '{name}' not found in "
                f"entities.sponsor (or is already merged); skipping"
            )
            continue
        sid, canon = row
        if sid in final and final[sid][2] == "auto":
            # Already picked automatically — no need to overwrite, but if
            # the curator explicitly listed it, mark origin='curated_include'
            # so the provenance is visible.
            final[sid] = (canon, final[sid][1], "curated_include")
            continue
        # Compute study_count for an included anchor that didn't make top-N.
        sc = duck_conn.execute("""
            SELECT COUNT(DISTINCT ss.nct_id)
            FROM norm.study_sponsors ss
            JOIN entities.sponsor_resolved r ON ss.sponsor_id = r.sponsor_id
            WHERE r.effective_sponsor_id = ?
        """, [sid]).fetchone()[0]
        final[sid] = (canon, int(sc), "curated_include")

    duck_conn.execute("DELETE FROM meta.sponsor_anchor_set")
    if not final:
        logger.info("sponsor anchor set: empty after curation")
        return 0

    rows = [
        (sid, name, study_count, origin)
        for sid, (name, study_count, origin) in final.items()
    ]
    duck_conn.executemany(
        "INSERT INTO meta.sponsor_anchor_set "
        "(sponsor_id, canonical_name, study_count, origin) VALUES (?, ?, ?, ?)",
        rows,
    )

    n_auto = sum(1 for r in rows if r[3] == "auto")
    n_curated = sum(1 for r in rows if r[3] == "curated_include")
    logger.info(
        f"meta.sponsor_anchor_set: {len(rows):,} rows "
        f"(auto={n_auto}, curated_include={n_curated}, top_n={top_n})"
    )
    return len(rows)


def _anchor_set_version(curation_path: Path = ANCHORS_JSON_PATH,
                        top_n: Optional[int] = None) -> str:
    """Checksum of the JSON contents + top_n, short enough to be human-readable."""
    if curation_path.exists():
        content = curation_path.read_bytes()
    else:
        content = b"{}"
    resolved_top_n = top_n if top_n is not None else DEFAULT_TOP_N
    h = hashlib.sha256(content + f"|top_n={resolved_top_n}".encode()).hexdigest()
    return h[:12]


def register_anchor_set(
    duck_conn,
    top_n: Optional[int] = None,
    curation_path: Path = ANCHORS_JSON_PATH,
) -> str:
    """Record this anchor-set build in meta.reference_sources. Returns version."""
    version = _anchor_set_version(curation_path, top_n)
    if not curation_path.exists():
        logger.warning(
            f"register_anchor_set: {curation_path} missing; skipping registration"
        )
        return version
    reference_sources.register_source(
        duck_conn,
        source_name="sponsor_anchors",
        version=version,
        path=str(curation_path),
        notes=f"top_n={top_n if top_n is not None else DEFAULT_TOP_N}; "
              f"include/exclude per curation file",
    )
    return version
