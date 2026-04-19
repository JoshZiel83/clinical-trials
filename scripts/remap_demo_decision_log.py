"""Remap demo-DB decision logs onto real-DB sponsor_ids (Phase 7D bridge).

The Phase 7D UI verification session used `data/demo_7d.duckdb` with its own
`entities.sponsor` sequence. Approvals written to
`data/reviews/decisions_*_sponsor.parquet` carry `anchor_sponsor_id` values
from the demo sequence — they do NOT correspond to the same sponsors in the
real `data/clinical_trials.duckdb`.

This script:
  1. Reads each demo decision log (identified by a stamp argument or by a
     marker file alongside the parquet).
  2. Resolves every approved sponsor row's `canonical_term` against the real
     DB's `entities.sponsor.canonical_name` to get the real `sponsor_id`.
  3. Writes a new decision-log parquet in `data/reviews/` with the
     remapped IDs (fresh timestamp so `meta.decision_log_applied` treats it
     as a new entry).
  4. Moves the original demo logs to `data/reviews/.demo_archive/` so
     subsequent `run_hitl_sync.py` invocations don't accidentally apply
     them against the real DB.

One-shot bridge — not a general-purpose tool. Once Phase 7D is live,
reviewers run Shiny directly against the real DB and this indirection
goes away.
"""

from __future__ import annotations

import shutil
import sys
from datetime import datetime
from pathlib import Path

import duckdb
import pandas as pd

from config.settings import PROJECT_ROOT
from src.logging_config import get_logger, setup_logging

logger = get_logger("remap_demo_decision_log")


REVIEWS_DIR = PROJECT_ROOT / "data" / "reviews"
ARCHIVE_DIR = REVIEWS_DIR / ".demo_archive"
REAL_DB_PATH = PROJECT_ROOT / "data" / "clinical_trials.duckdb"


def _resolve_real_ids(real_conn, canonical_terms: list[str]) -> dict[str, int]:
    """canonical_name → sponsor_id from the real DB. Missing names are omitted."""
    rows = real_conn.execute(
        """
        SELECT canonical_name, sponsor_id
        FROM entities.sponsor
        WHERE canonical_name IN (SELECT unnest(?))
        """,
        [canonical_terms],
    ).fetchall()
    return {name: sid for name, sid in rows}


def remap_log(log_path: Path, real_conn, archive_dir: Path,
              reviews_dir: Path) -> Path | None:
    """Remap a single demo sponsor log. Returns the new log path, or None
    if no rows need remapping / nothing was written.
    """
    df = pd.read_parquet(log_path)
    if "domain" not in df.columns or (df["domain"] != "sponsor").all() is False:
        # Mixed- or non-sponsor log — skip remapping (no anchor_sponsor_id issue).
        if "sponsor" not in set(df["domain"].unique()):
            logger.info(f"{log_path.name}: not a sponsor log; skipping remap")
            return None

    sponsor_rows = df[df["domain"] == "sponsor"]
    other_rows = df[df["domain"] != "sponsor"]

    if "anchor_sponsor_id" not in sponsor_rows.columns:
        logger.info(f"{log_path.name}: no anchor_sponsor_id column; skipping remap")
        return None

    # Resolve the real-DB sponsor_ids for every unique canonical_term referenced
    # as a merge target.
    merge_targets = sponsor_rows.loc[
        sponsor_rows["anchor_sponsor_id"].notna(), "canonical_term"
    ].unique().tolist()
    name_to_real_id = _resolve_real_ids(real_conn, merge_targets)

    missing = [n for n in merge_targets if n not in name_to_real_id]
    if missing:
        logger.error(
            f"{log_path.name}: {len(missing)} merge target(s) not found in "
            f"real DB entities.sponsor: {missing}. Run "
            f"scripts/seed_curated_anchors.py first if any curated parents "
            f"haven't been seeded."
        )
        return None

    # Remap in a copy so the original parquet is untouched until we archive it.
    remapped = sponsor_rows.copy()
    remapped["anchor_sponsor_id"] = remapped["canonical_term"].map(
        lambda ct: name_to_real_id[ct] if ct in name_to_real_id else None
    ).astype("Int64")

    merged_df = pd.concat([remapped, other_rows], ignore_index=True) \
        if not other_rows.empty else remapped

    stamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    out_name = f"decisions_{stamp}_remapped-from-{log_path.stem}.parquet"
    out_path = reviews_dir / out_name
    merged_df.to_parquet(out_path, index=False)
    logger.info(
        f"wrote remapped log: {out_path.name} "
        f"({len(sponsor_rows)} sponsor rows, "
        f"{len(merge_targets)} unique merge targets)"
    )

    archive_dir.mkdir(exist_ok=True)
    archive_path = archive_dir / log_path.name
    shutil.move(str(log_path), str(archive_path))
    logger.info(f"archived original: {archive_path}")
    return out_path


def find_demo_sponsor_logs(reviews_dir: Path, stamps: list[str] | None = None) -> list[Path]:
    """Return demo sponsor logs matching the given stamp prefix(es), or
    the default today-set if no stamps provided. Only sponsor logs — the
    non-sponsor logs from the same session are harmless without remapping.
    """
    if stamps:
        return [
            p for p in reviews_dir.glob("decisions_*.parquet")
            if any(stamp in p.name for stamp in stamps)
        ]
    # Default: today's (2026-04-18) session, sponsor logs only.
    return sorted(reviews_dir.glob("decisions_2026-04-18_*_sponsor.parquet"))


def main(argv):
    setup_logging()
    if argv:
        stamps = argv
    else:
        stamps = None

    if not REAL_DB_PATH.exists():
        raise SystemExit(f"real DB not found: {REAL_DB_PATH}")

    conn = duckdb.connect(str(REAL_DB_PATH), read_only=True)
    try:
        logs = find_demo_sponsor_logs(REVIEWS_DIR, stamps)
        if not logs:
            logger.info("no demo sponsor logs found to remap")
            return
        logger.info(f"found {len(logs)} demo sponsor log(s) to remap")
        for log in logs:
            remap_log(log, conn, ARCHIVE_DIR, REVIEWS_DIR)
    finally:
        conn.close()


if __name__ == "__main__":
    main(sys.argv[1:])
