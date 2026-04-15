"""Phase 6F — apply Shiny decision log(s) and rebuild downstream tables.

Scans `data/reviews/decisions_*.parquet` for logs not yet applied, imports
each via `src.hitl.import_decision_log` (which promotes approvals into the
target dictionary and flips candidate statuses), then rebuilds whichever
`norm.*` tables were affected and refreshes `views.study_summary`.

Tracks which decision logs have been applied in `meta.decision_log_applied`
so this is idempotent — re-running is a no-op.

Usage:
    python run_hitl_sync.py                # apply all unapplied logs
    python run_hitl_sync.py path/to/log.parquet ...  # apply specific logs
"""

import sys
from pathlib import Path

import pandas as pd

from config.settings import PROJECT_ROOT, get_duckdb_connection
from src import hitl
from src.logging_config import get_logger, setup_logging

logger = get_logger("run_hitl_sync")

REVIEWS_DIR = PROJECT_ROOT / "data" / "reviews"


def _ensure_applied_table(conn):
    conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.decision_log_applied (
            path        VARCHAR PRIMARY KEY,
            applied_at  TIMESTAMP DEFAULT current_timestamp,
            approved    INTEGER,
            rejected    INTEGER,
            promoted    INTEGER
        )
    """)


def _unapplied_logs(conn):
    if not REVIEWS_DIR.exists():
        return []
    all_logs = sorted(REVIEWS_DIR.glob("decisions_*.parquet"))
    if not all_logs:
        return []
    applied = {
        r[0]
        for r in conn.execute(
            "SELECT path FROM meta.decision_log_applied"
        ).fetchall()
    }
    return [p for p in all_logs if str(p) not in applied]


def _domains_touched(path):
    df = pd.read_parquet(path)
    return set(df["domain"].unique())


def _rebuild_downstream(conn, domains_touched):
    """Re-derive the `norm.*` tables whose `ref.*` dictionaries just changed,
    then rebuild `views.study_summary` so the analytical view reflects the
    new manual entries."""
    if "condition" in domains_touched:
        from src.transform.normalize_conditions import create_study_conditions
        logger.info("[sync] rebuilding norm.study_conditions")
        create_study_conditions(conn)
    if "drug" in domains_touched:
        from src.transform.normalize_drugs import create_study_drugs
        logger.info("[sync] rebuilding norm.study_drugs")
        create_study_drugs(conn)
    if "sponsor" in domains_touched:
        from src.transform.normalize_sponsors import create_study_sponsors
        logger.info("[sync] rebuilding norm.study_sponsors")
        create_study_sponsors(conn)
    # Views always get refreshed since any dict change may ripple through.
    try:
        from src.transform.views import build_study_summary
        logger.info("[sync] rebuilding views.study_summary")
        build_study_summary(conn)
    except Exception as exc:
        logger.warning(f"[sync] views rebuild skipped: {exc}")


def apply_logs(conn, log_paths):
    _ensure_applied_table(conn)
    hitl.ensure_candidates_table(conn)
    total = {"approved": 0, "rejected": 0, "promoted": 0}
    domains_touched = set()

    for path in log_paths:
        logger.info(f"[sync] applying {path.name}")
        result = hitl.import_decision_log(conn, str(path))
        conn.execute(
            """INSERT INTO meta.decision_log_applied
               (path, approved, rejected, promoted) VALUES (?, ?, ?, ?)
               ON CONFLICT DO NOTHING""",
            [str(path), result["approved"], result["rejected"], result["promoted"]],
        )
        for k in total:
            total[k] += result[k]
        domains_touched |= _domains_touched(path)

    if domains_touched:
        _rebuild_downstream(conn, domains_touched)
    return total, domains_touched


def main(argv):
    setup_logging()
    conn = get_duckdb_connection()
    try:
        if argv:
            log_paths = [Path(a) for a in argv]
            missing = [p for p in log_paths if not p.exists()]
            if missing:
                raise SystemExit(f"not found: {missing}")
        else:
            _ensure_applied_table(conn)
            log_paths = _unapplied_logs(conn)

        if not log_paths:
            logger.info("[sync] no unapplied decision logs")
            return

        total, domains = apply_logs(conn, log_paths)
        logger.info(
            f"[sync] done. logs={len(log_paths)} domains={sorted(domains)} "
            f"approved={total['approved']} rejected={total['rejected']} "
            f"promoted={total['promoted']}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main(sys.argv[1:])
