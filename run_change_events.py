"""Entry point for change-event tracking (A3).

Diffs the current vs prior dated Parquet snapshot into
``meta.trial_change_events``. Normally run as a phase of ``run_pipeline.py``;
this standalone entry is for re-running the diff against existing snapshots.
"""

import argparse

from config.settings import get_duckdb_connection
from src.logging_config import setup_logging
from src.transform.change_events import run_change_events


def main():
    parser = argparse.ArgumentParser(description="Diff snapshots into change events.")
    parser.add_argument(
        "--extract-date",
        help="Current snapshot (YYYY-MM-DD). Defaults to the newest snapshot dir.",
    )
    parser.add_argument(
        "--cohort-expansion",
        action="store_true",
        help="Suppress first_seen events (one-time active->full-cohort run).",
    )
    args = parser.parse_args()

    setup_logging()
    conn = get_duckdb_connection()
    try:
        run_change_events(
            conn,
            extract_date=args.extract_date,
            cohort_expansion=args.cohort_expansion,
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
