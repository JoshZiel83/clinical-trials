"""Entry point for the full-refresh pipeline orchestrator (A3).

Runs extract → hitl_sync → promote → normalize → classify → views →
change_events through one DuckDB connection. The enrichment agent is opt-in
(`--enrich` with per-domain budgets) since it's the only paid step.

Examples:
    python run_pipeline.py                      # refresh if AACT advanced
    python run_pipeline.py --force              # refresh regardless
    python run_pipeline.py --force --cohort-expansion   # one-time filter-removal run
    python run_pipeline.py --enrich --budget-condition 5 --budget-drug 5
"""

import argparse

from src.logging_config import setup_logging
from src.pipeline.orchestrator import run_pipeline


def main():
    parser = argparse.ArgumentParser(description="Run the full refresh pipeline.")
    parser.add_argument("--force", action="store_true",
                        help="Re-run even if the AACT build has not advanced.")
    parser.add_argument("--since",
                        help="Extract pre-filter: last_update_posted_date >= SINCE "
                        "(YYYY-MM-DD). Subset, not a snapshot — rarely used.")
    parser.add_argument("--cohort-expansion", action="store_true",
                        help="Suppress change_events first_seen (one-time "
                        "active->full-cohort run).")
    parser.add_argument("--enrich", action="store_true",
                        help="Run the Claude enrichment agent (costs API $).")
    parser.add_argument("--budget-condition", type=float, default=0)
    parser.add_argument("--budget-drug", type=float, default=0)
    parser.add_argument("--budget-sponsor", type=float, default=0)
    args = parser.parse_args()

    setup_logging()
    run_pipeline(
        force=args.force,
        since=args.since,
        cohort_expansion=args.cohort_expansion,
        enrich=args.enrich,
        enrich_budgets={
            "condition": args.budget_condition,
            "drug": args.budget_drug,
            "sponsor": args.budget_sponsor,
        },
    )


if __name__ == "__main__":
    main()
