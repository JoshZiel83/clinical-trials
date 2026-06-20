"""Convenience entry point for running the extraction pipeline (Phase 1)."""

import argparse

from src.extract.aact import run_extraction
from src.logging_config import setup_logging


def main():
    parser = argparse.ArgumentParser(description="Extract AACT into raw.* + Parquet.")
    parser.add_argument(
        "--since",
        help="Only pull studies with last_update_posted_date >= SINCE "
        "(YYYY-MM-DD). A3 pre-filter; NOT a full snapshot. Default: full pull.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Re-pull even if the AACT build has not advanced (override pin-gate).",
    )
    parser.add_argument(
        "--update-schema-baseline",
        action="store_true",
        help="Regenerate config/aact_expected_columns.json from this run.",
    )
    args = parser.parse_args()

    setup_logging()
    run_extraction(
        since=args.since,
        force=args.force,
        update_schema_baseline=args.update_schema_baseline,
    )


if __name__ == "__main__":
    main()
