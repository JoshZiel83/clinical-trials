"""Phase 6E — CLI entry point for the Claude enrichment agent.

Usage:
    python run_enrichment_agent.py --domain condition --budget 2.00 --limit 50
    python run_enrichment_agent.py --domain drug --budget 5.00
    python run_enrichment_agent.py --domain sponsor --budget 1.00 --max-pending 500
"""

import argparse

from config.settings import (
    AGENT_DEFAULT_CONCURRENCY,
    AGENT_DEFAULT_MAX_PENDING,
    AGENT_DEFAULT_MODEL,
)
from src.enrichment_agent import run_enrichment_agent
from src.logging_config import setup_logging


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--domain", required=True, choices=["condition", "drug", "sponsor"],
    )
    ap.add_argument(
        "--budget", type=float, required=True,
        help="USD cap for this run; agent stops scheduling new items when "
             "exceeded. In-flight items may push total spend slightly above "
             "(≤ concurrency × per-item cost).",
    )
    ap.add_argument(
        "--limit", type=int, default=500,
        help="Max items to attempt this run (top-impact first).",
    )
    ap.add_argument(
        "--max-pending", type=int, default=AGENT_DEFAULT_MAX_PENDING,
        help="Per-domain reviewer queue cap; agent refuses to run if already at cap "
             "and stops emitting once it reaches it.",
    )
    ap.add_argument(
        "--concurrency", type=int, default=AGENT_DEFAULT_CONCURRENCY,
        help="Max in-flight Claude API calls. Higher = faster but more bursty.",
    )
    ap.add_argument(
        "--model", default=AGENT_DEFAULT_MODEL,
        help="Claude model ID (default from config).",
    )
    args = ap.parse_args()

    setup_logging()
    stats = run_enrichment_agent(
        domain=args.domain,
        budget_usd=args.budget,
        limit=args.limit,
        max_pending=args.max_pending,
        model=args.model,
        concurrency=args.concurrency,
    )
    print(
        f"\nResults: finalized={stats.items_finalized}, "
        f"abstained={stats.items_abstained}, "
        f"cache_hits={stats.items_cache_hit}, "
        f"failed={stats.items_failed}, "
        f"spent=${stats.spent_usd:.4f}"
    )


if __name__ == "__main__":
    main()
