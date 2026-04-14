"""Entry point for Phase 6B: sponsor normalization + fuzzy candidates."""

from src.logging_config import setup_logging
from src.normalize_sponsors import run_sponsor_pipeline

if __name__ == "__main__":
    setup_logging()
    run_sponsor_pipeline()
