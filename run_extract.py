"""Convenience entry point for running the extraction pipeline."""

from src.logging_config import setup_logging
from src.extract.aact import run_extraction

if __name__ == "__main__":
    setup_logging()
    run_extraction()
