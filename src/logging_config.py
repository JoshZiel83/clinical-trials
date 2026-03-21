"""Centralized logging configuration for the clinical trials pipeline."""

import logging
import sys
from logging.handlers import TimedRotatingFileHandler
from pathlib import Path

_configured = False

LOG_DIR = Path(__file__).parent.parent / "data" / "logs"


def setup_logging(level=logging.INFO):
    """Configure the root logger with console and daily-rotating file output.

    Safe to call multiple times — only configures on the first call.
    Returns the 'clinical_trials' logger for pipeline modules to use.
    """
    global _configured

    logger = logging.getLogger("clinical_trials")

    if not _configured:
        logger.setLevel(level)

        formatter = logging.Formatter(
            "%(asctime)s | %(name)s | %(levelname)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        # Console handler
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)
        logger.addHandler(console_handler)

        # Daily rotating file handler
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = TimedRotatingFileHandler(
            LOG_DIR / "pipeline.log",
            when="midnight",
            backupCount=30,
            encoding="utf-8",
        )
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

        _configured = True

    return logger


def get_logger(module_name):
    """Get a child logger for a specific module.

    Usage: logger = get_logger(__name__)
    """
    setup_logging()
    return logging.getLogger(f"clinical_trials.{module_name}")
