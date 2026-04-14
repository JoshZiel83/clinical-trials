import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DUCKDB_PATH = PROJECT_ROOT / "data" / "clinical_trials.duckdb"
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"
REFERENCE_DATA_DIR = PROJECT_ROOT / "data" / "reference"

# AACT connection parameters
AACT_HOST = "aact-db.ctti-clinicaltrials.org"
AACT_PORT = 5432
AACT_DB = "aact"
AACT_SCHEMA = "ctgov"
AACT_USER = os.environ.get("AACT_USER", "")
AACT_PASSWORD = os.environ.get("AACT_PASSWORD", "")

# Anthropic / enrichment agent
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
AGENT_DEFAULT_MODEL = os.environ.get("AGENT_DEFAULT_MODEL", "claude-opus-4-6")
AGENT_MAX_TOKENS = int(os.environ.get("AGENT_MAX_TOKENS", "4096"))
AGENT_DEFAULT_MAX_PENDING = int(os.environ.get("AGENT_DEFAULT_MAX_PENDING", "500"))
AGENT_SYSTEM_PROMPT_VERSION = "v1"  # bump to invalidate cache on prompt changes

# Status filter for active/planned trials
ACTIVE_STATUSES = (
    "RECRUITING",
    "NOT_YET_RECRUITING",
    "ACTIVE_NOT_RECRUITING",
    "ENROLLING_BY_INVITATION",
    "AVAILABLE",
)


def get_aact_connection():
    """Open a psycopg2 connection to the AACT PostgreSQL database."""
    import psycopg2

    if not AACT_USER or not AACT_PASSWORD:
        raise ValueError(
            "AACT_USER and AACT_PASSWORD must be set in environment or .env file"
        )

    return psycopg2.connect(
        host=AACT_HOST,
        port=AACT_PORT,
        dbname=AACT_DB,
        user=AACT_USER,
        password=AACT_PASSWORD,
        options=f"-c search_path={AACT_SCHEMA}",
        connect_timeout=30,
    )


def get_duckdb_connection(path=None, read_only=False):
    """Open a DuckDB connection. Defaults to the project database path."""
    import duckdb

    db_path = str(path or DUCKDB_PATH)
    return duckdb.connect(db_path, read_only=read_only)
