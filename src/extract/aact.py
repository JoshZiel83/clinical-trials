"""Extract clinical trial data from AACT into Parquet + DuckDB.

The extract mirrors the active/planned AACT cohort into ``raw.*`` and a dated
Parquet snapshot. It is hardened along four axes (ROADMAP Epic A1/A2):

- **Postgres scanner** — rows are pulled with DuckDB's ``postgres`` extension
  (``ATTACH ... (TYPE postgres, READ_ONLY)``) and filtered scanner-side, instead
  of round-tripping through pandas.
- **Atomic stage-then-swap** — every table is built into a ``*__staging`` table
  and a hidden staging Parquet dir; only once all 14 succeed are they renamed
  into place in a single transaction. A mid-run failure leaves ``raw.*`` and the
  prior snapshot untouched.
- **Schema-drift detection** — incoming columns are compared to a pinned
  baseline (``config/aact_expected_columns.json``); a dropped column fails the
  run, a new column warns.
- **Provenance** — the AACT build is pinned in ``meta.reference_sources`` as
  ``aact@<build-date>`` (from ``max(studies.updated_at)``), and a run is
  short-circuited when that build has not advanced.

Two A3-readiness hooks are plumbed but inert by default: a ``since`` filter
(``last_update_posted_date >= since``) and the pin-gate ``force`` override.
"""

import datetime
import json
import os
import shutil
import time
import uuid

from config.settings import (
    AACT_SCHEMA,
    PROJECT_ROOT,
    RAW_DATA_DIR,
    get_aact_attach_params,
    get_duckdb_connection,
)
from config.tables import ANCHOR_TABLE, EXTRACT_TABLES, STATUS_WHERE_CLAUSE
from src import reference_sources
from src.logging_config import get_logger

logger = get_logger("extract")

# DuckDB catalog alias for the attached AACT Postgres database.
AACT_CATALOG = "aact_src"

# Pinned per-table column baseline used for schema-drift detection (#11).
EXPECTED_COLUMNS_PATH = PROJECT_ROOT / "config" / "aact_expected_columns.json"


def get_extract_query(table_name, since=None, catalog=AACT_CATALOG):
    """Build the SQL to mirror one AACT table through the Postgres scanner.

    The studies (anchor) table is filtered directly; child tables are filtered
    via an INNER JOIN to studies. ``since`` adds an optional
    ``last_update_posted_date >= since`` predicate.

    NOTE: a ``since``-filtered pull returns only studies whose content changed —
    it is a *subset*, not a complete snapshot, and cannot detect dropped studies
    (that needs full set membership). It is an A3 pre-filter for change
    detection; it must never be used as the canonical snapshot. Default
    (``since=None``) is a full pull.
    """
    schema = AACT_SCHEMA
    since_pred = f" AND last_update_posted_date >= DATE '{since}'" if since else ""

    if table_name == ANCHOR_TABLE:
        return (
            f"SELECT * FROM {catalog}.{schema}.{table_name} "
            f"WHERE {STATUS_WHERE_CLAUSE}{since_pred}"
        )

    # Child tables qualify the predicate columns through the studies alias `s`.
    child_since = since_pred.replace(
        "last_update_posted_date", "s.last_update_posted_date"
    )
    return (
        f"SELECT t.* FROM {catalog}.{schema}.{table_name} t "
        f"INNER JOIN {catalog}.{schema}.studies s ON t.nct_id = s.nct_id "
        f"WHERE s.{STATUS_WHERE_CLAUSE}{child_since}"
    )


# --------------------------------------------------------------------------- #
# Bootstrap (#3 — single shared schema/DDL helper, replacing connection_test)
# --------------------------------------------------------------------------- #

def ensure_extract_schema(duck_conn):
    """Create the schemas and the extraction log the extract path depends on."""
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.extraction_log (
            extraction_id VARCHAR,
            extract_date DATE,
            table_name VARCHAR,
            row_count INTEGER,
            duration_seconds DOUBLE,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            parquet_path VARCHAR
        )
    """)


def attach_aact(duck_conn, catalog=AACT_CATALOG):
    """Load the postgres extension and ATTACH the AACT database read-only.

    Credentials flow through PG* env vars (see ``get_aact_attach_params``) so
    they never land in SQL text or logs.
    """
    dsn, pg_env = get_aact_attach_params()
    os.environ.update(pg_env)
    duck_conn.execute("INSTALL postgres")
    duck_conn.execute("LOAD postgres")
    duck_conn.execute(
        f"ATTACH '{dsn}' AS {catalog} (TYPE postgres, READ_ONLY)"
    )


# --------------------------------------------------------------------------- #
# Pin-gate (A3-readiness — short-circuit unchanged AACT builds)
# --------------------------------------------------------------------------- #

def current_build_timestamp(duck_conn, catalog=AACT_CATALOG):
    """The AACT build watermark: max(studies.updated_at) on the attached DB."""
    return duck_conn.execute(
        f"SELECT max(updated_at) FROM {catalog}.{AACT_SCHEMA}.studies"
    ).fetchone()[0]


def last_pinned_build(duck_conn):
    """The ``acquired_at`` of the active ``aact`` reference source, or None."""
    if not reference_sources._table_exists(duck_conn):
        return None
    row = duck_conn.execute(
        """
        SELECT acquired_at FROM meta.reference_sources
        WHERE source_name = 'aact' AND is_active = TRUE
        """
    ).fetchone()
    return row[0] if row else None


# --------------------------------------------------------------------------- #
# Schema-drift detection (#11)
# --------------------------------------------------------------------------- #

def load_expected_columns():
    """Load the pinned per-table column baseline, or None if not yet seeded."""
    if not EXPECTED_COLUMNS_PATH.exists():
        return None
    with open(EXPECTED_COLUMNS_PATH) as f:
        return json.load(f)


def write_expected_columns(columns_by_table):
    """Persist the per-table column baseline (sorted for stable diffs)."""
    payload = {t: sorted(cols) for t, cols in sorted(columns_by_table.items())}
    with open(EXPECTED_COLUMNS_PATH, "w") as f:
        json.dump(payload, f, indent=2)
        f.write("\n")


def diff_columns(expected, actual):
    """Pure set diff -> (missing, added) sorted lists. Order-insensitive."""
    expected_set, actual_set = set(expected), set(actual)
    missing = sorted(expected_set - actual_set)
    added = sorted(actual_set - expected_set)
    return missing, added


def check_schema_drift(table_name, expected_cols, actual_cols):
    """Compare a table's incoming columns to the baseline.

    Dropped (missing) columns raise RuntimeError — the downstream `SELECT *`
    mirror would silently lose data. New columns only warn; the mirror absorbs
    them automatically.
    """
    missing, added = diff_columns(expected_cols, actual_cols)
    if added:
        logger.warning(
            f"  schema drift on {table_name}: new column(s) {added} "
            f"(absorbed by SELECT * mirror)"
        )
    if missing:
        raise RuntimeError(
            f"schema drift on {table_name}: expected column(s) {missing} are "
            f"absent upstream. Investigate AACT changes, then regenerate the "
            f"baseline with --update-schema-baseline if intended."
        )


# --------------------------------------------------------------------------- #
# Extraction log (#5 — write one row per table as it completes)
# --------------------------------------------------------------------------- #

def write_extraction_log_row(duck_conn, extraction_id, extract_date, meta):
    """Insert one ``meta.extraction_log`` row for a completed table.

    Written (and committed) as each table finishes staging, so a partial failure
    still leaves a forensic trail of how far the run got.
    """
    duck_conn.execute(
        """
        INSERT INTO meta.extraction_log
        (extraction_id, extract_date, table_name, row_count,
         duration_seconds, started_at, completed_at, parquet_path)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            extraction_id,
            extract_date,
            meta["table_name"],
            meta["row_count"],
            meta["duration_seconds"],
            meta["started_at"],
            meta["completed_at"],
            meta["parquet_path"],
        ],
    )


# --------------------------------------------------------------------------- #
# Per-table staging
# --------------------------------------------------------------------------- #

def stage_table(table_name, duck_conn, staging_dir, final_dir, since=None):
    """Build ``raw.<t>__staging`` from the scanner and COPY it to Parquet.

    Returns (metadata_dict, actual_columns). Does NOT touch the live ``raw.<t>``.
    ``final_dir`` is where the snapshot lands post-swap (recorded in the log).
    """
    logger.info(f"Staging {table_name}...")
    started_at = datetime.datetime.now()
    start_time = time.time()

    staging_table = f"raw.{table_name}__staging"
    query = get_extract_query(table_name, since=since)
    duck_conn.execute(f"CREATE OR REPLACE TABLE {staging_table} AS {query}")

    row_count = duck_conn.execute(
        f"SELECT count(*) FROM {staging_table}"
    ).fetchone()[0]
    actual_columns = [
        r[0] for r in duck_conn.execute(f"DESCRIBE {staging_table}").fetchall()
    ]

    parquet_path = staging_dir / f"{table_name}.parquet"
    duck_conn.execute(
        f"COPY {staging_table} TO '{parquet_path}' (FORMAT parquet)"
    )

    duration = time.time() - start_time
    completed_at = datetime.datetime.now()
    logger.info(
        f"  {table_name}: {row_count:,} rows in {duration:.1f}s -> {parquet_path}"
    )

    meta = {
        "table_name": table_name,
        "row_count": row_count,
        "duration_seconds": round(duration, 2),
        "started_at": started_at,
        "completed_at": completed_at,
        # Recorded as the final (post-swap) path, where the snapshot lands.
        "parquet_path": str(final_dir / f"{table_name}.parquet"),
    }
    return meta, actual_columns


def swap_into_place(duck_conn, table_names):
    """Atomically replace ``raw.<t>`` with the staged tables in one transaction.

    Either every table is swapped or none are (transactional DDL); on failure
    the live ``raw.*`` is untouched.
    """
    duck_conn.execute("BEGIN")
    try:
        for t in table_names:
            duck_conn.execute(f"DROP TABLE IF EXISTS raw.{t}")
            duck_conn.execute(f"ALTER TABLE raw.{t}__staging RENAME TO {t}")
        duck_conn.execute("COMMIT")
    except Exception:
        duck_conn.execute("ROLLBACK")
        raise


def drop_staging_tables(duck_conn, table_names):
    """Best-effort cleanup of leftover ``*__staging`` tables after a failure."""
    for t in table_names:
        try:
            duck_conn.execute(f"DROP TABLE IF EXISTS raw.{t}__staging")
        except Exception:  # pragma: no cover - cleanup must not mask the cause
            logger.warning(f"  could not drop staging table for {t}")


def register_aact_build(duck_conn, raw_dir, table_count):
    """Pin the just-loaded AACT build in ``meta.reference_sources`` (A2/#8)."""
    build_ts = duck_conn.execute(
        "SELECT max(updated_at) FROM raw.studies"
    ).fetchone()[0]
    content_wm = duck_conn.execute(
        "SELECT max(last_update_posted_date) FROM raw.studies"
    ).fetchone()[0]
    n_studies = duck_conn.execute("SELECT count(*) FROM raw.studies").fetchone()[0]
    version = build_ts.date().isoformat()
    notes = (
        f"AACT nightly build; content watermark "
        f"last_update_posted_date<={content_wm}; "
        f"{n_studies:,} studies across {table_count} tables"
    )
    reference_sources.register_source(
        duck_conn,
        "aact",
        version,
        str(raw_dir),
        acquired_at=build_ts,
        notes=notes,
    )
    return version


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #

def run_extraction(
    tables=None,
    extract_date=None,
    since=None,
    force=False,
    update_schema_baseline=False,
):
    """Run the full extraction.

    Args:
        tables: Tables to extract. Defaults to all ``EXTRACT_TABLES``.
        extract_date: YYYY-MM-DD stamp for the output dir. Defaults to today.
        since: Optional ``last_update_posted_date`` floor (A3 hook; see
            ``get_extract_query``). None = full pull (default).
        force: Re-pull even when the AACT build has not advanced (pin-gate).
        update_schema_baseline: Regenerate ``config/aact_expected_columns.json``
            from this run instead of enforcing it.

    Returns:
        A result dict ``{"status": "completed"|"skipped", ...}``.
    """
    tables = tables or EXTRACT_TABLES
    extract_date = extract_date or datetime.date.today().isoformat()
    extraction_id = str(uuid.uuid4())

    logger.info(f"Starting extraction run {extraction_id}")
    logger.info(f"Extract date: {extract_date} | tables: {len(tables)}"
                f"{' | since=' + since if since else ''}")

    # Ensure studies (anchor) is staged first; child filters depend on it.
    ordered_tables = []
    if ANCHOR_TABLE in tables:
        ordered_tables.append(ANCHOR_TABLE)
        ordered_tables.extend(t for t in tables if t != ANCHOR_TABLE)
    else:
        ordered_tables = list(tables)

    staging_dir = RAW_DATA_DIR / f".{extract_date}.staging"
    final_dir = RAW_DATA_DIR / extract_date

    duck_conn = get_duckdb_connection()
    try:
        ensure_extract_schema(duck_conn)
        attach_aact(duck_conn)

        # Pin-gate: skip when the AACT build has not advanced.
        build_ts = current_build_timestamp(duck_conn)
        last_pin = last_pinned_build(duck_conn)
        if last_pin is not None and build_ts == last_pin and not force:
            logger.info(
                f"AACT build unchanged (pinned {build_ts.date().isoformat()}); "
                f"skipping. Use --force to re-pull."
            )
            return {"status": "skipped", "build": build_ts, "extraction_id": extraction_id}

        # Fresh staging dir.
        if staging_dir.exists():
            shutil.rmtree(staging_dir)
        staging_dir.mkdir(parents=True, exist_ok=True)

        expected = None if update_schema_baseline else load_expected_columns()
        columns_by_table = {}

        total_start = time.time()
        try:
            for table_name in ordered_tables:
                meta, actual_cols = stage_table(
                    table_name, duck_conn, staging_dir, final_dir, since=since
                )
                columns_by_table[table_name] = actual_cols
                if expected is not None and table_name in expected:
                    check_schema_drift(table_name, expected[table_name], actual_cols)
                # Per-table forensic log row (committed before the swap).
                write_extraction_log_row(duck_conn, extraction_id, extract_date, meta)

            # Atomic swap: rename staged tables into place, then the Parquet dir.
            swap_into_place(duck_conn, ordered_tables)
            if final_dir.exists():
                shutil.rmtree(final_dir)
            os.replace(staging_dir, final_dir)
        except Exception:
            logger.error("Extraction failed before swap; raw.* left untouched.")
            drop_staging_tables(duck_conn, ordered_tables)
            if staging_dir.exists():
                shutil.rmtree(staging_dir, ignore_errors=True)
            raise

        # Seed / refresh the schema baseline.
        if expected is None:
            write_expected_columns(columns_by_table)
            logger.info(
                f"schema baseline {'updated' if update_schema_baseline else 'established'} "
                f"-> {EXPECTED_COLUMNS_PATH.name}"
            )

        version = register_aact_build(duck_conn, final_dir, len(ordered_tables))

        total_duration = time.time() - total_start
        total_rows = duck_conn.execute(
            "SELECT count(*) FROM raw.studies"
        ).fetchone()[0]
        logger.info(
            f"Extraction complete: pinned aact@{version}, "
            f"{len(ordered_tables)} tables in {total_duration:.1f}s "
            f"({total_rows:,} studies)"
        )
        return {
            "status": "completed",
            "extraction_id": extraction_id,
            "extract_date": extract_date,
            "version": version,
            "tables": ordered_tables,
        }
    finally:
        duck_conn.close()


if __name__ == "__main__":
    from run_extract import main

    main()
