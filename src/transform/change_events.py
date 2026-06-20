"""Change-event tracking (A3).

Diff the current dated Parquet snapshot against the prior one and record what
changed about each trial in ``meta.trial_change_events`` — the single home for
"what changed about a study" (absorbs issue #10; there is no separate change log).

Event types: ``first_seen``, ``dropped``, ``status_transition``,
``enrollment_changed``, ``phase_changed``, ``date_changed``,
``conditions_changed``, ``interventions_changed``, ``sponsors_changed``.

The diff source is the dated snapshots under ``data/raw/YYYY-MM-DD/`` (NOT the
live ``raw.*`` tables, which only hold the current build). Field-level
comparison is cheap-gated on ``last_update_posted_date`` advancing — in AACT any
content change bumps that watermark, so an unchanged watermark means no field
change to look for.
"""

import re
import uuid
from pathlib import Path

from config.settings import RAW_DATA_DIR
from src.logging_config import get_logger

logger = get_logger("change_events")

_DATE_DIR = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# Study-level scalar fields → event_type. Each emits one event per changed field.
_FIELD_EVENTS = [
    ("overall_status", "status_transition"),
    ("enrollment", "enrollment_changed"),
    ("enrollment_type", "enrollment_changed"),
    ("phase", "phase_changed"),
    ("start_date", "date_changed"),
    ("completion_date", "date_changed"),
    ("primary_completion_date", "date_changed"),
]

# Child tables compared as a per-study set of `name` values → event_type.
_CHILD_SETS = [
    ("conditions", "conditions_changed"),
    ("interventions", "interventions_changed"),
    ("sponsors", "sponsors_changed"),
]


def ensure_change_events_schema(duck_conn):
    """Create ``meta.trial_change_events`` if absent (house DDL style)."""
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.trial_change_events (
            run_id        VARCHAR,
            from_snapshot DATE,
            to_snapshot   DATE,
            nct_id        VARCHAR,
            event_type    VARCHAR,
            field         VARCHAR,
            old_value     VARCHAR,
            new_value     VARCHAR,
            detected_at   TIMESTAMP DEFAULT current_timestamp
        )
    """)


def list_snapshots(raw_dir=None):
    """Dated snapshot dir names under data/raw/, ascending (excludes staging)."""
    raw_dir = Path(raw_dir or RAW_DATA_DIR)
    if not raw_dir.exists():
        return []
    return sorted(
        d.name for d in raw_dir.iterdir() if d.is_dir() and _DATE_DIR.match(d.name)
    )


def prior_snapshot(snapshots, current):
    """The snapshot immediately before ``current`` (or None)."""
    earlier = [s for s in snapshots if s < current]
    return earlier[-1] if earlier else None


def _pq(snapshot_dir, table):
    """A DuckDB read_parquet(...) expression for one table in a snapshot dir."""
    return f"read_parquet('{snapshot_dir / (table + '.parquet')}')"


def run_change_events(
    duck_conn,
    extract_date=None,
    cohort_expansion=False,
    run_id=None,
    raw_dir=None,
):
    """Diff the current vs prior snapshot into ``meta.trial_change_events``.

    Args:
        duck_conn: DuckDB connection (the orchestrator passes its shared one).
        extract_date: Current snapshot (YYYY-MM-DD). Defaults to the newest dir.
        cohort_expansion: Suppress ``first_seen`` events — used on the one-time
            active→full-cohort run so the ~480K newly-included studies don't
            flood the log.
        run_id: Tag for these rows (the orchestrator passes its pipeline run_id).
        raw_dir: Override the snapshot root (tests).

    Returns:
        ``{"prior", "current", "events"}``.
    """
    ensure_change_events_schema(duck_conn)
    raw_dir = Path(raw_dir or RAW_DATA_DIR)
    snapshots = list_snapshots(raw_dir)

    current = extract_date or (snapshots[-1] if snapshots else None)
    if current is None:
        logger.info("no snapshots found; nothing to diff")
        return {"prior": None, "current": None, "events": 0}
    prior = prior_snapshot(snapshots, current)
    if prior is None:
        logger.info(f"no prior snapshot before {current}; skipping change events")
        return {"prior": None, "current": current, "events": 0}

    run_id = run_id or str(uuid.uuid4())
    cur_dir, pri_dir = raw_dir / current, raw_dir / prior
    logger.info(f"change events: diffing {prior} -> {current}"
                + (" (cohort-expansion: suppressing first_seen)" if cohort_expansion else ""))

    # Idempotent: replace any prior events recorded for this target snapshot.
    duck_conn.execute(
        "DELETE FROM meta.trial_change_events WHERE to_snapshot = CAST(? AS DATE)",
        [current],
    )

    cur_s, pri_s = _pq(cur_dir, "studies"), _pq(pri_dir, "studies")

    def insert(select_sql):
        duck_conn.execute(f"""
            INSERT INTO meta.trial_change_events
                (run_id, from_snapshot, to_snapshot, nct_id,
                 event_type, field, old_value, new_value)
            SELECT '{run_id}', DATE '{prior}', DATE '{current}',
                   nct_id, event_type, field, old_value, new_value
            FROM ({select_sql})
        """)

    # 1. Membership: first_seen (suppressed on cohort expansion) / dropped.
    if not cohort_expansion:
        insert(f"""
            SELECT c.nct_id, 'first_seen' AS event_type, NULL AS field,
                   NULL AS old_value, NULL AS new_value
            FROM {cur_s} c LEFT JOIN {pri_s} p USING (nct_id)
            WHERE p.nct_id IS NULL
        """)
    insert(f"""
        SELECT p.nct_id, 'dropped' AS event_type, NULL AS field,
               NULL AS old_value, NULL AS new_value
        FROM {pri_s} p LEFT JOIN {cur_s} c USING (nct_id)
        WHERE c.nct_id IS NULL
    """)

    # 2. Field-level changes for studies present in both, cheap-gated on the
    #    last_update_posted_date watermark advancing.
    field_branches = " UNION ALL ".join(
        f"""SELECT c.nct_id, '{event_type}' AS event_type, '{col}' AS field,
                   CAST(p.{col} AS VARCHAR) AS old_value,
                   CAST(c.{col} AS VARCHAR) AS new_value
            FROM {cur_s} c JOIN {pri_s} p USING (nct_id)
            WHERE c.last_update_posted_date IS DISTINCT FROM p.last_update_posted_date
              AND c.{col} IS DISTINCT FROM p.{col}"""
        for col, event_type in _FIELD_EVENTS
    )
    insert(field_branches)

    # 3. Child set-diffs keyed on (nct_id, name), restricted to studies in both.
    for table, event_type in _CHILD_SETS:
        cur_t, pri_t = _pq(cur_dir, table), _pq(pri_dir, table)
        insert(f"""
            WITH cur AS (SELECT DISTINCT nct_id, name FROM {cur_t}),
                 pri AS (SELECT DISTINCT nct_id, name FROM {pri_t}),
                 in_both AS (SELECT nct_id FROM {cur_s}
                             INTERSECT SELECT nct_id FROM {pri_s}),
                 added AS (
                     SELECT nct_id, string_agg(name, '; ' ORDER BY name) AS names
                     FROM (SELECT c.nct_id, c.name FROM cur c
                           LEFT JOIN pri p USING (nct_id, name)
                           WHERE p.nct_id IS NULL)
                     GROUP BY nct_id),
                 removed AS (
                     SELECT nct_id, string_agg(name, '; ' ORDER BY name) AS names
                     FROM (SELECT p.nct_id, p.name FROM pri p
                           LEFT JOIN cur c USING (nct_id, name)
                           WHERE c.nct_id IS NULL)
                     GROUP BY nct_id)
            SELECT b.nct_id, '{event_type}' AS event_type, NULL AS field,
                   r.names AS old_value, a.names AS new_value
            FROM in_both b
            LEFT JOIN added a USING (nct_id)
            LEFT JOIN removed r USING (nct_id)
            WHERE a.names IS NOT NULL OR r.names IS NOT NULL
        """)

    events = duck_conn.execute(
        "SELECT count(*) FROM meta.trial_change_events WHERE run_id = ?", [run_id]
    ).fetchone()[0]
    logger.info(f"change events: recorded {events:,} events ({prior} -> {current})")
    return {"prior": prior, "current": current, "events": events}
