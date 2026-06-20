"""Full-refresh orchestrator (A3).

Runs a longitudinal refresh end-to-end through **one** DuckDB write-connection
(DuckDB is single-writer), in dependency order:

    extract → hitl_sync → promote → normalize(conditions+TA / drugs / sponsors)
    → classify(design / innovative / ai) → views → change_events → [enrich]

Each phase is an existing, idempotent entry function called with the shared
connection — the orchestrator owns ordering and the `meta.pipeline_runs` audit
row, not the transforms themselves. The Claude enrichment agent is the only
paid step and is **off by default** (opt in with `enrich=True`).

The extract pin-gate gates the whole refresh: if the AACT build has not advanced
(and not forced), nothing downstream runs.
"""

import datetime
import uuid

from config.settings import get_duckdb_connection
from src.logging_config import get_logger

logger = get_logger("pipeline")

ENRICH_DOMAINS = ("condition", "drug", "sponsor")


def ensure_pipeline_runs_schema(duck_conn):
    """Create ``meta.pipeline_runs`` if absent (house DDL style)."""
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.pipeline_runs (
            run_id           VARCHAR,
            started_at       TIMESTAMP,
            completed_at     TIMESTAMP,
            status           VARCHAR,   -- running | completed | failed | skipped
            extract_status   VARCHAR,   -- completed | skipped
            extract_version  VARCHAR,
            force            BOOLEAN,
            cohort_expansion BOOLEAN,
            since            VARCHAR,
            phases_completed VARCHAR,
            error            VARCHAR
        )
    """)


def _start_run(duck_conn, run_id, force, cohort_expansion, since):
    duck_conn.execute(
        """INSERT INTO meta.pipeline_runs
           (run_id, started_at, status, force, cohort_expansion, since)
           VALUES (?, ?, 'running', ?, ?, ?)""",
        [run_id, datetime.datetime.now(), force, cohort_expansion, since],
    )


def _finish_run(duck_conn, run_id, status, phases, extract_status=None,
                extract_version=None, error=None):
    duck_conn.execute(
        """UPDATE meta.pipeline_runs
           SET completed_at = ?, status = ?, extract_status = ?,
               extract_version = ?, phases_completed = ?, error = ?
           WHERE run_id = ?""",
        [datetime.datetime.now(), status, extract_status, extract_version,
         ",".join(phases), error, run_id],
    )


def run_pipeline(
    force=False,
    since=None,
    cohort_expansion=False,
    enrich=False,
    enrich_budgets=None,
    duck_conn=None,
):
    """Run a full refresh. Returns a result dict.

    Args:
        force: Re-pull even if the AACT build is unchanged (passes through to
            extract's pin-gate). Required for the one-time filter-removal run.
        since: Optional ``last_update_posted_date`` pre-filter for extract.
        cohort_expansion: Suppress change_events `first_seen` flood (one-time
            active→full-cohort run).
        enrich: Run the Claude enrichment agent after the deterministic rebuild
            (costs API $). Off by default.
        enrich_budgets: ``{domain: usd}`` for the enrichment step; domains absent
            or with budget <= 0 are skipped.
        duck_conn: Optional external connection (tests). Default opens its own.
    """
    # Deferred imports keep this module import-light and avoid heavy transform
    # deps loading for callers that only need the schema helpers.
    from src.extract.aact import run_extraction
    from src.transform.promote import promote_to_enriched
    from src.transform.normalize_conditions import run_normalization_pipeline as run_conditions
    from src.transform.therapeutic_areas import run_ta_pipeline
    from src.transform.normalize_drugs import run_normalization_pipeline as run_drugs
    from src.transform.normalize_sponsors import run_sponsor_pipeline
    from src.transform.classify_design import classify_study_design
    from src.transform.innovative_features import detect_innovative_features, detect_ai_mentions
    from src.mart.study_summary import build_study_summary
    from src.transform.change_events import run_change_events
    import run_hitl_sync

    run_id = str(uuid.uuid4())
    close_conn = duck_conn is None
    duck_conn = duck_conn or get_duckdb_connection()
    ensure_pipeline_runs_schema(duck_conn)
    _start_run(duck_conn, run_id, force, cohort_expansion, since)
    logger.info(f"pipeline run {run_id} starting (force={force}, "
                f"cohort_expansion={cohort_expansion}, enrich={enrich})")

    phases = []
    try:
        # 1. Extract (pin-gate gates the whole refresh).
        ext = run_extraction(duck_conn=duck_conn, force=force, since=since)
        if ext["status"] == "skipped":
            logger.info("AACT build unchanged; nothing to refresh.")
            _finish_run(duck_conn, run_id, "skipped", phases,
                        extract_status="skipped")
            return {"run_id": run_id, "status": "skipped"}
        phases.append("extract")
        extract_version = ext["version"]
        extract_date = ext["extract_date"]

        # 2. Fold pending human decisions into the ref.* dictionaries.
        run_hitl_sync.sync_pending(duck_conn)
        phases.append("hitl_sync")

        # 3. Promote raw -> enriched (must precede classify, which reads enriched).
        promote_to_enriched(duck_conn)
        phases.append("promote")

        # 4. Normalize entities.
        run_conditions(duck_conn)
        run_ta_pipeline(duck_conn)
        run_drugs(duck_conn)
        run_sponsor_pipeline(duck_conn)
        phases.append("normalize")

        # 5. Classify design / innovative features / AI mentions.
        classify_study_design(duck_conn)
        detect_innovative_features(duck_conn)
        detect_ai_mentions(duck_conn)
        phases.append("classify")

        # 6. Analytical mart.
        build_study_summary(duck_conn)
        phases.append("views")

        # 7. Change events (diff the new snapshot vs prior).
        run_change_events(duck_conn, extract_date=extract_date,
                          cohort_expansion=cohort_expansion, run_id=run_id)
        phases.append("change_events")

        # 8. Enrichment agent (opt-in, paid).
        if enrich:
            budgets = enrich_budgets or {}
            from src.agent.enrichment_agent import run_enrichment_agent
            for domain in ENRICH_DOMAINS:
                budget = budgets.get(domain, 0)
                if budget and budget > 0:
                    logger.info(f"enrichment: {domain} (budget ${budget})")
                    run_enrichment_agent(domain, budget, duck_conn=duck_conn)
            phases.append("enrich")

        _finish_run(duck_conn, run_id, "completed", phases,
                    extract_status="completed", extract_version=extract_version)
        logger.info(f"pipeline run {run_id} completed: {', '.join(phases)}")
        return {"run_id": run_id, "status": "completed",
                "version": extract_version, "phases": phases}

    except Exception as exc:
        logger.error(f"pipeline run {run_id} failed during "
                     f"{phases[-1] if phases else 'extract'}: {exc}")
        _finish_run(duck_conn, run_id, "failed", phases, error=str(exc))
        raise
    finally:
        if close_conn:
            duck_conn.close()
