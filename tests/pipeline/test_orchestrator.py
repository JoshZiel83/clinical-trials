"""Tests for src/pipeline/orchestrator.py (phase order, gating, run audit)."""

import duckdb
import pytest
from unittest.mock import patch

from src.pipeline.orchestrator import run_pipeline


def _patches(calls, extract_result):
    """Patch every phase callable to record call order; return a context list."""
    def rec(name, ret=None):
        def _fn(*a, **k):
            calls.append(name)
            return ret
        return _fn

    return [
        patch("src.extract.aact.run_extraction",
              lambda *a, **k: (calls.append("extract"), extract_result)[1]),
        patch("run_hitl_sync.sync_pending", rec("hitl_sync")),
        patch("src.transform.promote.promote_to_enriched", rec("promote")),
        patch("src.transform.normalize_conditions.run_normalization_pipeline", rec("conditions")),
        patch("src.transform.therapeutic_areas.run_ta_pipeline", rec("ta")),
        patch("src.transform.normalize_drugs.run_normalization_pipeline", rec("drugs")),
        patch("src.transform.normalize_sponsors.run_sponsor_pipeline", rec("sponsors")),
        patch("src.transform.classify_design.classify_study_design", rec("classify")),
        patch("src.transform.innovative_features.detect_innovative_features", rec("innovative")),
        patch("src.transform.innovative_features.detect_ai_mentions", rec("ai")),
        patch("src.mart.study_summary.build_study_summary", rec("views")),
        patch("src.transform.change_events.run_change_events", rec("change_events")),
    ]


COMPLETED = {"status": "completed", "version": "2026-06-20", "extract_date": "2026-06-20"}


def _run(calls, extract_result, **kw):
    conn = duckdb.connect(":memory:")
    ctxs = _patches(calls, extract_result)
    for c in ctxs:
        c.start()
    try:
        return run_pipeline(duck_conn=conn, **kw), conn
    finally:
        for c in ctxs:
            c.stop()


def test_phase_order_and_completion():
    calls = []
    result, conn = _run(calls, COMPLETED)
    assert result["status"] == "completed"
    # promote precedes classify; change_events last (enrichment off)
    assert calls.index("promote") < calls.index("classify")
    assert calls.index("classify") < calls.index("views") < calls.index("change_events")
    assert calls[0] == "extract"
    row = conn.execute(
        "SELECT status, extract_status, extract_version, phases_completed "
        "FROM meta.pipeline_runs"
    ).fetchone()
    assert row[0] == "completed" and row[1] == "completed"
    assert row[2] == "2026-06-20"
    assert "change_events" in row[3]
    conn.close()


def test_skipped_extract_short_circuits():
    calls = []
    result, conn = _run(calls, {"status": "skipped"})
    assert result["status"] == "skipped"
    assert calls == ["extract"]  # nothing downstream ran
    assert conn.execute(
        "SELECT status FROM meta.pipeline_runs"
    ).fetchone()[0] == "skipped"
    conn.close()


def test_enrichment_off_by_default():
    calls = []
    with patch("src.agent.enrichment_agent.run_enrichment_agent") as enr:
        _run(calls, COMPLETED)
        enr.assert_not_called()


def test_enrichment_runs_when_enabled_with_budget():
    calls = []
    with patch("src.agent.enrichment_agent.run_enrichment_agent") as enr:
        _run(calls, COMPLETED, enrich=True, enrich_budgets={"condition": 5})
        enr.assert_called_once()
        assert enr.call_args.args[0] == "condition"


def test_failed_phase_marks_run_failed_and_raises():
    calls = []
    conn = duckdb.connect(":memory:")
    ctxs = _patches(calls, COMPLETED)
    for c in ctxs:
        c.start()
    # Make promote blow up.
    boom = patch("src.transform.promote.promote_to_enriched",
                 side_effect=RuntimeError("promote failed"))
    boom.start()
    try:
        with pytest.raises(RuntimeError, match="promote failed"):
            run_pipeline(duck_conn=conn)
        row = conn.execute(
            "SELECT status, error FROM meta.pipeline_runs"
        ).fetchone()
        assert row[0] == "failed" and "promote failed" in row[1]
    finally:
        boom.stop()
        for c in ctxs:
            c.stop()
        conn.close()
