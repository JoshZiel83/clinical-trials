"""Tests for src/transform/change_events.py."""

from pathlib import Path

import duckdb
import pandas as pd

from src.transform.change_events import (
    list_snapshots,
    prior_snapshot,
    run_change_events,
)


def test_list_snapshots_sorts_and_excludes_staging(tmp_path):
    for name in ["2026-02-01", "2026-01-01", ".2026-02-01.staging", "notadate"]:
        (tmp_path / name).mkdir()
    assert list_snapshots(tmp_path) == ["2026-01-01", "2026-02-01"]


def test_prior_snapshot():
    snaps = ["2026-01-01", "2026-02-01", "2026-03-01"]
    assert prior_snapshot(snaps, "2026-03-01") == "2026-02-01"
    assert prior_snapshot(snaps, "2026-01-01") is None


def _write_snapshot(root, date, studies, conditions):
    d = Path(root) / date
    d.mkdir(parents=True)
    pd.DataFrame(studies).to_parquet(d / "studies.parquet", index=False)
    pd.DataFrame(conditions).to_parquet(d / "conditions.parquet", index=False)
    # interventions/sponsors empty but present (the diff reads them).
    for t in ("interventions", "sponsors"):
        pd.DataFrame({"nct_id": [], "name": []}).to_parquet(d / f"{t}.parquet", index=False)


def _studies_row(nct, status, enrollment, phase, lup):
    return {
        "nct_id": nct, "overall_status": status, "enrollment": enrollment,
        "enrollment_type": "Actual", "phase": phase, "start_date": "2026-01-01",
        "completion_date": "2027-01-01", "primary_completion_date": "2026-12-01",
        "last_update_posted_date": lup,
    }


def _build_two_snapshots(root):
    # prior: NCT1 (recruiting), NCT2 (will drop), NCT3 (unchanged)
    _write_snapshot(
        root, "2026-01-01",
        studies=[
            _studies_row("NCT1", "RECRUITING", 100, "Phase 1", "2026-01-01"),
            _studies_row("NCT2", "RECRUITING", 10, "Phase 1", "2026-01-01"),
            _studies_row("NCT3", "RECRUITING", 50, "Phase 1", "2026-01-01"),
        ],
        conditions=[
            {"nct_id": "NCT1", "name": "Diabetes"},
            {"nct_id": "NCT3", "name": "Cancer"},
        ],
    )
    # current: NCT1 changed (status/enrollment/phase, lup advanced), NCT3 same, NCT4 new
    _write_snapshot(
        root, "2026-02-01",
        studies=[
            _studies_row("NCT1", "ACTIVE_NOT_RECRUITING", 200, "Phase 2", "2026-02-01"),
            _studies_row("NCT3", "RECRUITING", 50, "Phase 1", "2026-01-01"),
            _studies_row("NCT4", "RECRUITING", 5, "Phase 1", "2026-02-01"),
        ],
        conditions=[
            {"nct_id": "NCT1", "name": "Diabetes"},
            {"nct_id": "NCT1", "name": "Hypertension"},
            {"nct_id": "NCT3", "name": "Cancer"},
        ],
    )


def _events(conn):
    return {
        (r[0], r[1])
        for r in conn.execute(
            "SELECT nct_id, event_type FROM meta.trial_change_events"
        ).fetchall()
    }


def test_diff_classifies_all_event_types(tmp_path):
    _build_two_snapshots(tmp_path)
    conn = duckdb.connect(":memory:")
    res = run_change_events(conn, extract_date="2026-02-01", raw_dir=tmp_path)
    assert res["prior"] == "2026-01-01" and res["current"] == "2026-02-01"
    ev = _events(conn)
    assert ("NCT4", "first_seen") in ev
    assert ("NCT2", "dropped") in ev
    assert ("NCT1", "status_transition") in ev
    assert ("NCT1", "enrollment_changed") in ev
    assert ("NCT1", "phase_changed") in ev
    assert ("NCT1", "conditions_changed") in ev
    # NCT3 unchanged (last_update_posted_date didn't advance) -> no field events
    assert not any(nct == "NCT3" for nct, _ in ev)
    conn.close()


def test_cohort_expansion_suppresses_first_seen(tmp_path):
    _build_two_snapshots(tmp_path)
    conn = duckdb.connect(":memory:")
    run_change_events(conn, extract_date="2026-02-01", raw_dir=tmp_path,
                      cohort_expansion=True)
    ev = _events(conn)
    assert not any(et == "first_seen" for _, et in ev)
    assert ("NCT2", "dropped") in ev  # dropped still emitted
    conn.close()


def test_no_prior_snapshot_is_noop(tmp_path):
    _write_snapshot(
        tmp_path, "2026-01-01",
        studies=[_studies_row("NCT1", "RECRUITING", 1, "Phase 1", "2026-01-01")],
        conditions=[{"nct_id": "NCT1", "name": "Diabetes"}],
    )
    conn = duckdb.connect(":memory:")
    res = run_change_events(conn, extract_date="2026-01-01", raw_dir=tmp_path)
    assert res["events"] == 0
    conn.close()


def test_rerun_is_idempotent(tmp_path):
    _build_two_snapshots(tmp_path)
    conn = duckdb.connect(":memory:")
    r1 = run_change_events(conn, extract_date="2026-02-01", raw_dir=tmp_path)
    r2 = run_change_events(conn, extract_date="2026-02-01", raw_dir=tmp_path)
    assert r1["events"] == r2["events"]
    assert conn.execute(
        "SELECT count(*) FROM meta.trial_change_events"
    ).fetchone()[0] == r2["events"]
    conn.close()
