# ADR 0002 — Hardened extract: Postgres scanner, atomic swap, build pin

- **Status:** Accepted
- **Date:** 2026-06-20
- **Deciders:** Josh Ziel
- **Touches:** Phase 1 (raw extract), ROADMAP Epic A1/A2 (issues #3, #5, #7, #11, #8)

---

## 1. Context

The Phase 1 extract mirrored the active/planned AACT cohort into `raw.*` + a dated
Parquet snapshot, but it was a manual, non-atomic, weakly-provenanced pull:

- Rows round-tripped through pandas `read_sql` → Parquet → re-read into DuckDB.
- A mid-run failure could leave some `raw.*` refreshed, others stale, and — because
  the extraction log was written in one batch at the end — **no log row at all**.
- A `SELECT *` mirror silently absorbed any upstream AACT schema change.
- `extract_date` recorded *when we pulled*, not *what build* — nothing was reproducible.
- Dead `connection_test.py` duplicated the schema/log bootstrap DDL.

Epic A's direction is full-snapshot longitudinal refresh; this ADR hardens the extract
that work will sit on, without yet removing the status filter or building the diff
(those are A3).

## 2. Decision

**Postgres scanner (#7).** Pull through DuckDB's `postgres` extension
(`ATTACH '<dsn>' AS aact_src (TYPE postgres, READ_ONLY)`), filtering scanner-side, and
materialize each table with `CREATE TABLE … AS SELECT … FROM aact_src.ctgov.<t>` →
`COPY … TO '<parquet>'`. pandas leaves the extract hot path. Credentials flow through
`PG*` env vars (not the SQL/DSN) so they never reach logs.

**Atomic stage-then-swap (#5).** Every table builds into `raw.<t>__staging` + a hidden
`data/raw/.<date>.staging/` Parquet dir. Only once all 14 succeed are they renamed into
place in a single transaction (`DROP … ; ALTER TABLE …__staging RENAME TO …`), then the
Parquet dir is `os.replace`-swapped. A failure before the swap leaves `raw.*` and the
prior snapshot untouched; staging tables/dir are cleaned up. A per-table
`meta.extraction_log` row is committed as each table finishes staging, so a partial run
still leaves a forensic trail.

**Schema-drift detection (#11).** Incoming columns are compared to a pinned baseline,
`config/aact_expected_columns.json`. A **dropped** column fails the run (the mirror would
silently lose data); a **new** column only warns (the `SELECT *` mirror absorbs it). The
baseline self-seeds on first run; `--update-schema-baseline` regenerates it deliberately.

**Build pin + gate (#8 / A2).** The AACT build is pinned in `meta.reference_sources` as
`aact@<build-date>`, where the build date is `max(studies.updated_at)::date`. A run is
short-circuited when that build has not advanced (compared on the exact `acquired_at`
timestamp; `--force` overrides).

## 3. Why `max(studies.updated_at)` is the build identifier

Verified against the live DB: the legacy `nlm_download_date` /
`nlm_download_date_description` columns are **100% NULL** (deprecated in the
ClinicalTrials.gov API-v2 migration). AACT **wipes and re-inserts the whole corpus on
each load** — `studies.created_at == updated_at` for every row, child tables carry no
audit timestamps — so `updated_at` is a *build watermark* (its `max`), not a per-row
change signal. `max(updated_at)::date` matches AACT's dated static-copy naming
(`YYYYMMDD_clinical_trials_ctgov`), making the snapshot reproducible without downloading
the static copy. The stable per-study content-change signal is `last_update_posted_date`,
exposed only as the inert `since` extract hook (A3 consumes it).

## 4. Consequences

- New runtime dependency on DuckDB's `postgres` extension (auto-installed on first use);
  psycopg2 is no longer on the extract hot path (`get_aact_connection` retained, unused).
- True incremental extraction is **not** pursued — AACT's full-reload model makes
  `updated_at`-based deltas re-pull everything (corroborates deferring #9). Longitudinal
  change tracking is a snapshot diff (Parquet N vs N-1) in A3, not an incremental pull.
- The `since` filter is plumbed but default-off and is a *subset, not a snapshot* — it
  cannot detect dropped studies, so it must never be the canonical snapshot or diff source.
- The status filter remains; full-cohort removal is A3.
