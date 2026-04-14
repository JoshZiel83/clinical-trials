# HITL Review App

Read-only R/Shiny app for reviewing mapping candidates across the three HITL
domains (`condition`, `drug`, `sponsor`). Approvals/rejections are written to
a Parquet decision log that `python run_hitl_sync.py` applies to the DuckDB.

## Install R deps

```r
install.packages(c("shiny", "DT", "duckdb", "arrow", "dplyr", "jsonlite"),
                 repos = "https://cloud.r-project.org")
```

## Run

```bash
Rscript -e 'shiny::runApp("apps/review", launch.browser = TRUE)'
```

The app connects to `data/clinical_trials.duckdb` **read-only**, so the
Python pipeline can run concurrently.

## Workflow

1. Open a domain tab (Condition / Drug / Sponsor).
2. Filter by status / source / min score / min study count.
3. Select one or more rows — the rationale + tool trace for the first selected
   row appears below the table. Agent proposals have rich traces; deterministic
   fuzzy proposals don't.
4. Click **Approve selected** or **Reject selected**. A Parquet decision log is
   written to `data/reviews/decisions_<timestamp>_<domain>.parquet`.
5. From the project root: `python run_hitl_sync.py`. This:
   - imports the newest decision log(s) via `src.hitl.import_decision_log`,
   - promotes approvals into the target dictionaries as `manual` entries,
   - updates candidate rows to `approved` / `rejected`,
   - rebuilds affected `norm.*` tables and `views.study_summary`.
6. Re-run the app — the queue shrinks by your approvals; rejected candidates
   stay out of future generations for that `(domain, source_value, canonical_term, source)` tuple.

## What writes where

- **App writes**: `data/reviews/decisions_*.parquet` only — never touches DuckDB.
- **Sync writes**: `ref.condition_dictionary`, `ref.drug_dictionary`,
  `ref.sponsor_dictionary` (for approvals); `ref.mapping_candidates.status`;
  `norm.*` rebuilds; `views.study_summary` rebuild.
