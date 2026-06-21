# Documentation & issue discipline

Keeping the project's living docs in sync is part of every task, not an afterthought:

- **`ROADMAP.md`** — forward-looking work only, organized as epics. Every forward
  item links the GitHub issue(s) that track it (e.g. `[#13]`). When an item ships,
  move it to `CHANGELOG.md` and remove it here.
- **`CHANGELOG.md`** — shipped work, in completion order (the historical record).
- **`CLAUDE.md`** (this file) — operational ground truth: env, entry points,
  conventions. Update it the moment an entry point, path, kernel, or workflow changes.
- **`docs/adr/`** — architectural decisions; add or supersede ADRs, don't rewrite them.

**File issues for deferred work.** When you identify a bug, cleanup, or improvement
that you are *not* doing right now, file a GitHub issue for it — do not bury it in a
code comment or leave it only in conversation. The issue tracker, not memory, is the
source of truth for deferred work. Label it (`tech-debt`, `architecture`,
`provenance`, …) and, if it belongs to a roadmap epic, name that epic in the issue.

**Link issues and roadmap both ways.** If a filed issue maps to roadmap work, add its
`#n` under the owning epic in `ROADMAP.md`; if you defer a roadmap item, mark it
deferred and keep its issue open. The roadmap and the open-issue list should never
disagree about what is planned vs deferred.

# Virtual Environment
Conda is used for package and dependency management in a virtual environment:
- Environment is `clinical_trials_env` — the env **name** is the portable source of
  truth (this repo is developed on more than one machine, and the conda base differs
  per machine, e.g. `~/miniforge3` vs. a Homebrew Caskroom path). Never hard-code the
  interpreter path.
- Resolve the interpreter on the current machine by name:
  `conda run -n clinical_trials_env python -c 'import sys; print(sys.executable)'`
  (or `echo "$CONDA_PREFIX/bin/python"` once the env is activated). The `.Rprofile`
  reticulate bridge resolves it the same way, via `conda_python("clinical_trials_env")`.

Make sure the virtual environment is activated before installing new packages: 

```bash
conda activate clinical_trials_env
```

## Optional: QuickUMLS (Phase 6D/6E condition enrichment)
`quickumls` + `quickumls-simstring` are **not** in `environment.yml` on purpose:
they are pip-only, optional (lazy-loaded; only the QuickUMLS condition-enrichment
tool and the index builder need them), and `quickumls-simstring` needs a
macOS-specific dylib relink a conda manifest can't express. Install both with the
env active:

```bash
conda activate clinical_trials_env
scripts/install_quickumls.sh
```

The script pip-installs both packages and, on macOS, repoints `_simstring.so`
off `/usr/lib/libiconv.2.dylib` (which symbol-errors on modern macOS) to the
conda-provided libiconv. Then build the index once with
`python -m scripts.build_quickumls_index [path/to/umls.zip]` (~5GB, one-time) —
it writes the index directly to the version-pinned `data/reference/umls/<version>/
quickumls_index/`, exactly where `bootstrap_reference_sources.py` registers it.

> **Current state:** the **UMLS 2026AA** index is built and registered active in
> `meta.reference_sources` (`data/reference/umls/2026AA/quickumls_index/`, ~5.5GB,
> 10.7M terms); the QuickUMLS tool is live. The old flat-vs-versioned builder/
> registration path mismatch (#2) is resolved — both scripts now use the versioned
> `2026AA` path. Bumping releases: drop the new zip in `data/reference/umls/`, set
> `UMLS_VERSION` in `build_quickumls_index.py` + the `umls` entry in
> `bootstrap_reference_sources.py`, rebuild, re-bootstrap.

# R environment (rig + renv)
This is a multilingual repo: **conda owns Python, renv owns R, and the two never
manage each other** (see `docs/adr` philosophy / the multilingual-projects ref).
R is used by the Shiny review app (`apps/review/`) and the R-kernel notebook
(`notebooks/04_innovation_by_therapeutic_area.ipynb`).

- **R version** is managed by [`rig`](https://github.com/r-lib/rig). Current
  default is R 4.6.0 (pinned in `renv.lock`). Install/switch with
  `rig add release` / `rig default release`. NB: the unrelated Homebrew formula
  is also named `rig`; the R installer is the **cask** `r-lib/rig/rig` (installs
  as the `r-rig` formula / `/opt/homebrew/bin/rig`).
- **R packages** are managed by [`renv`](https://rstudio.github.io/renv/) at the
  repo root. `renv.lock` is the source of truth; snapshot mode is `"all"`.
  - Reproduce the library: open R at the repo root → `renv::restore()`.
  - Add a package: `renv::install("<pkg>"); renv::snapshot()`.
- **R ↔ Python bridge** is `reticulate`, **point-only**: `.Rprofile` resolves the
  conda env *by name* and sets `RETICULATE_PYTHON` to its interpreter (via
  `CONDA_EXE`). conda still owns Python entirely — never run `renv::use_python()`.
  The normal data handoff is on-disk via DuckDB/Parquet; reticulate is only for
  in-process pandas↔data.frame sharing when you want it.

**Running R code:**
- **Shiny app** — launch from the repo root so `.Rprofile` auto-activates renv:
  `Rscript -e 'shiny::runApp("apps/review", launch.browser=TRUE)'`.
- **R notebook** — the `ir` Jupyter kernel starts in `notebooks/`, where the
  repo-root `.Rprofile` does **not** run, so the notebook's first cell calls
  `renv::load("..")` to activate the project library. `IRkernel` is intentionally
  installed in the **user** library (kernel infrastructure), not in `renv.lock`;
  analysis packages (duckdb, ggplot2, …) come from renv. Re-register the kernel
  after an R-version change with `R --vanilla -e 'IRkernel::installspec(user=TRUE)'`.

# Pipeline Entry Points
- `run_pipeline.py` — **A3 full-refresh orchestrator.** Threads one DuckDB
  connection through every phase in order (extract → hitl_sync → promote → normalize →
  classify → views → change_events), writing a `meta.pipeline_runs` audit row. The
  extract pin-gate gates the whole refresh (skips if the AACT build hasn't advanced
  unless `--force`). Flags: `--force`, `--cohort-expansion` (suppress change-event
  `first_seen` on the one-time active→full-cohort run), `--enrich` + `--budget-{condition,
  drug,sponsor}` (the enrichment agent is **off by default** — the only paid step). The
  individual `run_*.py` below remain usable standalone.
- `run_change_events.py` — A3: diff the current vs prior dated Parquet snapshot into
  `meta.trial_change_events` (`--cohort-expansion`). `src/transform/change_events.py`.
- `run_extract.py` — Phase 1: raw AACT extraction. Mirrors the **full AACT cohort**
  (~600K; no status filter as of A3). Pulls via DuckDB's `postgres`
  scanner (`ATTACH … READ_ONLY`, auto-installed extension; creds via `PG*` env vars),
  stages each table then **atomically swaps** `raw.*` + the dated Parquet dir, checks
  schema drift against `config/aact_expected_columns.json`, and pins the build as
  `aact@<build-date>` (`max(studies.updated_at)::date`) in `meta.reference_sources`.
  Flags: `--force` (re-pull even if the build hasn't advanced — default is to skip),
  `--since YYYY-MM-DD` (A3 `last_update_posted_date` pre-filter; **subset, not a
  snapshot**), `--update-schema-baseline`. See [ADR 0002](docs/adr/0002-extract-scanner-atomic-swap.md).
- `run_normalize_conditions.py` — Phase 2A: condition normalization + therapeutic areas
- `run_classify_design.py` — Phase 2B: study design classification + innovative features + AI mentions
- `run_normalize_drugs.py` — Phase 2D: drug normalization
- `run_normalize_sponsors.py` — Phase 6B: sponsor normalization + fuzzy merger candidates
- `run_enrichment_agent.py` — Phase 6E: Claude enrichment agent (`--domain`, `--budget`, `--limit`, `--max-pending`)
- `run_hitl_sync.py` — Phase 6F: apply Shiny decision logs; rebuild affected `norm.*` + views
- `run_promote_enriched.py` — Phase 7C: project `raw.*` → `enriched.*` (stable inputs for the mart); stamps `meta.enriched_tables`
- `run_views.py` — Phase 4: denormalized analytical views (`views.study_summary`)

# R/Shiny apps
- `apps/review/` — HITL review app; `Rscript -e 'shiny::runApp("apps/review", launch.browser=TRUE)'`

# One-shot scripts
- `python -m scripts.migrate_condition_candidates` — Phase 6A one-time migration
- `python -m scripts.build_quickumls_index [path/to/umls-release.zip]` — Phase 6D: extracts MRCONSO.RRF + MRSTY.RRF, builds QuickUMLS index at `data/reference/umls/quickumls_index/`. Takes ~30-60 min and ~5GB. Run once per UMLS release.

# DuckDB
- Path: `data/clinical_trials.duckdb`
- DuckDB does not support concurrent write access — only one process can hold a write lock. Shut down any notebook kernels (R or Python) connected in read-write mode before running pipeline scripts.

# Notebooks
- All notebooks use the Python kernel **except** `04_innovation_by_therapeutic_area.ipynb`,
  which uses the **R kernel** (`ir`).
- The notebook ↔ phase verification mapping lives in `CHANGELOG.md`.