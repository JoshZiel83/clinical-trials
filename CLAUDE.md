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
- Environment is `clinical_trials_env`
- Python path: `/opt/homebrew/Caskroom/miniforge/base/envs/clinical_trials_env/bin/python`

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
`python -m scripts.build_quickumls_index <umls.zip>` (~5GB, one-time).

> **Current state:** the UMLS index was lost in a 2026-06 env/DB regen and is not
> currently built — the QuickUMLS tool is inactive until it is rebuilt. There is also
> a known builder/registration path mismatch (builder writes flat
> `data/reference/umls/quickumls_index/`; registration expects versioned
> `data/reference/umls/2025AB/quickumls_index/`). Both tracked in
> [#2](https://github.com/JoshZiel83/clinical-trials/issues/2).

# Pipeline Entry Points
- `run_extract.py` — Phase 1: raw AACT extraction. Pulls via DuckDB's `postgres`
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