# Virtual Environment
Conda is used for package and dependency management in a virtual environment:
- Environment is `clinical_trials_env`
- Python path: `/Users/joshuaziel/miniforge3/envs/clinical_trials_env/bin/python`

Make sure the virtual environment is activated before installing new packages: 

```bash
conda activate clinical_trials_env
```

## macOS note: quickumls_simstring libiconv linkage
The `quickumls-simstring` pip wheel links against `/usr/lib/libiconv.2.dylib` which symbol-errors on modern macOS. After installing quickumls, rewrite the dylib path to the conda-provided libiconv:

```bash
install_name_tool -change /usr/lib/libiconv.2.dylib \
  $CONDA_PREFIX/lib/libiconv.2.dylib \
  $CONDA_PREFIX/lib/python3.11/site-packages/quickumls_simstring/_simstring.so
```

# Pipeline Entry Points
- `run_extract.py` — Phase 1: raw AACT extraction
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
- Notebooks 01-03 use Python kernel
- Notebook 04 (`04_innovation_by_therapeutic_area.ipynb`) uses **R kernel** (`ir`)