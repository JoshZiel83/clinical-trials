# Virtual Environment
Conda is used for package and dependency management in a virtual environment:
- Environment is `clinical_trials_env`
- Python path: `/Users/joshuaziel/miniforge3/envs/clinical_trials_env/bin/python`

Make sure the virtual environment is activated before installing new packages: 

```bash
conda activate clinical_trials_env
```

# Pipeline Entry Points
- `run_extract.py` — Phase 1: raw AACT extraction
- `run_normalize_conditions.py` — Phase 2A: condition normalization + therapeutic areas
- `run_classify_design.py` — Phase 2B: study design classification + innovative features + AI mentions

# DuckDB
- Path: `data/clinical_trials.duckdb`
- DuckDB does not support concurrent write access — only one process can hold a write lock. Shut down any notebook kernels (R or Python) connected in read-write mode before running pipeline scripts.

# Notebooks
- Notebooks 01-03 use Python kernel
- Notebook 04 (`04_innovation_by_therapeutic_area.ipynb`) uses **R kernel** (`ir`)