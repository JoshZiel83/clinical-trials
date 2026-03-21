# Clinical Trials ETL Pipeline — Implementation Roadmap

## Context

The spec at `resources/pipeline_spec.md` defines a 4-layer pipeline (Raw Extract → Normalized Entities → Enriched Features → Analytical Views) built from AACT into local DuckDB. This roadmap sequences implementation to reach the core analytical goal — **comparing innovative trial designs across therapeutic areas** — as fast as possible, then fills in remaining normalizations.

## Sequencing Rationale

The critical path to the target analysis is: **raw data → conditions/TAs → study design classification → analysis notebook**. Drug normalization and sponsor dedup are valuable but not on that critical path, so they come after the first analytical milestone.

---

## Phase 0: Project Scaffolding + AACT Connection ✅

**Goal**: Project structure, dependencies, verified AACT connectivity.

**Completed 2026-03-21.**

- Directory layout created: `config/`, `src/`, `data/`, `data/raw/`, `data/reference/`, `notebooks/`, `tests/`
- Dependencies managed via conda (`clinical_trials_env`): `psycopg2`, `duckdb`, `pyarrow`, `pandas`, `jupyter`, `python-dotenv`, `pytest`, `matplotlib`
- `config/settings.py`: AACT connection params, DuckDB path, credential loading from `.env`
- `config/tables.py`: extraction table list, status filter definitions
- `src/logging_config.py`: centralized logging for all pipeline modules
- `src/connection_test.py`: verifies AACT connectivity, initializes DuckDB schemas (`raw`, `meta`)
- `.env.example`: credential template (`.env` is gitignored)
- DuckDB initialized at `data/clinical_trials.duckdb`

**Key finding**: AACT status values use uppercase with underscores (e.g., `RECRUITING`, `NOT_YET_RECRUITING`) rather than the mixed-case format shown in older documentation.

---

## Phase 1: Raw Extract (Layer 1) ✅

**Goal**: Active/planned trials from AACT in local DuckDB + Parquet.

**Completed 2026-03-21.**

- **Status filter**: `overall_status IN ('RECRUITING', 'NOT_YET_RECRUITING', 'ACTIVE_NOT_RECRUITING', 'ENROLLING_BY_INVITATION', 'AVAILABLE')`
- **14 tables extracted** via `nct_id` join to `studies`: `studies`, `designs`, `conditions`, `browse_conditions`, `interventions`, `browse_interventions`, `sponsors`, `keywords`, `brief_summaries`, `detailed_descriptions`, `design_groups`, `countries`, `eligibilities`, `calculated_values`
- `src/extract.py`: queries AACT → writes Parquet to `data/raw/YYYY-MM-DD/` → loads into DuckDB `raw` schema
- `run_extract.py`: convenience entry point
- Extraction metadata logged to `meta.extraction_log` in DuckDB
- **Unit tests**: `tests/test_settings.py`, `tests/test_tables.py`, `tests/test_extract.py` (19 tests, all passing)
- **Validation notebook** (`notebooks/01_raw_data_validation.ipynb`): row counts, null rates, status/study_type/phase distributions, join integrity, date ranges

**Results (2026-03-21 extraction)**:
- 119,753 studies (RECRUITING: 65,424 | NOT_YET_RECRUITING: 27,323 | ACTIVE_NOT_RECRUITING: 21,654 | ENROLLING_BY_INVITATION: 5,103 | AVAILABLE: 249)
- 3,464,691 total rows across 14 tables, extracted in ~29 seconds
- Zero orphaned nct_ids in child tables
- All validation checks passed

---

## Phase 2A: Condition Normalization + Therapeutic Areas

**Goal**: Map conditions → MeSH → therapeutic areas. Enables TA-level analysis.

1. **MeSH reference data**: Download MeSH descriptor XML from NLM. Parse to extract descriptor UI, name, tree numbers. Load into `ref_mesh_descriptors` table in DuckDB.
2. **TA mapping table**: Hand-curate `ref_therapeutic_areas` (~24 rows) mapping MeSH Category C top-level codes to ~12-15 TAs. Starting point: [NBK611886](https://www.ncbi.nlm.nih.gov/books/NBK611886/table/ch4.tab1/).
3. **AACT coverage pass**: Join `conditions` → `browse_conditions` → `ref_mesh_descriptors` → `ref_therapeutic_areas`. Multi-label: a condition in multiple MeSH branches gets all applicable TAs. Output: `norm_study_therapeutic_areas(nct_id, condition_name, mesh_term, mesh_tree_number, therapeutic_area)`.
4. **Coverage notebook** (`notebooks/02_condition_coverage.ipynb`): % of studies with TA tags, top unmapped conditions, TA distribution. This determines urgency of Phase 2C.

**Setup needed**: MeSH XML download (free, no account).

**Done when**: Can query "active studies per therapeutic area" from DuckDB.

**Module**: `src/mesh.py` (parsing), `src/normalize_conditions.py` (mapping), `src/therapeutic_areas.py` (TA rollup)

---

## Phase 2B: Study Design Classification

**Goal**: Classify every study by design type (5 levels). Can run in parallel with 2A.

1. **Levels 1, 2, 4, 5 from structured fields** (`studies` + `designs` tables):
   - L1 Study Type: from `study_type`
   - L2 Design Architecture: combinatorial rules on `allocation` + `intervention_model` (e.g., Randomized + Parallel → "Parallel RCT", Non-Randomized + Single Group → "Single-Arm"); observational uses `observational_model`
   - L4 Blinding: from `masking`
   - L5 Purpose: from `primary_purpose`
   - Output: `class_study_design(nct_id, study_type, design_architecture, blinding_level, purpose)`

2. **Level 3: Innovative features via keyword/regex NLP** on `brief_title`, `official_title`, `detailed_descriptions.description`, `keywords.name`:
   - Patterns: adaptive, basket, umbrella, platform, bayesian, SMART, N-of-1, pragmatic, enrichment, seamless, master protocol
   - Word-boundary regex with context-aware exclusions (e.g., "adaptive" not followed by "behavior"/"immunity")
   - Multi-label output: `class_innovative_features(nct_id, feature_type, source_field, matched_text)`

3. **Validation notebook** (`notebooks/03_design_classification.ipynb`): distributions, spot-check 10-20 per feature type for precision.

**Done when**: Every study classified. Innovative features flagged with known precision.

**Module**: `src/classify_design.py`, `src/innovative_features.py`

---

## Phase 3A: Core Analysis — Design Innovation by Therapeutic Area

**Goal**: The payoff. Join 2A + 2B outputs to answer the primary research question.

**Depends on**: Phases 2A and 2B both complete.

- **Notebook** (`notebooks/04_innovation_by_therapeutic_area.ipynb`):
  - % of trials with any innovative design feature, by TA
  - Breakdown by feature type (adaptive, basket, umbrella, platform) × TA
  - Trends over time (using `start_date`)
  - Phase distribution of innovative designs by TA
  - Geographic patterns (from `countries`)

**Done when**: Charts and tables answering "how do innovative trial designs vary across therapeutic areas?"

---

## Phase 2C: QuickUMLS for Unmapped Conditions (if needed)

**Goal**: Improve TA coverage from ~62% to ~80-90%.

**Depends on**: Phase 2A coverage analysis results. If 62% is sufficient for core TAs, defer further.

- Apply for free UMLS license at https://uts.nlm.nih.gov/uts/ **(do this early — 1-3 day approval)**
- Install QuickUMLS, build local index from UMLS Metathesaurus
- Run on unique unmapped condition strings → UMLS CUIs → MeSH tree numbers via MRCONSO
- Feed through existing TA pipeline from Phase 2A
- Manual curation for top ~200-500 high-frequency unmapped strings

**Setup needed**: UMLS license (free, apply during Phase 0/1)

---

## Phase 2D: Drug Normalization

**Goal**: Map intervention names to canonical drug identifiers.

1. AACT baseline: `interventions` (type Drug/Biological) → `browse_interventions` MeSH (~47% coverage)
2. ChEMBL API: string preprocess (strip dosage/route info, normalize casing) → query `molecule_synonyms` for unmatched names. Rate-limit to ~5 req/sec.
3. Output: `norm_drugs(nct_id, intervention_name, canonical_name, canonical_id, match_source)`
4. Validation notebook (`notebooks/05_drug_normalization.ipynb`)

**Module**: `src/normalize_drugs.py`

---

## Phase 2E: Sponsor Deduplication

**Goal**: Group sponsor name variants to canonical organizations.

- String normalization: case, legal suffixes (Inc/Ltd/Corp/GmbH), punctuation
- Fuzzy matching via `rapidfuzz` (Jaro-Winkler or token-sort)
- Use `agency_class` as grouping hint
- Manual review for top ~100 sponsors
- Output: `norm_sponsors(original_name, canonical_name, agency_class)`

**Module**: `src/normalize_sponsors.py`

---

## Phase 4: Analytical Views (Layer 4)

**Goal**: Denormalized, query-ready tables.

- Wide `view_study_summary` joining: studies + design classification + innovative features + TAs + canonical sponsor + drug names + countries
- Materialized in DuckDB (scale is small enough)

**Module**: `src/views.py`

---

## Phase 5: Refresh Automation

**Goal**: Single-command weekly refresh.

- `run_pipeline.py` orchestrates all phases in order
- Python `logging` for visibility
- Retry logic for external API calls (ChEMBL)
- Metadata table: `pipeline_runs(run_id, start_time, end_time, status, studies_extracted)`

---

## Dependency Graph

```
Phase 0 (Scaffolding)
  │
Phase 1 (Raw Extract)
  │
  ├── Phase 2A (Conditions + TAs) ──┐
  │                                  ├── Phase 3A (Core Analysis!)
  ├── Phase 2B (Study Design) ──────┘
  │
  ├── Phase 2C (QuickUMLS)     [after 2A coverage analysis]
  ├── Phase 2D (Drugs)          [independent, lower priority]
  └── Phase 2E (Sponsors)       [independent, lowest priority]
        │
      Phase 4 (Views)           [after all normalizations]
        │
      Phase 5 (Automation)
```

**Recommended solo-developer order**: 0 → 1 → 2A → 2B → 3A → 2C → 2D → 2E → 4 → 5

## Action Items

1. ~~**Register AACT account**~~ ✅ Done
2. **Apply for UMLS license** — 1-3 day approval, needed by Phase 2C (applied, pending)
3. **Download MeSH XML** — needed for Phase 2A
4. **Bookmark NBK611886 TA mapping table** — starting point for TA curation

## Key Files

- `resources/pipeline_spec.md` — authoritative spec
- `resources/documentation_20260321.csv` — AACT schema (53 tables, 479 fields)
- `resources/ctti_schema_documentation.md` — join conventions, data caveats
- `config/settings.py` — AACT connection, DuckDB path, constants
- `config/tables.py` — extraction table list, status filters
- `src/extract.py` — extraction pipeline
- `src/logging_config.py` — centralized logging

## Verification

After each phase, the corresponding notebook serves as the verification step:
- Phase 1 → `01_raw_data_validation.ipynb` (row counts, distributions)
- Phase 2A → `02_condition_coverage.ipynb` (TA coverage %, distribution)
- Phase 2B → `03_design_classification.ipynb` (precision spot-checks)
- Phase 3A → `04_innovation_by_therapeutic_area.ipynb` (the core analysis)
- Phase 2D → `05_drug_normalization.ipynb` (coverage rates, top unmatched)
