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

## Phase 2A: Condition Normalization + Therapeutic Areas ✅

**Goal**: Map conditions → canonical MeSH terms → therapeutic areas. Enables TA-level analysis.

**Completed 2026-04-05.**

**Key finding**: The `raw.browse_conditions` table already contains NLM-computed MeSH ancestors (e.g., "Neoplasms", "Cardiovascular Diseases") which directly correspond to therapeutic area categories. This eliminated the need for the planned 300MB MeSH XML download — ancestor names are mapped directly to TAs.

**Approach — two-layer architecture:**
1. **Condition dictionary** (`ref.condition_dictionary`): Maps free-text condition names to canonical MeSH terms using five layered methods:
   - *Exact match* (case-insensitive): 3,247 mappings
   - *1:1 study match* (study has exactly 1 condition + 1 MeSH term): 8,263 mappings
   - *Co-occurrence dominance* (most frequent co-occurring MeSH term): 2,712 mappings
   - *Cancer-synonym expansion* ("[Site] Cancer" → "[Site] Neoplasms"): 350 mappings
   - *Fuzzy matching* (rapidfuzz token_sort_ratio): 4,637 mappings — **needs refactoring** (see Phase 2A.1 below)
   - Dictionary is extensible: manual or QuickUMLS entries added to the dictionary are automatically picked up on the next pipeline run.
2. **TA mapping**: `browse_conditions` MeSH ancestor names joined to a hand-curated `ref.therapeutic_areas` mapping (21 entries → 21 TAs).

**Output tables:**
- `ref.condition_dictionary` — 19,209 entries (33.6% of unique condition names)
- `ref.therapeutic_areas` — 21 MeSH ancestor → TA mappings
- `norm.study_conditions` — 316,463 rows, 86.5% of studies have ≥1 canonical condition
- `norm.study_therapeutic_areas` — 202,132 rows, 78.2% of studies have ≥1 TA

**TA distribution (top 10):**
- Oncology: 27,515 | General/Symptoms: 27,197 | Neurology: 17,151
- Cardiovascular: 13,415 | Metabolic/Endocrine: 11,612 | Psychiatry: 10,365
- Gastroenterology: 9,977 | Immunology: 9,199 | Respiratory: 8,954
- Behavioral/Lifestyle: 8,529

**Coverage gap**: 25,438 studies have conditions but no NLM MeSH mapping at all — this is the primary target for Phase 2C (QuickUMLS).

**Unit tests**: 47 tests total (17 normalize_conditions + 11 therapeutic_areas + 19 existing), all passing.

**Module**: `src/normalize_conditions.py` (dictionary building + study conditions), `src/therapeutic_areas.py` (TA ref table + study TAs)
**Entry point**: `run_normalize_conditions.py`
**Validation**: `notebooks/02_condition_coverage.ipynb`

---

## Phase 2A.1: Fuzzy Match Enrichment Workflow ✅

**Goal**: Refactor fuzzy matching from an automatic dictionary layer into a HITL (human-in-the-loop) enrichment workflow. Fuzzy matches are too noisy to trust automatically — they should generate candidates for review, not populate the dictionary directly.

**Completed 2026-04-07.**

**What changed:**
1. **Removed fuzzy layer from `build_condition_dictionary()`** — the dictionary now only contains layers 1-4 (exact, 1:1-study, co-occurrence, cancer-synonym) plus manual/quickumls entries. `'fuzzy'` removed from `AUTOMATED_METHODS`.
2. **Created `ref.condition_candidates`** — staging table with columns: `condition_name`, `canonical_term`, `score` (rapidfuzz 75-100), `study_count`, `status` (pending/approved/rejected), `created_at`. Approved/rejected decisions persist across regenerations.
3. **Refactored `_build_fuzzy_mappings()` → `generate_fuzzy_candidates()`** — public function that writes to candidates table instead of dictionary. Returns DataFrame for notebook display.
4. **Added helper functions**: `promote_candidates()` (insert approved candidates as `manual`/`high`), `export_candidates_csv()`, `import_reviewed_csv()`.
5. **Enrichment notebook** (`notebooks/02a_condition_enrichment.ipynb`): generate candidates → review by impact/confidence → promote or export to CSV for offline review.

**Impact**: Removing ~4,637 fuzzy entries from the dictionary drops condition coverage. This is intentional — the enrichment notebook lets a reviewer recover coverage with verified quality.

**Unit tests**: 32 tests total (21 normalize_conditions + 11 therapeutic_areas), all passing.

**Module**: `src/normalize_conditions.py` (refactored), `notebooks/02a_condition_enrichment.ipynb` (review workflow)

---

## Phase 2B: Study Design Classification ✅

**Goal**: Classify every study by design type (5 levels). Can run in parallel with 2A.

**Completed 2026-04-07.**

**Levels 1, 2, 4, 5** (`class.study_design` — one row per study):
- L1 Study Type: from `study_type` (INTERVENTIONAL: 87,581 | OBSERVATIONAL: 30,931 | EXPANDED_ACCESS: 252)
- L2 Design Architecture: combinatorial rules on `allocation` + `intervention_model` for interventional; `observational_model` for observational. Top: Parallel RCT (47,427), Single-Arm (24,339), Cohort (20,441)
- L4 Blinding: mapped from `masking` (Open Label: 52,955 | Double Blind: 9,329 | Single Blind: 13,300 | Quadruple Blind: 6,907 | Triple Blind: 5,063)
- L5 Purpose: from `primary_purpose` (Treatment: 57,207 | Prevention: 8,315 | Other: 5,398 | ...)

**Level 3** (`class.innovative_features` — multi-label):
- 11 feature types detected via word-boundary regex with context-aware exclusions on `brief_title`, `official_title`, `detailed_descriptions.description`, `keywords.name`
- 4,680 studies (3.9%) flagged with at least one innovative feature
- Distribution: adaptive (2,519) | pragmatic (1,194) | platform (268) | bayesian (256) | SMART (222) | umbrella (182) | master protocol (167) | basket (151) | seamless (126) | N-of-1 (49) | enrichment (9)

**Data quality notes:**
- AACT uses "None" string as NULL placeholder for fields that don't apply (e.g., masking for observational studies) — handled by treating "None" as NULL
- 266 studies have no `raw.designs` record — L2/L4/L5 are NULL for these
- "platform" requires "trial/study/design/protocol" context to avoid tech-platform false positives
- "SMART" is case-sensitive to avoid "smart" adjective
- "enrichment" requires "design/strategy/trial/study" context to avoid nutritional enrichment

**Unit tests**: 26 tests (13 classify_design + 13 innovative_features), all passing.

**Module**: `src/classify_design.py`, `src/innovative_features.py`
**Entry point**: `run_classify_design.py`
**Validation**: `notebooks/03_design_classification.ipynb`

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

### Data evolution considerations

The pipeline must handle two kinds of change over time:

**A. AACT data changes between extractions:**
- New studies appear (recruiting starts), existing studies change status or update conditions/interventions
- NLM may update `browse_conditions` MeSH mappings as their algorithms improve
- The current full-rebuild approach (replace `raw.*`, re-derive `norm.*`) handles this correctly at today's scale (~120K studies, <60s total)
- At larger scale or higher frequency, consider: extraction diffing (what changed since last run), incremental normalization (only process new/changed studies), and archiving prior snapshots for longitudinal analysis

**B. Accumulated mappings improve with more data:**
- The condition dictionary's automated layers (exact, 1:1, co-occurrence, cancer-synonym) are re-derived each run — more studies = more co-occurrence evidence = better mappings
- Manual/HITL curations persist across runs and are never overwritten by automated methods
- Risk: a manual mapping could become stale if MeSH vocabulary is updated (unlikely but possible). Phase 5 should include a validation step that checks manual dictionary entries still reference valid MeSH terms in `browse_conditions`.

**When to address:**
- Current architecture (full rebuild + persistent manual entries) is sound through Phase 3A and likely through Phase 4
- Phase 5 is the right time to formalize: extraction diffing, incremental processing, stale mapping detection, and run-over-run quality metrics
- Until then, the key invariant to maintain: **automated dictionary layers are always re-derivable from raw data; manual entries are the only state that persists and must be protected**

---

## Dependency Graph

```
Phase 0 (Scaffolding) ✅
  │
Phase 1 (Raw Extract) ✅
  │
  ├── Phase 2A (Conditions + TAs) ✅ ──┐
  │     │                               ├── Phase 3A (Core Analysis!)
  │     └── Phase 2A.1 (Fuzzy HITL) ✅ │
  │                                     │
  ├── Phase 2B (Study Design) ✅ ──────┘
  │
  ├── Phase 2C (QuickUMLS)     [after 2A coverage analysis]
  ├── Phase 2D (Drugs)          [independent, lower priority]
  └── Phase 2E (Sponsors)       [independent, lowest priority]
        │
      Phase 4 (Views)           [after all normalizations]
        │
      Phase 5 (Automation)
```

**Recommended solo-developer order**: 0 → 1 → 2A → **2A.1** → 2B → 3A → 2C → 2D → 2E → 4 → 5

## Action Items

1. ~~**Register AACT account**~~ ✅ Done
2. **Apply for UMLS license** — 1-3 day approval, needed by Phase 2C (applied, pending)
3. ~~**Download MeSH XML**~~ — not needed for Phase 2A (ancestor-name approach used instead); may be needed for Phase 2C
4. ~~**Bookmark NBK611886 TA mapping table**~~ ✅ Used as starting point for `data/reference/therapeutic_area_mapping.json`

## Key Files

- `resources/pipeline_spec.md` — authoritative spec
- `resources/documentation_20260321.csv` — AACT schema (53 tables, 479 fields)
- `resources/ctti_schema_documentation.md` — join conventions, data caveats
- `config/settings.py` — AACT connection, DuckDB path, constants
- `config/tables.py` — extraction table list, status filters
- `src/extract.py` — extraction pipeline
- `src/logging_config.py` — centralized logging
- `src/normalize_conditions.py` — condition dictionary building + study conditions
- `src/therapeutic_areas.py` — TA reference table + study TA assignment
- `data/reference/therapeutic_area_mapping.json` — hand-curated MeSH ancestor → TA mapping (21 entries)
- `src/classify_design.py` — study design classification (L1/L2/L4/L5)
- `src/innovative_features.py` — innovative feature detection (L3, regex NLP)
- `data/DATABASE_SCHEMA.md` — DuckDB schema documentation

## Verification

After each phase, the corresponding notebook serves as the verification step:
- Phase 1 → `01_raw_data_validation.ipynb` ✅ (row counts, distributions)
- Phase 2A → `02_condition_coverage.ipynb` ✅ (dictionary stats, TA coverage %, distribution, spot-checks)
- Phase 2B → `03_design_classification.ipynb` (precision spot-checks)
- Phase 3A → `04_innovation_by_therapeutic_area.ipynb` (the core analysis)
- Phase 2D → `05_drug_normalization.ipynb` (coverage rates, top unmatched)
