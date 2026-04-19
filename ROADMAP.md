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

## Phase 3A: Core Analysis — Design Innovation by Therapeutic Area ✅

**Goal**: The payoff. Join 2A + 2B outputs to answer the primary research question.

**Completed 2026-04-08.**

**Depends on**: Phases 2A and 2B both complete.

**What was built:**

1. **Expanded innovative feature detection** — added 3 new feature types to `src/innovative_features.py`:
   - `digital twin` (30 studies) — virtual patient models for trial augmentation
   - `in silico` (1 study) — computational trial simulations (requires trial/study context)
   - `AI-augmented design` (6 studies) — AI genuinely augmenting trial methodology (AI-driven/guided + design context, reinforcement learning + dosing/allocation context). Excludes AI-as-intervention.
   - Total innovative features: 14 types, 4,714 studies (up from 4,681)

2. **AI/ML mention flag** (`class.ai_mentions`) — broad research flag for any study referencing AI/ML in titles, descriptions, or keywords. Not limited to design methodology; intended for further investigation.
   - 9 term categories: artificial intelligence (1,603), machine learning (927), deep learning (378), neural network (131), large language model (113), ChatGPT/GPT (59), NLP (65), computer vision (59), reinforcement learning (42)
   - 2,600 studies flagged (2.2% of all studies)
   - Wired into `run_classify_design.py` pipeline

3. **Analysis notebook** (`notebooks/04_innovation_by_therapeutic_area.ipynb`) — R kernel, 26 cells:
   - Innovation rate by TA (horizontal bar chart)
   - Feature type × TA heatmap + stacked bar for top 5 TAs
   - Time trends (overall + faceted by top 6 TAs)
   - Phase distribution of innovative designs by TA
   - Geographic patterns (country bar chart + country × TA heatmap)
   - AI/ML section: mention counts by term, TA distribution, overlap with innovative features, AI mention time trend
   - Summary table with all TAs + pivoted feature counts

**Unit tests**: 88 total (24 innovative features + 13 classify design + rest unchanged), all passing.

**Reference**: `resources/Innovative & Emerging Clinical Trial Designs.md` — comprehensive catalog of innovative/emerging trial design types used to guide feature detection gap analysis.

**Module**: `src/innovative_features.py` (expanded), `run_classify_design.py` (updated)
**Notebook**: `notebooks/04_innovation_by_therapeutic_area.ipynb`

---

## Phase 2C: QuickUMLS for Unmapped Conditions — **subsumed into Phase 6**

Originally planned as a standalone dictionary layer for the ~25K conditions without NLM MeSH mappings. After reviewing the landscape — fuzzy conditions (2A.1), drug residuals (2D), and sponsor dedup (2E) all share the same "propose candidate → human verify → promote to `manual`" shape — this phase has been folded into **Phase 6 (HITL Enrichment Platform)**. QuickUMLS becomes one tool among several that a Claude agent can call to generate candidates, rather than an automated dictionary layer.

UMLS license approved 2026-04-13; UMLS 2025AB Metathesaurus zip processed via `scripts/build_quickumls_index.py` (Phase 6D); QuickUMLS index lives at `data/reference/umls/quickumls_index/` (5.4 GB, 10.6M terms).

---

## Phase 2D: Drug Normalization ✅

**Goal**: Map intervention names to canonical drug identifiers.

**Completed 2026-04-09.**

**Approach — three-layer dictionary:**
1. **Control/comparator mapping** (`control-map`, high confidence): Regex-based mapping of placebo, vehicle, saline, standard-of-care, sham, and other control terms to 9 canonical names. Runs first so control terms are excluded from subsequent layers.
2. **MeSH exact match** (`mesh-exact`, high confidence): Normalized intervention name matches `browse_interventions.downcase_mesh_term` within the same study.
3. **ChEMBL local synonym lookup** (`chembl-synonym`, high confidence): Exact match against 128K synonyms extracted from ChEMBL 36 SQLite database into `data/reference/chembl_synonyms.parquet` (2.4MB). No API calls needed — runs in seconds.

**MeSH co-occurrence — removed**: An earlier version included a co-occurrence layer that mapped 1:1 Drug/Biological intervention to mesh-list term within a study. This was removed because the assumption that a single intervention and single mesh term refer to the same compound is unreliable for drugs. Analysis showed ~62% of co-occurrence entries had no name overlap between the intervention and the MeSH term, producing incorrect mappings (e.g., "mosunetuzumab" → "Dexamethasone", "chemotherapy" → "Drug Therapy"). The condition dictionary's co-occurrence layer works better because conditions and MeSH conditions are more directly linked; for drugs, NLM's `browse_interventions` MeSH terms can refer to different compounds than the listed intervention (e.g., a combination study listing only one drug as a formal intervention while NLM maps another). A future V2 could recover some of this coverage via fuzzy name matching with a similarity threshold.

**String preprocessing** (`normalize_drug_name()`): Strips dosage patterns (e.g., "500mg", "100 mg/m2"), route/formulation terms (IV, tablets, injection, etc.), parenthetical content, and normalizes casing/whitespace.

**Output tables:**
- `ref.drug_dictionary` — maps normalized intervention names to canonical drug identifiers
- `norm.study_drugs` — Drug + Biological intervention types only
- Manual entries are preserved across automated rebuilds (same pattern as condition dictionary)

**Unit tests**: 36 tests, all passing. Full suite: 125 tests.

**ChEMBL data source**: ChEMBL 36 (2025-07-28), CC BY-SA 3.0. Reference files in `data/raw/chembl_260410/` (release notes, schema docs, license). SQLite dump downloaded, synonyms extracted to Parquet, dump deleted.

**ChEMBL ID backfill**: After all layers run, a backfill pass looks up each MeSH/control `canonical_name` in the ChEMBL synonym table to populate `canonical_id`. Entries without a ChEMBL ID are typically MeSH-only terms or control substances.

**Module**: `src/normalize_drugs.py`
**Entry point**: `run_normalize_drugs.py`
**Validation**: `notebooks/05_drug_normalization.ipynb`

---

## Phase 2E: Sponsor Deduplication — **subsumed into Phase 6**

Originally planned as a standalone fuzzy-dedup module. Same rationale as 2C: the workflow (normalize → fuzzy candidates → manual review → canonical dictionary) is identical to the condition and drug enrichment patterns, so sponsor dedup becomes one more domain inside **Phase 6**. The deterministic normalization step (case folding, legal suffix stripping via `rapidfuzz`) still lives in `src/normalize_sponsors.py`; only the candidate generation + review layer is absorbed.

**Output once Phase 6 ships**: `norm.study_sponsors(original_name, canonical_name, agency_class)` backed by `ref.sponsor_dictionary` (manual entries promoted from `ref.mapping_candidates` where `domain='sponsor'`).

---

## Phase 6: HITL Enrichment Platform ✅

**Completed 2026-04-14** (slices 6A–6F).

- **6A** — `src/hitl.py` + `ref.mapping_candidates` (domain-tagged shared candidate table); `src/normalize_conditions.py` helpers refactored to thin wrappers; `scripts/migrate_condition_candidates.py` (no-op on current live DB).
- **6B** — `src/normalize_sponsors.py` (case-fold + legal-suffix stripping → `ref.sponsor_dictionary`); `norm.study_sponsors` (209,621 rows, 395 changed by normalization); 378 fuzzy merge candidates (top 2,000 canonicals, WRatio ≥88); `views.study_summary` swapped to canonical sponsors. Run via `run_normalize_sponsors.py`.
- **6C** — `generate_drug_fuzzy_candidates` in `src/normalize_drugs.py`; 903 drug fuzzy proposals on the live DB (e.g. `nab paclitaxel → PACLITAXEL` 95%, `bupivacain → BUPIVACAINE` 95%).
- **6D** — `scripts/build_quickumls_index.py` + `src/quickumls_tool.py`; UMLS 2025AB index built (5.4 GB, 10.6M terms); macOS libiconv linkage workaround documented in `CLAUDE.md`.
- **6E** — `src/enrichment_agent.py` (Anthropic SDK + beta tool runner + Opus 4.6 + adaptive thinking); `src/enrichment_tools.py` (per-domain tools: fuzzy/QuickUMLS/co-occurrence/dictionary lookups); per-domain `max_pending` throttle (default 500); USD budget; SHA cache (`meta.agent_cache`); grounding enforcement (`finalize_proposal` rejects empty trace). Live smoke test: `overweight and obesity → Overweight` (C0497406) with multi-tool rationale, ~$0.10/item Opus 4.6. Run via `run_enrichment_agent.py --domain ... --budget ... --limit ...`.
- **6F** — `apps/review/app.R` (read-only DuckDB Shiny app, one tab per domain, batch approve/reject → Parquet decision log); `run_hitl_sync.py` (imports unapplied logs, promotes approvals, rebuilds affected `norm.*` + `views.study_summary`); idempotent via `meta.decision_log_applied`.

**Tests**: 184 passing + 1 skipped (skip = `test_lookup_without_index_raises`, correctly inactive now that the QuickUMLS index exists).

**Modules** added: `src/hitl.py`, `src/normalize_sponsors.py`, `src/quickumls_tool.py`, `src/enrichment_tools.py`, `src/enrichment_agent.py`. **Entry points**: `run_normalize_sponsors.py`, `run_enrichment_agent.py`, `run_hitl_sync.py`. **Scripts**: `scripts/migrate_condition_candidates.py`, `scripts/build_quickumls_index.py`. **App**: `apps/review/`.

---

## Phase 6 (legacy detail — original plan, preserved for reference)

**Goal**: Consolidate all human-in-the-loop mapping workflows (fuzzy conditions, QuickUMLS conditions, unmatched drug interventions, sponsor dedup) into a single platform: shared candidate schema, a Claude agent that uses matching algorithms as tools to generate candidates, and a read-only R/Shiny app for reviewer promotion.

**Motivation**: Phases 2A.1, 2C, 2D-residual, and 2E all share the same shape — propose mappings from a noisy source, verify, persist as `manual` dictionary entries. Building three separate candidate workflows means duplicating schema, promotion logic, and review UX. A unified platform collapses them and replaces brittle per-domain rules with per-item agent reasoning that is cached, budgeted, and auditable.

**Depends on**: Phase 4 (Views) recommended first so the review app can show coverage dashboards; UMLS license approval (for the QuickUMLS tool, not blocking other domains).

**Architecture:**

1. **Shared candidate plumbing** (`src/hitl.py`) — generalize `ref.condition_candidates` into a domain-tagged `ref.mapping_candidates` (columns: `domain`, `source_value`, `canonical_term`, `canonical_id`, `score`, `study_count`, `source`, `rationale`, `tool_trace` JSON, `status`, timestamps). Lift existing helpers (`generate_candidates`, `promote_candidates`, `export_candidates_csv`, `import_reviewed_csv`) from `src/normalize_conditions.py` into `hitl.py`, parameterized by domain. Add `import_decision_log()` for Shiny roundtrip.

2. **Claude-agent candidate generator** (`src/enrichment_agent.py`) — one entry per domain (`condition`, `drug`, `sponsor`). Matching algorithms become **tools** the agent calls: fuzzy MeSH / fuzzy ChEMBL / fuzzy sponsor, QuickUMLS lookup, co-occurrence lookup, `normalize_drug_name`, dictionary lookups. For easy items the agent calls one tool and returns; for hard items it investigates across tools. Guardrails: SHA-cached by `(domain, normalized input)` in `meta.agent_cache` so reruns are free; per-run USD budget with resumable checkpointing; system prompt enforces tool-grounding (no ungrounded mappings); golden eval set (`tests/fixtures/enrichment_golden.json`, ~200 labeled items per domain) gates prompt/tool changes.

3. **R/Shiny review app** (`apps/review/app.R`) — read-only DuckDB connection, one tab per domain, sortable `DT::datatable` with filters (status, score, study_count, source), expandable row showing agent rationale + tool trace, batch approve/reject actions. Writes decisions to `data/reviews/decisions_YYYY-MM-DD.parquet` (not directly to DuckDB — avoids the single-writer constraint). Pipeline imports the decision log on the next run via `run_hitl_sync.py`.

**Entry points:**
- `run_enrichment_agent.py --domain {condition|drug|sponsor} --budget <USD> [--resume]`
- `run_hitl_sync.py` — imports latest decision log, promotes approvals, rebuilds affected dictionaries
- `scripts/build_quickumls_index.py` — one-shot UMLS Metathesaurus index build (gated on license)

**Modules**: `src/hitl.py`, `src/enrichment_agent.py`, `src/normalize_sponsors.py` (deterministic piece), `apps/review/app.R`
**Config**: `ANTHROPIC_API_KEY` in `.env`; model + budget defaults in `config/settings.py`
**Migration**: existing `ref.condition_candidates` rows migrated into `ref.mapping_candidates` with `domain='condition'`; old table dropped; `notebooks/02a_condition_enrichment.ipynb` continues to work via delegated helpers.

---

## Phase 4: Analytical Views (Layer 4) ✅

**Goal**: Denormalized, query-ready tables.

**Completed 2026-04-14.**

- `views.study_summary` — one row per study (119,753 rows, matches `raw.studies` exactly)
- Joins: `raw.studies` + `class.study_design` + `class.innovative_features` + `class.ai_mentions` + `norm.study_therapeutic_areas` + `norm.study_conditions` + `norm.study_drugs` + `raw.countries` + `raw.sponsors`
- Multi-valued dimensions aggregated into DuckDB `LIST` columns (therapeutic_areas, canonical_drugs, countries, etc.) plus 14 per-feature booleans (`is_adaptive`, `is_basket`, ...) for easy column-wise filtering
- Convenience scalars: `start_year`, `has_innovative_feature`, `has_ai_mention`, `primary_therapeutic_area` (most-frequent ancestor, alphabetical tiebreak)
- Sponsor columns are **interim**: `lead_sponsor_name` / `collaborator_names` carry raw un-normalized strings from `raw.sponsors`; will be replaced by `norm.study_sponsors` once Phase 6 ships
- Coverage (matches Phase 2/3A): 78.2% ≥1 TA | 20.5% ≥1 mapped drug | 3.9% innovative feature | 2.2% AI mention | 100% lead sponsor

**Unit tests**: 14 new tests in `tests/test_views.py` (list aggregation, boolean flag derivation, primary TA tiebreak, excluded `removed=TRUE` countries, missing-design-row nulls, sponsor fallback). Full suite: 139 tests, all passing.

**Module**: `src/views.py`
**Entry point**: `run_views.py`
**Validation**: `notebooks/06_views_validation.ipynb`

---

## Phase 7: Data Model Hardening

**Added 2026-04-14 from first-use observations during Phase 6F review.** Four architectural concerns surfaced after running the HITL app against live data. Phase 7A, 7B, 7C, and 7E are shipped; 7D remains deferred.

### Sequencing rationale

Each slice is independently valuable and ordered by dependency:

1. **7A — Rejection persistence** (cheapest; no migration). Unblocks reviewer productivity immediately.
2. **7B — Canonical entity tables** (foundation for 7C and cleaner analytics).
3. **7C — `enriched.*` layer** (depends on 7B). Decouples views from `raw.*`, enabling safe re-extraction.
4. **7D — Sponsor dedup v2 via agent** (depends on 7B + Phase 6E). Largest scope; most likely to evolve in design as we learn from 6E in production.
5. **7E — Reference source versioning** (naturally pairs with 7B). Gives every derived row a reproducibility provenance stamp.

Only after 7A–7C (and ideally 7E) land should Phase 5 (refresh automation) ship.

### Phase 7A: Unify rejection semantics ✅

**Completed 2026-04-15.**

**Symptom** (before): Rejected mappings regenerated inconsistently across domains — conditions over-aggressively banned any rejected `source_value` wholesale; drugs and sponsors only deduped exact `(source_value, canonical_term)` triples.

**Landed**:
- `src/hitl/candidates.py` centralizes the throttle. `REJECT_THROTTLE = 2`: after 2 distinct canonicals rejected for the same `(source_value, source)`, future candidates for that source are skipped.
- `insert_candidates()` enforces the throttle uniformly; generators no longer each reinvent the filter.
- New `hidden` decision status suppresses a `(source_value, source)` entirely, regardless of canonical. Shiny app adds a **"Hide source"** button with confirmation modal.
- Decision log parquet schema accepts `decision ∈ {approved, rejected, hidden}`; `import_decision_log` maps all three to `ref.mapping_candidates.status`.
- Removed the over-aggressive `WHERE status IN (approved,rejected)` `NOT IN` filter from `src/transform/normalize_conditions.py`.

**Tests**: 4 new tests in `tests/hitl/test_hitl.py` (throttle at N=2, single-rejection still open, hidden blocks source, decision-log hidden). Full suite stayed green.

### Phase 7B: Canonical entity tables ✅ + Phase 7E: Reference source versioning ✅

**Completed 2026-04-15 (shipped together — schema migrations touched adjacent tables).**

**Symptom** (before): `ref.condition_dictionary.canonical_term` was a free-text MeSH string; `ref.drug_dictionary.canonical_name` was free text despite having `canonical_id` (ChEMBL); `ref.sponsor_dictionary.canonical_name` was pure free text. Downstream `norm.*` and `views.*` keyed on these strings. External reference data (ChEMBL, UMLS, MeSH TA mapping) was loaded from hardcoded paths with no version metadata.

**Landed — 7B**:
- New `entities` schema with BIGINT surrogate PKs and external-ID crosswalks:
  - `entities.condition(condition_id, origin, mesh_descriptor_id UNIQUE, umls_cui UNIQUE, canonical_term, source_versions)` — seeded with all ~31k MeSH descriptors via `scripts/load_mesh_descriptors.py` (streams `desc2026.xml`, ~5s).
  - `entities.drug(drug_id, origin, canonical_name, chembl_id UNIQUE, mesh_descriptor_id, unii, source_versions)` — seeded from ChEMBL 36 parquet (47,960 pref_names).
  - `entities.sponsor(sponsor_id, origin, canonical_name UNIQUE, ror_id UNIQUE, ringgold_id, source_versions)` — seeded from deterministic `normalize_sponsor_name()` output on `raw.sponsors`.
- `origin` column on all three tables (`mesh | chembl | aact | manual | …`) answers *where* an identity came from, independently of version provenance.
- Dictionary tables rewritten to FK into entities: `ref.condition_dictionary(condition_name PK, condition_id, …)`, `ref.drug_dictionary(source_name PK, drug_id, …)`, `ref.sponsor_dictionary(source_name PK, sponsor_id, …)`.
- `norm.*` re-keyed: `norm.study_conditions.condition_id`, `norm.study_drugs.drug_id`, `norm.study_sponsors.sponsor_id`, `norm.study_therapeutic_areas.condition_id`.
- `views.study_summary` joins through `entities.*` for labels; output schema unchanged (`canonical_conditions`, `canonical_drugs`, `chembl_ids`, `lead_sponsor_name`, `collaborator_names`).
- `src/hitl/candidates.py::promote_candidates` refactored to resolve/create entities via `entities.upsert_{condition,drug,sponsor}` then insert the dictionary row with `*_id`. HITL-promoted entities are stamped `origin='manual'`.

**Landed — 7E**:
- `meta.reference_sources(source_name, version, acquired_at, built_at, path, checksum, is_active, notes, PK (source_name, version))`. `src/reference_sources.py` exposes `ensure_table`, `register_source`, `get_active_path`, `get_active_version`, `active_versions_snapshot`, `compute_checksum`.
- Directory reorg: `data/reference/{chembl/36/synonyms.parquet, umls/2025AB/quickumls_index/, mesh_ta_mapping/v1/mapping.json, mesh/2026/desc.xml}`. One-time move via `scripts/bootstrap_reference_sources.py`.
- Loaders now resolve paths via `reference_sources.get_active_path(...)` — `src/transform/normalize_drugs.py` (ChEMBL), `src/agent/quickumls_tool.py` (UMLS index), `src/transform/therapeutic_areas.py` (TA mapping), `scripts/load_mesh_descriptors.py` (MeSH).
- Entity rows stamp `source_versions` JSON (e.g. `{"chembl": "36"}`, `{"mesh": "2026"}`) at creation for row-level provenance.

**Invariant**: *entities come only from trusted external vocabularies (MeSH, ChEMBL) or from approved HITL decisions — never from unresolved candidates.* `ref.mapping_candidates` stays keyed on canonical_term/canonical_id strings; `promote_candidates` resolves/creates the entity at approve time. This aligns the "unresolved → identified" gate with the idempotency boundary of each normalize script.

**Regenerate results (2026-04-15, 119,753 studies)**:
- `entities.condition`: 31,505 rows (31,110 MeSH seed + 395 cancer-synonym-invented), all `origin='mesh'`.
- `entities.drug`: 50,125 rows (47,960 chembl + 2,149 mesh + 16 manual).
- `entities.sponsor`: 37,755 rows, all `origin='aact'`.
- Dictionary FK integrity: **zero orphan FKs** across all three tables.
- `norm.study_conditions`: 171,222 mapped / 253,711 total (67%); 84.7% of studies have ≥1 canonical condition.
- `norm.study_drugs`: 47,454 mapped / 82,334 total (57.6%); 20.6% of studies have ≥1 mapped drug.
- `norm.study_sponsors`: 209,621 rows, 100% mapped to `entities.sponsor`.
- `norm.study_therapeutic_areas`: 93,606 distinct studies (78.2%).
- `views.study_summary`: 119,753 rows — matches `raw.studies` exactly.
- HITL replay: 29 decision-log parquets applied (17 approved / 19 rejected / 16 promoted to manual dict entries); 1 manual condition + 19 manual drug + 0 manual sponsor dictionary rows survived.
- `meta.reference_sources`: 4 active rows — `chembl@36`, `mesh@2026`, `mesh_ta_mapping@v1`, `umls@2025AB`.

**Tests**: 211 passed, 1 skipped (up from 191 pre-7B). New modules tested end-to-end.

**Not migrated intentionally** (deferred to 7C): `views.study_summary` still reads directly from `raw.studies`, `raw.interventions`, `raw.countries`, `raw.browse_conditions`. Entity-keyed FKs for drugs/conditions/sponsors flow through `norm.*`, but raw surrogates remain on the study/country/intervention side.

### Phase 7C: Decouple analytical view from `raw.*` ✅

**Completed 2026-04-17.**

**Symptom** (before): `src/transform/views.py` read directly from `raw.studies`, `raw.interventions`, and `raw.countries` alongside the `class.*` / `norm.*` / `entities.*` layers. Cheap while re-extraction was manual and infrequent, but a hazard for Phase 5 automation: a re-extract could transiently expose partial data to mart consumers, and mart output had no way to pin itself to a specific upstream snapshot. (The roadmap originally listed `raw.browse_conditions` too; spot-check confirmed it already flowed through `norm.study_therapeutic_areas`.)

**Design principle** (from discussion): promote only where the transformation has real cross-consumer value or where the schema boundary itself is load-bearing. Don't rote-mirror `norm.*` / `class.*` — they're already stable analytical inputs. Do promote the raw reads so the single-rule contract ("the mart reads zero `raw.*`") is enforceable and extract-safe.

**Landed**:
- New `enriched` schema with three row-level tables. `src/transform/promote.py::promote_to_enriched()` is the narrow projection module; `run_promote_enriched.py` is the entry point.
  - `enriched.studies` (119,753 rows) — anchor columns + derived `start_year = YEAR(start_date)`. Only columns the mart consumes today; future columns added when a concrete consumer appears.
  - `enriched.interventions` (207,891 rows) — row-level projection; aggregation stays mart-side.
  - `enriched.countries` (164,068 rows) — `removed = FALSE OR removed IS NULL` filter applied once upstream, instead of being re-asserted in every consumer query.
- `meta.enriched_tables` registry: one row per enriched table stamped on each projection run with `last_built_at` (wall clock), `extraction_date` (pulled from `MAX(meta.extraction_log.extract_date)`), `source_expression`, `row_count`, `notes`. Answers *when was this rebuilt?* and *which raw extract does it reflect?*
- `src/transform/views.py` rewritten to read `enriched.studies` / `enriched.interventions` / `enriched.countries` instead of raw. The `start_year` derivation and the `removed` filter moved upstream with them. `grep -n "raw\." src/transform/views.py` returns zero matches.
- Output contract preserved: `views.study_summary` remains 119,753 rows with identical coverage (78.2% ≥1 TA, 20.6% ≥1 mapped drug, 3.9% innovative, 2.2% AI, 100% lead sponsor).

**Not migrated intentionally**: `norm.*` and `class.*` continue to read directly from `raw.*` — they *produce* derived data one abstraction below the mart, and fronting them with enriched mirrors would add a maintenance surface for no contract value. Per-row `source_extracted_at` stamps on enriched tables are also deferred; the table-level stamp is enough until longitudinal analysis actually needs it.

**Tests**: 219 passed, 1 skipped (up from 211 pre-7C). New `tests/transform/test_promote.py` covers schema creation, `start_year` derivation, removed filter, registry provenance, and idempotency. `tests/transform/test_views.py` fixtures retargeted to `enriched.*` (the `test_countries_excludes_removed` contract migrated to `test_promote.py` where it now belongs).

### Phase 7D: Sponsor dedup v2 — anchor-driven agent

**Symptom**: `src/transform/normalize_sponsors.py::generate_sponsor_fuzzy_candidates` produces a queue with high false-positive rate. Spot-check of live data: legitimately distinct institutions collide on `rapidfuzz.WRatio` (e.g. `Hunan Provincial People's Hospital` vs `Hunan Cancer Hospital`), while parent/subsidiary variants that SHOULD merge (e.g. `Novartis` vs `Novartis Pharmaceuticals`) look identical in score space to those false positives. Reviewer burden is high and signal is low.

**Root cause**: String similarity doesn't encode org identity. Pharma parent/subsidiary, university/hospital-system, and acronym-vs-full-name relationships all need semantic reasoning — and the existing sponsor flow only produces "variant → canonical" mappings; two `sponsor_id`s never actually merge.

**Approach**: Invert the search. Anchor on a curated set of high-frequency canonicals (top ~200 by `study_count`). For each lower-frequency canonical, the Phase 6E enrichment agent asks "is this a variant of any anchor?" using industry knowledge plus tool evidence (anchor fuzzy lookup, co-occurrence signal, ROR registry hierarchy). `rapidfuzz` becomes a coarse gate feeding the agent's input set, not the reviewer queue. Approved proposals execute a **true merge** of the child `sponsor_id` into the anchor, preserving both rows for audit.

**Depends on**: 7B ✅ (stable `entities.sponsor.sponsor_id`), 7E ✅ (reference versioning for the anchor set), 6E ✅.

#### Design decisions

- **Merge model**: self-FK `merged_into_id` on `entities.sponsor`. Child row preserved with its original `canonical_name` (audit + reversibility); `UNIQUE(canonical_name)` stays load-bearing since parent and child names differ by construction.
- **Anchor set**: auto top-N by `study_count` (N=200) plus curated override file `data/reference/sponsor_anchors.json` with `include` / `exclude` arrays. Regenerated per refresh; registered in `meta.reference_sources` as `sponsor_anchors@<checksum>`.
- **Agent tools** (additions to `DOMAIN_TOOLS["sponsor"]` beyond existing `fuzzy_sponsor`): `sponsor_anchor_lookup` (rapidfuzz ≥70 against anchor set, cached on ToolContext), `sponsor_co_occurrence` (canonicals that frequently co-sponsor with the input — strong parent/subsidiary signal), `sponsor_ror_api` (ror.org public API with 30-day cache + backoff). MeSH pharma skipped (ROR covers same ground with better recall). Claude's domain knowledge invited via system-prompt update; no separate tool.
- **Branch scope**: all variants merge into their parent (`Pfizer Germany` → `Pfizer`). Geographic fidelity lives natively in `raw.countries` at per-study-site level.
- **`norm.study_sponsors` is not rewritten on merge** — the `sponsor_id` keeps pointing at the raw-observed child for audit. Resolution happens at the view layer.

#### Architecture

**1. `entities.sponsor` schema changes** (`src/entities.py::ensure_schema`):

```sql
ALTER TABLE entities.sponsor ADD COLUMN IF NOT EXISTS merged_into_id BIGINT;
ALTER TABLE entities.sponsor ADD COLUMN IF NOT EXISTS merged_at TIMESTAMP;
ALTER TABLE entities.sponsor ADD COLUMN IF NOT EXISTS merge_rationale JSON;
```

`upsert_sponsor` gets a defensive one-hop flatten: if a resolved row has `merged_into_id IS NOT NULL`, return the parent's `sponsor_id`. Prevents ETL seeders from reintroducing merged rows.

**2. `merge_sponsor` helper** (`src/entities.py`):

New function `merge_sponsor(duck_conn, child_id, parent_id, rationale) -> parent_id`:
1. Validate: both rows exist; neither already merged; not merging into self; parent is not itself merged (reject and force reviewer to re-anchor rather than auto-flatten at write time).
2. `UPDATE entities.sponsor SET merged_into_id, merged_at, merge_rationale WHERE sponsor_id = child_id`.
3. `UPDATE ref.sponsor_dictionary SET sponsor_id = parent_id WHERE sponsor_id = child_id` — ensures the next `create_study_sponsors` rebuild lands fresh rows on the parent.
4. Idempotent: repeating a completed merge is a no-op.

**3. Three new agent tools** (`src/agent/enrichment_tools.py`):

- `sponsor_anchor_lookup(ctx, text, limit=10)`: `rapidfuzz.WRatio ≥ 70` of `normalize_sponsor_name(text)` against the active anchor set. Anchor set cached lazily on `ToolContext` via `_anchor_set` property reading `meta.reference_sources` → the active JSON path. Returns `[{canonical_term, sponsor_id, score, study_count}, ...]`.
- `sponsor_co_occurrence(ctx, text, limit=5)`: SQL over `norm.study_sponsors` joined to itself on `nct_id`, surfacing canonicals that co-sponsor ≥1 study with `text`. Returns `[{canonical_name, sponsor_id, shared_studies}, ...]`.
- `sponsor_ror_api(ctx, text, limit=5)`: new module `src/agent/ror_tool.py`. `GET https://api.ror.org/organizations?query=<text>` with exponential backoff on 429/5xx (max 3 attempts, User-Agent `clinical-trials-etl/0.1`). Cache in new `meta.ror_cache(query_sha PK, response_json, fetched_at)` (distinct from `meta.agent_cache`, which is per-agent-item, not per-query). 30-day TTL, lazy refresh. Returns `[{canonical_name, ror_id, country, aliases, parent, score}, ...]`. Error shape `{"error": "...", "results": []}` — a proposal citing only an error-trace fails `finalize_proposal` grounding, unchanged.

System prompt bumped (`AGENT_SYSTEM_PROMPT_VERSION`) with a sponsor-specific paragraph: prefer anchor matches over novel canonicals; treat ROR as ground truth for parent/subsidiary calls; require either a ROR hit or ≥5 co-occurring studies before proposing a merge.

**4. `Proposal` and `ref.mapping_candidates` schema**:

Add nullable `anchor_sponsor_id BIGINT` column to `ref.mapping_candidates`, `Proposal` dataclass, and decision-log parquets. Merge-vs-mapping distinction is **implicit in `anchor_sponsor_id IS NOT NULL`** — no new enum, no downstream branching beyond the promote path.

**5. Anchor set generation** — new module `src/transform/sponsor_anchors.py`:

- `build_anchor_set(duck_conn, top_n=200)`: top-N by distinct study impact from `ref.sponsor_dictionary` + `norm.study_sponsors`, filtered against the curated `include`/`exclude` arrays in `data/reference/sponsor_anchors.json`. Curated includes must already resolve to an `entities.sponsor` row (log + skip otherwise). Writes `meta.sponsor_anchor_set(sponsor_id PK, canonical_name, study_count, origin ∈ {auto, curated_include})`.
- `register_anchor_set`: bumps `meta.reference_sources` with version = checksum of the JSON + `top_n`.
- Invoked from `run_normalize_sponsors.py` after `build_sponsor_dictionary`, before the agent runs.

**6. Views merge resolution** (`src/entities.py` + `src/transform/views.py`):

New recursive view `entities.sponsor_resolved(sponsor_id, effective_sponsor_id)` flattens chains:

```sql
CREATE OR REPLACE VIEW entities.sponsor_resolved AS
WITH RECURSIVE chain(sponsor_id, hop, head_id) AS (
    SELECT sponsor_id, 0, sponsor_id FROM entities.sponsor
  UNION ALL
    SELECT c.sponsor_id, c.hop + 1, s.merged_into_id
    FROM chain c JOIN entities.sponsor s ON c.head_id = s.sponsor_id
    WHERE s.merged_into_id IS NOT NULL AND c.hop < 10
)
SELECT sponsor_id,
       last_value(head_id) OVER (PARTITION BY sponsor_id ORDER BY hop
                                 ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)
         AS effective_sponsor_id
FROM chain;
```

`views.study_summary` swaps `LEFT JOIN entities.sponsor` for a two-hop join through `sponsor_resolved`. Both joins are indexed PK lookups. `hop < 10` guards against cycles (belt and suspenders; `merge_sponsor` validation already prevents them).

**7. Promote path** (`src/hitl/candidates.py`):

In `promote_candidates` for `domain='sponsor'`, branch on `anchor_sponsor_id`:
- Present → call `merge_sponsor(child=resolve_by_name(source_value), parent=anchor_sponsor_id, rationale=row.rationale)`. Skip dictionary insert (merge_sponsor already rewrote).
- Absent → existing mapping path, unchanged.

**8. Shiny UI** (`apps/review/app.R`, sponsor tab):

Minimum viable:
- Surface `anchor_sponsor_id` column in the candidates DataFrame.
- When present on a row, add a visible cue (bold canonical_term or a "merge" badge).
- Reactive approve-button label: all rows have `anchor_sponsor_id` → "Merge into anchor"; none → "Approve mapping"; mixed → disable with tooltip. No new tab.
- Decision-log writer adds `anchor_sponsor_id` to the parquet column set.

**9. Legacy candidate disposition**:

`generate_sponsor_fuzzy_candidates` demoted: removed from `run_sponsor_pipeline`; function kept with deprecation docstring for one release, then deleted. One-shot migration (gated by `meta.migration_log` row): `UPDATE ref.mapping_candidates SET status='hidden' WHERE domain='sponsor' AND source='fuzzy' AND status='pending'`. Previously approved/rejected fuzzy rows untouched. The agent will re-propose any merges still valid under anchor-driven scoring.

**10. Agent input selection tweak** (`src/agent/enrichment_agent.py::_select_pending_inputs`):

Sponsor query adds `AND sponsor_id NOT IN (SELECT sponsor_id FROM meta.sponsor_anchor_set)` — anchors never get proposed as merge targets of other anchors. If two anchors legitimately should merge, it's a curation-file decision, not an agent decision.

#### Rollout

1. Schema additions + `sponsor_resolved` view + `views.study_summary` join swap (behavior unchanged, all `merged_into_id` NULL). Unit-test coverage.
2. Anchor set module + `meta.reference_sources` registration. Eyeball output JSON.
3. Three new agent tools behind feature flag `SPONSOR_AGENT_V2_ENABLED` in `config/settings.py`. When false, sponsor domain uses only `fuzzy_sponsor` (existing behavior). Smoke run: 20 canonicals, $2 budget.
4. `anchor_sponsor_id` wiring through `Proposal`, `ref.mapping_candidates`, decision log.
5. Shiny UI update (after 4, so the column exists when read).
6. `merge_sponsor` + `promote_candidates` branching. `promote_candidates` refuses merge paths if feature flag off.
7. One-shot hide of legacy fuzzy candidates + remove `generate_sponsor_fuzzy_candidates` from the pipeline.
8. Full production run: anchor regen → agent at $10 budget → reviewer session → promote.

Per-step validation: `views.study_summary` row count unchanged; `lead_sponsor_name` null-rate non-increasing; `entities.sponsor` row count non-decreasing (we never delete).

#### Risks + mitigations

- **False-positive parent/subsidiary merge** (e.g. Pfizer vs. Pfizer CentreOne, a CDMO that is arguably distinct). Mitigation: system-prompt grounding requires either a ROR tool hit or ≥5 co-occurring studies before a merge proposal is admissible.
- **ROR API reliability** — free endpoint, no SLA. Mitigation: 30-day cache, graceful empty-return on failure, never blocks other tools.
- **Cost per item** — sponsor tier adds ROR round-trip (~1 extra tool call). Estimate ~$0.04/item on Haiku, ~$0.20/item on Sonnet. Default Haiku; escalate on high abstain rate.
- **Merge-chain runaway** — `hop < 10` guard in resolved view + `merge_sponsor` rejects chain-into-merged-parent at write time.
- **UNIQUE(canonical_name) on rollback** — non-issue; reversal zeroes out `merged_into_id`, name unchanged.

#### Out of scope

- Automatic un-merge UI (DB supports reversal; operator runs SQL if needed).
- ROR-based seeding of `entities.sponsor` on fresh installs (future Phase 7F candidate).
- Country-level sponsor-site aggregation tables (geographic fidelity already in `raw.countries`).
- Ringgold ID population (column exists, remains unused).
- Collaborator hierarchy beyond lead sponsor — `collaborator_names` in `views.study_summary` inherits the resolved name automatically via the same join swap.

#### Modules + entry points

**New**:
- `src/agent/ror_tool.py` — ROR API client + `meta.ror_cache`.
- `src/transform/sponsor_anchors.py` — `build_anchor_set`, `register_anchor_set`.
- `data/reference/sponsor_anchors.json` — curated `include` / `exclude` override file.
- `tests/test_sponsor_merge.py` — `merge_sponsor` happy paths + rejects + chain resolution.
- Integration fixture extension in `tests/agent/test_enrichment_agent.py` — 3 anchors + 8 candidate canonicals covering merge, abstain, and true-negative cases.

**Modified**:
- `src/entities.py` — `merged_into_id` DDL, `merge_sponsor`, `sponsor_resolved` view, `upsert_sponsor` flatten guard.
- `src/hitl/candidates.py` — `ref.mapping_candidates.anchor_sponsor_id` column; sponsor-domain promote branching.
- `src/agent/enrichment_tools.py` — `sponsor_anchor_lookup`, `sponsor_co_occurrence`, register in `DOMAIN_TOOLS["sponsor"]`; `ToolContext._anchor_set` property.
- `src/agent/enrichment_agent.py` — `Proposal.anchor_sponsor_id`, `_select_pending_inputs` anchor-exclusion, system-prompt sponsor paragraph, `AGENT_SYSTEM_PROMPT_VERSION` bump.
- `src/transform/normalize_sponsors.py` — remove `generate_sponsor_fuzzy_candidates` from `run_sponsor_pipeline`; deprecation docstring.
- `src/transform/views.py` — join swap to `entities.sponsor_resolved`.
- `apps/review/app.R` — anchor column + reactive approve-button label + decision-log column.
- `config/settings.py` — `SPONSOR_AGENT_V2_ENABLED` feature flag.
- `run_normalize_sponsors.py` — invoke anchor-set build.

#### Verification

1. `pytest tests/` — new `test_sponsor_merge.py` covers schema, `merge_sponsor` invariants, `sponsor_resolved` chain depth 0/1/2 + cycle guard. Integration fixture exercises the full candidate → merge → view-resolution path.
2. Feature-flagged smoke: `python run_enrichment_agent.py --domain sponsor --budget 2.00 --limit 20` with `SPONSOR_AGENT_V2_ENABLED=true` — eyeball the proposals' rationales + tool traces in Shiny.
3. Anchor set eyeball: open `meta.sponsor_anchor_set` after first build; confirm top pharma + academic centers present, no surprising auto-picks, curated includes resolved.
4. Promote a small batch (e.g., 5 high-confidence pharma subsidiaries) → `run_hitl_sync.py` → confirm `entities.sponsor.merged_into_id` set, `ref.sponsor_dictionary` re-pointed, `views.study_summary.lead_sponsor_name` collapses the child into the parent.
5. Run `views.study_summary` row-count parity check post-merge (should equal `enriched.studies` row count). `collaborator_names` inherits the resolved name — spot check.

### Phase 7E: Reference source versioning ✅

Shipped alongside 7B above. See combined "Phase 7B + 7E" section for details.

**Limits** (preserved from original plan): reproducibility is *traceable* ("this mapping came from ChEMBL 36 + UMLS 2025AB") but not *automatic* — re-running last quarter's extraction against last quarter's references requires retaining those directories on disk. That's policy, not code.

### Phase 7F: Drug/intervention dedup v2 — agent-assisted

**Symptom**: `generate_drug_fuzzy_candidates` (Phase 6C) produces a pending queue that is too noisy for direct HITL review. ~800 candidates currently sit in `ref.mapping_candidates` with `domain='drug'`, `source='fuzzy'`, `status='pending'`; a spot-check puts the majority as incorrect (e.g., dosage-form variants colliding with chemically distinct compounds, brand/generic misalignments, salt-form confusion).

**Root cause** (same shape as Phase 7D sponsor problem): `rapidfuzz` string similarity doesn't encode chemical identity. Brand↔generic (e.g., Keytruda ↔ pembrolizumab), salt/ester variants (succinate, hydrochloride, fumarate, mesylate), combination products, and dose-form naming ("aspirin 81mg" vs "aspirin") all look either similar or dissimilar for wrong reasons in WRatio space.

**Proposed direction** (mirrors 7D): demote fuzzy matching to a coarse gate that narrows the agent's input set rather than the reviewer's queue. The Phase 6E enrichment agent evaluates each candidate with tools that encode actual chemical identity — ChEMBL synonym + salt-parent lookup, MeSH intervention co-occurrence, potentially RxNorm or UNII crosswalks, drug-class/parent queries. Merge semantics would parallel `entities.sponsor.merged_into_id` (salt → parent compound, brand → generic), using a `merged_into_id` self-FK on `entities.drug`.

**Depends on**: 7B ✅ (stable `entities.drug.drug_id`), 6E ✅, 7D ✅ (pattern + merge plumbing proven).

**Proposed near-term steps** (before a full design): one-shot migration to hide the existing ~800 fuzzy drug candidates (`status='pending' → 'hidden'`, analogous to `scripts/migrate_sponsor_fuzzy_hidden.py`); remove `generate_drug_fuzzy_candidates` from the standard pipeline; keep the function as a deprecated gate. A design pass when Phase 5 is shipped.

**Status**: deferred — tracked, not scheduled. Pending fuzzy drug queue should be treated as ignorable in its current form.

---

## Phase 5: Refresh Automation

**Goals**:
1. **Longitudinal change tracking.** The current extract filter (5 "active-ish" statuses) makes ~480K terminal-state trials invisible, so transitions (RECRUITING → COMPLETED, TERMINATED, WITHDRAWN) cannot be detected. Remove the filter, take full AACT snapshots, and emit per-trial change events so the dataset answers "what's new, what changed, what finished" run-over-run.
2. **Progressive enrichment.** Every refresh cascades all HITL-approved mappings globally (via the existing `run_hitl_sync.py`) and runs the Phase 6E enrichment agent against each domain — bounded by the agent's existing `max_pending` + USD-budget throttles, so work naturally scales to reviewer throughput.

**Depends on**: 7A ✅, 7B ✅, 7C ✅ (mart decoupled from `raw.*`), 7E ✅. Requires the `enriched.*` projection layer so a re-extract doesn't transiently expose partial data to mart consumers.

### Design decisions

- **Status filter removed.** Extract all AACT studies (~600K, ~17M total rows across all extracted tables — roughly 5× current). Retains full-rebuild semantics downstream; `enriched.*` and `views.study_summary` CTAS remain seconds even at 5× scale. `config/settings.py::ACTIVE_STATUSES` is demoted to a documentation constant; downstream code must not use it for extraction.
- **Event-log history model.** New `meta.trial_change_events` table; current snapshot continues to live in `enriched.studies`. The raw Parquet snapshots at `data/raw/YYYY-MM-DD/` (already retained per run) are the diff source — no new storage pattern.
- **Tracked change surface**: `overall_status` transitions, key dates (`primary_completion_date`, `completion_date`, `results_first_submitted_date`), enrollment (`enrollment` + `enrollment_type`), and study content (conditions, interventions, sponsors, phase).
- **Full rebuild, not incremental.** At 5× scale the full rebuild pattern still takes seconds-to-minutes end-to-end. Incremental normalization is deferred until profiling justifies it.

### Architecture

**1. Orchestrator — `run_pipeline.py` + `src/pipeline/orchestrator.py`.** Direct Python imports (every existing entry point already exposes a callable). Orchestrator opens **one** `duck_conn` and threads it through every phase — DuckDB's single-writer lock makes multi-connection orchestration a foot-gun. Requires one small refactor: `src/extract/aact.py::run_extraction()` (currently opens/closes its own connection at `aact.py:140,179`) accepts an optional external `duck_conn`, matching the existing pattern in `src/transform/promote.py::promote_to_enriched(duck_conn)`. Failure handling writes `meta.pipeline_runs` with `status='failed'` + `failed_phase` and re-raises; every phase is idempotent drop-and-recreate, so mid-run failures leave a consistent state.

**CLI**: `python run_pipeline.py [--skip-extract] [--skip-agent] [--budget-per-domain 2.00] [--dry-run] [--cohort-expansion]`. `--dry-run` reuses the latest existing Parquet snapshot instead of hitting AACT. `--cohort-expansion` is an explicit flag for the one post-filter-removal run that suppresses the `first_seen` flood (see Edge cases).

**2. Change-event detection — `src/transform/change_events.py`** (entry point `run_change_events.py`).

`meta.trial_change_events` schema:
```
event_id                BIGINT PRIMARY KEY         -- from meta.change_events_seq
run_id                  VARCHAR NOT NULL           -- links to meta.pipeline_runs
nct_id                  VARCHAR NOT NULL
extraction_date         DATE NOT NULL              -- current (newer) snapshot
prior_extraction_date   DATE                       -- NULL for first_seen
change_type             VARCHAR NOT NULL           -- enum below
field                   VARCHAR                    -- NULL for first_seen / dropped
from_value              VARCHAR                    -- JSON array for multi-valued
to_value                VARCHAR
detected_at             TIMESTAMP DEFAULT current_timestamp

UNIQUE (run_id, nct_id, field, change_type)        -- retry-safe
INDEX (nct_id, extraction_date)
INDEX (change_type, extraction_date)
```

`change_type` enum: `first_seen`, `dropped`, `status_transition`, `date_changed`, `enrollment_changed`, `phase_changed`, `conditions_changed`, `interventions_changed`, `sponsors_changed`.

Algorithm (`detect_and_record_changes(conn, run_id, current_extract_date)`):
1. Resolve prior snapshot from `meta.extraction_log` (`MAX(extract_date) < current` for `table_name='studies'`). If none, emit nothing — this is the first run.
2. Mount both snapshots via `read_parquet('data/raw/<date>/<table>.parquet')`. No dependency on prior `raw.*` tables still being live.
3. **Cheap gate**: skip field-level diff for studies where `last_update_submitted_date` is unchanged. Appearance/disappearance diffs run unconditionally. Expected >80% skip rate on weekly cadence.
4. Appearance: `current LEFT JOIN prior … WHERE prior IS NULL` → `first_seen`. Reverse → `dropped` (should be rare post-filter-removal; log loudly when observed).
5. Scalar fields on `studies`: one `INSERT … SELECT` per field using `IS DISTINCT FROM`.
6. Multi-valued content (conditions, interventions, sponsors): set-equality via `list_sort(array_agg(value))` grouped by nct_id; diff between snapshots; emit one event per changed study with from/to as JSON arrays (DuckDB `to_json`). No per-element diffs — noisy and analytically weaker than a "set changed" signal.
7. Single transaction; return counts for `meta.pipeline_runs`.

**3. Run audit — `meta.pipeline_runs`**:
```
run_id                  VARCHAR PRIMARY KEY   -- uuid4()
started_at              TIMESTAMP NOT NULL
completed_at            TIMESTAMP
status                  VARCHAR NOT NULL      -- running | completed | failed
failed_phase            VARCHAR
extraction_id           VARCHAR               -- links to meta.extraction_log
extract_date            DATE
studies_extracted       INTEGER
total_rows_extracted    BIGINT
change_events_emitted   INTEGER
decision_logs_applied   INTEGER
hitl_approved           INTEGER
hitl_rejected           INTEGER
hitl_promoted           INTEGER
agent_items_finalized   INTEGER
agent_items_abstained   INTEGER
agent_cache_hits        INTEGER
agent_spent_usd         DOUBLE
phase_durations         JSON                  -- {phase_name: seconds}
notes                   VARCHAR               -- cohort-expansion flag, ref-version warnings
```
Row inserted at phase 1 with `status='running'`; updated in success and exception paths.

**4. Canonical sequence**:
```
Open duck_conn; INSERT pipeline_runs(status='running')
 1. extract_aact              (src/extract/aact.run_extraction)
 2. hitl_sync                  (run_hitl_sync.apply_logs)
 3. normalize_conditions       ─┐
 4. normalize_drugs             │  sequential (single-writer)
 5. normalize_sponsors          ─┘
 6. classify_design             (norm.* → class.*)
 7. promote_enriched            (raw → enriched)
 8. build_views                 (views.study_summary)
 9. change_events               (diff current vs. prior parquet)
10. agent_condition             ┐
11. agent_drug                  │ queue + budget-bounded
12. agent_sponsor               ┘
UPDATE pipeline_runs SET status='completed', completed_at=...
```
- **hitl_sync before normalize**: sync already rebuilds domain `norm.*` internally; sequencing normalize after skips redundant rebuilds.
- **change_events after promote/views, before agent**: diffs raw fields (user's scope), doesn't depend on agent output, keeps detection simple and interpretable.
- **Agent after sync**: sync drains prior-cycle approvals into the dictionary; agent then refills the queue against the post-sync dictionary state.

**5. Enrichment agent per refresh.** Default per-domain budgets (tunable via `--budget-per-domain`): `condition` $2.00 / limit 300; `drug` $3.00 / limit 300; `sponsor` $1.00 / limit 200 (kept minimal until Phase 7D ships). Reuses existing queue-aware guardrails in `src/agent/enrichment_agent.py` — refuses when `pending >= max_pending` (default 500), caps new candidates to remaining slots, stops at budget. No new throttling logic.

### Edge cases

- **First post-filter-removal run**: ~480K terminal-state trials become newly visible; without mitigation, the event log floods with `first_seen`. Mitigation: explicit `--cohort-expansion` flag; `change_events` skips emission that run and writes `notes='cohort_expansion_run'` to `meta.pipeline_runs`. Subsequent runs resume normal diffing.
- **AACT metadata revisions** (NLM reclassifies MeSH ancestors, tweaks sponsor spelling): unavoidable false positives. `last_update_submitted_date` gate filters most; change-type granularity lets downstream consumers decide which to trust.
- **Reference version bumps** (ChEMBL 36 → 37, etc.): orchestrator compares current vs. prior-run `meta.reference_sources` active versions; writes a warning to `meta.pipeline_runs.notes` if any advanced. No automatic retrigger — reviewer signal only.
- **Parquet retention**: not deleted today. At 5× weekly cadence, ~15 GB/year — not immediate. Flagged as Phase 5.1 follow-up.
- **Extract memory at 5×**: `eligibilities` and `detailed_descriptions` (~10 KB/row) are the fat tables; `pd.read_sql` loads them fully into pandas. Watch the first real run; switch those two to chunked reads if memory spikes. Known risk, not blocker.

### Not in scope

- Parallel normalize phases (single-writer contention outweighs speedup; revisit only on profiling).
- Scheduler / cron / daemon (user wires their own cron around `run_pipeline.py`).
- Slack/email notifications (notebook + logs suffice).
- Derived-field change events (TA transitions, canonical-drug transitions) — raw-level is sufficient for longitudinal tracking today.
- Backfill of historical parquets into the event log — begins at Phase 5 cutover.
- Sponsor agent v2 (blocked on Phase 7D).
- Parquet snapshot retention / GC.

### Modules + entry points

**New**:
- `run_pipeline.py` — orchestrator CLI.
- `src/pipeline/orchestrator.py` — phase sequencing + `meta.pipeline_runs` lifecycle + per-domain agent config.
- `src/transform/change_events.py` — diff logic + event emission against prior Parquet snapshot.
- `run_change_events.py` — thin wrapper for standalone use.
- `notebooks/07_refresh_validation.ipynb` — verification dashboards.
- `tests/transform/test_change_events.py`, `tests/pipeline/test_orchestrator.py`.
- `tests/fixtures/snapshots/{day1,day2}/*.parquet`, `tests/fixtures/two_snapshot_db.py`.

**Modified**:
- `config/tables.py` — delete `STATUS_VALUES` / `STATUS_WHERE_CLAUSE`.
- `src/extract/aact.py` — drop status filter; accept optional external `duck_conn`.
- `src/extract/connection_test.py` — drop `STATUS_WHERE_CLAUSE` import.
- `config/settings.py` — demote `ACTIVE_STATUSES` to documentation constant.
- `tests/test_settings.py`, `tests/extract/test_tables.py` — update assertions.

**Reused (no changes)**: `run_hitl_sync.py`, `src/hitl/candidates.py`, `src/agent/enrichment_agent.py`, `src/transform/promote.py`, `src/transform/views.py`, `src/transform/normalize_*.py`.

### Verification

1. `pytest tests/` — two-snapshot fixture exercises every `change_type` end-to-end; orchestrator mock test asserts call order and `meta.pipeline_runs` row shape.
2. `python run_pipeline.py --dry-run` — confirms orchestrator wiring + `meta.pipeline_runs` lifecycle against latest existing Parquet without hitting AACT.
3. `python run_pipeline.py --cohort-expansion` — first real filter-removed extract. Expected: `raw.studies` ≈ 600K, `meta.trial_change_events` empty, `meta.pipeline_runs.notes='cohort_expansion_run'`. Spot-check memory/timing on `eligibilities` and `detailed_descriptions`.
4. `python run_pipeline.py` — second refresh, normal diffing. Expected: thousands of events (status transitions dominant), `views.study_summary` row count ≈ 600K, agent spend ≤ sum of per-domain budgets.
5. `notebooks/07_refresh_validation.ipynb` — run-over-run deltas, change-event distribution, HITL queue depth vs. agent fill rate, agent cost + cache-hit ratio, reference-version warnings, stale manual-mapping audit.

---

## Dependency Graph

```
Phase 0 (Scaffolding) ✅
  │
Phase 1 (Raw Extract) ✅
  │
  ├── Phase 2A (Conditions + TAs) ✅ ──┐
  │     │                               ├── Phase 3A (Core Analysis) ✅
  │     └── Phase 2A.1 (Fuzzy HITL) ✅ │
  │                                     │
  ├── Phase 2B (Study Design) ✅ ──────┘
  │
  ├── Phase 2C (QuickUMLS)     → subsumed into Phase 6
  ├── Phase 2D (Drugs) ✅       [independent; residuals flow into Phase 6]
  └── Phase 2E (Sponsors)      → subsumed into Phase 6
        │
      Phase 4 (Views) ✅         [after deterministic normalizations]
        │
      Phase 6 (HITL Platform) ✅  [unifies candidate + review workflows]
        │
      Phase 7 (Data Model Hardening) — partially shipped
        ├── 7A (Rejection persistence)     ✅ [independent]
        ├── 7B (Canonical entity tables)   ✅ [shipped with 7E; feeds 7C and 7D]
        │     └── 7C (enriched.* layer)    ✅ [feeds Phase 5]
        │     └── 7D (Sponsor agent v2)    [planned; also depends on Phase 6E ✅]
        │     └── 7F (Drug agent v2)       [deferred; mirrors 7D — depends on 6E ✅, 7D]
        └── 7E (Reference source versioning)  ✅ [shipped with 7B]
        │
      Phase 5 (Automation)       [after 7C to avoid raw-coupling / repro hazards]
```

**Recommended solo-developer order**: 0 → 1 → 2A → **2A.1** → 2B → 3A → 2D → 4 → 6 → **7A ✅ → (7B + 7E) ✅ → 7C ✅ → 7D** → 5

## Action Items

1. ~~**Register AACT account**~~ ✅ Done
2. ~~**Apply for UMLS license**~~ ✅ Approved 2026-04-13; QuickUMLS index built 2026-04-14 (Phase 6D)
3. ~~**Download MeSH XML**~~ — not needed for Phase 2A (ancestor-name approach used instead); may be needed for Phase 2C
4. ~~**Bookmark NBK611886 TA mapping table**~~ ✅ Used as starting point for `data/reference/therapeutic_area_mapping.json`

## Key Files

- `resources/pipeline_spec.md` — authoritative spec
- `resources/documentation_20260321.csv` — AACT schema (53 tables, 479 fields)
- `resources/ctti_schema_documentation.md` — join conventions, data caveats
- `config/settings.py` — AACT connection, DuckDB path, constants
- `config/tables.py` — extraction table list, status filters
- `src/extract/aact.py` — extraction pipeline (AACT → `raw.*`)
- `src/logging_config.py` — centralized logging
- `src/entities.py` — canonical entity schema + `upsert_{condition,drug,sponsor}` helpers (Phase 7B)
- `src/reference_sources.py` — `meta.reference_sources` register/lookup (Phase 7E)
- `src/transform/normalize_conditions.py` — condition dictionary + study conditions
- `src/transform/therapeutic_areas.py` — TA reference table + study TA assignment
- `data/reference/mesh_ta_mapping/v1/mapping.json` — hand-curated MeSH ancestor → TA mapping (21 entries)
- `src/transform/classify_design.py` — study design classification (L1/L2/L4/L5)
- `src/transform/innovative_features.py` — innovative feature detection (L3, regex NLP) + AI mention flag
- `resources/Innovative & Emerging Clinical Trial Designs.md` — reference catalog of innovative/emerging trial designs
- `src/transform/normalize_drugs.py` — drug dictionary + study drugs (3 layers: control mapping, MeSH exact, ChEMBL local)
- `src/transform/normalize_sponsors.py` — sponsor dictionary + fuzzy merger candidates
- `src/transform/views.py` — `views.study_summary` (joins through `entities.*`)
- `src/hitl/candidates.py` — `ref.mapping_candidates` plumbing, throttle, `promote_candidates` (Phase 7A + 7B)
- `src/agent/enrichment_agent.py`, `src/agent/enrichment_tools.py`, `src/agent/quickumls_tool.py` — Phase 6E agent + its tools
- `scripts/load_mesh_descriptors.py` — MeSH XML → `entities.condition` bulk loader (Phase 7B)
- `scripts/bootstrap_reference_sources.py` — one-time reference-directory reorg + `meta.reference_sources` seed (Phase 7E)
- `data/reference/chembl/36/synonyms.parquet` — 128K ChEMBL 36 synonyms (2.4 MB)
- `data/reference/mesh/2026/desc.xml` — MeSH 2026 descriptor file (313 MB, ~31k descriptors)
- `data/reference/umls/2025AB/quickumls_index/` — UMLS 2025AB QuickUMLS index (5 GB)
- `data/DATABASE_SCHEMA.md` — DuckDB schema documentation

## Verification

After each phase, the corresponding notebook serves as the verification step:
- Phase 1 → `01_raw_data_validation.ipynb` ✅ (row counts, distributions)
- Phase 2A → `02_condition_coverage.ipynb` ✅ (dictionary stats, TA coverage %, distribution, spot-checks)
- Phase 2B → `03_design_classification.ipynb` ✅ (precision spot-checks)
- Phase 3A → `04_innovation_by_therapeutic_area.ipynb` ✅ (the core analysis, R kernel)
- Phase 2D → `05_drug_normalization.ipynb` (coverage rates, top unmatched)
- Phase 4 → `06_views_validation.ipynb` ✅ (row count parity, column nulls, spot-checks)
- Phase 6 → `apps/review/app.R` (Shiny review UI) + golden eval (`tests/test_enrichment_agent.py`) + coverage deltas in notebooks 02 and 05 after a promotion batch
