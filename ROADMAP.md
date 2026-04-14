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

## Phase 7: Data Model Hardening (deferred)

**Added 2026-04-14 from first-use observations during Phase 6F review.** Four architectural concerns surfaced after running the HITL app against live data. None blocks today's pipeline, but each compounds with scale and with Phase 5 (refresh automation). Scheduled *before* Phase 5 — re-running on top of a raw-coupled view + inconsistent rejection semantics is a footgun.

### Sequencing rationale

Each slice is independently valuable and ordered by dependency:

1. **7A — Rejection persistence** (cheapest; no migration). Unblocks reviewer productivity immediately.
2. **7B — Canonical entity tables** (foundation for 7C and cleaner analytics).
3. **7C — `enriched.*` layer** (depends on 7B). Decouples views from `raw.*`, enabling safe re-extraction.
4. **7D — Sponsor dedup v2 via agent** (depends on 7B + Phase 6E). Largest scope; most likely to evolve in design as we learn from 6E in production.
5. **7E — Reference source versioning** (naturally pairs with 7B). Gives every derived row a reproducibility provenance stamp.

Only after 7A–7C (and ideally 7E) land should Phase 5 (refresh automation) ship.

### Phase 7A: Unify rejection semantics

**Symptom**: Rejected mappings can regenerate, and behavior is inconsistent across domains:

| Domain | Current behavior | Location |
|---|---|---|
| `condition` | Over-aggressive: any rejected `source_value` is banned from *all* future proposals | `src/normalize_conditions.py:319` — `WHERE status IN ('approved','rejected')` |
| `drug` | Weak: only exact `(source_value, canonical_term, source)` triples are deduped | `src/hitl.py:111-127` + `generate_drug_fuzzy_candidates` has no source-level filter |
| `sponsor` | Same as drug | `generate_sponsor_fuzzy_candidates` has no source-level filter |

**Root cause**: Rejection semantics were never specified; each domain's generator shipped with different filters.

**Proposed direction**: Define *reject* as "this specific `(source_value → canonical_term)` mapping is wrong" — different canonicals for the same source *may* reappear. Layer on a count-based throttle: after N (default 2) rejections of distinct canonicals for a given `source_value`, skip that source_value for that `source` type. An explicit "hide" action in the Shiny app (distinct from reject) handles the case where the reviewer wants to suppress a source entirely. Align all three domains to this behavior, remove the condition-domain's `NOT IN` filter, and add the throttle to `src/hitl.py` so generators don't each reinvent it.

**Status**: deferred — tracked, not scheduled.

### Phase 7B: Canonical entity tables

**Symptom**: `ref.condition_dictionary.canonical_term` is a free-text MeSH string; `ref.drug_dictionary.canonical_name` is free text despite having `canonical_id` (ChEMBL); `ref.sponsor_dictionary.canonical_name` is pure free text. Downstream `norm.*` and `views.*` key on these strings.

**Root cause**: The pipeline grew domain by domain without a shared entity model. Each dictionary carries labels, not identities.

**Proposed direction**: Introduce an `entities` schema with stable surrogate IDs:
- `entities.condition(condition_id BIGINT PK, canonical_term VARCHAR NOT NULL UNIQUE, mesh_descriptor_id VARCHAR, umls_cui VARCHAR, …)`
- `entities.drug(drug_id BIGINT PK, canonical_name VARCHAR NOT NULL UNIQUE, chembl_id VARCHAR, mesh_descriptor_id VARCHAR, unii VARCHAR, …)`
- `entities.sponsor(sponsor_id BIGINT PK, canonical_name VARCHAR NOT NULL UNIQUE, ror_id VARCHAR, ringgold_id VARCHAR, …)`

Dictionary tables reference `*_id` via FK instead of repeating strings; `norm.*` and `views.study_summary` join via IDs. Renames are safe (update one label row, not thousands of joins). Multi-ID crosswalks (MeSH D-code, UMLS CUI, ChEMBL, RxCUI, UNII, ROR, Ringgold) get a durable home.

**Migration effort**: rewrite three dictionaries; re-key four `norm.*` tables and the view. Adds one new schema. No new external data sources required — current columns are repurposed as the seed for `entities.*`.

**Status**: deferred — tracked, not scheduled.

### Phase 7C: Decouple analytical view from `raw.*`

**Symptom**: `src/views.py` reads directly from `raw.studies`, `raw.interventions`, `raw.countries`, `raw.browse_conditions`, `raw.sponsors` alongside the `class.*` / `norm.*` layers.

**Root cause**: Phase 4 prioritized shipping the wide view over layering. The raw dependency was cheap because re-extraction happened manually and infrequently.

**Risk on re-run** (Phase 5):
- During an extraction the view transiently points at stale/partial data.
- `raw.interventions.id`, `raw.sponsors.id` etc. are surrogate keys that are *not* stable across extractions — nothing depends on them today, but any future snapshot/audit work would break.
- Downstream analytical snapshots keyed to `views.study_summary` get silently redefined on each re-extract with no versioning.

**Proposed direction**: Introduce an `enriched.*` schema as the stable intermediate layer. `views.study_summary` reads only from `enriched.*` + `entities.*` (7B). Population from `raw.*` into `enriched.*` lives in a narrow "projection" module run at the tail of Phase 1/2; only those modules see `raw.*`. Enables snapshotting (`enriched.study_YYYYMMDD` tables) and extraction-diffing to make Phase 5 safer.

**Status**: deferred — tracked, not scheduled. Prerequisite for Phase 5.

### Phase 7D: Sponsor dedup v2 — anchor-driven agent

**Symptom**: `src/normalize_sponsors.py::generate_sponsor_fuzzy_candidates` produces a queue with high false-positive rate. Spot-check of live data: legitimately distinct institutions collide on `rapidfuzz.WRatio` (e.g. `Hunan Provincial People's Hospital` vs `Hunan Cancer Hospital`), while parent/subsidiary variants that SHOULD merge (e.g. `Novartis` vs `Novartis Pharmaceuticals`) look identical in score space to those false positives. Reviewer burden is high and signal is low.

**Root cause**: String similarity doesn't encode org identity. Pharma parent/subsidiary, university/hospital-system, and acronym-vs-full-name relationships all need semantic reasoning.

**Proposed direction**: Invert the search. Anchor on a curated set of high-frequency canonicals (top ~200 by `study_count`, optionally human-blessed). For each lower-frequency canonical, use the Phase 6E enrichment agent to ask "is this a variant of any anchor?" The agent can use industry knowledge plus evidence (co-occurring study metadata, MeSH pharma entries, shared city/country, ROR hierarchy if available) to propose or reject a merge with rationale. Deterministic `rapidfuzz` becomes a *coarse gate* that narrows the candidate set the agent sees — not the direct reviewer queue.

**Depends on**: 7B (stable sponsor IDs make merge operations auditable) + Phase 6E (already shipped).

**Status**: deferred — tracked, not scheduled.

### Phase 7E: Reference source versioning

**Symptom**: External reference data (ChEMBL 36 synonyms, UMLS 2025AB Metathesaurus, MeSH TA mapping) is loaded from hardcoded paths with no version metadata inside the DB. Derived tables (`ref.drug_dictionary`, `meta.agent_cache`, QuickUMLS outputs) can't be traced back to the specific reference version that produced them. If ChEMBL releases 37 next quarter and someone rebuilds, there's no audit trail.

**Root cause**: The reference datasets were added one at a time during Phase 2D and 6D with flat paths (`data/reference/chembl_synonyms.parquet`, `data/reference/umls/quickumls_index/`) and no in-DB registration. Reproducibility wasn't a goal until the pipeline started producing reviewed artifacts.

**Why not load everything into DuckDB**: full UMLS MRCONSO is 2.2 GB uncompressed and QuickUMLS needs its own binary on-disk index either way — duplicating it into DuckDB just costs disk for no SQL-side win. ChEMBL synonyms *could* go in (only 2.4 MB), but keeping both references on the same "file + metadata row" pattern is easier to reason about than an asymmetric "ChEMBL in DB, UMLS on disk" split.

**Proposed direction** — hybrid: metadata in DB, data stays on disk, provenance stamped on derived rows.

1. **`meta.reference_sources` table** — single source of truth for "what's current, where it lives, when it was built":
   ```
   source_name  VARCHAR  (chembl | umls | mesh_ta_mapping | …)
   version      VARCHAR  (36 | 2025AB | …)
   acquired_at  TIMESTAMP
   built_at     TIMESTAMP
   path         VARCHAR  (e.g. data/reference/chembl/36/synonyms.parquet)
   checksum     VARCHAR  (sha256 of the file or index manifest)
   is_active    BOOLEAN  (exactly one true per source_name)
   notes        VARCHAR  (license, URL, post-processing)
   PRIMARY KEY (source_name, version)
   ```

2. **Directory convention: `data/reference/<source>/<version>/…`** instead of today's flatter layout. New version = new directory alongside the old; old stays for rollback until explicitly purged. One-time migration:
   - `data/reference/chembl_synonyms.parquet → data/reference/chembl/36/synonyms.parquet`
   - `data/reference/umls/quickumls_index → data/reference/umls/2025AB/quickumls_index`
   - `data/reference/therapeutic_area_mapping.json → data/reference/mesh_ta_mapping/v1/mapping.json` (with a `version` bumped whenever the JSON is edited).

3. **Loaders read the active version from `meta.reference_sources`**, not a hardcoded path. `CHEMBL_SYNONYMS_PATH` in `src/normalize_drugs.py` becomes a lookup; `DEFAULT_INDEX_PATH` in `src/quickumls_tool.py` likewise.

4. **Provenance stamping on derived tables** — each dictionary entry and agent cache row records which reference versions it was built against. Two options, pick per table:
   - Simplest: a `source_versions` JSON column on `ref.drug_dictionary` / `ref.condition_dictionary` / `meta.agent_cache` — e.g. `{"chembl": "36", "umls": "2025AB"}`.
   - More normalized: a run-level `meta.reference_snapshot(run_id, source_name, version)` joined from `meta.pipeline_runs` once Phase 5 lands.

5. **Retention policy** — ChEMBL is tiny; keep all versions indefinitely. UMLS indexes are ~5 GB each; policy-dependent, default to keeping the last two releases.

**Limits** (worth being explicit about): this makes reproducibility *traceable* ("this mapping came from ChEMBL 36 + UMLS 2025AB") but not *automatic*. You can't re-run last quarter's extraction against last quarter's references unless you retained those directories. Automatic rollback would require keeping the old versions around — policy, not code.

**Depends on**: nothing hard. Pairs naturally with 7B (entity tables want `chembl_release` / `umls_release` on the row level once the entity `*_id` scheme is in place), so ideally run 7B and 7E together — the schema migrations touch adjacent tables.

**Status**: deferred — tracked, not scheduled.

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
      Phase 7 (Data Model Hardening)  — DEFERRED, scheduled before 5
        ├── 7A (Rejection persistence)     [independent]
        ├── 7B (Canonical entity tables)   [pairs with 7E; feeds 7C and 7D]
        │     └── 7C (enriched.* layer)    [feeds Phase 5]
        │     └── 7D (Sponsor agent v2)    [also depends on Phase 6E]
        └── 7E (Reference source versioning)  [independent; pairs with 7B]
        │
      Phase 5 (Automation)       [after 7A–7C + 7E to avoid raw-coupling / repro hazards]
```

**Recommended solo-developer order**: 0 → 1 → 2A → **2A.1** → 2B → 3A → 2D → 4 → 6 → **7A → (7B + 7E) → 7C → 7D** → 5

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
- `src/extract.py` — extraction pipeline
- `src/logging_config.py` — centralized logging
- `src/normalize_conditions.py` — condition dictionary building + study conditions
- `src/therapeutic_areas.py` — TA reference table + study TA assignment
- `data/reference/therapeutic_area_mapping.json` — hand-curated MeSH ancestor → TA mapping (21 entries)
- `src/classify_design.py` — study design classification (L1/L2/L4/L5)
- `src/innovative_features.py` — innovative feature detection (L3, regex NLP) + AI mention flag
- `resources/Innovative & Emerging Clinical Trial Designs.md` — reference catalog of innovative/emerging trial designs
- `src/normalize_drugs.py` — drug dictionary building + study drugs (3 layers: control mapping, MeSH exact, ChEMBL local)
- `data/reference/chembl_synonyms.parquet` — 128K ChEMBL synonyms extracted from ChEMBL 36 SQLite (2.4MB)
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
