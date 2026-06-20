# Changelog — Clinical Trials ETL Pipeline

Shipped work, in completion order. Forward-looking plans live in
[`ROADMAP.md`](ROADMAP.md); the canonicalization redesign lives in
[`docs/adr/0001`](docs/adr/0001-canonicalization-enrichment-agent-refactor.md).

The pipeline is a 4-layer build from AACT into local DuckDB (Raw Extract →
Normalized Entities → Enriched Features → Analytical Views). Spec:
`resources/pipeline_spec.md`.

---

## Phase 0 — Project scaffolding + AACT connection · ✅ 2026-03-21
Project structure, conda env (`clinical_trials_env`), `config/settings.py` +
`config/tables.py`, centralized logging, DuckDB init (`raw`, `meta` schemas).
**Key finding:** AACT status values are uppercase-underscore (`RECRUITING`,
`NOT_YET_RECRUITING`), not the mixed-case in older docs.

## Phase 1 — Raw extract (Layer 1) · ✅ 2026-03-21
14 AACT tables extracted to Parquet (`data/raw/YYYY-MM-DD/`) + DuckDB `raw.*`,
filtered to 5 active/planned statuses, child tables joined via `nct_id`.
Metadata → `meta.extraction_log`.
**Results:** 119,753 studies; 3,464,691 rows across 14 tables in ~29s; zero
orphaned `nct_id`s. **Modules:** `src/extract/aact.py`; entry `run_extract.py`.
Validation: `notebooks/01_raw_data_validation.ipynb`.

## Phase 2A — Condition normalization + therapeutic areas · ✅ 2026-04-05
Two-layer architecture: `ref.condition_dictionary` (exact, 1:1-study,
co-occurrence, cancer-synonym, + manual/quickumls) and TA mapping via
`raw.browse_conditions` MeSH ancestors → hand-curated `ref.therapeutic_areas`
(21 entries).
**Key finding:** NLM-computed MeSH ancestors in `browse_conditions` correspond
directly to TAs — eliminated a planned 300MB MeSH XML download.
**Outputs:** `norm.study_conditions` (86.5% of studies ≥1 canonical condition),
`norm.study_therapeutic_areas` (78.2% ≥1 TA). Top TAs: Oncology, General/Symptoms,
Neurology. **Modules:** `src/transform/normalize_conditions.py`,
`src/transform/therapeutic_areas.py`; entry `run_normalize_conditions.py`.

## Phase 2A.1 — Fuzzy match → HITL enrichment workflow · ✅ 2026-04-07
Demoted fuzzy matching from an automatic dictionary layer to a review-gated
candidate workflow (too noisy to trust). `'fuzzy'` removed from
`AUTOMATED_METHODS`; candidates staged for human review (later generalized into
`ref.mapping_candidates` in Phase 6). Intentional coverage drop, recoverable with
verified quality.

## Phase 2B — Study design classification · ✅ 2026-04-07 (updated 2026-06-19)
`class.study_design` (one row/study): L2 design architecture (combinatorial rules
on `allocation` + `intervention_model` / `observational_model`), L4 blinding
(from `masking`), L5 purpose (from `primary_purpose`). L1 study_type read straight
off the source. L3 innovative features in `class.innovative_features` (word-boundary
regex with context exclusions); 4,680 studies (3.9%) flagged.
**2026-06-19:** repointed off `raw.*` onto `enriched.studies`/`enriched.designs`
(Phase 7C leak fix) → **promote (7C) now runs before classify (2B)**; redundant L1
column dropped. **Modules:** `src/transform/classify_design.py`,
`src/transform/innovative_features.py`; entry `run_classify_design.py`.

## Phase 3A — Core analysis: design innovation by therapeutic area · ✅ 2026-04-08
The payoff analysis. Expanded innovative-feature detection to 14 types (added
digital twin, in silico, AI-augmented design); added broad `class.ai_mentions`
flag (9 term categories, 2,600 studies / 2.2%). Analysis notebook
`notebooks/04_innovation_by_therapeutic_area.ipynb` (R kernel): innovation rate by
TA, feature×TA heatmaps, time trends, phase/geography, AI/ML section. Reference:
`resources/Innovative & Emerging Clinical Trial Designs.md`.

## Phase 2C — QuickUMLS for unmapped conditions · ➡️ subsumed into Phase 6
Folded into the HITL platform: QuickUMLS became one agent tool among several rather
than an automated dictionary layer. UMLS license approved 2026-04-13; UMLS 2025AB
index built (Phase 6D). *(Index lost in a 2026-06 env regen — see Open issues.)*

## Phase 2D — Drug normalization · ✅ 2026-04-09
Three-layer `ref.drug_dictionary`: control-map (regex), mesh-exact, chembl-synonym
(128K ChEMBL 36 synonyms in Parquet, no API calls). `normalize_drug_name()` strips
dosage/route/parenthetical. MeSH co-occurrence layer **removed** — ~62% of entries
had no name overlap (unreliable for drugs). `norm.study_drugs` (Drug + Biological
only). **Module:** `src/transform/normalize_drugs.py`; entry `run_normalize_drugs.py`.

## Phase 2E — Sponsor deduplication · ➡️ subsumed into Phase 6
Deterministic normalization (case-fold, legal-suffix strip) kept in
`src/transform/normalize_sponsors.py`; the candidate-generation + review layer
absorbed into the Phase 6 platform.

## Phase 6 — HITL enrichment platform · ✅ 2026-04-14 (slices 6A–6F)
Unified all human-in-the-loop mapping workflows: shared `ref.mapping_candidates`
(domain-tagged), a Claude enrichment agent that calls matching algorithms as tools,
and a read-only R/Shiny review app.
- **6A** — `src/hitl/` + `ref.mapping_candidates`.
- **6B** — sponsor normalization → `ref.sponsor_dictionary`; `norm.study_sponsors`
  (209,621 rows); fuzzy merge candidates. Entry `run_normalize_sponsors.py`.
- **6C** — drug fuzzy candidate generator (~903 proposals).
- **6D** — `scripts/build_quickumls_index.py` + QuickUMLS tool; UMLS 2025AB index
  (5.4 GB, 10.6M terms); macOS libiconv workaround documented in `CLAUDE.md`.
- **6E** — `src/agent/enrichment_agent.py` (Anthropic SDK + `tool_runner` + adaptive
  thinking); per-domain tools; `max_pending` throttle; USD budget; SHA cache
  (`meta.agent_cache`); grounding enforcement. Entry `run_enrichment_agent.py`.
- **6F** — `apps/review/app.R` (decision-log Parquet) + `run_hitl_sync.py`
  (idempotent via `meta.decision_log_applied`).

## Phase 4 — Analytical views (Layer 4) · ✅ 2026-04-14
`views.study_summary` — one row/study (119,753). Joins design + features + AI +
TAs + conditions + drugs + countries + sponsors; multi-valued dims as DuckDB `LIST`
columns + 14 per-feature booleans + convenience scalars. **Module:**
`src/mart/study_summary.py` (relocated from `src/transform/views.py` in 7C); entry
`run_views.py`. Validation: `notebooks/06_views_validation.ipynb`.

## Phase 7A — Unify rejection semantics · ✅ 2026-04-15
Centralized throttle in `src/hitl/candidates.py` (`REJECT_THROTTLE = 2`); new
`hidden` decision status (Shiny "Hide source" button); decision-log schema accepts
`approved|rejected|hidden`. Removed the over-aggressive condition-normalizer ban.

## Phase 7B + 7E — Canonical entity tables + reference-source versioning · ✅ 2026-04-15
**7B:** new `entities` schema (`condition`/`drug`/`sponsor`) with BIGINT surrogate
PKs + external-ID crosswalks + `origin` column. Dictionaries and `norm.*` re-keyed
to FK into entities; `views.study_summary` resolves labels through `entities.*` at
query time. `promote_candidates` resolves/creates entities at approve time.
**Invariant:** entities come only from trusted vocab (MeSH, ChEMBL) or approved HITL.
Seeds: 31,505 conditions / 50,125 drugs / 37,755 sponsors; zero orphan FKs.
**7E:** `meta.reference_sources` (single source of truth for active reference
versions) + `src/reference_sources.py`; directory reorg to
`data/reference/<source>/<version>/`; loaders resolve paths via `get_active_path`;
entity rows stamp `source_versions` JSON. 4 active: `chembl@36`, `mesh@2026`,
`mesh_ta_mapping@v1`, `umls@2025AB`.

## Phase 7C — Decouple analytical view from `raw.*` · ✅ 2026-04-17 (amended 2026-06-19)
New `enriched` schema (`src/transform/promote.py`, entry `run_promote_enriched.py`):
`enriched.studies`, `enriched.interventions`, `enriched.countries`, and (added
2026-06-19) `enriched.designs`. `meta.enriched_tables` registry stamps each
projection with `last_built_at` + `extraction_date`. The mart reads zero `raw.*`.
**2026-06-19 amendment:** `class.study_design` repointed onto `enriched.designs`
(closing a transitive raw leak) → promote-before-classify ordering. `norm.*`
intentionally still reads `raw.*` (produces entity FKs through the `norm` contract).

## Phase 7D — Sponsor dedup v2 (reactive anchor-merge agent) · ⛔ superseded by ADR 0001
Planned as a reactive "is this a variant of an anchor?" agent over the top-N
canonicals. **Superseded** by the **sponsor oracle** approach (offline clustering →
lookup) in [ADR 0001](docs/adr/0001-canonicalization-enrichment-agent-refactor.md),
Axis B″. Reusable plumbing (`entities.sponsor.merged_into_id` self-FK,
`entities.sponsor_resolved` recursive view) is preserved and referenced by the ADR
for the hierarchical-grouping option (ADR Q5). Now tracked under ROADMAP **Epic C**.

## Phase 7F — Drug/intervention dedup v2 · ⛔ superseded by ADR 0001
Planned as a 7D-mirrored agent over noisy drug fuzzy candidates. **Superseded** by
ADR 0001: the drug agent is re-homed onto the new harness as part of Epic C. The
~800 pending `domain='drug', source='fuzzy'` candidates remain ignorable in their
current form pending that rebuild.

---

## Reference appendix

### Action items (all resolved)
- AACT account registered ✅
- UMLS license approved 2026-04-13; QuickUMLS index built 2026-04-14 ✅
  *(index lost in 2026-06 env regen — rebuild tracked in [#2](https://github.com/JoshZiel83/clinical-trials/issues/2))*
- MeSH XML — not needed for the ancestor-name TA approach ✅
- NBK611886 TA mapping table — used as the `ref.therapeutic_areas` seed ✅

### Open issues carried forward
- **[#1](https://github.com/JoshZiel83/clinical-trials/issues/1)** — `meta.decision_log_applied` stores absolute paths (breaks if repo moves).
- **[#2](https://github.com/JoshZiel83/clinical-trials/issues/2)** — rebuild QuickUMLS index (lost in env regen) + fix flat-vs-versioned index path mismatch.
- **[#3–#12](https://github.com/JoshZiel83/clinical-trials/issues)** — extract-hardening + cadence backlog, owned by ROADMAP **Epic A**.

### Key files
- `resources/pipeline_spec.md` — authoritative spec
- `resources/documentation_20260321.csv` — AACT schema (53 tables, 479 fields)
- `config/settings.py`, `config/tables.py` — connection, paths, table list
- `src/extract/aact.py` — AACT → `raw.*`
- `src/entities.py` — entity schema + `upsert_{condition,drug,sponsor}` (7B)
- `src/reference_sources.py` — `meta.reference_sources` register/lookup (7E)
- `src/transform/{normalize_conditions,normalize_drugs,normalize_sponsors,classify_design,innovative_features,therapeutic_areas,promote,sponsor_anchors}.py`
- `src/mart/study_summary.py` — `views.study_summary` (joins through `entities.*`)
- `src/hitl/candidates.py` — `ref.mapping_candidates` plumbing, throttle, promote
- `src/agent/{enrichment_agent,enrichment_tools,quickumls_tool,ror_tool}.py` — Phase 6E agent + tools
- `scripts/{load_mesh_descriptors,bootstrap_reference_sources,build_quickumls_index}.py`
- `data/reference/{chembl/36,mesh/2026,mesh_ta_mapping/v1,umls/2025AB}/...`
- `data/DATABASE_SCHEMA.md` — DuckDB schema documentation

### Per-phase verification (notebooks)
| Phase | Notebook |
|---|---|
| 1 | `01_raw_data_validation.ipynb` |
| 2A | `02_condition_coverage.ipynb` |
| 2B | `03_design_classification.ipynb` |
| 3A | `04_innovation_by_therapeutic_area.ipynb` (R) |
| 2D | `05_drug_normalization.ipynb` |
| 4 | `06_views_validation.ipynb` |
| 7D scoping | `sponsor_synonym_scoping.ipynb`, `sponsor_synonym_distributions.ipynb` |
