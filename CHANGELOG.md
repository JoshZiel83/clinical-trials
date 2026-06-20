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

## Epic A3 + A4 — Refresh automation + docs hygiene · ✅ 2026-06-20 (Epic A complete)
Turned the hardened extract into a one-command full-cohort longitudinal refresh. Closes
#4, #10, #6. Spec: [ADR 0002](docs/adr/0002-extract-scanner-atomic-swap.md).
- **Full cohort (#4):** removed the active-status filter — the extract now mirrors the
  whole ~590K AACT corpus (was ~120K). `get_extract_query` builds its WHERE conditionally
  (only the optional `--since` predicate); `STATUS_VALUES`/`ACTIVE_STATUSES` collapsed to
  one documentation constant `config.tables.ACTIVE_STATUS_VALUES`.
- **Orchestrator:** `run_pipeline.py` + `src/pipeline/orchestrator.py` thread **one**
  DuckDB connection through every phase (extract → hitl_sync → promote → normalize →
  classify → views → change_events) with a `meta.pipeline_runs` audit row
  (running → completed/failed/skipped). The extract pin-gate gates the whole refresh.
  `run_extraction()` now accepts an external `duck_conn`. The Claude enrichment agent is
  opt-in (`--enrich` + per-domain budgets) — the only paid step, off by default.
- **Change events (#10):** `meta.trial_change_events` (`src/transform/change_events.py`,
  entry `run_change_events.py`) diffs the current vs prior dated Parquet snapshot →
  first_seen / dropped / status_transition / enrollment_changed / phase_changed /
  date_changed / conditions_/interventions_/sponsors_changed. Cheap-gated on
  `last_update_posted_date`; `--cohort-expansion` suppresses the one-time first_seen flood.
  The single home for "what changed about a study" (no separate `change_log`).
- **A4 docs (#6):** `data/DATABASE_SCHEMA.md` reconciled to the live full-cohort DB via a
  repeatable `scripts/schema_counts.py` (added a live-counts block, fixed the active-only
  framing, documented `class.ai_mentions` + the new `meta.*` tables).
- **Incidental fix:** the full-cohort run surfaced a latent `normalize_drugs` bug — a few
  ChEMBL synonyms carry a null/NaN `pref_name` that violated the dictionary's NOT NULL
  (only reached at full scale); now filtered. A stale empty `ref.drug_dictionary` (old
  schema, 0 rows) was dropped so it rebuilt with the current `drug_id` schema.
**Verified:** `run_pipeline.py --force --cohort-expansion` completed end-to-end —
`raw.studies` 590,350, all downstream rebuilt (`views.study_summary` 590,350), 46,282
change events (zero first_seen), `meta.pipeline_runs` `completed`; pin-gate re-run skipped;
suite 286 passed / 1 skipped.
**Known gap (#2, not A3):** `entities.condition` is a partial 5,000-row MeSH seed (the
MeSH/UMLS reference index was lost in the 2026-06 regen); condition mapping still ~80%.

## Epic A1 + A2 — Extract hardening + snapshot provenance · ✅ 2026-06-20
Rebuilt the Phase 1 extract (`src/extract/aact.py`; entry `run_extract.py`) into an
atomic, provenanced, drift-checked pull. Closes #3, #5, #7, #11, #8. Spec:
[ADR 0002](docs/adr/0002-extract-scanner-atomic-swap.md).
- **Postgres scanner (#7):** rows pulled via DuckDB's `postgres` extension
  (`ATTACH … (TYPE postgres, READ_ONLY)`, creds through `PG*` env vars), filtered
  scanner-side; pandas `read_sql` left the hot path.
- **Atomic stage-then-swap (#5):** each table builds into `raw.<t>__staging` + a hidden
  `data/raw/.<date>.staging/` dir, swapped into place in one transaction +
  `os.replace`; a pre-swap failure leaves `raw.*` and the prior snapshot intact. Per-table
  `meta.extraction_log` rows are committed as each completes (forensic trail).
- **Schema-drift detection (#11):** incoming columns compared to
  `config/aact_expected_columns.json` — dropped column fails, new column warns;
  self-seeds, `--update-schema-baseline` regenerates.
- **Build pin + gate (#8 / A2):** the build is pinned `aact@<build-date>` in
  `meta.reference_sources` (`max(studies.updated_at)::date`, checksum over the snapshot);
  a run short-circuits when the build hasn't advanced (`--force` overrides).
- **#3:** deleted dead `connection_test.py`; bootstrap DDL consolidated into
  `ensure_extract_schema`.
- **A3-readiness hooks (inert):** `--since` (`last_update_posted_date` pre-filter — a
  *subset, not a snapshot*) and the pin-gate, for the A3 change-events diff.
**Key finding:** AACT full-reloads its corpus nightly (`studies.created_at == updated_at`
for every row; child tables carry no audit timestamps; legacy `nlm_download_date` columns
100% NULL), so `updated_at` is a build watermark, not a delta gate — corroborating the
roadmap's full-snapshot direction over incremental (#9). The stable per-study change
signal is `last_update_posted_date`.
**Verified:** live full-cohort pull pinned `aact@2026-06-20`, 14 tables / 120,739 studies
in 157s; immediate re-run skipped via the pin-gate; full suite 279 passed / 1 skipped.

---

## Tooling — Reproducible R environment (rig + renv) · ✅ 2026-06-20
Brought the repo's R usage (Shiny review app + R-kernel notebook 04) up to a
reproducible multilingual standard: **conda owns Python, renv owns R, reticulate
only points between them.** Adopted `rig` for the R binary (R 4.6.0, replacing the
ad-hoc Homebrew R), initialized `renv` at the repo root (snapshot mode `"all"`),
and locked the union of R deps (`shiny, DT, duckdb, arrow, dplyr, tidyr, ggplot2,
scales, jsonlite, DBI, RColorBrewer, reticulate`) into `renv.lock`. `.Rprofile`
resolves the conda env by name (via `CONDA_EXE`) and sets `RETICULATE_PYTHON` —
point-only, no `renv::use_python()`. `IRkernel` re-registered against rig's R and
kept user-level (kernel infra, not in the lock); notebook 04 gained a first-cell
`renv::load("..")` since the Jupyter kernel starts in `notebooks/`.
**Files:** `renv.lock`, `.Rprofile`, `renv/`; docs in `CLAUDE.md` ("R environment"),
`apps/review/README.md`. **Reproduce:** `rig add <ver>` → `renv::restore()`.

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
