# DuckDB Schema Documentation

**Database**: `data/clinical_trials.duckdb`
**Last updated**: 2026-04-17 (Phase 7C)

---

## Schema Overview

| Schema | Purpose | Tables |
|--------|---------|--------|
| `raw` | Direct mirrors of AACT tables, filtered to active/planned studies. No transformations. | 14 tables |
| `entities` | Canonical entity tables with stable surrogate IDs + external-ID crosswalks (Phase 7B). | 3 tables |
| `ref` | Reference/lookup tables for normalization. Dictionaries FK into `entities.*`. | 5 tables |
| `norm` | Normalized entities with provenance tracking. FK into `entities.*`. | 4 tables |
| `class` | Study design classification and innovative feature detection. | 3 tables |
| `enriched` | Stable analytical inputs promoted from `raw.*` with table-level provenance (Phase 7C). The mart reads these instead of `raw.*`. | 3 tables |
| `meta` | Pipeline metadata (reference-source provenance, extraction logs, HITL sync, enriched-table registry). | 5 tables |
| `views` | Denormalized analytical views (query-ready). | 1 table |

**Identity invariant (Phase 7B)**: entity rows (`entities.*`) come only from trusted external vocabularies (MeSH, ChEMBL) or from approved HITL decisions — never from unresolved candidates. Dictionaries and `norm.*` FK into surrogate IDs; labels are resolved at query/view time.

---

## `raw` Schema

All tables are extracted from AACT via `src/extract/aact.py`. Filtered to studies where `overall_status IN ('RECRUITING', 'NOT_YET_RECRUITING', 'ACTIVE_NOT_RECRUITING', 'ENROLLING_BY_INVITATION', 'AVAILABLE')`. Child tables are filtered via `INNER JOIN` to `studies` on `nct_id`.

### `raw.studies`
The anchor table. One row per clinical trial.

| Column | Type | Description |
|--------|------|-------------|
| `nct_id` | VARCHAR | ClinicalTrials.gov identifier (e.g., NCT12345678) |
| `overall_status` | VARCHAR | Recruitment status (RECRUITING, etc.) |
| `study_type` | VARCHAR | Interventional, Observational, Expanded Access |
| `phase` | VARCHAR | Phase 1, Phase 2, Phase 3, etc. |
| `brief_title` | VARCHAR | Short public title |
| `official_title` | VARCHAR | Full scientific title |
| `enrollment` | DOUBLE | Target/actual enrollment count |
| `start_date` | DATE | Study start date |
| `completion_date` | DATE | Study completion date |
| `source` | VARCHAR | Organization responsible for data submission |
| ... | ... | 60+ additional columns (dates, regulatory flags, etc.) |

**119,753 rows** (as of 2026-03-21 extraction)

### `raw.conditions`
Free-text condition names assigned by investigators. Not controlled vocabulary.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Primary key |
| `nct_id` | VARCHAR | FK → studies |
| `name` | VARCHAR | Condition name as entered (e.g., "Breast Cancer") |
| `downcase_name` | VARCHAR | Lowercase version |

**253,711 rows** — ~2.1 conditions per study

### `raw.browse_conditions`
NLM-assigned MeSH term mappings. Two types: `mesh-list` (direct MeSH match) and `mesh-ancestor` (ancestor terms in MeSH hierarchy, pre-computed by NLM).

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Primary key |
| `nct_id` | VARCHAR | FK → studies |
| `mesh_term` | VARCHAR | MeSH descriptor name (e.g., "Breast Neoplasms") |
| `downcase_mesh_term` | VARCHAR | Lowercase version |
| `mesh_type` | VARCHAR | `mesh-list` (direct) or `mesh-ancestor` (hierarchy) |

**903,687 rows** — covers 94,306 studies (78.8%)

### `raw.designs`
Study design parameters for interventional and observational studies.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Primary key |
| `nct_id` | VARCHAR | FK → studies |
| `allocation` | VARCHAR | Randomized, Non-Randomized |
| `intervention_model` | VARCHAR | Single Group, Parallel, Crossover, Factorial, Sequential |
| `observational_model` | VARCHAR | Cohort, Case-Control, etc. |
| `primary_purpose` | VARCHAR | Treatment, Prevention, Diagnostic, etc. |
| `masking` | VARCHAR | None, Single, Double, Triple, Quadruple |
| ... | ... | Additional masking detail columns |

**119,490 rows**

### `raw.interventions`
Drug, device, and other intervention details.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Primary key |
| `nct_id` | VARCHAR | FK → studies |
| `intervention_type` | VARCHAR | Drug, Device, Biological, Procedure, etc. |
| `name` | VARCHAR | Intervention name |
| `description` | VARCHAR | Intervention description |

**207,891 rows**

### `raw.browse_interventions`
NLM-assigned MeSH term mappings for interventions. Same structure as `browse_conditions`.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Primary key |
| `nct_id` | VARCHAR | FK → studies |
| `mesh_term` | VARCHAR | MeSH term |
| `downcase_mesh_term` | VARCHAR | Lowercase version |
| `mesh_type` | VARCHAR | `mesh-list` or `mesh-ancestor` |

**449,434 rows**

### `raw.sponsors`
Study sponsors and collaborators.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Primary key |
| `nct_id` | VARCHAR | FK → studies |
| `agency_class` | VARCHAR | Industry, NIH, FED, OTHER |
| `lead_or_collaborator` | VARCHAR | Lead or Collaborator |
| `name` | VARCHAR | Organization name |

**209,621 rows**

### `raw.countries`
Countries where trial sites are located.

| Column | Type | Description |
|--------|------|-------------|
| `id` | BIGINT | Primary key |
| `nct_id` | VARCHAR | FK → studies |
| `name` | VARCHAR | Country name |
| `removed` | BOOLEAN | Whether the country was removed from the study |

**166,851 rows**

### Other `raw` tables

| Table | Rows | Description |
|-------|------|-------------|
| `raw.brief_summaries` | 119,753 | One-paragraph study summary |
| `raw.detailed_descriptions` | 119,753 | Full study description |
| `raw.eligibilities` | 119,753 | Inclusion/exclusion criteria, age, gender |
| `raw.keywords` | 329,442 | Investigator-assigned keywords |
| `raw.design_groups` | 225,799 | Study arms/groups |
| `raw.calculated_values` | 119,753 | NLM-derived values (facility counts, age ranges) |

---

## `entities` Schema (Phase 7B)

Canonical identity tables. One row = one distinct concept with a stable surrogate BIGINT primary key. External identifiers (MeSH D-code, ChEMBL ID, UMLS CUI, ROR ID) live as crosswalk columns, not as identity — concepts can exist without any particular external ID and can accumulate additional external IDs over time.

Every entity row carries an `origin` column (`mesh | chembl | aact | umls | manual | …`) answering *which vocabulary claimed this identity*, and an optional `source_versions` JSON stamping the reference release (e.g. `{"chembl": "36"}`). The version dimension is tracked separately in `meta.reference_sources`.

**Invariant** — entity rows come from trusted external vocabularies during pipeline runs, or from approved HITL decisions via `src/hitl/candidates.py::promote_candidates`. Never from unresolved candidates.

### `entities.condition`
Seeded from MeSH descriptor XML via `scripts/load_mesh_descriptors.py`. One row per MeSH descriptor; HITL promotions or cancer-synonym layer may add rows with other origins.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `condition_id` | BIGINT | NOT NULL (PK) | Surrogate identity. From `entities.condition_id_seq`. |
| `origin` | VARCHAR | NOT NULL | `mesh` \| `umls` \| `manual` |
| `mesh_descriptor_id` | VARCHAR | UNIQUE | MeSH D-code (e.g., `D001943`) |
| `umls_cui` | VARCHAR | UNIQUE | UMLS concept unique identifier |
| `canonical_term` | VARCHAR | NOT NULL | Human-readable canonical label |
| `source_versions` | JSON | YES | Per-source release stamp, e.g. `{"mesh": "2026"}` |

**31,505 rows** (31,110 MeSH 2026 seed + 395 cancer-synonym-invented). All current rows `origin='mesh'`.

### `entities.drug`
Seeded from ChEMBL 36 synonyms parquet (one row per distinct `chembl_id`). MeSH-only drugs and HITL-promoted drugs add more rows.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `drug_id` | BIGINT | NOT NULL (PK) | Surrogate identity. From `entities.drug_id_seq`. |
| `origin` | VARCHAR | NOT NULL | `chembl` \| `mesh` \| `manual` |
| `canonical_name` | VARCHAR | NOT NULL | ChEMBL pref_name, MeSH label, or HITL-approved string |
| `chembl_id` | VARCHAR | UNIQUE | `CHEMBLnnn` (NULL for non-ChEMBL drugs) |
| `mesh_descriptor_id` | VARCHAR | YES | MeSH D-code for drug-class entries |
| `unii` | VARCHAR | YES | FDA UNII — reserved; not yet populated |
| `source_versions` | JSON | YES | E.g. `{"chembl": "36"}` |

**50,125 rows** (47,960 chembl + 2,149 mesh + 16 manual).

### `entities.sponsor`
Seeded from `normalize_sponsor_name()` output on distinct `raw.sponsors.name` strings. No external vocabulary today; 7D will backfill ROR IDs.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `sponsor_id` | BIGINT | NOT NULL (PK) | Surrogate identity. From `entities.sponsor_id_seq`. |
| `origin` | VARCHAR | NOT NULL | `aact` \| `ror` \| `manual` |
| `canonical_name` | VARCHAR | NOT NULL, UNIQUE | Canonical organization name |
| `ror_id` | VARCHAR | UNIQUE | ROR identifier (reserved for 7D) |
| `ringgold_id` | VARCHAR | YES | Ringgold identifier (reserved) |
| `source_versions` | JSON | YES | |

**37,755 rows** — all `origin='aact'`.

---

## `ref` Schema

Reference and lookup tables used by the normalization pipeline. Dictionaries FK into `entities.*`; canonical labels live on the entity row, not the dictionary.

### `ref.condition_dictionary`
Maps free-text condition names to canonical entity IDs. Built by `src/transform/normalize_conditions.py`. Extensible: manual entries added here are automatically picked up on the next pipeline run.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `condition_name` | VARCHAR | NOT NULL (PK) | Lowercase condition name (e.g., "breast cancer") |
| `condition_id` | BIGINT | NOT NULL | FK → `entities.condition.condition_id` |
| `mapping_method` | VARCHAR | NOT NULL | How the mapping was derived (see below) |
| `confidence` | VARCHAR | NOT NULL | `high` or `medium` |

**Mapping methods** (in priority order — earlier methods take precedence):
- `exact` — condition name exactly matches a MeSH term (case-insensitive) within the same study
- `1:1-study` — study has exactly 1 condition + 1 MeSH term, creating an unambiguous pairing
- `co-occurrence` — condition and MeSH term co-occur dominantly across studies (≥3 studies, ≥2x the runner-up)
- `cancer-synonym` — `[Site] Cancer` → `[Site] Neoplasms` pattern matching
- `manual` — hand-curated entries, including reviewed fuzzy candidates; preserved across automated rebuilds
- `quickumls` — QuickUMLS mapping (Phase 6D); preserved across automated rebuilds

**14,117 rows** post-7B regen (3,247 exact + 8,263 1:1-study + 2,712 co-occurrence + 350 cancer-synonym + manual).

### `ref.drug_dictionary`
Maps normalized intervention names to canonical drug entity IDs. Built by `src/transform/normalize_drugs.py`. Manual entries are preserved across automated rebuilds.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `source_name` | VARCHAR | NOT NULL (PK) | Normalized intervention name (lowercase, dosage/route stripped) |
| `drug_id` | BIGINT | NOT NULL | FK → `entities.drug.drug_id` |
| `mapping_method` | VARCHAR | NOT NULL | How the mapping was derived (see below) |
| `confidence` | VARCHAR | NOT NULL | `high` or `medium` |

**Mapping methods** (in priority order):
- `control-map` — regex-based mapping of placebo, vehicle, saline, standard-of-care, and other control terms
- `mesh-exact` — normalized name exactly matches `browse_interventions.downcase_mesh_term` within the same study
- `chembl-synonym` — exact match against the active ChEMBL synonym Parquet (path resolved via `meta.reference_sources`)
- `manual` — hand-curated entries; preserved across automated rebuilds

**6,227 rows** post-7B regen.

### `ref.sponsor_dictionary`
Maps raw sponsor names to canonical sponsor entity IDs. Built by `src/transform/normalize_sponsors.py`. Manual entries are preserved across rebuilds.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `source_name` | VARCHAR | NOT NULL (PK) | Lowercased raw sponsor name |
| `sponsor_id` | BIGINT | NOT NULL | FK → `entities.sponsor.sponsor_id` |
| `mapping_method` | VARCHAR | NOT NULL | `exact-after-normalize` \| `manual` |
| `confidence` | VARCHAR | NOT NULL | `high` \| `medium` |

`exact-after-normalize` groups raw names by `normalize_sponsor_name()` (case-fold, strip legal suffixes `Inc`/`Ltd`/`LLC`/`GmbH`/`S.A.`/..., strip leading `The`); canonical_name is the most-frequent original form per group (alphabetical tiebreak). Fuzzy near-duplicate mergers beyond that layer are proposed via `generate_sponsor_fuzzy_candidates` → `ref.mapping_candidates(domain='sponsor')` for HITL review.

**38,021 rows** post-7B regen.

### `ref.mapping_candidates`
Shared staging table for mapping proposals awaiting human review, across all HITL domains (`condition`, `drug`, `sponsor`). Populated by domain-specific generators (fuzzy in `src/transform/normalize_*.py`, and the Claude enrichment agent in `src/agent/enrichment_agent.py`). Approved candidates are promoted via `src/hitl/candidates.py::promote_candidates`, which resolves/creates the `entities.*` row (origin=`manual`) and inserts the dictionary FK.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `domain` | VARCHAR | NOT NULL | `condition` \| `drug` \| `sponsor` |
| `source_value` | VARCHAR | NOT NULL | Input string being mapped (lowercased / domain-normalized) |
| `canonical_term` | VARCHAR | NOT NULL | Proposed canonical label |
| `canonical_id` | VARCHAR | YES | Optional external ID (ChEMBL ID, etc.) |
| `score` | FLOAT | NOT NULL | Match score (rapidfuzz, QuickUMLS sim, or agent confidence) |
| `study_count` | INTEGER | NOT NULL | Impact — studies touched by the source_value |
| `source` | VARCHAR | NOT NULL | `fuzzy` \| `quickumls` \| `co-occurrence` \| `agent` \| ... |
| `rationale` | VARCHAR | YES | Agent-supplied justification (NULL for non-agent sources) |
| `tool_trace` | JSON | YES | Agent tool-call trace (NULL for non-agent sources) |
| `status` | VARCHAR | NOT NULL | `pending` \| `approved` \| `rejected` \| `hidden` |
| `created_at` | TIMESTAMP | | When the candidate was generated |
| PRIMARY KEY | | | (`domain`, `source_value`, `canonical_term`, `source`) |

Approved/rejected/hidden decisions persist across regenerations — only `pending` rows for the active `(domain, source)` pair are cleared when candidates are regenerated.

**Phase 7A rejection throttle (`src/hitl/candidates.py::REJECT_THROTTLE = 2`)**: `insert_candidates` skips a row when its `(domain, source_value, source)` has ≥ 2 distinct rejected canonicals, or any `hidden` decision. This terminates regeneration for a source that's been definitively ruled unmappable, without the over-aggressive "first rejection bans the source" behavior that previously lived in the condition normalizer.

**Decision statuses**:
- `pending` — proposed, awaiting reviewer
- `approved` — reviewer accepted; promoted to dictionary on next HITL sync
- `rejected` — reviewer said this specific `(source_value → canonical_term)` mapping is wrong. Alternate canonicals may still be proposed for the same source_value until the throttle triggers.
- `hidden` — reviewer suppressed this `(source_value, source)` entirely. No future candidates for this source regardless of canonical. Set via the Shiny app's "Hide source" button (confirmation modal).

### `ref.therapeutic_areas`
Hand-curated mapping from MeSH ancestor names to therapeutic areas. Source: `data/reference/mesh_ta_mapping/v1/mapping.json` (path resolved via `meta.reference_sources`).

| Column | Type | Description |
|--------|------|-------------|
| `mesh_ancestor` | VARCHAR | MeSH ancestor term name (e.g., "Neoplasms") |
| `therapeutic_area` | VARCHAR | Therapeutic area label (e.g., "Oncology") |

**21 rows** — maps to 21 therapeutic areas. Multiple ancestors can map to the same TA (e.g., "Endocrine System Diseases" and "Nutritional and Metabolic Diseases" both → "Metabolic/Endocrine").

---

## `norm` Schema

Normalized entity tables with provenance tracking. After Phase 7B, all keyed on `entities.*` surrogate IDs; canonical labels are resolved at view time.

### `norm.study_conditions`
Every row from `raw.conditions`, linked to the matching `entities.condition` via dictionary lookup. Unmapped conditions have NULL `condition_id`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `nct_id` | VARCHAR | | Study identifier |
| `condition_name` | VARCHAR | | Original condition name from `raw.conditions` |
| `condition_id` | BIGINT | YES | FK → `entities.condition.condition_id` (NULL if unmapped) |
| `mapping_method` | VARCHAR | YES | From dictionary: exact, 1:1-study, co-occurrence, manual, etc. |
| `confidence` | VARCHAR | YES | From dictionary: high, medium |

**253,711 rows** — one per `raw.conditions` row. 171,222 mapped (67%); 84.7% of studies have ≥1 mapped condition.

### `norm.study_therapeutic_areas`
Study-level therapeutic area assignments derived from `raw.browse_conditions` MeSH ancestors joined to `ref.therapeutic_areas`. Multi-label: a study can have multiple TAs.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `nct_id` | VARCHAR | | Study identifier |
| `condition_id` | BIGINT | YES | FK → `entities.condition.condition_id` (NULL if the ancestor MeSH term isn't in `entities.condition`) |
| `therapeutic_area` | VARCHAR | | Therapeutic area label |
| `match_source` | VARCHAR | | `mesh-ancestor` or `mesh-list` |

**~202k rows** — 93,606 distinct studies (78.2% coverage).

### `norm.study_sponsors`
Raw sponsor rows linked to the canonical sponsor entity via `ref.sponsor_dictionary`. One row per `raw.sponsors` row.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `nct_id` | VARCHAR | | Study identifier |
| `original_name` | VARCHAR | | Original name from `raw.sponsors.name` |
| `sponsor_id` | BIGINT | YES | FK → `entities.sponsor.sponsor_id` (rare NULL on unmatched names) |
| `agency_class` | VARCHAR | | `Industry` / `NIH` / `FED` / `OTHER` |
| `lead_or_collaborator` | VARCHAR | | `lead` / `collaborator` |

**209,621 rows** — 100% mapped to `entities.sponsor`.

### `norm.study_drugs`
Drug/Biological interventions linked to `entities.drug` via `ref.drug_dictionary`. Only includes Drug and Biological intervention types. Unmapped drugs have NULL `drug_id`.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `nct_id` | VARCHAR | | Study identifier |
| `intervention_type` | VARCHAR | | DRUG or BIOLOGICAL |
| `intervention_name` | VARCHAR | | Original name from `raw.interventions` |
| `drug_id` | BIGINT | YES | FK → `entities.drug.drug_id` (NULL if unmapped) |
| `mapping_method` | VARCHAR | | control-map, mesh-exact, chembl-synonym, manual, or unmatched |
| `confidence` | VARCHAR | YES | high, medium, or NULL (if unmatched) |

**82,334 rows** — 47,454 mapped (57.6%); 60.3% of studies have ≥1 mapped drug.

---

## `class` Schema

Study design classification and innovative feature detection. Created by `src/transform/classify_design.py` and `src/transform/innovative_features.py`.

### `class.study_design`
One row per study with 4 classification levels derived from structured AACT fields.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `nct_id` | VARCHAR | NOT NULL | Study identifier |
| `study_type` | VARCHAR | | L1: INTERVENTIONAL, OBSERVATIONAL, EXPANDED_ACCESS |
| `design_architecture` | VARCHAR | YES | L2: Parallel RCT, Single-Arm, Cohort, etc. |
| `blinding_level` | VARCHAR | YES | L4: Open Label, Single/Double/Triple/Quadruple Blind |
| `purpose` | VARCHAR | YES | L5: TREATMENT, PREVENTION, DIAGNOSTIC, etc. |

L2 is derived from combinatorial rules on `allocation` + `intervention_model` (interventional) or `observational_model` (observational). Studies without a `raw.designs` record have NULL for L2/L4/L5.

**118,764 rows** — one per study (100% coverage)

### `class.innovative_features`
Multi-label innovative design feature detection via regex NLP on free-text fields. A study can have multiple features; a feature can be detected in multiple source fields.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `nct_id` | VARCHAR | NOT NULL | Study identifier |
| `feature_type` | VARCHAR | NOT NULL | adaptive, basket, umbrella, platform, bayesian, SMART, N-of-1, pragmatic, enrichment, seamless, master protocol |
| `source_field` | VARCHAR | NOT NULL | brief_title, official_title, description, keyword |
| `matched_text` | VARCHAR | NOT NULL | The text that triggered the match |

**7,014 rows** — 4,680 distinct studies (3.9% of all studies)

---

## `enriched` Schema

Stable analytical inputs promoted from `raw.*` by `src/transform/promote.py` (Phase 7C). `views.study_summary` reads only from `enriched.*` + `norm.*` + `class.*` + `entities.*` — never from `raw.*` directly. Each promoted table is stamped in `meta.enriched_tables` with when it was rebuilt and which raw extract it reflects.

**Promotion discipline**: tables are promoted only when they have real cross-consumer utility or carry a transformation worth centralizing (not rote mirrors of raw).

### `enriched.studies`
Anchor table for the mart. Only the columns currently consumed by the mart are promoted; other raw.studies columns stay in `raw` until a concrete downstream consumer appears.

| Column | Type | Description |
|--------|------|-------------|
| `nct_id` | VARCHAR | ClinicalTrials.gov identifier |
| `overall_status` | VARCHAR | Recruitment status |
| `study_type` | VARCHAR | Interventional / Observational / Expanded Access |
| `phase` | VARCHAR | PHASE1, PHASE2, ... |
| `brief_title` | VARCHAR | Short public title |
| `official_title` | VARCHAR | Full scientific title |
| `enrollment` | DOUBLE | Target/actual enrollment count |
| `start_date` | DATE | Study start date |
| `completion_date` | DATE | Study completion date |
| `source` | VARCHAR | Submitting organization |
| `start_year` | INTEGER | `YEAR(start_date)` — derived; NULL if `start_date IS NULL` |

**119,753 rows** (one per study).

### `enriched.interventions`
Row-level projection of `raw.interventions`. Aggregation stays in `views.study_summary` (mart-specific).

| Column | Type | Description |
|--------|------|-------------|
| `nct_id` | VARCHAR | FK → studies |
| `intervention_type` | VARCHAR | DRUG, BIOLOGICAL, BEHAVIORAL, DEVICE, PROCEDURE, OBSERVATIONAL, ... |

**207,891 rows**.

### `enriched.countries`
Row-level projection of `raw.countries` with the `removed != TRUE` filter applied once upstream. Consumers no longer need to re-apply the filter.

| Column | Type | Description |
|--------|------|-------------|
| `nct_id` | VARCHAR | FK → studies |
| `name` | VARCHAR | Country name |

**164,068 rows** (filter drops `removed = TRUE` rows from the raw total).

---

## `meta` Schema

### `meta.reference_sources` (Phase 7E)
Single source of truth for active versions of external reference datasets (ChEMBL synonyms, UMLS index, MeSH descriptor XML, MeSH→TA mapping). Loaders resolve their file paths via `src/reference_sources.py::get_active_path(source_name)` instead of hardcoded constants.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `source_name` | VARCHAR | NOT NULL | `chembl` \| `umls` \| `mesh` \| `mesh_ta_mapping` |
| `version` | VARCHAR | NOT NULL | `36` \| `2025AB` \| `2026` \| `v1` \| … |
| `acquired_at` | TIMESTAMP | YES | When the file was downloaded/obtained |
| `built_at` | TIMESTAMP | | When registered (defaults to `current_timestamp`) |
| `path` | VARCHAR | NOT NULL | Filesystem path under `data/reference/<source>/<version>/...` |
| `checksum` | VARCHAR | YES | SHA-256 of the file, or a manifest hash for directory indexes |
| `is_active` | BOOLEAN | NOT NULL | Exactly one TRUE per `source_name` |
| `notes` | VARCHAR | YES | License, URL, post-processing notes |
| PRIMARY KEY | | | (`source_name`, `version`) |

**Current rows (4 active)**: `chembl@36`, `mesh@2026`, `mesh_ta_mapping@v1`, `umls@2025AB`.

Entity rows carry a `source_versions` JSON column stamped at creation time (e.g. `{"chembl": "36"}`, `{"mesh": "2026"}`) for row-level reference provenance.

### `meta.agent_cache`
Phase 6E per-item SHA cache for the Claude enrichment agent. Keyed by `(domain, normalized source_value, model, prompt_version)`; re-runs on unchanged inputs are served from cache with zero API cost.

| Column | Type | Description |
|--------|------|-------------|
| `cache_key` | VARCHAR PK | SHA-256 of the key tuple |
| `domain` | VARCHAR | `condition` / `drug` / `sponsor` |
| `source_value` | VARCHAR | Input at time of call |
| `model` | VARCHAR | Model used |
| `prompt_version` | VARCHAR | System-prompt version string (bump to invalidate) |
| `response_json` | JSON | Full agent payload (`kind`, `canonical_term`, `rationale`, `tool_trace`, ...) |
| `cost_usd` | DOUBLE | Measured cost of the underlying API call |
| `created_at` | TIMESTAMP | |

### `meta.decision_log_applied`
Phase 6F — tracks which `data/reviews/decisions_*.parquet` files have been applied by `run_hitl_sync.py`. Lets the sync be idempotent (re-running is a no-op).

| Column | Type | Description |
|--------|------|-------------|
| `path` | VARCHAR PK | Absolute path to the applied log |
| `applied_at` | TIMESTAMP | |
| `approved` / `rejected` / `promoted` | INTEGER | Counts from that log |

### `meta.enriched_tables` (Phase 7C)
One row per table in the `enriched.*` schema. Refreshed on every `promote_to_enriched()` run. Answers two questions: *when was this layer last rebuilt?* and *which raw extract does it reflect?*

| Column | Type | Description |
|--------|------|-------------|
| `table_name` | VARCHAR | Fully-qualified enriched table (PK, e.g. `enriched.countries`) |
| `last_built_at` | TIMESTAMP | Wall-clock time of the last projection run |
| `extraction_date` | DATE | `MAX(meta.extraction_log.extract_date)` at the time of the projection — pins enriched to a specific raw snapshot |
| `source_expression` | VARCHAR | Human-readable origin of the rows (e.g. `raw.countries WHERE removed != TRUE`) |
| `row_count` | INTEGER | Row count after projection |
| `notes` | VARCHAR | Free-text promotion rationale — the decision to promote lives in data, not just roadmap prose |

**3 rows** (one per enriched table).

### `meta.extraction_log`
Records metadata for each table extraction from AACT.

| Column | Type | Description |
|--------|------|-------------|
| `extraction_id` | VARCHAR | UUID for the extraction run |
| `extract_date` | DATE | Date stamp for the extraction |
| `table_name` | VARCHAR | Name of the extracted table |
| `row_count` | INTEGER | Number of rows extracted |
| `duration_seconds` | DOUBLE | Extraction time |
| `started_at` | TIMESTAMP | When extraction started |
| `completed_at` | TIMESTAMP | When extraction completed |
| `parquet_path` | VARCHAR | Path to the Parquet archive file |

**14 rows** (one per table per extraction run)

---

## `views` Schema

Denormalized, query-ready analytical views built by `src/transform/views.py`.

### `views.study_summary`

One row per study (`nct_id`). Joins `enriched.studies` + `class.study_design` + `class.innovative_features` + `class.ai_mentions` + `norm.study_therapeutic_areas` + `norm.study_conditions` + `norm.study_drugs` + `norm.study_sponsors` + `enriched.interventions` + `enriched.countries`, with label lookups through `entities.condition` / `entities.drug` / `entities.sponsor`. Multi-valued dimensions are aggregated into DuckDB `LIST` (VARCHAR[]) columns plus convenience scalars/flags. Phase 7C moved the three remaining `raw.*` reads behind `enriched.*`, so the mart is fully decoupled from raw.

| Column group | Columns |
|---|---|
| Identity | `nct_id`, `overall_status`, `study_type`, `phase`, `brief_title`, `official_title`, `enrollment`, `start_date`, `completion_date`, `start_year`, `source` |
| Design | `design_architecture`, `blinding_level`, `purpose` |
| Innovative features | `innovative_feature_types` (LIST), `innovative_feature_count`, `has_innovative_feature`, plus 14 booleans: `is_adaptive`, `is_basket`, `is_umbrella`, `is_platform`, `is_bayesian`, `is_smart`, `is_n_of_1`, `is_pragmatic`, `is_enrichment`, `is_seamless`, `is_master_protocol`, `is_digital_twin`, `is_in_silico`, `is_ai_augmented_design` |
| AI/ML | `has_ai_mention`, `ai_mention_terms` (LIST) |
| Therapeutic areas | `therapeutic_areas` (LIST), `therapeutic_area_count`, `primary_therapeutic_area` (most-frequent ancestor, alphabetical tiebreak) |
| Conditions | `raw_condition_names` (LIST), `canonical_conditions` (LIST, non-null), `condition_count`, `mapped_condition_count` |
| Interventions | `intervention_types` (LIST — e.g., DRUG, BEHAVIORAL, DEVICE, PROCEDURE, BIOLOGICAL, ...), `intervention_count` |
| Drugs | `canonical_drugs` (LIST, non-null), `chembl_ids` (LIST, non-null), `drug_intervention_count`, `mapped_drug_count` |
| Countries | `countries` (LIST, `removed=TRUE` excluded), `country_count` |
| Sponsors | `lead_sponsor_name`, `lead_sponsor_agency_class`, `collaborator_names` (LIST) |

**119,753 rows** (matches `raw.studies` exactly, post-7B regen). Coverage: 78.2% ≥1 TA, 20.6% ≥1 mapped drug, 3.9% innovative feature, 2.2% AI mention, 100% lead sponsor.

`lead_sponsor_name` and `collaborator_names` carry canonical names sourced from `norm.study_sponsors → entities.sponsor` (Phase 6B — deterministic `exact-after-normalize` layer). Near-duplicates beyond that layer are candidates in `ref.mapping_candidates(domain='sponsor')` awaiting HITL review.

`chembl_ids` is now authoritative: it sources from `entities.drug.chembl_id`, which is UNIQUE and populated from ChEMBL at seed time — not the best-effort backfill that lived in the pre-7B drug dictionary.

---

## Relationships

```
                              entities.condition ─┐
                              entities.drug      ─┤  (surrogate PKs;
                              entities.sponsor   ─┘   external-ID crosswalks)
                                    ▲
                                    │ FK
                                    │
raw.studies (nct_id)          ref.condition_dictionary ─┐
  ├── raw.conditions          ref.drug_dictionary      ─┤ (source_name → *_id)
  │     └── norm.study_conditions ────(condition_id)────┘
  ├── raw.browse_conditions
  │     └── norm.study_therapeutic_areas ──(condition_id)─→ entities.condition
  ├── raw.designs
  │     └── class.study_design
  ├── raw.studies + detailed_descriptions + keywords
  │     └── class.innovative_features (regex NLP)
  │     └── class.ai_mentions
  ├── raw.interventions
  │     └── norm.study_drugs ──(drug_id)──→ entities.drug
  ├── raw.browse_interventions
  ├── raw.sponsors
  │     └── norm.study_sponsors ──(sponsor_id)──→ entities.sponsor
  ├── raw.countries
  ├── raw.keywords
  ├── raw.eligibilities
  ├── raw.brief_summaries
  ├── raw.detailed_descriptions
  ├── raw.design_groups
  └── raw.calculated_values

views.study_summary ← raw.studies × class.* × norm.* × entities.*
                      (labels looked up through entities at view time)
```

All child tables join to `raw.studies` on `nct_id`. `norm.*` tables FK into `entities.*` via surrogate IDs; dictionaries in `ref.*` provide the `source_value → *_id` lookup layer. `views.study_summary` resolves canonical labels (`canonical_conditions`, `canonical_drugs`, `chembl_ids`, `lead_sponsor_name`) at query time via joins through `entities.*`.
