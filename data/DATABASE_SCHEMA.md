# DuckDB Schema Documentation

**Database**: `data/clinical_trials.duckdb`
**Last updated**: 2026-04-09

---

## Schema Overview

| Schema | Purpose | Tables |
|--------|---------|--------|
| `raw` | Direct mirrors of AACT tables, filtered to active/planned studies. No transformations. | 14 tables |
| `ref` | Reference/lookup tables for normalization. Hand-curated or derived. | 4 tables |
| `norm` | Normalized entities with provenance tracking. | 3 tables |
| `class` | Study design classification and innovative feature detection. | 2 tables |
| `meta` | Pipeline metadata (extraction logs, run statistics). | 1 table |

Future schemas (not yet created): `views` (analytical views).

---

## `raw` Schema

All tables are extracted from AACT via `src/extract.py`. Filtered to studies where `overall_status IN ('RECRUITING', 'NOT_YET_RECRUITING', 'ACTIVE_NOT_RECRUITING', 'ENROLLING_BY_INVITATION', 'AVAILABLE')`. Child tables are filtered via `INNER JOIN` to `studies` on `nct_id`.

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

## `ref` Schema

Reference and lookup tables used by the normalization pipeline.

### `ref.condition_dictionary`
Maps free-text condition names to canonical MeSH terms. Built by `src/normalize_conditions.py`. Extensible: manual entries added here are automatically picked up on the next pipeline run.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `condition_name` | VARCHAR | NOT NULL | Lowercase condition name (e.g., "breast cancer") |
| `canonical_term` | VARCHAR | NOT NULL | Canonical MeSH term (e.g., "Breast Neoplasms") |
| `mapping_method` | VARCHAR | NOT NULL | How the mapping was derived (see below) |
| `confidence` | VARCHAR | NOT NULL | `high` or `medium` |

**Mapping methods** (in priority order — earlier methods take precedence):
- `exact` — condition name exactly matches a MeSH term (case-insensitive) within the same study
- `1:1-study` — study has exactly 1 condition + 1 MeSH term, creating an unambiguous pairing
- `co-occurrence` — condition and MeSH term co-occur dominantly across studies (≥3 studies, ≥2x the runner-up)
- `cancer-synonym` — `[Site] Cancer` → `[Site] Neoplasms` pattern matching
- `manual` — hand-curated entries, including reviewed fuzzy candidates; preserved across automated rebuilds
- `quickumls` — QuickUMLS mapping (future Phase 2C; preserved across automated rebuilds)

**14,572 rows** (3,247 exact + 8,263 1:1-study + 2,712 co-occurrence + 350 cancer-synonym)

### `ref.condition_candidates`
Staging table for fuzzy match proposals awaiting human review. Generated by `generate_fuzzy_candidates()` in `src/normalize_conditions.py`. Approved candidates are promoted to `ref.condition_dictionary` as `manual` entries via the enrichment notebook.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `condition_name` | VARCHAR | NOT NULL | Lowercase condition name |
| `canonical_term` | VARCHAR | NOT NULL | Proposed MeSH term match |
| `score` | FLOAT | NOT NULL | rapidfuzz token_sort_ratio score (75-100) |
| `study_count` | INTEGER | NOT NULL | Number of studies with this condition |
| `status` | VARCHAR | NOT NULL | `pending`, `approved`, or `rejected` |
| `created_at` | TIMESTAMP | | When the candidate was generated |

Approved/rejected decisions persist across regenerations — only `pending` rows are cleared when candidates are regenerated.

### `ref.therapeutic_areas`
Hand-curated mapping from MeSH ancestor names to therapeutic areas. Source: `data/reference/therapeutic_area_mapping.json`.

| Column | Type | Description |
|--------|------|-------------|
| `mesh_ancestor` | VARCHAR | MeSH ancestor term name (e.g., "Neoplasms") |
| `therapeutic_area` | VARCHAR | Therapeutic area label (e.g., "Oncology") |

**21 rows** — maps to 21 therapeutic areas. Multiple ancestors can map to the same TA (e.g., "Endocrine System Diseases" and "Nutritional and Metabolic Diseases" both → "Metabolic/Endocrine").

### `ref.drug_dictionary`
Maps normalized intervention names to canonical drug identifiers. Built by `src/normalize_drugs.py`. Manual entries are preserved across automated rebuilds.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `source_name` | VARCHAR | NOT NULL | Normalized intervention name (lowercase, dosage/route stripped) |
| `canonical_name` | VARCHAR | NOT NULL | Canonical drug name (MeSH term or ChEMBL pref_name) |
| `canonical_id` | VARCHAR | YES | ChEMBL ID (NULL for MeSH-only matches) |
| `mapping_method` | VARCHAR | NOT NULL | How the mapping was derived (see below) |
| `confidence` | VARCHAR | NOT NULL | `high` or `medium` |

**Mapping methods** (in priority order):
- `control-map` — regex-based mapping of placebo, vehicle, saline, standard-of-care, and other control terms
- `mesh-exact` — normalized name exactly matches `browse_interventions.downcase_mesh_term` within the same study
- `chembl-synonym` — exact match against local ChEMBL 36 synonym Parquet file (128K synonyms)
- `manual` — hand-curated entries; preserved across automated rebuilds

---

## `norm` Schema

Normalized entity tables with provenance tracking.

### `norm.study_conditions`
Every row from `raw.conditions`, enriched with canonical MeSH term from the condition dictionary. Unmapped conditions have NULL values.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `nct_id` | VARCHAR | | Study identifier |
| `condition_name` | VARCHAR | | Original condition name from `raw.conditions` |
| `canonical_term` | VARCHAR | YES | Canonical MeSH term (NULL if unmapped) |
| `mapping_method` | VARCHAR | YES | From dictionary: exact, 1:1-study, co-occurrence, manual, etc. |
| `confidence` | VARCHAR | YES | From dictionary: high, medium |

**316,463 rows** — same as `raw.conditions` plus joined `raw.conditions` entries with multiple rows. 84.5% of studies have ≥1 mapped condition.

### `norm.study_therapeutic_areas`
Study-level therapeutic area assignments derived from `raw.browse_conditions` MeSH ancestors joined to `ref.therapeutic_areas`. Multi-label: a study can have multiple TAs.

| Column | Type | Description |
|--------|------|-------------|
| `nct_id` | VARCHAR | Study identifier |
| `matched_ancestor` | VARCHAR | The MeSH ancestor/term that matched a TA |
| `therapeutic_area` | VARCHAR | Therapeutic area label |
| `match_source` | VARCHAR | `mesh-ancestor` or `mesh-list` |

**202,132 rows** — 93,606 distinct studies (78.2% coverage)

### `norm.study_drugs`
Drug/Biological interventions enriched with canonical drug name from the drug dictionary. Only includes Drug and Biological intervention types. Unmapped drugs have NULL canonical fields.

| Column | Type | Nullable | Description |
|--------|------|----------|-------------|
| `nct_id` | VARCHAR | | Study identifier |
| `intervention_type` | VARCHAR | | DRUG or BIOLOGICAL |
| `intervention_name` | VARCHAR | | Original name from `raw.interventions` |
| `canonical_name` | VARCHAR | YES | Canonical drug name (NULL if unmapped) |
| `canonical_id` | VARCHAR | YES | ChEMBL ID (NULL if MeSH-only or unmapped) |
| `mapping_method` | VARCHAR | | control-map, mesh-exact, chembl-synonym, manual, or unmatched |
| `confidence` | VARCHAR | YES | high, medium, or NULL (if unmatched) |

---

## `class` Schema

Study design classification and innovative feature detection. Created by `src/classify_design.py` and `src/innovative_features.py`.

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

## `meta` Schema

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

## Relationships

```
raw.studies (nct_id)
  ├── raw.conditions (nct_id)
  │     └── norm.study_conditions (condition_name → ref.condition_dictionary)
  ├── raw.browse_conditions (nct_id)
  │     └── norm.study_therapeutic_areas (mesh_term → ref.therapeutic_areas)
  ├── raw.designs (nct_id)
  │     └── class.study_design (nct_id — joined with raw.studies)
  ├── raw.studies + raw.detailed_descriptions + raw.keywords
  │     └── class.innovative_features (regex NLP on free-text fields)
  ├── raw.interventions (nct_id)
  │     └── norm.study_drugs (intervention_name → ref.drug_dictionary)
  ├── raw.browse_interventions (nct_id)
  ├── raw.sponsors (nct_id)
  ├── raw.countries (nct_id)
  ├── raw.keywords (nct_id)
  ├── raw.eligibilities (nct_id)
  ├── raw.brief_summaries (nct_id)
  ├── raw.detailed_descriptions (nct_id)
  ├── raw.design_groups (nct_id)
  └── raw.calculated_values (nct_id)
```

All child tables join to `raw.studies` on `nct_id`. The `norm` tables provide enriched views with provenance for downstream analysis.
