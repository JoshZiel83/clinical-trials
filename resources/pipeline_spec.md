# Clinical Trials Research Database — Specification

## What This Is

A unified research database built from clinical trials data (AACT/ClinicalTrials.gov) that solves the core problem: **raw trial data is messy, inconsistent, and hard to analyze across studies**. The database normalizes key entities — drugs, conditions, sponsors, and study designs — so that a researcher can ask questions like "how many Phase 3 oncology trials is Pfizer running?" without fighting data quality issues.

## The Problem

AACT provides a rich relational mirror of ClinicalTrials.gov (53 tables, ~479 fields), but the data has significant consistency issues that make cross-trial analysis difficult:

1. **Drug names are inconsistent across a drug's lifecycle.** A compound might appear as "XYZ-1234" in early trials and "imadeitupercept" in later ones. The same drug in different trials may be spelled differently, use brand vs generic names, or include dosage info in the name field. There is no built-in way to track one drug across all its trials.

2. **Condition terms are inconsistent.** ClinicalTrials.gov does not enforce MeSH vocabulary for conditions. Investigators enter free-text condition names that vary widely ("non-small cell lung cancer" vs "NSCLC" vs "carcinoma, non-small-cell lung"). The `browse_conditions` table has NLM-derived MeSH mappings, but these are incomplete and don't roll up to therapeutic areas.

3. **Sponsor names have duplicates and variants.** The same organization may appear under slightly different names across trials.

4. **Study design is described across multiple fields** (`allocation`, `masking`, `intervention_model`, `primary_purpose`, `observational_model`) with no unified classification. There is no simple way to ask "show me all adaptive designs" or compare design innovation across therapeutic areas.

5. **No therapeutic area classification exists.** There is no field that says "this is an oncology trial" or "this is a cardiology trial."

## What the Pipeline Produces

A local analytical database (DuckDB) containing:

### Layer 1: Raw Extract
A filtered mirror of AACT tables for active/planned trials (`overall_status` in: Recruiting, Not yet recruiting, Active not recruiting, Enrolling by invitation, Available). Stored as-is, no transformations.

### Layer 2: Normalized Entities

**Normalized Drugs** — A mapping table that resolves drug name variants to a canonical identifier.
- V1 pipeline: AACT `browse_interventions` MeSH mappings (~47% coverage) → ChEMBL `molecule_synonyms` API lookup for unmatched entries (best free resource for development codes)
- Canonical identifier: ChEMBL ID where available
- Normalizes casing, whitespace, common abbreviations via string preprocessing
- Drug class tagging (e.g., PD-1 inhibitor, SSRI) deferred to V2 (requires ATC or manual curation)

**Normalized Conditions** — A two-level mapping:
- Level 1: Start with AACT `browse_conditions` MeSH mappings (~62% coverage). For remaining ~38%, use QuickUMLS (fast Python tool, requires free UMLS license) to map free-text → UMLS CUIs → MeSH tree numbers. Manual curation for top ~200-500 high-frequency unmapped strings.
- Level 2: Map MeSH terms to ~12-15 therapeutic areas via the MeSH Category C top-level branches. Starting point: NCBI NBK611886 mapping table. Multi-label: a condition in multiple MeSH branches gets tagged with all applicable TAs.
- Reference data: MeSH tree download (XML/RDF from NLM, free)

**Normalized Sponsors** — Deduplicated sponsor names. Group variants of the same organization to a canonical name.

### Layer 3: Enriched Features

**Therapeutic Category** — Each study tagged with one or more therapeutic areas, derived from its normalized conditions mapped through the MeSH hierarchy.

**Simplified Study Design Classification** — A multi-level taxonomy combining structured AACT fields with NLP-based detection:
- **Level 1 — Study Type**: from `study_type` (Interventional, Observational, Expanded Access)
- **Level 2 — Design Architecture**: combinatorial rules on `allocation` + `intervention_model` + `masking` (e.g., Randomized + Parallel = Parallel RCT; Non-Randomized + Single Group = Single-Arm)
- **Level 3 — Innovative Features**: keyword/regex detection on `brief_title`, `official_title`, `detailed_description`, `keywords` for: adaptive, basket, umbrella, platform, Bayesian, SMART, N-of-1, pragmatic, enrichment, seamless. Informed by FDA Master Protocols Guidance (2022) and Adaptive Design Guidance (2019) definitions.
- **Level 4 — Blinding Level**: from `masking` (Open Label, Single Blind, Double Blind, Triple/Quadruple Blind)
- **Level 5 — Purpose**: from `primary_purpose` (Treatment, Prevention, Diagnostic, etc.)
- Enables the core analytical question: comparing design innovation rates across therapeutic areas (e.g., % of oncology trials using adaptive designs vs cardiology)

### Layer 4: Analytical Views

Denormalized, query-ready tables that join the normalized entities back to studies (e.g., a wide study summary table with therapeutic area, normalized lead sponsor, simplified design type, drug names, etc.)

## Data Sources

### V1 (Current Scope)
- **AACT PostgreSQL database** — queried directly via SQL. Schema documented in `clinical_trials/resources/`
- **MeSH vocabulary** — NLM's MeSH tree (XML/RDF download from https://www.nlm.nih.gov/databases/download/mesh.html). For condition → therapeutic area mapping.
- **ChEMBL** (EMBL-EBI) — free API + bulk download. `molecule_synonyms` table maps development codes to compound names. Best free resource for investigational drug normalization.
- **DrugBank** — free academic XML download. Comprehensive synonym database covering investigational + approved drugs. Includes development codes, brand names, generic names.
- **RxNorm** (NLM) — free API. For approved drug name standardization and fuzzy matching. Also provides ATC class via RxClass.
- **UMLS** (NLM) — free license. Required for QuickUMLS condition normalization tool.
- **NCBI NBK611886** — published MeSH-to-therapeutic-area mapping table as starting point.

### V2 (Future)
- **PubMed** — link trial results publications back to trials via `study_references.pmid`
- **WHO ATC** — drug class enrichment (EUR 200 for official files; free scraped versions exist on GitHub)
- **PubChem** — additional synonym coverage for drug normalization fallback

## Key Analytical Questions This Should Enable

- How many active trials exist per therapeutic area? How is that trending?
- What drugs are being tested for a given condition, and in how many trials?
- Track a single drug across all its trials (early phase → late phase) despite name changes
- Compare trial design patterns across therapeutic areas (e.g., what % of oncology trials use adaptive designs vs cardiology?)
- Who are the top sponsors in a given therapeutic area?
- Geographic distribution of trials by therapeutic area

## Research Findings

### Study Design Classification

**No single existing tool classifies ClinicalTrials.gov trials by design type.** This must be built. But strong frameworks exist to guide the taxonomy.

**AACT's structured fields** provide the foundation — five fields with controlled vocabularies:
- `intervention_model`: Single Group, Parallel, Crossover, Factorial, Sequential (5 values)
- `allocation`: Randomized, Non-Randomized
- `masking`: None/Open Label, Single, Double, Triple, Quadruple
- `primary_purpose`: Treatment, Prevention, Diagnostic, Supportive Care, Screening, Health Services Research, Basic Science, Device Feasibility, Other (9 values)
- `observational_model`: Cohort, Case-Control, Case-Only, Case-Crossover, Ecologic or Community, Other

**Critical gap**: These fields cannot capture adaptive, basket, umbrella, or platform designs. Those must be detected via NLP on free-text fields (`brief_title`, `official_title`, `detailed_description`).

**Key frameworks**:
- **FDA Master Protocols Guidance (2022)** — formally defines basket (one therapy, many diseases), umbrella (one disease, many therapies), and platform (perpetual, therapies enter/leave) subtypes
- **FDA Adaptive Design Guidance (2019)** — covers group sequential, sample size re-estimation, enrichment, adaptive randomization, seamless phase II/III, Bayesian designs
- **ICH E9 (1998)** — establishes traditional categories: parallel group, crossover, factorial, group sequential
- **ICH E20 (2025 draft)** — 5 adaptation categories: early stopping, sample size, population selection, treatment selection, allocation
- **Chow & Chang (2008)** — 10 adaptive design types
- **Woodcock & LaVange (2017, NEJM)** — seminal master protocol taxonomy

**Formal ontologies** (machine-readable, downloadable):
- **OCRe (Ontology of Clinical Research)** — OWL 2 on BioPortal. 4 interventional + 4 observational top-level types
- **EDDA Study Designs Taxonomy v2.0** — hierarchical with synonyms, CC BY-NC-SA, on BioPortal
- **CTO (Core Clinical Trial Ontology)** — on GitHub, built on BFO/OBO Foundry

**Recommended approach**: A multi-level classification:
1. **Level 1 — Study Type**: from `study_type` (Interventional, Observational, Expanded Access)
2. **Level 2 — Design Architecture**: combinatorial rules on structured fields (Randomized + Parallel = Parallel RCT, etc.)
3. **Level 3 — Innovative Features**: NLP on free-text to detect adaptive, basket, umbrella, platform, Bayesian, SMART, N-of-1, pragmatic, enrichment, seamless
4. **Level 4 — Blinding Level**: from `masking`
5. **Level 5 — Purpose**: from `primary_purpose`

---

### Drug Name Normalization

**The core challenge**: no single resource handles development codes (XYZ-1234) well AND provides a clean API. A layered lookup is required.

**Key resources assessed**:

| Resource | Covers Dev Codes? | Free? | API? | Best For |
|----------|-------------------|-------|------|----------|
| **ChEMBL** | Yes — specifically tracks drugs through clinical development | Yes (CC) | Yes | Best free option for development code → name mapping |
| **DrugBank** | Yes — includes investigational drugs | Academic use free | Paid API; free XML download | Comprehensive synonym database |
| **RxNorm** | No — approved US drugs only | Yes | Yes (20 req/sec) | Standardizing approved drug names, fuzzy matching |
| **WHO ATC** | Near approval only | EUR 200 (free scraped versions on GitHub) | No | Drug class tagging after normalization |
| **PubChem** | Yes — extensive synonyms | Yes | Yes | High-coverage synonym lookup fallback |
| **AACT browse_interventions** | Partial | Yes (comes with AACT) | N/A | ~47% hit rate, free starting point |
| **OpenFDA** | No — approved only | Yes | Yes | Cross-references for approved drugs |
| **UNII/GSRS** | Some | Yes | Yes | Canonical substance-level identifier |

**Existing Python tools**: `drugstandards` (Jaro-Winkler fuzzy matching), `DrugNorm` (RxNorm/UMLS dictionary), `DrugLinker` (DrugBank matching). None solve the CT.gov development-code problem specifically, but provide building blocks.

**Recommended layered pipeline**:
1. Start with AACT `browse_interventions` MeSH mappings (~47% coverage, free)
2. Exact match against ChEMBL `molecule_synonyms` (best for dev codes, free API)
3. Exact match against DrugBank synonyms (free academic XML download)
4. PubChem synonym lookup (free API, high coverage fallback)
5. RxNorm `approximateTerm` API (fuzzy matching, approved drugs only)
6. `drugstandards` / `DrugNorm` fuzzy matching (final fallback)
7. Manual review for remaining unmatched high-frequency names
8. Enrich with ATC drug class via RxClass API
9. Canonical identifiers: ChEMBL ID (all phases) + RxCUI (approved drugs)

---

### Condition Normalization & Therapeutic Areas

**AACT's browse_conditions coverage**: Per Miron et al. (2020), only **62% of CT.gov condition values get an exact MeSH match**, and **81% match any UMLS ontology**. The remaining ~19-38% are unmatched free-text. Even matched entries store conditions as plain strings without concept IDs, so synonyms remain unharmonized.

**MeSH disease hierarchy**: Category C has ~24 top-level branches (C01-C26). Freely downloadable in XML and RDF from NLM (https://www.nlm.nih.gov/databases/download/mesh.html). **Polyhierarchy is the main complication** — a disease can appear in multiple branches (e.g., Diabetic Retinopathy under both Eye and Endocrine).

**Therapeutic area classification**: No universal standard exists. Typical approaches:
- **12-15 categories** — high-level industry reporting (Oncology, Cardiovascular, CNS, Infectious Disease, Immunology, Respiratory, Metabolic/Endocrine, Rare Disease, Hematology, Ophthalmology, Dermatology, GI, Mental Health)
- **27 categories** — MedDRA System Organ Classes (organ-system-based, not TA-based; requires paid license)
- **24 categories** — MeSH C-tree top-level branches (free, already linked to conditions)

A starting point for MeSH-to-TA mapping exists at NCBI Bookshelf (NBK611886): https://www.ncbi.nlm.nih.gov/books/NBK611886/table/ch4.tab1/

**Other ontologies considered**: Mondo Disease Ontology (~20k classes, open-source, bridges MeSH/SNOMED/ICD); SNOMED CT (comprehensive but overkill); ICD-10/11 (chapter headings serve as TA proxies).

**Python tools for condition normalization**:
- **QuickUMLS** — fast SimString approximate matching, best for batch processing 60-80k conditions (requires free UMLS license)
- **scispaCy** — spaCy NER + entity linking to MeSH/UMLS (pip install)
- **MetaMap** — gold standard NLP but slow and Java-based
- **text2term** — Levenshtein + TF-IDF matching

**Recommended approach**:
1. Start with AACT `browse_conditions` (~62% already MeSH-mapped, zero effort)
2. Run QuickUMLS on remaining ~38% unmapped conditions → UMLS CUIs → MeSH tree numbers
3. Build a manual MeSH C-category → therapeutic area mapping table (~24 rows; use NBK611886 as starting point)
4. Handle polyhierarchy with either multi-label assignment or a priority ordering of therapeutic areas
5. Manual curation for top ~200-500 high-frequency unmapped strings

**Key decision needed**: How to handle polyhierarchy (a disease mapping to multiple therapeutic areas). Options: allow multi-label, pick primary by priority, or pick the most specific branch.

---

## Design Decisions (Resolved)

1. **Polyhierarchy → multi-label**: Trials will be tagged with all applicable therapeutic areas. A trial for Diabetic Retinopathy gets both Endocrine and Ophthalmology tags. This means trial counts across TAs will not be mutually exclusive — acceptable tradeoff for accuracy.

2. **Drug normalization V1 → AACT MeSH + ChEMBL**: Start with AACT `browse_interventions` MeSH mappings (~47% coverage) plus ChEMBL API lookups for development codes. Defer DrugBank XML, RxNorm fuzzy matching, and PubChem to V2.

3. **Innovative design detection → V1 via NLP**: Detecting adaptive, basket, umbrella, and platform designs from free-text fields is a V1 deliverable. This is central to the project's analytical goals (comparing design innovation across therapeutic areas). Approach: keyword/regex patterns on `brief_title`, `official_title`, `detailed_description`, and `keywords` to flag innovative design features, layered on top of the structured-field classification.

## Tech Stack

- **Source**: AACT PostgreSQL (remote, queried via SQL)
- **Pipeline**: Python (extraction, transformation, orchestration)
- **Local store**: DuckDB + Parquet (raw archive)
- **Analysis**: Jupyter notebooks
- **Refresh**: Weekly full re-extract (scale is manageable: ~60-80k active studies)

## Key Reference Files
- `clinical_trials/resources/documentation_20260321.csv` — full AACT schema (53 tables, 479 fields) with ClinicalTrials.gov API field path mappings
- `clinical_trials/resources/ctti_schema_documentation.md` — naming conventions, join patterns, important caveats (date handling, group code unreliability for ~25% of studies, facility/country removal logic)
