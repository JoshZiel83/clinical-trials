# Clinical Trials ETL Pipeline — Roadmap

**Forward-looking only.** Shipped work (Phases 0–4, 6, 7A–7C, 7E) lives in
[`CHANGELOG.md`](CHANGELOG.md). The canonicalization redesign is specified in
[ADR 0001](docs/adr/0001-canonicalization-enrichment-agent-refactor.md).

## Where we are

The 4-layer pipeline (Raw Extract → Normalized Entities → Enriched Features →
Analytical Views) is built and answers the core question — *comparing innovative
trial designs across therapeutic areas* — over a snapshot of ~119,753 active/planned
studies. Canonical entity tables (`entities.*`), reference-source versioning
(`meta.reference_sources`), and a raw-decoupled mart (`enriched.*` → `views.study_summary`)
are in place.

What remains is **consolidation, not expansion**. Three things need to be true before
this is a system rather than a one-shot build:

1. **Extract is trustworthy and refreshes on a cadence** — today it's a manual,
   non-atomic, single-snapshot pull with weak provenance.
2. **The transforms are measured, not assumed** — there is *no* accuracy measurement
   anywhere; correctness rests on mechanical unit tests and eyeballed notebooks.
3. **Canonicalization does an analyst's job** — the enrichment agent is
   context-starved, provider-coupled, and has no real eval gate (ADR 0001).

The work below is organized into three epics around exactly those gaps.

---

## Epic A — Extract hardening + refresh cadence

*Make extract trustworthy, then automate a full-cohort longitudinal refresh.*
Backlog issues [#3–#12](https://github.com/JoshZiel83/clinical-trials/issues).

**Direction (decided):** full-snapshot longitudinal. Remove the active-status filter,
snapshot the full AACT cohort (~600K), full-rebuild downstream, and track per-trial
change events run-over-run. Incremental extraction is deferred (see below).

### A1 — Extract hardening
- **[#3]** Remove dead `src/extract/connection_test.py`; consolidate the
  schema/`meta.extraction_log` bootstrap DDL into one shared helper in `aact.py`.
- **[#5]** Make an extraction run atomic — transaction / stage-then-swap so a mid-run
  failure can't leave some `raw.*` refreshed, others stale, and no log row; log
  per-table as each completes.
- **[#7]** Replace the pandas `read_sql` pass-through with DuckDB's Postgres scanner
  (`ATTACH … (READ_ONLY)` → `CREATE TABLE raw.x AS SELECT …`). Prerequisite for the
  scanner-side filtering and the full-cohort pull.
- **[#11]** Detect upstream AACT schema drift on the `SELECT *` mirror (compare
  incoming columns to an expected set; warn/fail).

### A2 — Snapshot provenance
- **[#8]** Pin the extract to a dated AACT static release and register it in
  `meta.reference_sources` (version, `acquired_at`, checksum), like `chembl@36`.
  Today `extract_date` records *when* we pulled, not *what* — the dated Parquet at
  `data/raw/YYYY-MM-DD/` (already the change-event diff source) becomes reproducible.

### A3 — Refresh automation (full-snapshot longitudinal)
- Remove the status filter (~600K studies, ~5× current). **[#4]** `STATUS_VALUES` /
  `ACTIVE_STATUSES` collapse to a single **documentation constant** — not a dedup,
  since the filter is deleted from the extract path.
- `run_pipeline.py` + `src/pipeline/orchestrator.py` — one `duck_conn` threaded
  through every idempotent phase (DuckDB single-writer); `meta.pipeline_runs` audit
  row (running → completed/failed). `aact.run_extraction()` accepts an optional
  external `duck_conn`.
- **Change events — `meta.trial_change_events`** (`src/transform/change_events.py`,
  entry `run_change_events.py`): diff current vs prior Parquet snapshot →
  `first_seen` / `dropped` / `status_transition` / `date_changed` /
  `enrollment_changed` / `phase_changed` / `conditions_changed` /
  `interventions_changed` / `sponsors_changed`. **This is the single home for "what
  changed about a study," absorbing [#10]** (do not build a separate `meta.change_log`).
  `last_update_submitted_date` cheap-gate; `--cohort-expansion` flag suppresses the
  one-time `first_seen` flood on the filter-removal run.
- Per-refresh: `run_hitl_sync` (cascade approvals) → normalize → classify → promote →
  views → change_events → enrichment agent (budget/`max_pending`-bounded).

### A4 — Docs hygiene
- **[#6]** Reconcile `data/DATABASE_SCHEMA.md` to the live DB (enriched/class table
  counts, the stale relationships diagram, conflicting coverage stats). Can land
  early and independently.

### Deferred (revisit on profiling)
- **[#9]** Incremental manifest-diff extraction — full-rebuild is fast enough at 5×;
  incremental is a later optimization.
- **[#12]** Delta-driven selective transform recompute — depends on #9, and the
  corpus-global mapping methods (condition co-occurrence) can't be incrementalized
  correctly anyway.

---

## Epic B — Transform pressure-testing + ML upgrade

*Measure where the transforms are actually weak, then upgrade the weakest with a real
model — starting with innovative-features.* Measure-first.

### B1 — Transform accuracy eval substrate
There is **no accuracy measurement today** — ~247 tests are all mechanical, and the
notebooks only report coverage % + manual spot-checks. Build a measurement harness +
labeled data spanning condition mapping, drug mapping, design classification
(L2/L4/L5), TA assignment, and innovative-feature detection. Two-tier gold (per ADR
0001 §5): an **auto-harvested regression floor** (high-confidence deterministic
outputs stripped back to inputs) + a **curated hard-tail set**; a best-in-class
frontier model sets the achievable ceiling, cheaper backends scored as a fraction.
- **Reuse:** the per-test in-memory DuckDB builders and the `_FakeClient` /
  `_FakeAsyncClient` mocks in `tests/agent/test_enrichment_agent.py`; parametrize the
  notebook spot-check samples (nb 03/05) into scored assertions. No `conftest.py`
  exists yet — add shared fixtures here.
- **First shared piece:** a golden-eval *runner* that actually consumes
  `tests/fixtures/enrichment_golden.json` (orphaned today, 10 items) and grows it.
- **Benchmark targets (ADR 0001 §5.2):** precision-at-coverage, not accuracy.
  Auto-accept (no human) **≥99%** precision; route-to-review **~80–90%**; below
  threshold abstain. Per-domain: condition ≥98–99%, drug ≥99%, sponsor no auto-accept
  (merge precision ≥95–99% + block recall), innovative-features per-class P/R + macro-F1
  vs the regex baseline. **Sizing is the precondition** — ~200–400 labeled hard-tail
  items/domain to make a 95–99% claim measurable (current fixture: 10).

### B2 — Innovative-features: regex → NLP/ML model
Keep the regex (`INNOVATIVE_PATTERNS` in `src/transform/innovative_features.py`) as
the baseline/floor. Build a model (LLM-classifier vs fine-tuned vs embedding+classifier
— decided in design) evaluated against B1's gold set, integrated behind the existing
`class.innovative_features` contract so the mart is untouched.

### B4 — Base-layer inversion: stop re-deriving `browse_*` (measured)
First measured B1 finding ([#13](https://github.com/JoshZiel83/clinical-trials/issues/13)).
The deterministic condition/drug canonicalization **re-derives what AACT's
`browse_conditions` / `browse_interventions` already provide** and is also lossy:
~**62%** of mapped condition rows (the `exact` layer **100%**) are directly
recoverable from `browse_conditions`, drug `mesh-exact` is **99%** recoverable from
`browse_interventions`, yet the lexical-string keying **drops 42,134 (study, MeSH)
pairs** AACT hands us for free. The genuine, non-replicable value is the cross-study
string→MeSH transfer (~38%, mostly `1:1-study`) + the ~11K studies NLM never tagged,
and — for drugs — ChEMBL IDs + control-term normalization.
**Direction:** invert the base layer — read `browse_*` mesh-list directly as the
authoritative study→MeSH layer, and use the dictionary only as a **gap-filler** on
top (the "resolve-or-extend against an oracle" framing of ADR 0001 §5.0 applied to
the deterministic layer). Deletes the redundant work, recovers the dropped pairs,
keeps the genuine 38%. Spot-check precision before committing.

### B3 — Opportunistic upgrades
Other transforms B1 flags as weak get queued — not pre-committed. (TA mapping, design
L2, and `innovative_features`/`ai_mentions` were screened as genuinely non-replicating.)

---

## Epic C — Canonicalization rebuild + sponsor oracle

*Execute [ADR 0001](docs/adr/0001-canonicalization-enrichment-agent-refactor.md).*
Supersedes the old Phase 7D (reactive sponsor merge) and 7F (drug fuzzy v2).

Sequenced per ADR §9:
0. **Loop + eval first.** Hand-rolled tool-calling loop behind a one-method inference
   adapter (litellm); assemble the two-tier dataset; set the per-domain ceiling with a
   frontier model. *No model is the baseline — the labeled data is.* (Blocking.)
1. **Lock the adapter behind the eval.** Confirm no harness regression; exercise a
   second backend; delete the sync/async fork and the arity dispatch.
2. **Tool redesign.** Add `read_trial_record` (the missing context tool) + semantic
   retrieval over the canonical vocabulary; keep QuickUMLS + ROR; demote rapidfuzz to
   a hidden pre-filter.
3. **Condition override layer + ambiguity router.** New
   `norm.study_condition_overrides`; `create_study_conditions` precedence
   (study override > global dictionary); promote-path branch. The global dictionary
   stays the default; only ambiguous strings (the `diabetes` class) pay per-study cost.
   Gate on view-parity + coverage tests.
4. **Backend A/B + tiering.** Score a second backend (OpenAI and/or local) on the same
   data; decide cheap-vs-frontier tiering from evidence.
5. **Build the sponsor oracle** (ADR Axis B″). Block (rapidfuzz + embeddings +
   co-occurrence + ROR aliases) → leader/canopy clustering with an LLM similarity
   judge → human-review the highest-impact groups → seed `entities.sponsor` + register
   in `meta.reference_sources`. Retires the 7D reactive agent; sponsor leaves the
   per-item loop and becomes an offline build + cheap incremental assignment.
   Separately, re-home the drug agent (7F) onto the new harness.

**Preserved invariants (ADR §3, non-negotiable):** precision ≫ recall; grounding
(cite ≥1 tool or be rejected); abstain over guess; SHA cache; USD budget;
`max_pending`; HITL review→promote; entities only from trusted vocab or approved HITL;
manual entries survive rebuilds.

---

## Shared foundations (build once, used by B and C)

The consolidation insight: Epics B and C are not two agent problems but **one
inference + eval substrate** used twice.
- **Two-tier eval harness** — gold datasets + frontier ceiling + backend scoring.
- **LLM inference adapter** — litellm, one-method `propose(...)` boundary.
- **Embeddings** — semantic retrieval (C) and clustering blockers (C5), reusable for
  feature classification (B2).

**Dependency reality:** the repo carries only `anthropic` + `rapidfuzz` + `duckdb`
today — no sklearn / spacy / sentence-transformers / torch / litellm / pydantic-ai.
litellm, an embedding backend, and any ML model class are **net-new deps**, in tension
with ADR D5 ("keep the repo lean"). Make this an explicit decision in this step, not a
per-epic accretion; prefer the lightest option that clears the eval (e.g. local
sentence-transformers over a hosted embedding API — ADR Q3).

---

## Sequencing

```
Epic A (harden extract + cadence)        ← near-term priority
        │
Shared foundations (eval + adapter + embeddings)
        │
        ├── Epic B (transform QA → innovative-features ML)
        └── Epic C (canonicalization rebuild → sponsor oracle)   ← B and C interleave
```

Epic A is the floor — trustworthy, regularly-refreshing data — and its cleanups
(A1) and docs pass (A4 / #6) can start immediately. B and C share the foundations and
can run in parallel once those exist.

## Open questions (resolve in design, non-blocking)
- Innovative-features model class — LLM vs fine-tuned vs embedding+classifier (B2).
- Embedding backend — local vs hosted (ADR Q3; affects both B and C).
- Contextual-condition grain — per-study vs per study-cluster (ADR Q1).
- Whether to keep frontier-only on the sponsor tier even if cheaper backends suffice
  elsewhere (ADR §B′ / R6 — sharpest precision asymmetry, no external oracle).
