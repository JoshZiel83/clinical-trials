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

> **Epic A complete (A1–A4) — shipped 2026-06-20.**
> - **A1/A2** (#3/#5/#7/#11/#8): Postgres scanner, atomic stage-then-swap, schema-drift
>   detection, `aact@<build-date>` pin. [ADR 0002](docs/adr/0002-extract-scanner-atomic-swap.md).
> - **A3** (#4, #10): full-cohort extract (status filter removed, ~600K); `run_pipeline.py`
>   + `src/pipeline/orchestrator.py` threading one `duck_conn` through every phase with a
>   `meta.pipeline_runs` audit row; change-event tracking in `meta.trial_change_events`
>   (`run_change_events.py`) — the single home for "what changed," absorbing #10.
> - **A4** (#6): `data/DATABASE_SCHEMA.md` reconciled to the live full-cohort DB
>   (`scripts/schema_counts.py`).
>
> Details in [`CHANGELOG.md`](CHANGELOG.md). Only the deferred items below remain open.

### Deferred (revisit on profiling)
- **[#9]** Incremental manifest-diff extraction — full-rebuild is fast enough at 5×;
  incremental is a later optimization.
- **[#12]** Delta-driven selective transform recompute — depends on #9, and the
  corpus-global mapping methods (condition co-occurrence) can't be incrementalized
  correctly anyway. **Generalized by Epic D ([#21]):** one recompute engine, two delta
  sources (records + vocabs); #12 stays the data-clock half.

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

## Epic D — Reference data lifecycle + canonicalization-version migration

*AACT has a refresh story (Epic A); the reference vocabularies (MeSH, ChEMBL,
UMLS, ROR) do not. Two independent clocks make a row stale — the **data clock**
(AACT changes) and the **reference clock** (a vocab release bumps, changing
mappings even for records that never moved). This epic gives the reference clock
the same trustworthiness Epic A gave the data clock, and defines what a
reference update does.*

Every canonical mapping is `f(record@v, vocab@v, code@v, agent@v)`. Migration
stance is set by recompute economics, not uniformly:
- **Deterministic** mappings — disposable cache; auto-recompute against the new vocab.
- **Agent** gap-fills — persisted *paid* ground truth; never auto-rerun. A vocab-diff
  splits them into oracle-grew (**auto-reclaim**), oracle-target-moved (**gate** for
  review), untouched (leave). ADR 0001 §5.0 across a version bump.
- **HITL/manual** — survives all rebuilds (ADR 0001 §3). Disagreements route to review,
  never clobbered.

- **[#18]** Register ROR in `meta.reference_sources` (currently unpinned). *Small.*
- **[#19]** Version provenance at the mapping grain + canonicalization-version tuple. *Precondition.*
- **[#20]** Per-source reference acquire/refresh/diff + retention policy.
- **[#21]** Selective re-canonicalization engine — generalizes **[#12]** to the reference clock.
- **[#22]** Staleness-as-a-query + auditable migrations.

**Decisions:** full-snapshot+diff as the default AACT refresh (subset is a later
optimization — corpus-global co-occurrence can't be incrementalized, [#12]); agent
re-runs on a vocab bump are auto-reclaim / human-gated, never blanket-automatic.

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
        ├── Epic C (canonicalization rebuild → sponsor oracle)   ← B and C interleave
        └── Epic D (reference data lifecycle + version migration) ← provenance foundation; interleaves with B/C
```

Epic A is the floor — trustworthy, regularly-refreshing data. A1/A2 (hardening +
provenance) are done; A3 (refresh automation) and the A4 docs pass (#6) remain. B and C
share the foundations and can run in parallel once those exist.

## Open questions (resolve in design, non-blocking)
- Innovative-features model class — LLM vs fine-tuned vs embedding+classifier (B2).
- Embedding backend — local vs hosted (ADR Q3; affects both B and C).
- Contextual-condition grain — per-study vs per study-cluster (ADR Q1).
- Whether to keep frontier-only on the sponsor tier even if cheaper backends suffice
  elsewhere (ADR §B′ / R6 — sharpest precision asymmetry, no external oracle).
