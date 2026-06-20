# ADR 0001 — Refactor the canonicalization enrichment agent

- **Status:** Accepted
- **Date:** 2026-06-12 (accepted 2026-06-20)
- **Deciders:** Josh Ziel
- **Supersedes / touches:** Phase 6E (enrichment agent), Phase 7D (sponsor merge, flag-gated), Phase 7F (drug dedup, deferred)

---

## 1. Context and problem statement

Canonicalization is the system's primary value-add. Source records in AACT are
*technically correct but messy* — `Novartis` vs `Novartis Pharmaceuticals`,
`nab paclitaxel` vs `PACLITAXEL`, `diabetes` vs `Diabetes Mellitus, Type 2`.
Without resolving these to stable canonical entities, the dataset cannot answer
the questions that justify it ("all Type 1 diabetes trials", "AI-relevant trials
run by Pfizer"). Deterministic layers (exact / 1:1-study / co-occurrence / MeSH /
ChEMBL) resolve the easy bulk; a Claude agent (Phase 6E) is meant to do what a
human analyst would do for the residual tail — look at the record, make the
obvious mapping, otherwise investigate.

In practice the agent under-performs that vision for three reasons, all
confirmed in code:

1. **Context starvation.** The agent's unit of work is a *bare lowercased
   string*. `_select_pending_inputs` for conditions does
   `SELECT LOWER(c.name) ... GROUP BY LOWER(c.name)` — every trial the string
   appeared in is discarded before the agent sees it. `_run_one_item_async`
   then hands the model only `source_value` + `study_count`. The agent is
   structurally prevented from doing an analyst's first move: *open the trial
   and read it*. `diabetes` → Type 1 vs Type 2 is unanswerable from the string
   alone, so the agent abstains or guesses.

2. **Bad instruments.** Because we removed the record, we gave the agent
   string-shape similarity (`rapidfuzz.WRatio`) as a substitute for meaning.
   String shape was never the signal; it adds noise (the same root cause that
   motivated deferring 7D/7F).

3. **Provider-coupled harness.** The loop runs on Anthropic's
   `client.beta.messages.tool_runner` with `@beta_tool` / `@beta_async_tool`
   decorators and `thinking={"type": "adaptive"}`. We want an open-source
   harness with a **swappable inference backend** (Anthropic today; possibly
   OpenAI or a validated local model later) that is **not built by a model
   provider**.

This ADR decides how to rebuild the subsystem. It is deliberately scoped to
**three coupled axes** — work unit, tooling, and harness — because none can be
chosen independently: giving the agent record context changes the message shape
and the tool surface, which changes what the harness must carry.

### Non-goal / preserved invariant

The error cost is asymmetric and that does not change: an **abstain** leaves a
row uncanonicalized (limited value, honest); a **wrong merge/map** silently
corrupts every downstream query forever. Precision ≫ recall. The grounding
contract ("cite ≥1 tool return or be rejected; abstain rather than guess")
stays a hard requirement. The refactor widens the agent's *information and
instruments*; it does **not** loosen its guardrails.

---

## 2. Decision drivers

- **D1 — Canonicalization quality on the hard tail** (the actual product).
- **D2 — Backend swappability**, validated empirically per-backend, on an
  open-source harness not authored by a model provider.
- **D3 — Precision over recall**; preserve grounding + abstain + HITL gate.
- **D4 — Contained blast radius.** The dictionaries plug broadly into the ETL
  (`norm.*`, `enriched.*`, `views.study_summary`); changes must not force a
  schema rewrite of the whole mapping layer.
- **D5 — Keep the repo lean.** Today's agent deps are `anthropic` + `rapidfuzz`
  + `duckdb`. Avoid dragging in a large, fast-moving framework stack.
- **D6 — Remove accidental complexity** already flagged: the dual sync/async
  duplication and the tool-name arity dispatch in `_build_tools_for_agent`.
- **D7 — Error-hardened execution.** The hand-rolled loop must own robust
  retry / backoff / timeout and isolate per-item failures; losing the Anthropic
  SDK's built-in retries (`max_retries=5`) is not acceptable. See §5.1.

---

## 3. Current architecture (the surface a refactor must respect)

**Lifecycle:** `normalize_*` builds a per-domain dictionary → `norm.study_*`
LEFT-JOINs raw rows to the dictionary (unmatched ⇒ NULL FK) → agent reads the
unmatched residual → writes `ref.mapping_candidates(status='pending')` → Shiny
review writes a decision-log parquet → `run_hitl_sync` → `promote_candidates`
upserts an `entities.*` row (`origin='manual'`) and inserts the dictionary row →
`views.study_summary` joins `norm.* → entities.*` for labels.

**Load-bearing assumptions (must preserve or explicitly migrate):**

- **LB1 — Global string keying.** All three dictionaries are
  `PRIMARY KEY (source string)`. Conditions join `ON LOWER(c.name) =
  d.condition_name`; **one canonical per source string, globally, independent
  of study.** This is the assumption `diabetes` breaks.
- **LB2 — Dictionary is the sole gatekeeper.** `norm.study_*` join the
  *dictionary*, not `entities.*`. Views read the FK (`condition_id` /
  `drug_id` / `sponsor_id`) into `entities.*` for the label.
- **LB3 — Unmatched ⇒ NULL FK** (`condition_id`/`drug_id`/`sponsor_id`);
  drugs also carry `mapping_method='unmatched'`. Coverage stats and the agent's
  input selection key off this.
- **LB4 — Entities only from trusted vocab or approved HITL**, resolved at
  approve-time; never from unresolved candidates.
- **LB5 — Sponsor merge lineage** (`merged_into_id` + `entities.sponsor_resolved`
  recursive view) resolves at the view layer (Phase 7D, flag-gated).
- **LB6 — Manual entries survive rebuilds** (pinned by tests across all three
  domains).

**Harness surface to replace:** `@beta_tool`/`@beta_async_tool`;
`client.beta.messages.tool_runner(model, max_tokens, system=[{...ephemeral}],
tools, messages, thinking={"type":"adaptive"})` yielding messages with a
`.usage` (`input_tokens`, `output_tokens`, `cache_read_input_tokens`,
`cache_creation_input_tokens`).

**Eval reality:** `tests/fixtures/enrichment_golden.json` holds **10 items**
total (5 condition / 3 drug / 2 sponsor) and is **not consumed by any test
runner**. The roadmap's "~200 labeled items per domain, gates prompt/tool
changes" is aspirational, not built.

---

## 4. Considered options

### Axis A — Harness / backend

| Option | Verdict |
|---|---|
| **A0. Keep Anthropic `tool_runner`** | Rejected. Fails D2 (provider-coupled). It's also the source of the dual sync/async duplication. |
| **A1. LangChain `deepagents`** | **Rejected.** Wrong altitude. `deepagents` (v0.6.8, Beta, pre-1.0) is built for *long-horizon* work — it injects planning (`write_todos`), sub-agents (`task`), a virtual filesystem, and a summarization middleware **by default**, none of which a shallow single-item finalize-or-abstain task needs. It has **no native finalize/abstain terminal** (you model it yourself anyway). Its **default model is Anthropic**, and `langchain-anthropic` + `langchain-google-genai` + `langsmith` + full `langchain` v1 are **hard dependencies** — a large, fast-moving surface dropped onto a lean repo, violating D5. Known rough edges on non-Anthropic backends (e.g. `ChatOpenAI` dropping system content under middleware). Right tool for a different problem. |
| **A2. Hand-rolled loop + thin provider adapter (litellm)** | **Chosen.** ~80-line `while` loop: send → parse `tool_calls` → dispatch via a registry → append results → terminate when the model calls `finalize` or `abstain`. The grounding gate ("must have cited a tool before finalize") is a stateful check we *want* to own — it's the precision-critical core. Inference sits behind a one-method adapter; the first implementation uses **litellm** (lightest deps, truest "swap the model string", OpenAI-format tool calls, authored by BerriAI not a model provider). Satisfies D2/D3/D5/D6. |
| **A3. pydantic-ai** | Strong runner-up; recorded as the sanctioned upgrade path. Model-agnostic, not a model provider, and its validated structured output maps cleanly onto a `Mapping \| Abstain` discriminated union. Deferred because it adds framework idioms (`RunContext`/DI) and we prefer to own the terminal/grounding logic explicitly first. Revisit if hand-rolled output-validation becomes a maintenance burden. |

The **real decision is the adapter boundary, not the library.** Isolating
inference behind `propose(record, candidates) -> Proposal | Abstain` makes
litellm⇄pydantic-ai⇄direct-SDK a contained swap and makes per-backend golden-eval
runs trivial.

### Axis B — Work unit & the condition contextual-mapping problem

| Option | Verdict |
|---|---|
| **B0. Keep bare-string, global-only** | Rejected. Cannot resolve LB1 / the diabetes class of failures (D1). |
| **B1. Re-key condition dictionary on `(nct_id, condition_name)`** | Rejected. Largest blast radius — rewrites every dictionary schema, every `norm.*` join, entity resolution, views aggregation, and all pinned tests. Disproportionate (violates D4). |
| **B2. Ambiguity router + per-study override layer** | **Chosen.** Keep the global dictionary as the default for unambiguous strings (most strings; the existing 1:1-study layer is evidence). *Detect* ambiguity cheaply — a source string co-occurring with **multiple distinct MeSH terms** across studies (the existing `co_occurrence_condition` signal, repurposed as a router). Route only ambiguous strings to **per-study resolution**, writing to a new `norm.study_condition_overrides(nct_id, condition_name, condition_id, …)`. `norm.study_conditions` gains a precedence rule: **study override > global dictionary**. Views are unchanged — they still read `condition_id → entities.condition`. Blast radius confined to the `create_study_conditions` build step + one new table. Honors the precision asymmetry: only ambiguous strings pay the per-study cost. |

Sponsor and drug remain **global** domains — the string very nearly determines
the canonical (`Novartis Pharmaceuticals → Novartis` is true in every trial).
Their context need is *evidence for a global decision* (co-sponsorship, ROR
hierarchy, chemical parent), not per-study divergence. So B2 applies to
conditions only.

### Axis B′ — authoritative vocabulary vs self-constructed (why sponsor is special)

There is a second, orthogonal axis the original plan under-weighted:

| Domain | Mapping grain | Canonical authority |
|---|---|---|
| Condition | **contextual** | MeSH / UMLS — authoritative external ontology |
| Drug | global | ChEMBL / MeSH / UNII — authoritative external ontology |
| **Sponsor** | global | **none** — ROR is partial; the canonical space is *self-constructed* (`entities.sponsor.origin='aact'`) |

For conditions and drugs the dictionary is essentially a **cache** over an
external truth table: the agent *retrieves against* MeSH/ChEMBL (C2) and the
entity usually already exists. For sponsors there is no such oracle — ROR's
coverage of pharma subsidiaries, hospital systems, and CROs is patchy, and
deterministic normalization only collapses *spelling* variants
(`Pfizer Inc.`/`PFIZER INC`), never the substantive merges
(`Novartis Pharmaceuticals → Novartis`). So the sponsor **dictionary + anchor
set + merge graph IS the canonical authority** — HITL here is genuine knowledge
*creation*, not lookup. This is exactly why the dictionary primitive is most
load-bearing for sponsors and least for conditions. Consequences for the plan:

- **Sponsor keeps the Phase 7D tool set** (anchor lookup, co-occurrence, ROR)
  and does **not** get the C2 "retrieve over authoritative vocab" tool — there
  is no vocab to retrieve over. Its retrieval surface is our own anchor set + ROR.
- **Sponsor gold cannot be auto-harvested** from deterministic agreement (the
  regression-floor trick in §5 yields only trivial spelling-collapse labels for
  sponsors). Sponsor gold = the curated `data/reference/sponsor_anchors.json`
  include/exclude file + ROR-confirmed merges + human-labeled hard cases. The
  anchor curation file is, in effect, the hand-built sponsor gold set; version
  it as such.
- **The precision asymmetry is sharpest here.** A wrong condition/drug map can be
  caught later against the ontology; a wrong sponsor merge has *no external
  oracle* to catch it. Sponsor proposals therefore stay the most conservative
  (ROR hit or ≥5 co-occurring studies required — already in the 7D prompt) and
  keep a higher human-review bar. This is the strongest standalone argument for
  keeping a best-in-class frontier reasoning model on the **sponsor tier**
  specifically, even if cheaper backends prove adequate for condition/drug.

### Axis B″ — build the sponsor oracle proactively, by clustering (chosen)

The original 7D plan is *reactive*: anchor on the top ~200 canonicals and ask the
agent, per lower-frequency canonical, "is this a variant of an anchor?" That
structurally leaves the **long tail uncanonicalized** — variants of mid- and
low-frequency sponsors never merge, so "trials run by [mid-size sponsor]" stays
broken.

**Decision:** instead, **construct a sponsor oracle offline**, converting the
"no external oracle" problem (Axis B′) into a *self-built* one. Once built,
sponsor canonicalization becomes a **lookup** against the oracle — the same shape
as condition/drug lookup against MeSH/ChEMBL. This **supersedes 7D's reactive
anchor-merge agent.**

**Scoping result (measured 2026-06-12, stratified + random-order — `notebooks/sponsor_synonym_scoping.ipynb`):**
synonyms are common enough to justify the oracle, and — overturning the initial
"mostly research institutes" read — concentrated in INDUSTRY and UNKNOWN, not
academic. Per-stratum Horvitz–Thompson over random-order samples with a
conservative LLM judge; all figures are **recall floors**:

| stratum | corpus | prevalence (≥1 synonym) | est. synonym pairs (95% CI) | group sizes |
|---|---|---|---|---|
| UNKNOWN | 12,973 | 19.9% | 1,551 (1,254–1,875) | up to 6 |
| INDUSTRY | 7,226 | 14.7% | 1,007 (792–1,218) | up to 8 (**clustered**) |
| OTHER (academic) | 15,783 | 6.5% | 560 (470–651) | mostly pairs |

Overall floor ≈ **13%** of names (~4,700 across these strata; ~3,100 pairs),
higher once recall < 1 and cross-stratum pairs are counted. Three consequences:
- **Industry is NOT clean** — it has the most *clustered* synonymy (groups to
  size 8: parent/subsidiary/legal-entity sprawl), so it needs the oracle + ROR +
  hierarchy reasoning, *not* a downscope. (Reverses an earlier "lean on ROR, tier
  industry down" idea.)
- **Stratify the *measurement*, not the *build*.** Within-stratum sampling misses
  cross-stratum pairs (e.g. an INDUSTRY name whose variant sits in UNKNOWN), so
  the clustering must span all strata; `agency_class` is a blocker *hint*, not a
  hard partition.
- **Caveat:** the conservative prompt most suppresses *academic* grouping (shared
  generic tokens like "University Hospital"), so OTHER's 6.5% is the softest
  floor and the INDUSTRY–OTHER gap may be narrower than shown.

**Implied true entity count (`notebooks/sponsor_synonym_distributions.ipynb`):**
the per-record synonym distribution is **overdispersed** — strongly for INDUSTRY
(var/mean ≈ 3.0: parent/subsidiary/legal-entity families), near-Poisson for
academic (≈ 1.2). Modelling per-org variant counts (Poisson fit to pairs-per-name,
and a negative binomial fit to **both** pairs *and* prevalence), the **~37.5k
normalized names collapse to ≈ 33–35k true organizations** — i.e. **~2,500–3,000
redundant names, a floor** (recall < 1, cross-stratum pairs unseen). Where
overdispersion is real (industry), NB is the correct shape and puts redundancy at
~640 vs Poisson's ~940 — **Poisson overstates the collapse** for clustered
families. So the oracle's collapse is single-digit-percent in *count* but
concentrated (industry/unknown) and meaningful in absolute terms (each redundant
name silently splits a "trials by X" query). The definitive number falls out of
running the clustering itself.

**Algorithm — leader/canopy clustering with an LLM similarity judge:**
1. Operate on the **normalized names** — `normalize_sponsor_name()` over distinct
   `raw.sponsors.name` yields **37,483** names (measured 2026-06-12). Note: the
   deterministic suffix-stripping collapses only 37,755 → 37,483 (<1%), so
   clustering does essentially all the real merging. (NB: this DB currently has
   *no* `entities.sponsor` / `ref.sponsor_dictionary` — the oracle build will
   populate `entities.sponsor` from `raw.sponsors` directly.)
2. **Block before you compare (non-negotiable).** Generate each name's
   candidate-mates cheaply via a *union* of blockers — normalized-string
   similarity (rapidfuzz), embedding nearest-neighbors, **co-occurrence** (shared
   trials; strong for subsidiaries and acronym↔expansion pairs that string/
   embedding similarity miss), and ROR aliases. The LLM only adjudicates *within*
   a name's candidate set.
3. **Tier by trial volume (measured 2026-06-12).** 63% of names (23,651) appear
   in exactly one trial and carry ~11% of links; 4,414 names have >5 trials and
   carry ~75%. So: **seed leaders from the >5-trial set** (+ curated anchors) —
   clean, high-frequency, highest-impact groups form first; **adjudicate the rest
   of the ~13.8k multi-trial names**; and treat the **23.6k single-trial tail as
   assignment-only** — block them against existing groups, leave non-matches as
   singletons, and never spend careful adjudication *or* human review on a
   one-trial sponsor. The real clustering problem is **~4–14k names, not 37.5k**.
4. **Judge by similarity to the leader, not transitively.** Leader-membership
   avoids the single-linkage *chaining* failure (a generic fragment like
   "University Hospital" bridging two distinct institutions). Add an explicit
   generic-fragment guard.
5. **Batch the comparison:** one prompt judges the leader against *k* candidates,
   not *k* prompts.
6. Output flat synonym groups → one canonical per group → seed/refresh
   `entities.sponsor`; register the oracle version in `meta.reference_sources`.
   New sponsors from refreshes are assigned incrementally (block + compare against
   group leaders, else open a new group).

**Scale note — why blocking is non-negotiable (measured):** all-pairs over 37,483
names is **702M** LLM comparisons (infeasible). Blocking at ~10 mates/name ≈ 375k
candidate pairs; batched 20-per-prompt ≈ **~18.7k LLM calls** (low-hundreds of $,
one-time). The O(n²) naive sweep is the single thing that makes seed-and-sweep
fail; the blocker is what rescues it.

**Verification (no oracle to check against):** human-review the highest-
study-impact groups (a wrong merge there corrupts the most queries); ROR as a
partial cross-check; the anchor file as the human-verified core. Conservative
when unsure — an un-merged variant is honest, a wrong merge is silent corruption.

**Relationship to 7D:** subsumes the reactive anchor-merge agent. The 7D
*plumbing* is reused selectively — `merged_into_id` + `sponsor_resolved` only if
v1 goes hierarchical (Q5); co-occurrence and ROR become **blockers**, not
per-item agent tools. So sponsor leaves the per-item enrichment loop entirely and
becomes an offline oracle build + cheap incremental assignment.

### Axis C — Tools

Replace "matching algorithms" with what an analyst reaches for:

- **C1. `read_trial_record(nct_id…)`** — title, brief/detailed summary, other
  conditions, interventions, phase, eligibility. The missing context tool. The
  single highest-leverage change for D1.
- **C2. Semantic retrieval over the canonical vocabulary** (embeddings over
  MeSH descriptors / ChEMBL pref-names / sponsor canonicals) — retrieval over
  *meaning*, the thing fuzzy matching was a bad proxy for. **Keep QuickUMLS**
  (real ontology grounding). **Demote `rapidfuzz`** to a coarse pre-filter that
  narrows the agent's input set; never a reviewer-facing candidate.
- **C3. Keep external lookups** — ROR for orgs (sponsors); web/UMLS for novel
  terms. Keep `entities.sponsor.merged_into_id` merge plumbing (7D) intact.

With C1+C2 the agentic loop *earns* its multi-step shape (read record → form
hypothesis → check against vocabulary → escalate), which the bare-string version
never did.

---

## 5. Decision outcome

### 5.0 Unifying principle — every canonicalization resolves-or-extends an oracle

The three domains stop being three problems. Each has (or is given) an
**oracle**: a canonical entity table that is the single source of truth for that
domain. Canonicalization is one operation everywhere — **resolve** a source value
to an oracle entry, or, on a genuine miss, **extend** the oracle by minting a new
entry (grounded in evidence, human-reviewed at high impact). `entities.condition`,
`entities.drug`, and `entities.sponsor` already *are* these oracles (Phase 7B);
this ADR makes "resolve-or-extend against the oracle" the explicit, uniform
mechanism and pays off that architecture rather than replacing it.

The realization the original plan missed: **MeSH and ChEMBL are themselves
incomplete oracles.** A condition absent from MeSH and a sponsor with no registry
entry are the *same* problem — a coverage gap — differing only in size (a hole
vs. an empty table) and in whether an external source can vouch for the fix. So
"build the sponsor oracle" (Axis B″) is not a special case; it is the extreme
point of a gap-fill that conditions and drugs need too (the ~25k unmapped
conditions are exactly this gap).

The corollary on the *resolve* side (measured 2026-06-20, issue #13): AACT's
`browse_conditions` / `browse_interventions` tables are themselves a **pre-resolved,
per-study oracle layer** — NLM has already attached MeSH to most studies. The
current deterministic layer *re-derives* this rather than reading it: ~62% of mapped
condition rows (the `exact` layer 100%; drug `mesh-exact` 99%) are recoverable
straight from `browse_*`, while lexical-string keying drops ~42k (study, MeSH) pairs
AACT already provides. So "resolve-or-extend against an oracle" applies one level
below the agent too: the deterministic base should **read** `browse_*` as the
study-level oracle and reserve the dictionary/agent for the genuine gap (cross-study
transfer + the studies NLM never tagged). This sharpens, not changes, the principle.

What differs per domain is **not the mechanism** but how the oracle is seeded,
how resolution is done, and how a mint is trusted — *uniform mechanism,
domain-specific trust model* (Axis B′):

| | Oracle seed | Resolve tactic | Mint trust / verification |
|---|---|---|---|
| **Condition** | MeSH ∪ UMLS (holes: ~25k studies) | semantic retrieval + read trial record; per-study (contextual) | cross-checkable vs UMLS |
| **Drug** | ChEMBL ∪ MeSH ∪ UNII (holes: biologics/combos/new codes) | semantic retrieval over vocab | cross-checkable vs ChEMBL/MeSH |
| **Sponsor** | *none* — built by clustering (Axis B″) | blocking + cluster-assignment | **no external check** → human review at impact; ROR partial |

Everything below is an instance of this principle.

Rebuild the enrichment subsystem along all three axes:

1. **Harness (A2):** a single hand-rolled tool-calling loop. One implementation
   (delete the sync/async fork), uniform `(ctx, **kwargs)` tool signature
   (delete the name-string arity dispatch). Inference behind a one-method
   provider adapter; implement with **litellm**. Terminal `finalize` / `abstain`
   tools and the grounding gate live in *our* loop, not the library.
2. **Work unit (B2):** mapping task carries **record context**. Conditions gain
   an **ambiguity router + per-study override layer**; the global dictionary
   stays the default. Sponsor/drug stay global.
3. **Tools (C1–C3):** add `read_trial_record`; add semantic retrieval; keep
   QuickUMLS + ROR; demote rapidfuzz to a hidden pre-filter.
4. **Eval (new, blocking) — the gold dataset is the benchmark, not any model.**
   No model (Anthropic included) is the reference. We build the loop, assemble a
   labeled dataset, and run it through a **best-in-class frontier reasoning
   model** (need not be Claude) to establish the *achievable quality ceiling*;
   every other backend — cheaper, swappable, or local — is then scored as a
   fraction of that ceiling on the same data. The model is a measuring
   instrument, not ground truth. The dataset is **two-tier**:
   - **Auto-harvested regression floor.** Hold out a sample of mappings the
     deterministic layers *already* produce with high confidence (condition
     `exact`/`1:1-study`/`co-occurrence`; drug `mesh-exact`/`chembl-synonym`),
     strip them back to the raw string, and reserve the known canonical as the
     label. Free, large, unambiguous. **Caveat:** this is the *easy*
     distribution the agent won't face in production — it guards plumbing and
     regressions, it is **not** a measure of hard-tail quality. (Does not work
     for sponsors — see Axis B′.)
   - **Curated hard-tail set.** Human-labeled (optionally frontier-assisted,
     human-confirmed) ambiguous cases — the distribution the agent actually
     sees. This is the real product-quality gate: smaller, expensive, the thing
     that matters. Sponsor's curated set is anchored on
     `data/reference/sponsor_anchors.json` (Axis B′).
5. **Guardrails preserved (D3):** grounding, abstain-over-guess, SHA cache,
   USD budget, `max_pending`, HITL review→promote, the LB4 entity invariant.
6. **Error-hardened loop (D7):** the loop owns retry/backoff/timeout and per-item
   failure isolation that the SDK previously gave us for free. See §5.1.

### 5.1 Error handling & retry (the hand-rolled loop)

Moving off the SDK means we no longer inherit `max_retries=5`; the loop must be
explicitly hardened. Error classes and policy:

- **Transient transport** (429, 500/502/503/504, connection reset, read
  timeout): exponential backoff **with jitter**, capped attempts (≈5); honor a
  `Retry-After` header when present. Per-request timeout so a hung socket can't
  occupy a concurrency slot indefinitely.
- **Fatal / auth** (401/403, bad key, quota exhausted): **abort the run fast** —
  no retry, no further budget spend. (Preserve the existing `auth_fail`
  short-circuit.)
- **Model-side malformed action** (invalid JSON tool args, call to an unknown
  tool, `finalize` without a prior tool call): feed the error back to the model
  as a tool result and let it self-correct, **bounded** by a per-item correction
  cap; never crash the item.
- **Non-termination** (model never calls `finalize`/`abstain`): cap iterations
  per item; on exhaustion record `failed` — *not* a silent guess — and continue.
- **Item isolation:** any item that exhausts retries is counted in `items_failed`
  and the **batch continues**; one poison item never kills the run (current
  behavior, preserved).
- **Resumability:** the SHA cache already makes completed items free on rerun, so
  a crashed/aborted batch resumes cheaply against the same budget. Keep it.
- **Optional provider fallback:** litellm's Router can fail a persistently-erroring
  primary over to a secondary model — wire once multi-backend is real (step 4),
  off by default.

Jitter matters specifically because workers run concurrently
(`AGENT_DEFAULT_CONCURRENCY=4`); un-jittered backoff would synchronize retries
into a thundering herd against a rate-limited endpoint. All of this gets unit
tests against a **fault-injecting fake adapter** — retry logic is exactly what
rots silently when untested.

---

### 5.2 Benchmark targets — precision at coverage, not accuracy

The benchmark metric is **precision at a chosen coverage level**, never raw
"accuracy." Two facts force this: the **HITL gate** (the model is a candidate
generator feeding a reviewer, not an autonomous writer) and the **asymmetric error
cost** (§1 non-goal: a wrong write silently corrupts every downstream query forever;
an abstain just leaves a gap — roughly a 100:1 cost ratio). Accuracy is also
degenerate under class imbalance: at 3.9% innovative-feature prevalence, predicting
"no feature" everywhere scores 96% accuracy and is useless.

So there is no single target — there are **operating points** on a precision–coverage
curve, with coverage (recall) maximized *subject to* a precision floor, never traded
against it:

| Path | Precision target | Rationale |
|---|---|---|
| **Auto-accept** (no human) | **≥99%** | Matches/beats the deterministic `exact`/`1:1` layers it augments; a wrong auto-write is permanent silent corruption. |
| **Route-to-review** | **~80–90%** | Below ~70% reviewers lose trust and the queue is noise (the fuzzy 7D/7F failure mode). Below threshold ⇒ **abstain**. |

**Per-domain bars** (track the oracle, Axis B′):
- **Condition** — auto-accept ≥98–99% (cross-checkable vs UMLS); contextual cases
  route to per-study review, never auto-accept.
- **Drug** — auto-accept ≥99% (brand/generic/salt errors are insidious;
  cross-checkable vs ChEMBL/MeSH).
- **Sponsor** — **no auto-accept** (no external oracle). Benchmark **merge precision
  on high-impact groups** (≥95–99%) *and* **block recall** — a missed pair never
  reaches the judge, the silent failure here.
- **Innovative-features (B2)** — not identity, doesn't corrupt joins: **per-class
  precision/recall + macro-F1 vs the regex baseline**, not accuracy. Bar: beat regex
  on rare-class recall (some classes <100 studies) without tanking precision.

**The two gold tiers answer different questions.** The auto-harvested regression
floor should score ~100% — it's a plumbing gate, not a quality measure. The curated
hard tail is where the real number lives and where the frontier model sets the
achievable ceiling; on genuinely ambiguous cases ~90% precision at ~50% coverage is a
fine result *because the rest abstains to review*.

**Sizing precondition (blocking; cf. R5).** A precision number is only as credible as
the gold set behind it: ~**400 labeled hard-tail items/domain** to claim ≥99%
(95% CI ≈ ±1%), ~**200/domain** for ≥95% (±3%). The current fixture is **10 items
wired to nothing** — sizing the curated set to ~200–400/domain is step-0, not optional.

**How each number is set empirically** (not by decree): **floor** = the measured
precision of the deterministic layers (the auto-accept bar to beat); **ceiling** = the
frontier model on the hard tail (what's achievable); **operating threshold** = where
calibrated confidence yields the target precision, with coverage tuned up to that line.

## 6. Side-effect surface & migration

| Component | Change | Risk |
|---|---|---|
| `src/agent/enrichment_agent.py` | Replace `tool_runner` loop with hand-rolled loop over the adapter; delete sync/async duplication; keep `RunStats`, cache, budget, `max_pending`, writer-coroutine concurrency. | Med — core rewrite, but covered by the new eval + existing concurrency tests. |
| `config/settings.py` | Inference config becomes provider-neutral (`AGENT_DEFAULT_MODEL` → litellm model string e.g. `anthropic/claude-opus-4-8`); pricing table keyed by litellm id. | Low. |
| `src/agent/enrichment_tools.py` | New `read_trial_record`, semantic-retrieval tool; rapidfuzz demoted; uniform tool signatures; `DOMAIN_TOOLS` updated. | Low–Med. |
| `src/agent/ror_tool.py`, `quickumls_tool.py` | Unchanged interfaces; just re-registered. | Low. |
| **`src/transform/normalize_conditions.py`** | `create_study_conditions` gains override precedence; add `co_occurrence`-based ambiguity flag. **Preserves LB1 for unambiguous strings.** | **Med–High — most invasive change.** Pin with new tests; existing condition tests must still pass. |
| New `norm.study_condition_overrides` | Per-study contextual mappings; promoted from HITL like the dictionary. | Med — new table, new promote path branch in `src/hitl/candidates.py`. |
| `src/hitl/candidates.py` | `promote_candidates` learns a condition-override branch (parallel to the existing 7D sponsor-merge branch). `ref.mapping_candidates` may gain a nullable `nct_id` for contextual condition candidates. | Med. |
| `src/transform/views.py` (now `src/mart/study_summary.py`) | **No change** — still reads `condition_id` from `norm.study_conditions`. (LB2 holds.) | Low — this is the point of B2. |
| Anthropic SDK dep | Add `litellm`; `anthropic` becomes a transitive/optional backend. | Low. |
| Tests | New eval runner; new override + ambiguity-router tests; rewrite the `_FakeClient` double around the adapter interface, not `tool_runner`. | Med. |

**Untouched by design:** `enriched.*`, `views.study_summary` output contract,
`entities.*` schema (except already-present 7D columns), sponsor merge lineage,
the deterministic normalize layers for the easy bulk.

---

## 7. Consequences

**Positive**
- The agent can finally do the analyst's job (reads the record) → higher hard-tail quality (D1).
- Backend is swappable and empirically validated; no model-provider lock-in (D2).
- Contextual conditions resolved without a mapping-layer schema rewrite (D4).
- Lean deps preserved; litellm ≪ langchain stack (D5).
- Sync/async duplication and arity dispatch deleted (D6).
- A real eval gate exists for the first time.

**Negative / costs**
- Condition build gains real complexity (override precedence, ambiguity router).
- Per-study condition resolution enlarges the agent's input space for ambiguous
  strings (mitigate by clustering studies on the disambiguating signal so we
  don't call the agent once per study — deferred optimization).
- litellm standardizes on OpenAI tool-call format; Anthropic extended-thinking
  becomes a per-adapter concern, not a core-loop parameter.
- Building the eval is upfront work that gates the migration.

---

## 8. Risks & mitigations

- **R1 — Local model raises wrong-mapping rate.** World-knowledge disambiguation
  is exactly where small local models hallucinate. *Mitigation:* local is opt-in
  and only adopted if it clears the golden eval per domain; frontier stays
  default. Likely outcome is a **tier** — cheap/local for retrieval + easy
  pre-filter, frontier for the ambiguous tail.
- **R2 — Override layer corrupts coverage stats / view parity.** *Mitigation:*
  `views.study_summary` row count and coverage % are pinned in existing tests;
  run them as the migration gate.
- **R3 — Ambiguity router false-negatives** (treats an ambiguous string as
  global). *Mitigation:* conservative router (any multi-MeSH co-occurrence ⇒
  contextual); err toward routing to per-study, where the agent can still abstain.
- **R4 — litellm passthrough gaps** (thinking, cache_control, usage fields).
  *Mitigation:* the adapter normalizes usage; verify each provider against the
  eval before trusting it.
- **R5 — Eval too small to be a real gate.** *Mitigation:* expanding the fixture
  is a blocking step-0 deliverable, not optional.
- **R6 — Sponsor has no oracle, so its eval is the weakest** (Axis B′).
  Auto-harvested labels are trivial for sponsors; the real gate is the curated
  anchor file + human review. *Mitigation:* treat `sponsor_anchors.json` as
  versioned gold, keep sponsor proposals conservative, and keep a frontier model
  on the sponsor tier even if cheaper backends suffice elsewhere.
- **R7 — Hand-rolled loop drops the SDK's retries.** *Mitigation:* §5.1 retry
  policy is a first-class deliverable with fault-injection tests, not an
  afterthought (D7).

---

## 9. Sequenced rollout

0. **Build the loop + assemble the dataset, then set the ceiling with a frontier
   model.** Hand-rolled loop + adapter first; assemble the two-tier dataset
   (auto-harvested regression floor + curated hard tail); run it through a
   best-in-class frontier reasoning model to establish the achievable ceiling
   per domain. *No model is the baseline — the labeled data is.* (Blocking —
   nothing ships without a measuring stick.)
1. **Lock the adapter behind the eval.** Confirm the loop + litellm adapter
   reproduce the frontier-model results on the same backend (no regression from
   the harness itself), then exercise the swap by scoring a second backend on the
   same data. Delete the sync/async fork.
2. **Tool redesign** (`read_trial_record`, semantic retrieval, rapidfuzz demoted).
   Re-run eval; expect hard-tail gains, especially conditions.
3. **Condition override layer + ambiguity router.** New table, `create_study_conditions`
   precedence, promote-path branch. Gate on view-parity + coverage tests.
4. **Backend A/B:** run the eval on a second backend (OpenAI and/or a local
   model) to exercise swappability for real; decide tiering from data.
5. **Build the sponsor oracle** (Axis B″) — blocking infra (rapidfuzz +
   embeddings + co-occurrence + ROR) → leader/canopy clustering with an LLM
   judge → human review of high-impact groups → seed `entities.sponsor` + register
   in `meta.reference_sources`. Sponsor's reactive 7D anchor-merge agent is
   **retired**. Separately, re-home the drug agent (7F) onto the new harness.

---

## 10. Open questions

- **Q1 — Contextual mapping grain.** Per-study, or per study-cluster (studies
  sharing the disambiguating signal)? Affects agent call volume and cost.
  *Leaning:* cluster; resolve once per (string, signal-cluster).
- **Q2 — Does any downstream consumer assume one-canonical-per-string for
  conditions** beyond the views layer (e.g. a notebook)? Audit before step 3.
- **Q3 — Embedding backend** for C2 — local sentence-transformers (lean, free,
  offline) vs a hosted embedding API. *Leaning:* local, to keep the offline-batch
  property and avoid a second provider dependency.
- **Q4 — Adapter scope:** does `finalize/abstain` stay a *tool* the model calls,
  or become `response_format` structured output? Tool keeps the grounding gate
  explicit; revisit if we adopt pydantic-ai (A3).
- **Q5 — Flat synonym groups vs hierarchical parent/child for the sponsor oracle**
  (Axis B″). **Decided 2026-06-12: flat for v1** — one canonical per group, which
  answers the analytical questions ("trials run by Pfizer") and is far simpler.
  Hierarchy (reuse 7D `merged_into_id`, e.g. `Pfizer CentreOne` as a distinct
  child of `Pfizer`) is **deferred, not ruled out** — revisit only if a concrete
  question needs to separate a subsidiary from its parent. The INDUSTRY size-8
  clusters (Axis B″ scoping) are where that would matter.
- **Q6 — Blocking recall on the hard tail.** Acronyms, multilingual names, and
  generic-fragment orgs are exactly where cheap blockers can *miss* true pairs
  (a missed pair never reaches the LLM). Co-occurrence + ROR-alias blocking
  mitigate, but measure block recall against the curated anchor set before
  trusting the oracle's completeness.
