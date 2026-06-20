# ADR 0003 — dbt for the deterministic transform DAG (Python keeps the identity spine + agent)

- **Status:** Proposed (pending approval)
- **Date:** 2026-06-20
- **Deciders:** Josh Ziel
- **Touches:** the transform layer (`src/transform/*`, `src/mart/*`), the orchestrator
  (`run_pipeline.py`); adjacent to [ADR 0001](0001-canonicalization-enrichment-agent-refactor.md)
  (enrichment agent) and [ADR 0002](0002-extract-scanner-atomic-swap.md) (extract).

> This ADR records a **proposed direction and boundary**, not a commitment to execute.
> No migration is scheduled by accepting it; it exists so the seam is agreed before any
> dbt work starts. `dbt-duckdb` is already in `environment.yml`.

---

## 1. Context

The transform layer is hand-written Python that issues SQL against DuckDB
(`run_normalize_*`, `classify_*`, `promote_to_enriched`, `build_study_summary`, etc.),
sequenced by the A3 orchestrator (`src/pipeline/orchestrator.py`). The question raised:
could this move to **dbt** (dbt-duckdb), turning the transforms into declarative models
with lineage, tests, and docs?

A read-through classified every transform operation as pure-SQL vs. imperative-Python.
Findings:

- **~65% of the transform logic is already pure SQL** — single `CREATE TABLE AS SELECT` /
  `INSERT … SELECT` statements that port to dbt models almost verbatim.
- The remainder is **not** "hard canonicalization." The deterministic dictionary-building
  (exact / 1:1-study / co-occurrence / mesh-exact / control-map matching) is mostly SQL
  too. What genuinely resists dbt is narrower and structural:
  1. **The entity-identity spine** (`src/entities.py`): surrogate IDs come from DuckDB
     `SEQUENCE`s via `nextval()`, and `upsert_*` does **lookup-then-insert to preserve IDs
     across rebuilds**. The per-row Python upsert loops exist *only* because of this design.
  2. **Curated/agentic state**: the manual-survives-rebuild invariant
     (`DELETE … WHERE mapping_method IN (automated…)`), HITL decision application, and the
     Claude enrichment agent (tool-calling loop, USD budget, SHA cache, ROR/QuickUMLS,
     rapidfuzz).

dbt is a *transformation* engine (declarative, full-refresh). The resistant parts are
*stateful entity management* and *post-conditions* (stable IDs, manual rows persist) —
a different job, which dbt models cannot express without breaking their own contract.

## 2. Decision (proposed)

Adopt a **hybrid** architecture with an explicit seam — **not** an all-dbt rewrite:

- **Python owns the identity spine and curated/external state** (the "spine"):
  `entities.*` (sequence IDs + `upsert_*`), the deterministic dictionary canonicalization
  that assigns/preserves entity IDs, HITL apply, and the enrichment agent.
- **dbt owns the deterministic downstream DAG**, reading `entities.*` and the `ref.*`
  dictionaries as **sources** (and reference data like the TA-mapping JSON as **seeds**):
  `norm.*` fact tables, `class.*`, `enriched.*`, `views.study_summary`, and optionally
  `meta.trial_change_events` (dbt-duckdb can read the dated Parquet snapshots via
  `read_parquet()` sources).
- **`run_pipeline.py` stays the conductor**: Python spine → `dbt build` (deterministic
  middle) → Python agent. The `meta.pipeline_runs` audit row wraps the whole thing.

**Phasing.** Start with the zero-risk Tier-1 models (mechanical ports), leave the spine and
agent in Python behind the source boundary, and **keep sequence-based IDs** for now.

| Tier | Scope | Effort |
|------|-------|--------|
| 1 — port as-is | `promote`, `classify_design`, `innovative_features`/`ai_mentions` (patterns→seed + macro), the four `norm.*` joins, `build_study_summary`, optionally `change_events` | Low / mechanical |
| 2 — port with effort | deterministic dictionary layers; the two regex UDFs (`normalize_drug_name`, `normalize_sponsor_name`) as dbt-duckdb Python UDFs or SQL `regexp_replace`; TA JSON → seed | Moderate |
| 3 — stays Python | `entities.*` identity spine, manual-preservation, HITL apply, enrichment agent (Claude/ROR/QuickUMLS/rapidfuzz) | n/a (by design) |

## 3. Consequences

**Gains**
- Lineage/DAG, `dbt docs`, and **schema tests** (`not_null`, `unique`, `relationships`)
  that formalize the FK invariants currently only prose in `data/DATABASE_SCHEMA.md`.
- Incremental models where useful; clearer separation of "deterministic transform" from
  "stateful canonicalization."

**Costs / risks**
- A new dependency surface and build step in the loop; contributors must learn dbt.
- Two execution models (dbt + Python) the orchestrator must sequence — single-writer
  DuckDB discipline still applies (dbt and the Python steps must not hold the write lock
  concurrently).
- Risk of bifurcating logic if the seam blurs; the source boundary (`entities.*` + `ref.*`
  dictionaries) must stay the contract.

## 4. Alternatives considered

- **Status quo (all Python).** Lowest churn; forgoes lineage/tests/docs on the layer that
  would benefit most. Rejected as the long-term shape, acceptable as the default if dbt
  isn't pursued.
- **All-dbt rewrite.** Rejected: the identity spine (sequences + ID-preserving upsert),
  manual-preservation post-condition, and the LLM agent cannot live in dbt without
  breaking full-refresh semantics.
- **Hash-based surrogate keys → ~90% dbt-native.** Replace `SEQUENCE`/`nextval` with
  `generate_surrogate_key(natural_key)`; entity tables become pure dbt models (dedup via
  `GROUP BY`, no upsert loops), manual entries become a seed `UNION`'d with derived rows.
  Deferred: it changes ID format (BIGINT → hash) — a migration touching every FK and the
  mart — and reworks the merge-lineage pattern (`entities.sponsor.merged_into_id`). Revisit
  alongside Epic C, where the agent/entity boundary is already in play.

## 5. Open questions (resolve if/when this is scheduled)
- dbt project layout and how `entities.*`/`ref.*` are declared as sources vs. seeds.
- Whether `change_events` is a dbt model (Parquet sources) or stays Python.
- Hash-key migration (alternative 3) — only if rebuild-determinism outweighs integer IDs.
- Sequencing vs. Epic B (eval substrate) / Epic C (agent rebuild) so it doesn't collide.
