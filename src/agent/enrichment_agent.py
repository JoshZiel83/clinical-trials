"""Phase 6E — Claude enrichment agent (async core + sync wrapper).

Per-item workflow: Claude is given a single `source_value` and a domain-specific
toolbelt. It calls tools (fuzzy/QuickUMLS/co-occurrence/dictionary lookups) to
investigate, then either records a proposal via `finalize_proposal` or opts out
via `abstain`. Both of those are themselves tools — capturing them lets the
agent loop terminate deterministically on every item.

Concurrency model:
  * `asyncio.Semaphore(concurrency)` bounds in-flight API calls.
  * A single writer coroutine owns the DuckDB connection — avoids the
    single-writer constraint; workers only push completed payloads into a
    queue.
  * SDK handles 429 / 5xx retries (bumped to `max_retries=5`). No proactive
    TPM tracker — revisit if 429s surface.

Guardrails:
  * SHA-cached in `meta.agent_cache` keyed by (domain, normalized source_value,
    model, prompt version). Re-runs are free.
  * Per-run USD budget tracked from usage deltas; agent stops scheduling new
    items when exceeded. In-flight items may push total spend a bounded
    amount above budget (≤ concurrency × per-item cost).
  * Per-domain `max_pending` throttle: refuses to start if the queue is
    already at cap, and stops emitting once the cap is reached.
  * Grounding: every proposal must cite at least one tool return. Enforced
    in the system prompt + checked post-hoc in `finalize_proposal`.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import dataclass, field
from typing import Optional

import anthropic
import pandas as pd

from config.settings import (
    AGENT_DEFAULT_CONCURRENCY,
    AGENT_DEFAULT_MAX_PENDING,
    AGENT_DEFAULT_MODEL,
    AGENT_MAX_TOKENS,
    AGENT_SDK_MAX_RETRIES,
    AGENT_SYSTEM_PROMPT_VERSION,
    ANTHROPIC_API_KEY,
    get_duckdb_connection,
)
from src import hitl
from src.agent.enrichment_tools import DOMAIN_TOOLS, ToolContext
from src.logging_config import get_logger

logger = get_logger("enrichment_agent")


MODEL_PRICING_PER_1K = {
    "claude-opus-4-6":   {"input": 0.005,  "output": 0.025,  "cache_read": 0.0005, "cache_write": 0.00625},
    "claude-sonnet-4-6": {"input": 0.003,  "output": 0.015,  "cache_read": 0.0003, "cache_write": 0.00375},
    "claude-haiku-4-5":  {"input": 0.001,  "output": 0.005,  "cache_read": 0.0001, "cache_write": 0.00125},
}


SYSTEM_PROMPT = """You are a medical-terminology mapping expert for clinical trial metadata.

Your job: given a single free-text input in a specific domain (condition, drug, or sponsor), propose its best canonical mapping. You have tools for fuzzy matching, UMLS lookup, co-occurrence analysis, and dictionary checks.

RULES
1. You MUST call at least one investigation tool before proposing a mapping. Ungrounded proposals are rejected.
2. Use multiple tools when the first signal is weak or ambiguous.
3. When confident, call `finalize_proposal` with (canonical_term, optional canonical_id, score 0-1, rationale). Rationale must cite specific tool returns (e.g., "QuickUMLS C0011860 score=1.0, confirmed by fuzzy MeSH match").
4. When no tool returns a trustworthy match (e.g., fuzzy scores all < 0.7, no QuickUMLS hit), call `abstain` with a brief reason. Do NOT guess.
5. Prefer canonical forms that already exist in the dictionary (via `lookup_*_dictionary`). When a dictionary hit confirms an existing mapping, that's usually the right answer.
6. For drugs, if the input is a control/placebo term, abstain — those are handled by a separate rule-based layer.
7. Be concise. One finalize_proposal OR one abstain per item."""


@dataclass
class RunStats:
    items_attempted: int = 0
    items_finalized: int = 0
    items_abstained: int = 0
    items_cache_hit: int = 0
    items_failed: int = 0
    items_skipped_throttle: int = 0
    spent_usd: float = 0.0
    input_tokens: int = 0
    output_tokens: int = 0
    cache_read_tokens: int = 0
    cache_write_tokens: int = 0


@dataclass
class Proposal:
    canonical_term: str
    canonical_id: Optional[str]
    score: float
    rationale: str
    tool_trace: list[dict] = field(default_factory=list)


def _ensure_agent_tables(duck_conn):
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.agent_cache (
            cache_key       VARCHAR PRIMARY KEY,
            domain          VARCHAR NOT NULL,
            source_value    VARCHAR NOT NULL,
            model           VARCHAR NOT NULL,
            prompt_version  VARCHAR NOT NULL,
            response_json   JSON    NOT NULL,
            cost_usd        DOUBLE  NOT NULL,
            created_at      TIMESTAMP DEFAULT current_timestamp
        )
    """)


def _cache_key(domain: str, source_value: str, model: str, prompt_version: str) -> str:
    raw = json.dumps(
        {"d": domain, "s": source_value.lower().strip(), "m": model, "v": prompt_version},
        sort_keys=True,
    )
    return hashlib.sha256(raw.encode()).hexdigest()


def _usage_cost_usd(model: str, usage) -> float:
    price = MODEL_PRICING_PER_1K.get(model, MODEL_PRICING_PER_1K["claude-opus-4-6"])
    cr = usage.cache_read_input_tokens or 0
    cw = usage.cache_creation_input_tokens or 0
    it = usage.input_tokens or 0
    ot = usage.output_tokens or 0
    return (
        (it / 1000) * price["input"]
        + (ot / 1000) * price["output"]
        + (cr / 1000) * price["cache_read"]
        + (cw / 1000) * price["cache_write"]
    )


def _select_pending_inputs(duck_conn, domain: str, limit: int) -> pd.DataFrame:
    if domain == "condition":
        return duck_conn.execute("""
            SELECT LOWER(c.name) AS source_value,
                   COUNT(DISTINCT c.nct_id) AS study_count
            FROM raw.conditions c
            WHERE LOWER(c.name) NOT IN (SELECT condition_name FROM ref.condition_dictionary)
              AND LOWER(c.name) NOT IN (
                  SELECT source_value FROM ref.mapping_candidates
                  WHERE domain = 'condition' AND source = 'agent'
              )
            GROUP BY LOWER(c.name)
            ORDER BY study_count DESC
            LIMIT ?
        """, [limit]).fetchdf()
    if domain == "drug":
        return duck_conn.execute("""
            SELECT intervention_name AS source_value,
                   COUNT(DISTINCT nct_id) AS study_count
            FROM norm.study_drugs
            WHERE mapping_method = 'unmatched'
              AND intervention_name NOT IN (
                  SELECT source_value FROM ref.mapping_candidates
                  WHERE domain = 'drug' AND source = 'agent'
              )
            GROUP BY intervention_name
            ORDER BY study_count DESC
            LIMIT ?
        """, [limit]).fetchdf()
    if domain == "sponsor":
        return duck_conn.execute("""
            SELECT e.canonical_name AS source_value,
                   COUNT(DISTINCT s.nct_id) AS study_count
            FROM ref.sponsor_dictionary d
            JOIN entities.sponsor e ON d.sponsor_id = e.sponsor_id
            LEFT JOIN raw.sponsors s ON d.source_name = LOWER(s.name)
            WHERE d.mapping_method = 'exact-after-normalize'
              AND e.canonical_name NOT IN (
                  SELECT source_value FROM ref.mapping_candidates
                  WHERE domain = 'sponsor' AND source = 'agent'
              )
            GROUP BY e.canonical_name
            ORDER BY study_count DESC
            LIMIT ?
        """, [limit]).fetchdf()
    raise ValueError(f"Unknown domain: {domain}")


def _pending_count(duck_conn, domain: str) -> int:
    row = duck_conn.execute(
        "SELECT COUNT(*) FROM ref.mapping_candidates "
        "WHERE domain = ? AND status = 'pending'",
        [domain],
    ).fetchone()
    return int(row[0]) if row else 0


def _build_tools_for_agent(ctx: ToolContext, domain: str, proposal_slot: dict,
                           use_async: bool = True):
    """Wrap matching-algorithm functions + finalize/abstain as tool callables.

    When `use_async=True`, returns `@beta_async_tool` functions that offload
    sync matchers via `asyncio.to_thread`. Otherwise returns `@beta_tool`
    synchronous variants (useful for tests using the sync engine).
    """
    if use_async:
        from anthropic import beta_async_tool as _tool_dec
    else:
        from anthropic import beta_tool as _tool_dec

    domain_funcs = DOMAIN_TOOLS[domain]

    wrapped_tools = []
    for fn in domain_funcs:
        name = fn.__name__
        doc = fn.__doc__ or ""

        def _make_wrapper(algo, name=name, doc=doc):
            if use_async:
                @_tool_dec(name=name, description=doc)
                async def _tool(text: str, limit: int = 5) -> str:
                    try:
                        if name == "co_occurrence_condition":
                            result = await asyncio.to_thread(algo, ctx, text)
                        elif name in ("lookup_condition_dictionary",
                                      "lookup_drug_dictionary"):
                            result = await asyncio.to_thread(algo, ctx, text)
                        else:
                            result = await asyncio.to_thread(algo, ctx, text, limit)
                    except Exception as exc:
                        result = {"error": str(exc)}
                    proposal_slot.setdefault("trace", []).append(
                        {"tool": name, "input": {"text": text}, "result": result}
                    )
                    return json.dumps(result, default=str)
                return _tool
            else:
                @_tool_dec(name=name, description=doc)
                def _tool(text: str, limit: int = 5) -> str:
                    try:
                        if name == "co_occurrence_condition":
                            result = algo(ctx, text)
                        elif name in ("lookup_condition_dictionary",
                                      "lookup_drug_dictionary"):
                            result = algo(ctx, text)
                        else:
                            result = algo(ctx, text, limit=limit)
                    except Exception as exc:
                        result = {"error": str(exc)}
                    proposal_slot.setdefault("trace", []).append(
                        {"tool": name, "input": {"text": text}, "result": result}
                    )
                    return json.dumps(result, default=str)
                return _tool

        wrapped_tools.append(_make_wrapper(fn))

    if use_async:
        @_tool_dec(
            name="finalize_proposal",
            description=("Record a canonical mapping proposal for the current "
                         "source_value. Rationale must cite specific tool returns."),
        )
        async def finalize_proposal(canonical_term: str, rationale: str,
                                    score: float,
                                    canonical_id: Optional[str] = None) -> str:
            if not proposal_slot.get("trace"):
                return ("ERROR: you must call at least one investigation tool "
                        "before finalizing a proposal.")
            proposal_slot["decision"] = Proposal(
                canonical_term=canonical_term,
                canonical_id=canonical_id,
                score=float(score),
                rationale=rationale,
                tool_trace=proposal_slot.get("trace", []),
            )
            return "Proposal recorded. You may stop."

        @_tool_dec(name="abstain",
                   description="Abstain — no trustworthy canonical exists.")
        async def abstain(reason: str) -> str:
            proposal_slot["decision"] = "abstain"
            proposal_slot["abstain_reason"] = reason
            return "Abstention recorded. You may stop."
    else:
        @_tool_dec(
            name="finalize_proposal",
            description=("Record a canonical mapping proposal for the current "
                         "source_value. Rationale must cite specific tool returns."),
        )
        def finalize_proposal(canonical_term: str, rationale: str,
                              score: float,
                              canonical_id: Optional[str] = None) -> str:
            if not proposal_slot.get("trace"):
                return ("ERROR: you must call at least one investigation tool "
                        "before finalizing a proposal.")
            proposal_slot["decision"] = Proposal(
                canonical_term=canonical_term,
                canonical_id=canonical_id,
                score=float(score),
                rationale=rationale,
                tool_trace=proposal_slot.get("trace", []),
            )
            return "Proposal recorded. You may stop."

        @_tool_dec(name="abstain",
                   description="Abstain — no trustworthy canonical exists.")
        def abstain(reason: str) -> str:
            proposal_slot["decision"] = "abstain"
            proposal_slot["abstain_reason"] = reason
            return "Abstention recorded. You may stop."

    return wrapped_tools + [finalize_proposal, abstain]


async def _run_one_item_async(
    client,
    ctx: ToolContext,
    domain: str,
    source_value: str,
    study_count: int,
    model: str,
) -> tuple[dict, float]:
    proposal_slot: dict = {"trace": []}
    tools = _build_tools_for_agent(ctx, domain, proposal_slot, use_async=True)

    user_prompt = (
        f"Domain: {domain}\n"
        f"Source value: {source_value!r}\n"
        f"Study impact: {study_count} studies.\n\n"
        "Investigate with the tools, then call finalize_proposal or abstain."
    )

    total_cost = 0.0
    runner = client.beta.messages.tool_runner(
        model=model,
        max_tokens=AGENT_MAX_TOKENS,
        system=[{"type": "text", "text": SYSTEM_PROMPT,
                 "cache_control": {"type": "ephemeral"}}],
        tools=tools,
        messages=[{"role": "user", "content": user_prompt}],
        thinking={"type": "adaptive"},
    )

    async for message in runner:
        total_cost += _usage_cost_usd(model, message.usage)

    return _payload_from_slot(proposal_slot), total_cost


def _payload_from_slot(proposal_slot: dict) -> dict:
    decision = proposal_slot.get("decision")
    trace = proposal_slot.get("trace", [])
    if isinstance(decision, Proposal):
        return {
            "kind": "proposal",
            "canonical_term": decision.canonical_term,
            "canonical_id": decision.canonical_id,
            "score": decision.score,
            "rationale": decision.rationale,
            "tool_trace": trace,
        }
    if decision == "abstain":
        return {
            "kind": "abstain",
            "reason": proposal_slot.get("abstain_reason", ""),
            "tool_trace": trace,
        }
    return {"kind": "no_decision", "tool_trace": trace}


# --- Sync variant kept for the test harness ---------------------------------

def _run_one_item_sync(
    client,
    ctx: ToolContext,
    domain: str,
    source_value: str,
    study_count: int,
    model: str,
) -> tuple[dict, float]:
    proposal_slot: dict = {"trace": []}
    tools = _build_tools_for_agent(ctx, domain, proposal_slot, use_async=False)

    user_prompt = (
        f"Domain: {domain}\n"
        f"Source value: {source_value!r}\n"
        f"Study impact: {study_count} studies.\n\n"
        "Investigate with the tools, then call finalize_proposal or abstain."
    )
    total_cost = 0.0
    runner = client.beta.messages.tool_runner(
        model=model, max_tokens=AGENT_MAX_TOKENS,
        system=[{"type": "text", "text": SYSTEM_PROMPT,
                 "cache_control": {"type": "ephemeral"}}],
        tools=tools,
        messages=[{"role": "user", "content": user_prompt}],
        thinking={"type": "adaptive"},
    )
    for message in runner:
        total_cost += _usage_cost_usd(model, message.usage)
    return _payload_from_slot(proposal_slot), total_cost


def _write_candidate(duck_conn, domain: str, source_value: str,
                     study_count: int, payload: dict) -> bool:
    if payload.get("kind") != "proposal":
        return False
    hitl.ensure_candidates_table(duck_conn)
    df = pd.DataFrame([{
        "source_value": source_value,
        "canonical_term": payload["canonical_term"],
        "canonical_id": payload.get("canonical_id"),
        "score": payload.get("score", 0.0),
        "study_count": int(study_count),
        "rationale": payload.get("rationale"),
        "tool_trace": json.dumps(payload.get("tool_trace", []), default=str),
    }])
    hitl.insert_candidates(duck_conn, domain, df, source="agent")
    return True


# --- Async orchestrator -----------------------------------------------------

async def _run_async(
    domain: str,
    budget_usd: float,
    limit: int,
    max_pending: int,
    model: str,
    concurrency: int,
    duck_conn,
    client,
    stats: RunStats,
) -> None:
    _ensure_agent_tables(duck_conn)
    hitl.ensure_candidates_table(duck_conn)

    pending = _pending_count(duck_conn, domain)
    if pending >= max_pending:
        logger.info(
            f"[{domain}] pending queue is {pending} ≥ {max_pending}; "
            f"refusing to run. Review candidates first."
        )
        return
    remaining_slots = max_pending - pending

    inputs = _select_pending_inputs(duck_conn, domain, limit=limit)
    if inputs.empty:
        logger.info(f"[{domain}] no unresolved items found")
        return

    logger.info(
        f"[{domain}] starting async agent: {len(inputs)} items, "
        f"concurrency={concurrency}, budget=${budget_usd:.2f}, model={model}, "
        f"pending={pending}/{max_pending} (slots={remaining_slots})"
    )

    ctx = ToolContext(duck_conn=duck_conn)
    sem = asyncio.Semaphore(concurrency)
    state_lock = asyncio.Lock()
    result_queue: asyncio.Queue = asyncio.Queue()
    auth_fail = asyncio.Event()
    # Sentinel signals writer to drain and exit
    SENTINEL = object()

    async def _process_one(source_value: str, study_count: int):
        # Budget gate
        async with state_lock:
            if stats.spent_usd >= budget_usd:
                return
            stats.items_attempted += 1

        ck = _cache_key(domain, source_value, model,
                        AGENT_SYSTEM_PROMPT_VERSION)
        cache_hit = duck_conn.execute(
            "SELECT response_json FROM meta.agent_cache WHERE cache_key = ?",
            [ck],
        ).fetchone()

        if cache_hit:
            payload = json.loads(cache_hit[0])
            async with state_lock:
                stats.items_cache_hit += 1
            await result_queue.put(
                ("cache", source_value, study_count, payload, 0.0)
            )
            return

        # API call, guarded by semaphore
        async with sem:
            if auth_fail.is_set():
                return
            try:
                payload, cost = await _run_one_item_async(
                    client, ctx, domain, source_value, study_count, model
                )
            except anthropic.AuthenticationError:
                logger.error(f"[{domain}] auth failure — aborting run")
                auth_fail.set()
                return
            except Exception as exc:
                logger.warning(
                    f"[{domain}] agent failure on {source_value!r}: {exc}"
                )
                async with state_lock:
                    stats.items_failed += 1
                return

        async with state_lock:
            stats.spent_usd += cost
        await result_queue.put(
            ("api", source_value, study_count, payload, cost, ck)
        )

    async def _writer():
        """Drains result_queue, writes to DuckDB, enforces max_pending cap."""
        nonlocal remaining_slots
        while True:
            item = await result_queue.get()
            if item is SENTINEL:
                return

            kind, source_value, study_count, payload, *rest = item

            # Cache the API result
            if kind == "api":
                cost = rest[0]
                ck = rest[1]
                duck_conn.execute(
                    """INSERT INTO meta.agent_cache
                       (cache_key, domain, source_value, model, prompt_version,
                        response_json, cost_usd) VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT DO NOTHING""",
                    [ck, domain, source_value, model,
                     AGENT_SYSTEM_PROMPT_VERSION,
                     json.dumps(payload, default=str), cost],
                )

            # Write candidate or record abstention
            if payload.get("kind") == "proposal":
                if remaining_slots <= 0:
                    logger.info(
                        f"[{domain}] max_pending reached; dropping proposal "
                        f"for {source_value!r}"
                    )
                    continue
                _write_candidate(duck_conn, domain, source_value,
                                 study_count, payload)
                stats.items_finalized += 1
                remaining_slots -= 1
            elif payload.get("kind") == "abstain":
                stats.items_abstained += 1

    # Schedule workers
    t0 = time.monotonic()
    writer_task = asyncio.create_task(_writer())

    worker_tasks = []
    for _, row in inputs.iterrows():
        if auth_fail.is_set():
            break
        worker_tasks.append(asyncio.create_task(
            _process_one(row["source_value"], int(row["study_count"]))
        ))

    try:
        await asyncio.gather(*worker_tasks, return_exceptions=False)
    finally:
        await result_queue.put(SENTINEL)
        await writer_task

    elapsed = time.monotonic() - t0
    logger.info(
        f"[{domain}] done in {elapsed:.1f}s — "
        f"attempted={stats.items_attempted} "
        f"finalized={stats.items_finalized} "
        f"abstained={stats.items_abstained} "
        f"cache_hits={stats.items_cache_hit} "
        f"failed={stats.items_failed} "
        f"spent=${stats.spent_usd:.4f}"
    )


# --- Public entry points ----------------------------------------------------

def run_enrichment_agent(
    domain: str,
    budget_usd: float,
    limit: int = 500,
    max_pending: int = AGENT_DEFAULT_MAX_PENDING,
    model: str = AGENT_DEFAULT_MODEL,
    concurrency: int = AGENT_DEFAULT_CONCURRENCY,
    duck_conn=None,
    client=None,
) -> RunStats:
    """Run the enrichment agent (async implementation, sync facade).

    `client` may be an `anthropic.AsyncAnthropic` or a test double exposing
    the same `.beta.messages.tool_runner` surface. Pass a sync client only
    when paired with `_run_enrichment_agent_sync_legacy`.
    """
    if domain not in DOMAIN_TOOLS:
        raise ValueError(f"unknown domain: {domain}")

    close_conn = duck_conn is None
    duck_conn = duck_conn or get_duckdb_connection()
    if client is None:
        if not ANTHROPIC_API_KEY:
            raise RuntimeError("ANTHROPIC_API_KEY not set in environment / .env")
        client = anthropic.AsyncAnthropic(
            api_key=ANTHROPIC_API_KEY, max_retries=AGENT_SDK_MAX_RETRIES
        )

    stats = RunStats()
    try:
        asyncio.run(_run_async(
            domain=domain, budget_usd=budget_usd, limit=limit,
            max_pending=max_pending, model=model, concurrency=concurrency,
            duck_conn=duck_conn, client=client, stats=stats,
        ))
        return stats
    finally:
        if close_conn:
            duck_conn.close()


def _run_enrichment_agent_sync_legacy(
    domain: str,
    budget_usd: float,
    limit: int = 500,
    max_pending: int = AGENT_DEFAULT_MAX_PENDING,
    model: str = AGENT_DEFAULT_MODEL,
    duck_conn=None,
    client=None,
) -> RunStats:
    """Sequential implementation — retained for tests that use a sync
    `_FakeClient` and for debugging. Not exposed via the CLI."""
    if domain not in DOMAIN_TOOLS:
        raise ValueError(f"unknown domain: {domain}")

    close_conn = duck_conn is None
    duck_conn = duck_conn or get_duckdb_connection()
    if client is None:
        if not ANTHROPIC_API_KEY:
            raise RuntimeError("ANTHROPIC_API_KEY not set in environment / .env")
        client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)

    stats = RunStats()
    try:
        _ensure_agent_tables(duck_conn)
        hitl.ensure_candidates_table(duck_conn)

        pending = _pending_count(duck_conn, domain)
        if pending >= max_pending:
            logger.info(
                f"[{domain}] pending queue is {pending} ≥ {max_pending}; "
                f"refusing to run. Review candidates first."
            )
            return stats
        remaining_slots = max_pending - pending

        inputs = _select_pending_inputs(duck_conn, domain, limit=limit)
        if inputs.empty:
            logger.info(f"[{domain}] no unresolved items found")
            return stats

        ctx = ToolContext(duck_conn=duck_conn)
        t0 = time.monotonic()

        for _, row in inputs.iterrows():
            source_value = row["source_value"]
            study_count = int(row["study_count"])
            if stats.spent_usd >= budget_usd:
                break
            if remaining_slots <= 0:
                break
            stats.items_attempted += 1
            ck = _cache_key(domain, source_value, model, AGENT_SYSTEM_PROMPT_VERSION)
            cache_hit = duck_conn.execute(
                "SELECT response_json FROM meta.agent_cache WHERE cache_key = ?",
                [ck],
            ).fetchone()
            if cache_hit:
                payload = json.loads(cache_hit[0])
                stats.items_cache_hit += 1
            else:
                try:
                    payload, cost = _run_one_item_sync(
                        client, ctx, domain, source_value, study_count, model
                    )
                except Exception as exc:
                    logger.warning(f"[{domain}] agent failure on {source_value!r}: {exc}")
                    stats.items_failed += 1
                    continue
                stats.spent_usd += cost
                duck_conn.execute(
                    """INSERT INTO meta.agent_cache
                       (cache_key, domain, source_value, model, prompt_version,
                        response_json, cost_usd) VALUES (?, ?, ?, ?, ?, ?, ?)
                       ON CONFLICT DO NOTHING""",
                    [ck, domain, source_value, model, AGENT_SYSTEM_PROMPT_VERSION,
                     json.dumps(payload, default=str), cost],
                )
            if _write_candidate(duck_conn, domain, source_value, study_count, payload):
                stats.items_finalized += 1
                remaining_slots -= 1
            elif payload.get("kind") == "abstain":
                stats.items_abstained += 1

        logger.info(
            f"[{domain}] sync done in {time.monotonic()-t0:.1f}s — "
            f"attempted={stats.items_attempted} "
            f"finalized={stats.items_finalized} "
            f"abstained={stats.items_abstained} "
            f"cache_hits={stats.items_cache_hit} "
            f"failed={stats.items_failed} "
            f"spent=${stats.spent_usd:.4f}"
        )
        return stats
    finally:
        if close_conn:
            duck_conn.close()
