"""Tests for the Phase 6E enrichment agent.

The heavy tests mock the Anthropic client so they run offline — the point is
to verify throttle logic, SHA caching, budget enforcement, and DB writes, not
to exercise model behavior. (Model behavior is the domain of the golden-eval
run under tests/fixtures/enrichment_golden.json, which is gated on an API
key being available.)
"""

import json

import duckdb
import pandas as pd
import pytest

from src import enrichment_agent as ea


# ---------- shared fixtures -------------------------------------------------


def _make_conn():
    """In-memory DuckDB with the minimal tables the agent touches."""
    conn = duckdb.connect(":memory:")
    for s in ("raw", "ref", "norm", "class", "meta"):
        conn.execute(f"CREATE SCHEMA {s}")
    conn.execute("""
        CREATE TABLE ref.condition_dictionary (
            condition_name  VARCHAR NOT NULL,
            canonical_term  VARCHAR NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)
    from src import hitl
    hitl.ensure_candidates_table(conn)
    conn.execute("""
        CREATE TABLE raw.conditions AS SELECT * FROM (VALUES
            (1, 'NCT001', 'Breast Cancer Typo'),
            (2, 'NCT002', 'Breast Cancer Typo'),
            (3, 'NCT003', 'Diabetic Retinoppathy')
        ) AS t(id, nct_id, name)
    """)
    conn.execute("""
        CREATE TABLE raw.browse_conditions AS SELECT * FROM (VALUES
            (1, 'NCT001', 'Breast Neoplasms', 'breast neoplasms', 'mesh-list'),
            (2, 'NCT003', 'Diabetic Retinopathy', 'diabetic retinopathy', 'mesh-list')
        ) AS t(id, nct_id, mesh_term, downcase_mesh_term, mesh_type)
    """)
    return conn


class _FakeUsage:
    def __init__(self, input_tokens=100, output_tokens=50,
                 cache_read_input_tokens=0, cache_creation_input_tokens=0):
        self.input_tokens = input_tokens
        self.output_tokens = output_tokens
        self.cache_read_input_tokens = cache_read_input_tokens
        self.cache_creation_input_tokens = cache_creation_input_tokens


class _FakeMessage:
    def __init__(self):
        self.usage = _FakeUsage()


class _FakeRunner:
    """Iterable stand-in for client.beta.messages.tool_runner()."""
    def __init__(self, on_iter):
        self._on_iter = on_iter

    def __iter__(self):
        yield from self._on_iter()


class _FakeClient:
    """Records the proposal_slot the agent builds and lets a test scenario
    populate `proposal_slot["decision"]` before returning messages."""
    def __init__(self, scenario):
        self.beta = self
        self.messages = self
        self._scenario = scenario
        self.calls = []

    def tool_runner(self, **kwargs):
        self.calls.append(kwargs)
        tools = kwargs["tools"]

        # Simulate the agent calling: (1) a lookup tool, (2) finalize/abstain.
        # We reach into the tool closures through their _handler attribute
        # isn't standard — instead, invoke the underlying wrapped functions
        # via the tool objects' registered callables from the runner.
        def _drive():
            # Look up tools by name — the tool runner wraps @beta_tool funcs;
            # the raw callable is on .function (implementation detail of the
            # SDK's beta_tool decorator as of 0.94).
            tool_map = {}
            for t in tools:
                # Each is a BetaTool; access its function attribute
                fn = getattr(t, "function", None) or getattr(t, "_fn", None) or t
                name = getattr(t, "name", None) or getattr(fn, "__name__", "?")
                tool_map[name] = fn
            self._scenario(tool_map)
            yield _FakeMessage()
        return _FakeRunner(_drive)


# ---------- tests -----------------------------------------------------------


def test_select_pending_inputs_condition():
    conn = _make_conn()
    df = ea._select_pending_inputs(conn, "condition", limit=10)
    # Only conditions NOT in the dictionary and NOT already agent-proposed
    assert "breast cancer typo" in set(df["source_value"])
    assert "diabetic retinoppathy" in set(df["source_value"])
    conn.close()


def test_throttle_refuses_when_pending_at_cap():
    conn = _make_conn()
    ea._ensure_agent_tables(conn)
    # Seed candidates to simulate a full queue (one insert_candidates call —
    # subsequent calls with the same (domain, source) clear pending rows first)
    from src import hitl
    hitl.ensure_candidates_table(conn)
    hitl.insert_candidates(conn, "condition", pd.DataFrame([
        {"source_value": f"seed-{i}", "canonical_term": "X",
         "score": 80.0, "study_count": 1}
        for i in range(3)
    ]), source="fuzzy")

    stats = ea._run_enrichment_agent_sync_legacy(
        domain="condition", budget_usd=10.0, limit=5,
        max_pending=3, duck_conn=conn,
        client=_FakeClient(lambda tm: None),
    )
    assert stats.items_attempted == 0
    assert stats.items_finalized == 0
    conn.close()


def test_finalize_writes_candidate_row():
    conn = _make_conn()

    def scenario(tool_map):
        # Agent calls fuzzy, then finalizes
        tool_map["fuzzy_mesh_condition"]("breast cancer typo")
        tool_map["finalize_proposal"](
            canonical_term="Breast Neoplasms",
            rationale="fuzzy MeSH match score=95",
            score=0.95,
            canonical_id=None,
        )

    stats = ea._run_enrichment_agent_sync_legacy(
        domain="condition", budget_usd=10.0, limit=1,
        max_pending=500, duck_conn=conn,
        client=_FakeClient(scenario),
    )
    assert stats.items_finalized == 1
    row = conn.execute("""
        SELECT source_value, canonical_term, source, status, rationale
        FROM ref.mapping_candidates
        WHERE domain = 'condition'
    """).fetchone()
    assert row[0] == "breast cancer typo"
    assert row[1] == "Breast Neoplasms"
    assert row[2] == "agent"
    assert row[3] == "pending"
    assert "fuzzy MeSH" in row[4]
    conn.close()


def test_abstain_does_not_write_candidate():
    conn = _make_conn()

    def scenario(tool_map):
        tool_map["fuzzy_mesh_condition"]("breast cancer typo")
        tool_map["abstain"]("fuzzy scores all below threshold")

    stats = ea._run_enrichment_agent_sync_legacy(
        domain="condition", budget_usd=10.0, limit=1,
        max_pending=500, duck_conn=conn,
        client=_FakeClient(scenario),
    )
    assert stats.items_abstained == 1
    assert stats.items_finalized == 0
    n = conn.execute("""
        SELECT COUNT(*) FROM ref.mapping_candidates WHERE domain = 'condition'
    """).fetchone()[0]
    assert n == 0
    conn.close()


def test_finalize_without_tool_call_is_rejected():
    """Grounding check: finalize_proposal must reject if no tool was called first."""
    conn = _make_conn()

    def scenario(tool_map):
        # Skip any investigation tool and go straight to finalize
        result = tool_map["finalize_proposal"](
            canonical_term="Whatever",
            rationale="ungrounded",
            score=0.9,
        )
        # finalize_proposal should return an error string and NOT record a decision
        assert "ERROR" in result

    stats = ea._run_enrichment_agent_sync_legacy(
        domain="condition", budget_usd=10.0, limit=1,
        max_pending=500, duck_conn=conn,
        client=_FakeClient(scenario),
    )
    # No decision recorded → no candidate, no finalized count
    assert stats.items_finalized == 0
    conn.close()


def test_cache_hit_skips_api_call():
    conn = _make_conn()

    # Pre-populate the cache
    ea._ensure_agent_tables(conn)
    source = "breast cancer typo"
    ck = ea._cache_key("condition", source, ea.AGENT_DEFAULT_MODEL,
                      ea.AGENT_SYSTEM_PROMPT_VERSION)
    cached_payload = {
        "kind": "proposal",
        "canonical_term": "Breast Neoplasms",
        "canonical_id": None,
        "score": 0.95,
        "rationale": "cached",
        "tool_trace": [{"tool": "fuzzy_mesh_condition", "input": {}, "result": []}],
    }
    conn.execute(
        """INSERT INTO meta.agent_cache
           (cache_key, domain, source_value, model, prompt_version,
            response_json, cost_usd)
           VALUES (?, ?, ?, ?, ?, ?, ?)""",
        [ck, "condition", source, ea.AGENT_DEFAULT_MODEL,
         ea.AGENT_SYSTEM_PROMPT_VERSION, json.dumps(cached_payload), 0.01],
    )

    def scenario(tool_map):
        raise AssertionError("tool_runner must not be called on cache hit")

    stats = ea._run_enrichment_agent_sync_legacy(
        domain="condition", budget_usd=10.0, limit=1,
        max_pending=500, duck_conn=conn,
        client=_FakeClient(scenario),
    )
    assert stats.items_cache_hit == 1
    assert stats.items_finalized == 1
    assert stats.spent_usd == 0.0
    conn.close()


def test_budget_exhaustion_stops_loop():
    conn = _make_conn()

    def scenario(tool_map):
        tool_map["fuzzy_mesh_condition"]("x")
        tool_map["finalize_proposal"](
            canonical_term="Y", rationale="test", score=0.9,
        )

    # Very tight budget: first item will push spent over
    stats = ea._run_enrichment_agent_sync_legacy(
        domain="condition", budget_usd=0.0001, limit=10,
        max_pending=500, duck_conn=conn,
        client=_FakeClient(scenario),
    )
    assert stats.items_attempted <= 2  # budget check runs before each item
    conn.close()


def test_cache_key_is_deterministic():
    a = ea._cache_key("condition", "Foo Bar", "claude-opus-4-6", "v1")
    b = ea._cache_key("condition", " foo bar ", "claude-opus-4-6", "v1")  # case+ws
    assert a == b
    c = ea._cache_key("condition", "Foo Bar", "claude-opus-4-6", "v2")
    assert a != c


def test_unknown_domain_raises():
    with pytest.raises(ValueError, match="unknown domain"):
        ea.run_enrichment_agent(domain="bogus", budget_usd=1.0)


# ---------- async-path tests -----------------------------------------------


class _FakeAsyncRunner:
    """Async iterable stand-in for the AsyncAnthropic tool_runner."""
    def __init__(self, on_iter, delay=0.0):
        self._on_iter = on_iter
        self._delay = delay

    def __aiter__(self):
        return self._gen()

    async def _gen(self):
        if self._delay:
            import asyncio
            await asyncio.sleep(self._delay)
        for m in self._on_iter():
            yield m


class _FakeAsyncClient:
    """Async tool_runner stand-in. `scenario(tool_map)` is called once per
    item; `raise_for` (set of source_values) triggers a BadRequestError.
    `delay_s` lets concurrency tests observe overlap."""
    def __init__(self, scenario, raise_for=None, delay_s=0.0,
                 track=None):
        self.beta = self
        self.messages = self
        self._scenario = scenario
        self._raise_for = raise_for or set()
        self._delay_s = delay_s
        self._track = track  # optional {'active': int, 'peak': int}
        self.calls = []

    def tool_runner(self, **kwargs):
        self.calls.append(kwargs)
        tools = kwargs["tools"]
        user_text = kwargs["messages"][0]["content"]

        async def _drive():
            import anthropic as _aa
            # Extract source_value from the user prompt
            source_value = ""
            for line in user_text.split("\n"):
                if line.startswith("Source value:"):
                    source_value = line.split(":", 1)[1].strip().strip("'\"")
                    break
            if source_value in self._raise_for:
                raise _aa.BadRequestError(
                    message="injected", response=None, body=None
                )
            if self._track is not None:
                self._track["active"] = self._track.get("active", 0) + 1
                self._track["peak"] = max(self._track["peak"],
                                          self._track["active"])
            try:
                if self._delay_s:
                    import asyncio as _asyncio
                    await _asyncio.sleep(self._delay_s)

                tool_map = {}
                for t in tools:
                    fn = getattr(t, "function", None) or getattr(t, "_fn", None) or t
                    name = getattr(t, "name", None) or getattr(fn, "__name__", "?")
                    tool_map[name] = fn

                result = self._scenario(tool_map)
                if result is not None and hasattr(result, "__await__"):
                    await result
            finally:
                if self._track is not None:
                    self._track["active"] -= 1

            yield _FakeMessage()

        class _Runner:
            def __aiter__(self_inner):
                return _drive()

        return _Runner()


def test_async_finalize_writes_candidate_row():
    conn = _make_conn()

    async def scenario(tool_map):
        await tool_map["fuzzy_mesh_condition"]("breast cancer typo")
        await tool_map["finalize_proposal"](
            canonical_term="Breast Neoplasms",
            rationale="fuzzy MeSH match score=95",
            score=0.95,
            canonical_id=None,
        )

    def scenario_sync(tool_map):
        # Wrap in a coroutine and schedule via the runner
        return scenario(tool_map)

    stats = ea.run_enrichment_agent(
        domain="condition", budget_usd=10.0, limit=1,
        max_pending=500, concurrency=2, duck_conn=conn,
        client=_FakeAsyncClient(scenario_sync),
    )
    assert stats.items_finalized == 1
    row = conn.execute("""
        SELECT source_value, canonical_term, source, status, rationale
        FROM ref.mapping_candidates WHERE domain = 'condition'
    """).fetchone()
    assert row[0] == "breast cancer typo"
    assert row[2] == "agent"
    assert row[3] == "pending"
    conn.close()


def test_async_concurrency_respected():
    """With concurrency=2, at most 2 tool_runner calls active at once."""
    conn = _make_conn()

    async def scenario(tool_map):
        await tool_map["fuzzy_mesh_condition"]("x")
        await tool_map["abstain"]("test")

    track = {"active": 0, "peak": 0}
    stats = ea.run_enrichment_agent(
        domain="condition", budget_usd=10.0, limit=2,
        max_pending=500, concurrency=2, duck_conn=conn,
        client=_FakeAsyncClient(
            lambda tm: scenario(tm), delay_s=0.05, track=track
        ),
    )
    assert stats.items_attempted == 2
    assert stats.items_abstained == 2
    assert track["peak"] <= 2
    conn.close()


def test_async_non_retriable_error_skips_item_and_continues():
    """A BadRequestError on one item should fail that item but not abort."""
    conn = _make_conn()

    async def scenario(tool_map):
        await tool_map["fuzzy_mesh_condition"]("x")
        await tool_map["abstain"]("test")

    stats = ea.run_enrichment_agent(
        domain="condition", budget_usd=10.0, limit=2,
        max_pending=500, concurrency=2, duck_conn=conn,
        client=_FakeAsyncClient(
            lambda tm: scenario(tm),
            raise_for={"diabetic retinoppathy"},
        ),
    )
    assert stats.items_failed == 1
    assert stats.items_abstained == 1
    assert stats.items_attempted == 2
    conn.close()
