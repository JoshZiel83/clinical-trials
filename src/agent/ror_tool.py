"""ROR (Research Organization Registry) API client (Phase 7D).

Queries https://api.ror.org for org identity lookups. ROR is the authoritative
open registry for academic, research, and corporate organizations — it
publishes parent/subsidiary relationships, aliases, and country metadata that
string similarity alone can't capture.

Results are cached per-query in `meta.ror_cache` with a TTL (default 30 days)
to minimize API traffic across enrichment-agent runs. Errors (HTTP failures,
parse issues) return a sentinel so the calling tool can route around them
rather than crashing the agent.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import time
import urllib.parse
from typing import Optional

import requests

from config.settings import ROR_API_BASE, ROR_CACHE_TTL_DAYS
from src.logging_config import get_logger

logger = get_logger("ror_tool")


_QUERY_KEY_VERSION = "v1"   # bump to invalidate cached rows
_USER_AGENT = "clinical-trials-etl/0.1"
_REQUEST_TIMEOUT_SECONDS = 10
_BACKOFF_DELAYS = (1, 2, 4)


def ensure_cache_table(duck_conn) -> None:
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.ror_cache (
            query_sha     VARCHAR PRIMARY KEY,
            query_text    VARCHAR NOT NULL,
            response_json JSON    NOT NULL,
            fetched_at    TIMESTAMP DEFAULT current_timestamp
        )
    """)


def _query_sha(query: str) -> str:
    raw = (query.lower().strip() + "|" + _QUERY_KEY_VERSION).encode()
    return hashlib.sha256(raw).hexdigest()


def _cache_get(duck_conn, sha: str, ttl_days: int) -> Optional[list[dict]]:
    row = duck_conn.execute(
        "SELECT response_json, fetched_at FROM meta.ror_cache WHERE query_sha = ?",
        [sha],
    ).fetchone()
    if row is None:
        return None
    fetched_at = row[1]
    if isinstance(fetched_at, str):
        fetched_at = dt.datetime.fromisoformat(fetched_at)
    age = dt.datetime.now() - fetched_at
    if age > dt.timedelta(days=ttl_days):
        return None
    return json.loads(row[0])


def _cache_put(duck_conn, sha: str, query: str, parsed: list[dict]) -> None:
    duck_conn.execute(
        """
        INSERT OR REPLACE INTO meta.ror_cache
            (query_sha, query_text, response_json, fetched_at)
        VALUES (?, ?, ?, current_timestamp)
        """,
        [sha, query, json.dumps(parsed)],
    )


def _parse_ror_response(raw: dict, limit: int) -> list[dict]:
    """Extract the fields the enrichment agent actually uses.

    ROR's /organizations endpoint returns `items: [{id, name, country,
    aliases, relationships, ...}, ...]` — we keep the essentials and resolve
    the parent link (if any) so the agent can reason about hierarchy without
    chasing additional calls.
    """
    items = raw.get("items", []) or []
    parsed = []
    for idx, item in enumerate(items[:limit]):
        ror_url = item.get("id", "")
        ror_id = ror_url.rsplit("/", 1)[-1] if ror_url else None
        country = (item.get("country") or {}).get("country_name")
        aliases = item.get("aliases", []) or []
        parent = None
        for rel in item.get("relationships", []) or []:
            if rel.get("type") == "Parent":
                parent = {
                    "name": rel.get("label"),
                    "ror_id": (rel.get("id") or "").rsplit("/", 1)[-1] or None,
                }
                break
        parsed.append({
            "canonical_name": item.get("name"),
            "ror_id": ror_id,
            "country": country,
            "aliases": aliases,
            "parent": parent,
            "score": 1.0 if idx == 0 else round(1.0 / (idx + 1), 3),
        })
    return parsed


def _fetch_with_backoff(url: str) -> dict:
    """HTTP GET with exponential backoff on 429/5xx. Raises on final failure."""
    last_exc: Optional[Exception] = None
    for attempt, delay in enumerate([0, *_BACKOFF_DELAYS]):
        if delay:
            time.sleep(delay)
        try:
            resp = requests.get(
                url,
                headers={"User-Agent": _USER_AGENT},
                timeout=_REQUEST_TIMEOUT_SECONDS,
            )
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429,) or 500 <= resp.status_code < 600:
                last_exc = RuntimeError(f"ROR {resp.status_code}: {resp.text[:200]}")
                continue
            # Non-retryable: raise immediately
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            last_exc = exc
            continue
    raise RuntimeError(f"ROR API failed after {len(_BACKOFF_DELAYS) + 1} attempts: {last_exc}")


def lookup(duck_conn, query: str, limit: int = 5,
           ttl_days: int = ROR_CACHE_TTL_DAYS) -> list[dict]:
    """Query ROR; return parsed results or an error sentinel.

    Returns a list. On success: up to `limit` dicts (see `_parse_ror_response`).
    On failure: `[{"error": "...", "results": []}]` — the caller (enrichment
    agent tool wrapper) propagates this to Claude, which can route around it
    instead of crashing the run. Cache stores successful responses only.
    """
    if not query or not query.strip():
        return []
    ensure_cache_table(duck_conn)
    sha = _query_sha(query)

    cached = _cache_get(duck_conn, sha, ttl_days)
    if cached is not None:
        return cached[:limit]

    url = f"{ROR_API_BASE}/organizations?query={urllib.parse.quote(query)}"
    try:
        raw = _fetch_with_backoff(url)
    except Exception as exc:
        logger.warning(f"ROR lookup failed for {query!r}: {exc}")
        return [{"error": str(exc), "results": []}]

    parsed = _parse_ror_response(raw, limit=max(limit, 10))
    _cache_put(duck_conn, sha, query, parsed)
    return parsed[:limit]
