"""Matching-algorithm tools for the Phase 6E enrichment agent.

Each function here is exposed to Claude as a tool via `@beta_tool`. The tools
are parametrized on a `Context` object (holding the DuckDB connection + any
expensive matchers) so the agent loop can pass stable references without
re-opening connections per call.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Optional

import pandas as pd
from rapidfuzz import fuzz, process

from src import reference_sources
from src.logging_config import get_logger
from src.transform.normalize_drugs import normalize_drug_name
from src.transform.normalize_sponsors import normalize_sponsor_name

logger = get_logger("enrichment_tools")


@dataclass
class ToolContext:
    """Holds read-only state accessed by tool functions.

    The agent creates one of these at the start of a run and hands tool
    functions a closure over it. Keeps the DuckDB connection and cached
    target lists out of global state.
    """
    duck_conn: Any
    # Lazily populated target lists
    _mesh_conditions: Optional[list[str]] = field(default=None, repr=False)
    _chembl_targets: Optional[list[tuple[str, str, Optional[str]]]] = field(
        default=None, repr=False
    )  # list of (lowercased_synonym, canonical_name, chembl_id)
    _sponsor_canonicals: Optional[list[str]] = field(default=None, repr=False)

    # ---- Target accessors ---------------------------------------------------

    def mesh_conditions(self) -> list[str]:
        if self._mesh_conditions is None:
            rows = self.duck_conn.execute("""
                SELECT DISTINCT mesh_term FROM raw.browse_conditions
                WHERE mesh_type = 'mesh-list'
            """).fetchall()
            self._mesh_conditions = [r[0] for r in rows]
        return self._mesh_conditions

    def chembl_targets(self) -> list[tuple[str, str, Optional[str]]]:
        if self._chembl_targets is None:
            from pathlib import Path
            try:
                chembl_path = Path(reference_sources.get_active_path(self.duck_conn, "chembl"))
            except LookupError:
                chembl_path = None
            if chembl_path is not None and chembl_path.exists():
                df = pd.read_parquet(chembl_path).dropna(
                    subset=["synonym", "pref_name"]
                )
                targets = []
                for _, r in df.iterrows():
                    syn = str(r["synonym"]).lower().strip()
                    if len(syn) < 4 or syn.isdigit():
                        continue
                    targets.append((syn, str(r["pref_name"]), r.get("chembl_id")))
                self._chembl_targets = targets
            else:
                self._chembl_targets = []
        return self._chembl_targets

    def sponsor_canonicals(self) -> list[str]:
        if self._sponsor_canonicals is None:
            rows = self.duck_conn.execute("""
                SELECT DISTINCT e.canonical_name
                FROM ref.sponsor_dictionary d
                JOIN entities.sponsor e ON d.sponsor_id = e.sponsor_id
                WHERE d.mapping_method IN ('exact-after-normalize', 'manual')
            """).fetchall()
            self._sponsor_canonicals = [r[0] for r in rows]
        return self._sponsor_canonicals


# ---------------------------------------------------------------------------
# Tool implementations (plain functions — agent wraps them with @beta_tool)
# ---------------------------------------------------------------------------


def fuzzy_mesh_condition(ctx: ToolContext, text: str, limit: int = 5) -> list[dict]:
    """Top fuzzy matches for a free-text condition against MeSH condition terms."""
    if not text.strip():
        return []
    matches = process.extract(
        text.lower(), ctx.mesh_conditions(), scorer=fuzz.WRatio,
        limit=limit, score_cutoff=70,
    )
    return [{"canonical_term": m[0], "score": float(m[1])} for m in matches]


def quickumls_condition(ctx: ToolContext, text: str, limit: int = 5) -> list[dict]:
    """QuickUMLS lookup for a free-text condition.

    Returns CUIs + preferred terms. Requires a built index — raises clearly
    if unavailable so the agent can route around it.
    """
    from src.agent.quickumls_tool import lookup
    results = lookup(text)
    return [
        {
            "cui": r["cui"],
            "canonical_term": r["canonical"],
            "score": r["score"],
            "semtypes": r["semtypes"],
        }
        for r in results[:limit]
    ]


def fuzzy_chembl_drug(
    ctx: ToolContext, text: str, limit: int = 5
) -> list[dict]:
    """Top fuzzy matches against ChEMBL synonyms. Also runs against MeSH
    intervention terms for drugs that are in MeSH but not ChEMBL."""
    if not text.strip():
        return []
    normalized = normalize_drug_name(text) or text.lower().strip()
    syn_index = [t[0] for t in ctx.chembl_targets()]
    matches = process.extract(
        normalized, syn_index, scorer=fuzz.WRatio,
        limit=limit, score_cutoff=70,
    )
    out = []
    for syn, score, idx in matches:
        _, canonical, chembl_id = ctx.chembl_targets()[idx]
        out.append({
            "canonical_term": canonical,
            "canonical_id": chembl_id,
            "matched_synonym": syn,
            "score": float(score),
        })
    return out


def fuzzy_sponsor(ctx: ToolContext, text: str, limit: int = 5) -> list[dict]:
    """Top fuzzy matches against known canonical sponsor names."""
    if not text.strip():
        return []
    normalized = normalize_sponsor_name(text) or text.lower().strip()
    candidates = ctx.sponsor_canonicals()
    matches = process.extract(
        normalized, candidates, scorer=fuzz.WRatio,
        limit=limit, score_cutoff=70,
    )
    return [{"canonical_term": m[0], "score": float(m[1])} for m in matches]


def co_occurrence_condition(ctx: ToolContext, source_value: str) -> list[dict]:
    """MeSH terms that co-occur with this raw condition name, ranked by frequency.

    Useful disambiguator for short/ambiguous condition names.
    """
    rows = ctx.duck_conn.execute("""
        SELECT bc.mesh_term, COUNT(DISTINCT c.nct_id) AS studies
        FROM raw.conditions c
        JOIN raw.browse_conditions bc USING (nct_id)
        WHERE LOWER(c.name) = LOWER(?)
          AND bc.mesh_type = 'mesh-list'
        GROUP BY bc.mesh_term
        ORDER BY studies DESC
        LIMIT 5
    """, [source_value]).fetchall()
    return [{"canonical_term": r[0], "study_count": int(r[1])} for r in rows]


def lookup_condition_dictionary(ctx: ToolContext, text: str) -> Optional[dict]:
    """Exact lookup of a condition name already in the dictionary (may avoid work)."""
    row = ctx.duck_conn.execute(
        """
        SELECT canonical_term, mapping_method, confidence
        FROM ref.condition_dictionary WHERE condition_name = LOWER(?)
        """,
        [text.strip()],
    ).fetchone()
    if row is None:
        return None
    return {"canonical_term": row[0], "mapping_method": row[1], "confidence": row[2]}


def lookup_drug_dictionary(ctx: ToolContext, text: str) -> Optional[dict]:
    row = ctx.duck_conn.execute(
        """
        SELECT canonical_name, canonical_id, mapping_method, confidence
        FROM ref.drug_dictionary WHERE source_name = LOWER(?)
        """,
        [normalize_drug_name(text)],
    ).fetchone()
    if row is None:
        return None
    return {
        "canonical_term": row[0],
        "canonical_id": row[1],
        "mapping_method": row[2],
        "confidence": row[3],
    }


# Domain → tool list mapping (used by the agent)
DOMAIN_TOOLS = {
    "condition": [
        fuzzy_mesh_condition,
        quickumls_condition,
        co_occurrence_condition,
        lookup_condition_dictionary,
    ],
    "drug": [
        fuzzy_chembl_drug,
        lookup_drug_dictionary,
    ],
    "sponsor": [
        fuzzy_sponsor,
    ],
}
