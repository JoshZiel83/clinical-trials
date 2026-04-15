"""QuickUMLS lookup tool.

Thin wrapper around the QuickUMLS package. Exposes a single
`lookup(text)` function; the underlying index is loaded lazily once
per process. Designed to be called by the Phase 6E enrichment agent
and by ad-hoc exploration notebooks.
"""

from pathlib import Path

from src.logging_config import get_logger

logger = get_logger("quickumls_tool")

_MATCHER = None


def _resolve_index_path() -> Path:
    """Look up the active UMLS index path via meta.reference_sources."""
    from config.settings import get_duckdb_connection
    from src import reference_sources

    conn = get_duckdb_connection(read_only=True)
    try:
        return Path(reference_sources.get_active_path(conn, "umls"))
    finally:
        conn.close()


def _get_matcher(index_path=None,
                 threshold: float = 0.7,
                 similarity_name: str = "jaccard"):
    """Lazily build/return the QuickUMLS matcher (one per process)."""
    global _MATCHER
    if _MATCHER is not None:
        return _MATCHER
    if index_path is None:
        index_path = _resolve_index_path()
    if not index_path.exists() or not any(index_path.iterdir()):
        raise RuntimeError(
            f"QuickUMLS index not found at {index_path}. "
            f"Run `python -m scripts.build_quickumls_index` first."
        )
    from quickumls import QuickUMLS
    logger.info(f"loading QuickUMLS index from {index_path}")
    _MATCHER = QuickUMLS(
        quickumls_fp=str(index_path),
        threshold=threshold,
        similarity_name=similarity_name,
    )
    return _MATCHER


def lookup(text: str, threshold: float = 0.7, best_only: bool = True):
    """Return QuickUMLS matches for free-text input.

    Args:
        text: input string (e.g., a condition or drug name).
        threshold: Jaccard similarity threshold in [0, 1].
        best_only: if True, return only the single best match per span.

    Returns:
        List of dicts with keys: cui, canonical, score, semtypes, term.
        Empty list if nothing matches. The index is lazy-loaded.
    """
    if not text or not str(text).strip():
        return []
    matcher = _get_matcher(threshold=threshold)
    raw = matcher.match(str(text), best_match=best_only, ignore_syntax=True)

    results = []
    # QuickUMLS returns List[List[dict]] — outer list is spans.
    for span_matches in raw:
        for m in span_matches:
            results.append({
                "cui": m.get("cui"),
                "canonical": m.get("term"),
                "is_preferred": bool(m.get("preferred", 0)),
                "score": float(m.get("similarity", 0.0)),
                "semtypes": list(m.get("semtypes", []) or []),
            })
    # Deduplicate by CUI, keep highest score
    best_by_cui = {}
    for r in results:
        cui = r["cui"]
        if cui not in best_by_cui or r["score"] > best_by_cui[cui]["score"]:
            best_by_cui[cui] = r
    return sorted(best_by_cui.values(), key=lambda r: r["score"], reverse=True)


def is_available(index_path=None) -> bool:
    """Return True iff a QuickUMLS index exists and is usable."""
    if index_path is None:
        try:
            index_path = _resolve_index_path()
        except LookupError:
            return False
    return index_path.exists() and any(index_path.iterdir())
