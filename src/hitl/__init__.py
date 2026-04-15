"""HITL (human-in-the-loop) review surface.

Re-exports the public API of `src.hitl.candidates` so existing consumers
can keep using `from src import hitl; hitl.insert_candidates(...)` after
the src/ reorg.
"""

from src.hitl.candidates import (  # noqa: F401
    DOMAIN_TARGETS,
    DOMAINS,
    REJECT_THROTTLE,
    ensure_candidates_table,
    export_candidates_csv,
    import_decision_log,
    import_reviewed_csv,
    insert_candidates,
    promote_candidates,
)
