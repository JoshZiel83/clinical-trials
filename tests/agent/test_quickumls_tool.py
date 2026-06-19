"""Tests for src/quickumls_tool.py.

The heavy lookup tests are skipped when no QuickUMLS index is present
(i.e., in any environment where `scripts.build_quickumls_index` has
not been run). The shape/availability tests always run.
"""

import pytest

from src.agent import quickumls_tool


def test_is_available_returns_bool():
    assert isinstance(quickumls_tool.is_available(), bool)


def test_lookup_empty_input_returns_empty():
    assert quickumls_tool.lookup("") == []
    assert quickumls_tool.lookup("   ") == []
    assert quickumls_tool.lookup(None) == []


def test_lookup_without_index_raises():
    """If QuickUMLS isn't set up, a clear, actionable error should surface.

    Two distinct unconfigured states are both valid: no 'umls' reference source
    registered (LookupError), or a registered source whose index dir is missing
    (RuntimeError). Accept either — both tell the user what to run next.
    """
    if quickumls_tool.is_available():
        pytest.skip("index exists; this test only runs when unavailable")
    with pytest.raises(
        (RuntimeError, LookupError),
        match="QuickUMLS index not found|reference source registered for 'umls'",
    ):
        quickumls_tool.lookup("glioblastoma")


@pytest.mark.skipif(
    not quickumls_tool.is_available(),
    reason="QuickUMLS index not built; run scripts/build_quickumls_index.py",
)
def test_lookup_glioblastoma_returns_cui():
    """Smoke test: a common medical term should produce at least one CUI."""
    results = quickumls_tool.lookup("glioblastoma multiforme")
    assert results, "expected at least one match"
    top = results[0]
    assert top["cui"].startswith("C") and len(top["cui"]) == 8
    assert 0.0 <= top["score"] <= 1.0
    assert "semtypes" in top
