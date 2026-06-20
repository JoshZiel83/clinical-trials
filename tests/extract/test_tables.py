"""Tests for config/tables.py."""

from config.tables import ACTIVE_STATUS_VALUES, ANCHOR_TABLE, EXTRACT_TABLES


def test_extract_tables_count():
    assert len(EXTRACT_TABLES) == 14


def test_extract_tables_contains_required():
    required = [
        "studies", "designs", "conditions", "browse_conditions",
        "interventions", "browse_interventions", "sponsors", "keywords",
        "brief_summaries", "detailed_descriptions", "design_groups",
        "countries", "eligibilities", "calculated_values",
    ]
    for table in required:
        assert table in EXTRACT_TABLES, f"Missing table: {table}"


def test_anchor_table_is_studies():
    assert ANCHOR_TABLE == "studies"


def test_anchor_table_in_extract_tables():
    assert ANCHOR_TABLE in EXTRACT_TABLES


def test_active_status_values_is_documentation_constant():
    # The status filter was removed in A3; this stays as a reference dimension.
    assert len(ACTIVE_STATUS_VALUES) == 5
    assert "RECRUITING" in ACTIVE_STATUS_VALUES
