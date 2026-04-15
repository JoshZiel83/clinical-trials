"""Tests for config/tables.py."""

from config.tables import ANCHOR_TABLE, EXTRACT_TABLES, STATUS_VALUES, STATUS_WHERE_CLAUSE


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


def test_status_values_count():
    assert len(STATUS_VALUES) == 5


def test_status_where_clause_contains_all_statuses():
    for status in STATUS_VALUES:
        assert status in STATUS_WHERE_CLAUSE


def test_status_where_clause_is_valid_sql_fragment():
    assert STATUS_WHERE_CLAUSE.startswith("overall_status IN (")
    assert STATUS_WHERE_CLAUSE.endswith(")")
