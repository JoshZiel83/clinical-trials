"""Tests for src/transform/promote.py — raw.* → enriched.* projection (Phase 7C)."""

import duckdb

from src.transform.promote import promote_to_enriched


def _setup_promote_test_db(extract_date="2026-04-17"):
    """In-memory DuckDB seeded with raw.* and meta.extraction_log.

    Edge cases covered:
      NCT001: valid start_date, removed=FALSE country, multiple interventions
      NCT002: NULL start_date (→ NULL start_year)
      NCT003: removed=TRUE country (should be filtered out)
      NCT004: removed IS NULL country (should be retained)
      NCT005: no interventions / no countries (should still appear in enriched.studies)
    """
    conn = duckdb.connect(":memory:")
    for schema in ("raw", "meta"):
        conn.execute(f"CREATE SCHEMA {schema}")

    conn.execute("""
        CREATE TABLE raw.studies AS SELECT * FROM (VALUES
            ('NCT001', 'RECRUITING', 'INTERVENTIONAL', 'PHASE3',
             'Trial 1', 'Full Trial 1', 100.0, DATE '2024-01-15',
             DATE '2026-01-15', 'Sponsor A'),
            ('NCT002', 'RECRUITING', 'OBSERVATIONAL', NULL,
             'Trial 2', 'Full Trial 2', 50.0, NULL,
             NULL, 'Sponsor B'),
            ('NCT003', 'RECRUITING', 'INTERVENTIONAL', 'PHASE1',
             'Trial 3', 'Full Trial 3', 20.0, DATE '2025-03-01',
             DATE '2027-03-01', 'Sponsor C'),
            ('NCT004', 'RECRUITING', 'INTERVENTIONAL', 'PHASE2',
             'Trial 4', 'Full Trial 4', 75.0, DATE '2023-06-01',
             DATE '2025-06-01', 'Sponsor D'),
            ('NCT005', 'RECRUITING', 'INTERVENTIONAL', 'PHASE3',
             'Trial 5', 'Full Trial 5', 300.0, DATE '2024-09-01',
             DATE '2028-09-01', 'Sponsor E')
        ) AS t(nct_id, overall_status, study_type, phase, brief_title,
               official_title, enrollment, start_date, completion_date, source)
    """)

    conn.execute("""
        CREATE TABLE raw.interventions AS SELECT * FROM (VALUES
            (1, 'NCT001', 'DRUG', 'aspirin'),
            (2, 'NCT001', 'BEHAVIORAL', 'counseling'),
            (3, 'NCT002', 'OBSERVATIONAL', 'blood draw'),
            (4, 'NCT003', 'DEVICE', 'sensor'),
            (5, 'NCT004', 'DRUG', 'placebo')
        ) AS t(id, nct_id, intervention_type, name)
    """)

    # Countries: NCT003 has removed=TRUE (drop), NCT004 has removed=NULL (keep).
    conn.execute("""
        CREATE TABLE raw.countries AS SELECT * FROM (VALUES
            (1, 'NCT001', 'United States', FALSE),
            (2, 'NCT001', 'Canada', FALSE),
            (3, 'NCT003', 'Japan', TRUE),
            (4, 'NCT004', 'Germany', NULL)
        ) AS t(id, nct_id, name, removed)
    """)

    conn.execute(f"""
        CREATE TABLE meta.extraction_log AS SELECT * FROM (VALUES
            ('run-1', DATE '{extract_date}', 'studies', 5),
            ('run-1', DATE '{extract_date}', 'interventions', 5),
            ('run-1', DATE '{extract_date}', 'countries', 4)
        ) AS t(extraction_id, extract_date, table_name, row_count)
    """)

    return conn


def test_promote_creates_enriched_schema_and_tables():
    conn = _setup_promote_test_db()
    promote_to_enriched(conn)
    tables = set(
        r[0]
        for r in conn.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'enriched'"
        ).fetchall()
    )
    assert tables == {"studies", "interventions", "countries"}
    conn.close()


def test_studies_row_count_matches_raw():
    conn = _setup_promote_test_db()
    promote_to_enriched(conn)
    assert conn.execute("SELECT COUNT(*) FROM enriched.studies").fetchone()[0] == 5
    conn.close()


def test_start_year_derivation():
    conn = _setup_promote_test_db()
    promote_to_enriched(conn)
    rows = dict(
        conn.execute(
            "SELECT nct_id, start_year FROM enriched.studies"
        ).fetchall()
    )
    assert rows["NCT001"] == 2024
    assert rows["NCT002"] is None  # NULL start_date → NULL start_year
    assert rows["NCT003"] == 2025
    assert rows["NCT004"] == 2023
    assert rows["NCT005"] == 2024
    conn.close()


def test_countries_removed_filter():
    conn = _setup_promote_test_db()
    promote_to_enriched(conn)
    rows = set(
        (nct, name)
        for nct, name in conn.execute(
            "SELECT nct_id, name FROM enriched.countries"
        ).fetchall()
    )
    # removed=TRUE (NCT003/Japan) must be excluded; removed=NULL (NCT004/Germany) retained.
    assert ("NCT003", "Japan") not in rows
    assert ("NCT004", "Germany") in rows
    assert rows == {
        ("NCT001", "United States"),
        ("NCT001", "Canada"),
        ("NCT004", "Germany"),
    }
    conn.close()


def test_interventions_passthrough_no_filter():
    conn = _setup_promote_test_db()
    promote_to_enriched(conn)
    assert conn.execute("SELECT COUNT(*) FROM enriched.interventions").fetchone()[0] == 5
    types = {
        r[0] for r in conn.execute(
            "SELECT DISTINCT intervention_type FROM enriched.interventions"
        ).fetchall()
    }
    assert types == {"DRUG", "BEHAVIORAL", "OBSERVATIONAL", "DEVICE"}
    conn.close()


def test_meta_enriched_tables_populated():
    conn = _setup_promote_test_db(extract_date="2026-04-17")
    promote_to_enriched(conn)
    rows = conn.execute("""
        SELECT table_name, extraction_date, row_count, last_built_at, notes
        FROM meta.enriched_tables
        ORDER BY table_name
    """).fetchall()
    assert len(rows) == 3
    by_name = {r[0]: r for r in rows}
    assert set(by_name) == {
        "enriched.studies",
        "enriched.interventions",
        "enriched.countries",
    }
    # All three stamped with the extract date from meta.extraction_log.
    assert all(str(r[1]) == "2026-04-17" for r in rows)
    # Row counts match actual enriched table counts.
    assert by_name["enriched.studies"][2] == 5
    assert by_name["enriched.interventions"][2] == 5
    assert by_name["enriched.countries"][2] == 3  # post-filter
    # last_built_at and notes non-null.
    assert all(r[3] is not None for r in rows)
    assert all(r[4] for r in rows)
    conn.close()


def test_extraction_date_pulled_from_log():
    conn = _setup_promote_test_db(extract_date="2026-01-15")
    promote_to_enriched(conn)
    dates = {
        str(r[0])
        for r in conn.execute(
            "SELECT DISTINCT extraction_date FROM meta.enriched_tables"
        ).fetchall()
    }
    assert dates == {"2026-01-15"}
    conn.close()


def test_idempotent_rebuild_refreshes_last_built_at():
    import time

    conn = _setup_promote_test_db()
    promote_to_enriched(conn)
    first = conn.execute(
        "SELECT last_built_at FROM meta.enriched_tables WHERE table_name = 'enriched.studies'"
    ).fetchone()[0]

    time.sleep(0.01)  # ensure timestamp advances
    promote_to_enriched(conn)
    second = conn.execute(
        "SELECT last_built_at FROM meta.enriched_tables WHERE table_name = 'enriched.studies'"
    ).fetchone()[0]

    assert second >= first
    # Row counts unchanged.
    assert conn.execute("SELECT COUNT(*) FROM enriched.studies").fetchone()[0] == 5
    assert conn.execute("SELECT COUNT(*) FROM enriched.countries").fetchone()[0] == 3
    # Registry still has exactly three rows (no duplicates from the re-run).
    assert conn.execute("SELECT COUNT(*) FROM meta.enriched_tables").fetchone()[0] == 3
    conn.close()
