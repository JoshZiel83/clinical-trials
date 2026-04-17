"""Build denormalized analytical views (Phase 4).

Creates views.study_summary — one row per study, joining design classification,
innovative features, AI mentions, therapeutic areas, conditions, drugs, countries,
and sponsors into a wide, query-ready table.
"""

from config.settings import get_duckdb_connection
from src.logging_config import get_logger

logger = get_logger("views")


# (feature_type value in class.innovative_features, boolean column name)
FEATURE_FLAGS = [
    ("adaptive", "is_adaptive"),
    ("basket", "is_basket"),
    ("umbrella", "is_umbrella"),
    ("platform", "is_platform"),
    ("bayesian", "is_bayesian"),
    ("SMART", "is_smart"),
    ("N-of-1", "is_n_of_1"),
    ("pragmatic", "is_pragmatic"),
    ("enrichment", "is_enrichment"),
    ("seamless", "is_seamless"),
    ("master protocol", "is_master_protocol"),
    ("digital twin", "is_digital_twin"),
    ("in silico", "is_in_silico"),
    ("AI-augmented design", "is_ai_augmented_design"),
]


def build_study_summary(duck_conn):
    """Create views.study_summary — one row per study with joined dimensions.

    Multi-valued dimensions (TAs, features, drugs, countries, collaborators)
    are aggregated into LIST columns; per-feature booleans provided for
    simple column filtering.

    Returns the number of rows created.
    """
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS views")
    duck_conn.execute("DROP TABLE IF EXISTS views.study_summary")

    feature_flag_sql = ",\n                ".join(
        f"BOOL_OR(feature_type = '{ftype}') AS {col}"
        for ftype, col in FEATURE_FLAGS
    )

    duck_conn.execute(f"""
        CREATE TABLE views.study_summary AS
        WITH features_agg AS (
            SELECT
                nct_id,
                LIST(DISTINCT feature_type) AS innovative_feature_types,
                COUNT(DISTINCT feature_type) AS innovative_feature_count,
                {feature_flag_sql}
            FROM class.innovative_features
            GROUP BY nct_id
        ),
        ai_agg AS (
            SELECT
                nct_id,
                LIST(DISTINCT ai_term) AS ai_mention_terms
            FROM class.ai_mentions
            GROUP BY nct_id
        ),
        tas_agg AS (
            SELECT
                nct_id,
                LIST(DISTINCT therapeutic_area) AS therapeutic_areas,
                COUNT(DISTINCT therapeutic_area) AS therapeutic_area_count
            FROM norm.study_therapeutic_areas
            GROUP BY nct_id
        ),
        ta_counts AS (
            SELECT
                nct_id,
                therapeutic_area,
                COUNT(*) AS ancestor_hits
            FROM norm.study_therapeutic_areas
            GROUP BY nct_id, therapeutic_area
        ),
        primary_ta AS (
            SELECT nct_id, therapeutic_area AS primary_therapeutic_area
            FROM (
                SELECT
                    nct_id,
                    therapeutic_area,
                    ROW_NUMBER() OVER (
                        PARTITION BY nct_id
                        ORDER BY ancestor_hits DESC, therapeutic_area ASC
                    ) AS rn
                FROM ta_counts
            )
            WHERE rn = 1
        ),
        conditions_agg AS (
            SELECT
                sc.nct_id,
                LIST(DISTINCT sc.condition_name) AS raw_condition_names,
                LIST(DISTINCT ec.canonical_term) FILTER (WHERE ec.canonical_term IS NOT NULL)
                    AS canonical_conditions,
                COUNT(*) AS condition_count,
                COUNT(DISTINCT ec.canonical_term) FILTER (WHERE ec.canonical_term IS NOT NULL)
                    AS mapped_condition_count
            FROM norm.study_conditions sc
            LEFT JOIN entities.condition ec ON sc.condition_id = ec.condition_id
            GROUP BY sc.nct_id
        ),
        drugs_agg AS (
            SELECT
                sd.nct_id,
                LIST(DISTINCT ed.canonical_name) FILTER (WHERE ed.canonical_name IS NOT NULL)
                    AS canonical_drugs,
                LIST(DISTINCT ed.chembl_id) FILTER (WHERE ed.chembl_id IS NOT NULL)
                    AS chembl_ids,
                COUNT(*) AS drug_intervention_count,
                COUNT(DISTINCT ed.canonical_name) FILTER (WHERE ed.canonical_name IS NOT NULL)
                    AS mapped_drug_count
            FROM norm.study_drugs sd
            LEFT JOIN entities.drug ed ON sd.drug_id = ed.drug_id
            GROUP BY sd.nct_id
        ),
        interventions_agg AS (
            SELECT
                nct_id,
                LIST(DISTINCT intervention_type) AS intervention_types,
                COUNT(*) AS intervention_count
            FROM enriched.interventions
            GROUP BY nct_id
        ),
        countries_agg AS (
            SELECT
                nct_id,
                LIST(DISTINCT name) AS countries,
                COUNT(DISTINCT name) AS country_count
            FROM enriched.countries
            GROUP BY nct_id
        ),
        lead_sponsor AS (
            SELECT nct_id, lead_sponsor_name, lead_sponsor_agency_class
            FROM (
                SELECT
                    ss.nct_id,
                    es.canonical_name AS lead_sponsor_name,
                    ss.agency_class   AS lead_sponsor_agency_class,
                    ROW_NUMBER() OVER (PARTITION BY ss.nct_id ORDER BY ss.nct_id) AS rn
                FROM norm.study_sponsors ss
                LEFT JOIN entities.sponsor es ON ss.sponsor_id = es.sponsor_id
                WHERE LOWER(ss.lead_or_collaborator) = 'lead'
            )
            WHERE rn = 1
        ),
        collaborators AS (
            SELECT
                ss.nct_id,
                LIST(DISTINCT es.canonical_name) AS collaborator_names
            FROM norm.study_sponsors ss
            LEFT JOIN entities.sponsor es ON ss.sponsor_id = es.sponsor_id
            WHERE LOWER(ss.lead_or_collaborator) = 'collaborator'
            GROUP BY ss.nct_id
        )
        SELECT
            s.nct_id,
            s.overall_status,
            s.study_type,
            s.phase,
            s.brief_title,
            s.official_title,
            s.enrollment,
            s.start_date,
            s.completion_date,
            s.start_year,
            s.source,

            -- Design classification
            d.design_architecture,
            d.blinding_level,
            d.purpose,

            -- Innovative features
            COALESCE(f.innovative_feature_types, []::VARCHAR[]) AS innovative_feature_types,
            COALESCE(f.innovative_feature_count, 0) AS innovative_feature_count,
            COALESCE(f.innovative_feature_count, 0) > 0 AS has_innovative_feature,
            {", ".join(f"COALESCE(f.{col}, FALSE) AS {col}" for _, col in FEATURE_FLAGS)},

            -- AI/ML mentions
            (a.ai_mention_terms IS NOT NULL) AS has_ai_mention,
            COALESCE(a.ai_mention_terms, []::VARCHAR[]) AS ai_mention_terms,

            -- Therapeutic areas
            COALESCE(t.therapeutic_areas, []::VARCHAR[]) AS therapeutic_areas,
            COALESCE(t.therapeutic_area_count, 0) AS therapeutic_area_count,
            pt.primary_therapeutic_area,

            -- Conditions
            COALESCE(c.raw_condition_names, []::VARCHAR[]) AS raw_condition_names,
            COALESCE(c.canonical_conditions, []::VARCHAR[]) AS canonical_conditions,
            COALESCE(c.condition_count, 0) AS condition_count,
            COALESCE(c.mapped_condition_count, 0) AS mapped_condition_count,

            -- Interventions (all types — drug, behavioral, device, procedure, ...)
            COALESCE(iv.intervention_types, []::VARCHAR[]) AS intervention_types,
            COALESCE(iv.intervention_count, 0) AS intervention_count,

            -- Drugs
            COALESCE(dr.canonical_drugs, []::VARCHAR[]) AS canonical_drugs,
            COALESCE(dr.chembl_ids, []::VARCHAR[]) AS chembl_ids,
            COALESCE(dr.drug_intervention_count, 0) AS drug_intervention_count,
            COALESCE(dr.mapped_drug_count, 0) AS mapped_drug_count,

            -- Countries
            COALESCE(co.countries, []::VARCHAR[]) AS countries,
            COALESCE(co.country_count, 0) AS country_count,

            -- Sponsors (pre-Phase-6, un-normalized)
            ls.lead_sponsor_name,
            ls.lead_sponsor_agency_class,
            COALESCE(cb.collaborator_names, []::VARCHAR[]) AS collaborator_names

        FROM enriched.studies s
        LEFT JOIN class.study_design d ON s.nct_id = d.nct_id
        LEFT JOIN features_agg f ON s.nct_id = f.nct_id
        LEFT JOIN ai_agg a ON s.nct_id = a.nct_id
        LEFT JOIN tas_agg t ON s.nct_id = t.nct_id
        LEFT JOIN primary_ta pt ON s.nct_id = pt.nct_id
        LEFT JOIN conditions_agg c ON s.nct_id = c.nct_id
        LEFT JOIN interventions_agg iv ON s.nct_id = iv.nct_id
        LEFT JOIN drugs_agg dr ON s.nct_id = dr.nct_id
        LEFT JOIN countries_agg co ON s.nct_id = co.nct_id
        LEFT JOIN lead_sponsor ls ON s.nct_id = ls.nct_id
        LEFT JOIN collaborators cb ON s.nct_id = cb.nct_id
    """)

    row_count = duck_conn.execute(
        "SELECT COUNT(*) FROM views.study_summary"
    ).fetchone()[0]

    stats = duck_conn.execute("""
        SELECT
            SUM(CASE WHEN has_innovative_feature THEN 1 ELSE 0 END) AS innovative,
            SUM(CASE WHEN therapeutic_area_count > 0 THEN 1 ELSE 0 END) AS has_ta,
            SUM(CASE WHEN mapped_drug_count > 0 THEN 1 ELSE 0 END) AS has_drug,
            SUM(CASE WHEN has_ai_mention THEN 1 ELSE 0 END) AS ai,
            SUM(CASE WHEN lead_sponsor_name IS NOT NULL THEN 1 ELSE 0 END) AS has_lead
        FROM views.study_summary
    """).fetchone()

    logger.info(f"Created views.study_summary: {row_count:,} rows")
    if row_count:
        innovative, has_ta, has_drug, ai, has_lead = stats
        logger.info(f"  innovative: {innovative:,} ({100*innovative/row_count:.1f}%)")
        logger.info(f"  with TA:    {has_ta:,} ({100*has_ta/row_count:.1f}%)")
        logger.info(f"  mapped drug:{has_drug:,} ({100*has_drug/row_count:.1f}%)")
        logger.info(f"  AI mention: {ai:,} ({100*ai/row_count:.1f}%)")
        logger.info(f"  lead spon.: {has_lead:,} ({100*has_lead/row_count:.1f}%)")

    return row_count


def run_views_pipeline(duck_conn=None):
    """Run the analytical views pipeline."""
    close_conn = duck_conn is None
    duck_conn = duck_conn or get_duckdb_connection()

    try:
        logger.info("Building analytical views...")
        build_study_summary(duck_conn)
        return duck_conn
    finally:
        if close_conn:
            duck_conn.close()


if __name__ == "__main__":
    from src.logging_config import setup_logging

    setup_logging()
    run_views_pipeline()
