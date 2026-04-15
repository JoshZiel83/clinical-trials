"""Classify studies by design type (Levels 1, 2, 4, 5)."""

from config.settings import get_duckdb_connection
from src.logging_config import get_logger

logger = get_logger("classify_design")


def classify_study_design(duck_conn):
    """Create class.study_design from structured fields in raw.studies and raw.designs.

    Levels:
    1. Study Type — from study_type
    2. Design Architecture — combinatorial rules on allocation + intervention_model
       (interventional) or observational_model (observational)
    4. Blinding Level — mapped from masking
    5. Purpose — from primary_purpose

    Returns the number of rows created.
    """
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS class")
    duck_conn.execute("DROP TABLE IF EXISTS class.study_design")

    duck_conn.execute("""
        CREATE TABLE class.study_design AS
        SELECT
            s.nct_id,

            -- L1: Study Type (pass through)
            s.study_type,

            -- L2: Design Architecture
            CASE
                -- Expanded Access
                WHEN s.study_type = 'EXPANDED_ACCESS' THEN 'Expanded Access'

                -- Observational: use observational_model
                WHEN s.study_type = 'OBSERVATIONAL' THEN
                    CASE d.observational_model
                        WHEN 'COHORT' THEN 'Cohort'
                        WHEN 'CASE_CONTROL' THEN 'Case-Control'
                        WHEN 'CASE_ONLY' THEN 'Case-Only'
                        WHEN 'CASE_CROSSOVER' THEN 'Case-Crossover'
                        WHEN 'ECOLOGIC_OR_COMMUNITY' THEN 'Ecologic/Community'
                        WHEN 'FAMILY_BASED' THEN 'Family-Based'
                        WHEN 'OTHER' THEN 'Other Observational'
                        ELSE NULL
                    END

                -- Interventional: allocation + intervention_model
                WHEN s.study_type = 'INTERVENTIONAL' THEN
                    CASE
                        -- Randomized designs
                        WHEN d.allocation = 'RANDOMIZED' THEN
                            CASE d.intervention_model
                                WHEN 'PARALLEL' THEN 'Parallel RCT'
                                WHEN 'CROSSOVER' THEN 'Crossover RCT'
                                WHEN 'FACTORIAL' THEN 'Factorial RCT'
                                WHEN 'SEQUENTIAL' THEN 'Sequential RCT'
                                WHEN 'SINGLE_GROUP' THEN 'Randomized Single Group'
                                ELSE 'RCT (Model Unknown)'
                            END

                        -- Non-randomized designs
                        WHEN d.allocation = 'NON_RANDOMIZED' THEN
                            CASE d.intervention_model
                                WHEN 'SINGLE_GROUP' THEN 'Single-Arm'
                                WHEN 'PARALLEL' THEN 'Non-Randomized Parallel'
                                WHEN 'CROSSOVER' THEN 'Non-Randomized Crossover'
                                WHEN 'SEQUENTIAL' THEN 'Non-Randomized Sequential'
                                WHEN 'FACTORIAL' THEN 'Non-Randomized Factorial'
                                ELSE 'Non-Randomized (Model Unknown)'
                            END

                        -- Allocation missing/NA — infer from model
                        ELSE
                            CASE d.intervention_model
                                WHEN 'SINGLE_GROUP' THEN 'Single-Arm'
                                WHEN 'PARALLEL' THEN 'Parallel (Allocation Unknown)'
                                WHEN 'CROSSOVER' THEN 'Crossover (Allocation Unknown)'
                                WHEN 'SEQUENTIAL' THEN 'Sequential (Allocation Unknown)'
                                WHEN 'FACTORIAL' THEN 'Factorial (Allocation Unknown)'
                                ELSE NULL
                            END
                    END

                ELSE NULL
            END AS design_architecture,

            -- L4: Blinding Level
            CASE d.masking
                WHEN 'NONE' THEN 'Open Label'
                WHEN 'SINGLE' THEN 'Single Blind'
                WHEN 'DOUBLE' THEN 'Double Blind'
                WHEN 'TRIPLE' THEN 'Triple Blind'
                WHEN 'QUADRUPLE' THEN 'Quadruple Blind'
                ELSE NULL
            END AS blinding_level,

            -- L5: Purpose (pass through, None string → NULL)
            CASE
                WHEN d.primary_purpose IS NULL OR d.primary_purpose = 'None'
                    THEN NULL
                ELSE d.primary_purpose
            END AS purpose

        FROM raw.studies s
        LEFT JOIN raw.designs d ON s.nct_id = d.nct_id
    """)

    row_count = duck_conn.execute(
        "SELECT COUNT(*) FROM class.study_design"
    ).fetchone()[0]

    # Log distribution summary
    arch_dist = duck_conn.execute("""
        SELECT design_architecture, COUNT(*) AS cnt
        FROM class.study_design
        WHERE design_architecture IS NOT NULL
        GROUP BY design_architecture
        ORDER BY cnt DESC
        LIMIT 10
    """).fetchall()

    logger.info(f"Created class.study_design: {row_count:,} rows")
    logger.info("Top design architectures:")
    for arch, cnt in arch_dist:
        logger.info(f"  {arch}: {cnt:,}")

    return row_count


def run_design_classification_pipeline(duck_conn=None):
    """Run the study design classification pipeline.

    Returns the connection for chaining.
    """
    close_conn = duck_conn is None
    duck_conn = duck_conn or get_duckdb_connection()

    try:
        logger.info("Classifying study designs...")
        classify_study_design(duck_conn)
        return duck_conn
    finally:
        if close_conn:
            duck_conn.close()


if __name__ == "__main__":
    from src.logging_config import setup_logging

    setup_logging()
    run_design_classification_pipeline()
