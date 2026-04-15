"""Build condition dictionary and map study conditions to canonical MeSH terms."""

import os
import re

from config.settings import get_duckdb_connection
from src import entities
from src.logging_config import get_logger

logger = get_logger("normalize_conditions")

AUTOMATED_METHODS = (
    "exact", "1:1-study", "co-occurrence", "cancer-synonym",
)

# Qualifiers stripped before fuzzy matching
QUALIFIER_PATTERNS = [
    r'\b(advanced|metastatic|refractory|recurrent|relapsed)\b',
    r'\b(chronic|acute|severe|mild|moderate)\b',
    r'\b(unresectable|localized|primary|secondary)\b',
    r'\b(stage\s+[iv]+|anatomic\s+stage\s+[iv]+)\b',
    r'\bajcc\s+v\d+\b',
    r'\b(adult|pediatric|childhood|juvenile|neonatal|infantile)\b',
]

# Terms that are not diseases/conditions — skip fuzzy matching
NON_CONDITION_PATTERNS = [
    r'^healthy\b', r'\bhealthy\s+(volunteer|subject|participant|adult|individual)',
    r'^(children|elderly|parenting|breastfeeding|contraception)$',
    r'^(artificial intelligence|virtual reality|machine learning)',
    r'^(immunotherapy|chemotherapy|ultrasound|surgery|anesthesia)$',
    r'^(exercise|physical activity|physical inactivity|diet|self efficacy)$',
    r'^(quality of life|mental health|blood pressure|dental implant)$',
    r'^(pregnancy related|pregnancy)$',
]

# Cancer term → MeSH neoplasm synonyms
CANCER_SYNONYMS = {
    'cancer': 'Neoplasms',
    'cancers': 'Neoplasms',
    'carcinoma': 'Carcinoma',
    'tumor': 'Neoplasms',
    'tumors': 'Neoplasms',
    'tumour': 'Neoplasms',
    'tumours': 'Neoplasms',
    'sarcoma': 'Sarcoma',
    'lymphoma': 'Lymphoma',
    'leukemia': 'Leukemia',
    'leukaemia': 'Leukemia',
    'melanoma': 'Melanoma',
    'myeloma': 'Multiple Myeloma',
    'glioma': 'Glioma',
    'glioblastoma': 'Glioblastoma',
    'mesothelioma': 'Mesothelioma',
    'neuroblastoma': 'Neuroblastoma',
    'blastoma': 'Neoplasms',
}


def normalize_condition(name):
    """Normalize a condition name for fuzzy matching.

    Strips parenthetical text, qualifiers, staging info, and extra whitespace.
    """
    s = name.lower()
    # Remove parenthetical text
    s = re.sub(r'\([^)]*\)', '', s)
    # Remove qualifiers
    for pattern in QUALIFIER_PATTERNS:
        s = re.sub(pattern, '', s, flags=re.IGNORECASE)
    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s).strip()
    # Remove leading/trailing punctuation
    s = s.strip(' ,-/')
    return s


def is_non_condition(name):
    """Check if a term is likely not a disease/condition."""
    lower = name.lower().strip()
    for pattern in NON_CONDITION_PATTERNS:
        if re.search(pattern, lower):
            return True
    return False


def build_condition_dictionary(duck_conn):
    """Build ref.condition_dictionary from layered matching against browse_conditions.

    Layers (in priority order — earlier layers take precedence):
    1. Exact: condition.downcase_name = browse_conditions.downcase_mesh_term (same study)
    2. 1:1-study: study has exactly 1 condition + 1 mesh-list term
    3. Co-occurrence: dominant co-occurring mesh term across studies
    4. Cancer-synonym: '[Site] Cancer' → '[Site] Neoplasms'

    Fuzzy matching is handled separately via generate_fuzzy_candidates() — see
    notebooks/02a_condition_enrichment.ipynb for the HITL review workflow.

    Preserves any manual/quickumls rows already in the dictionary.
    Returns the total number of dictionary entries.
    """
    entities.ensure_schema(duck_conn)
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS ref")

    # Drop old-schema table if present (carries canonical_term column).
    old_schema = duck_conn.execute("""
        SELECT COUNT(*) FROM information_schema.columns
        WHERE table_schema = 'ref' AND table_name = 'condition_dictionary'
          AND column_name = 'canonical_term'
    """).fetchone()[0]
    if old_schema:
        duck_conn.execute("DROP TABLE ref.condition_dictionary")

    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS ref.condition_dictionary (
            condition_name  VARCHAR PRIMARY KEY,
            condition_id    BIGINT NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)

    # Staging table — layers write canonical_term strings, resolve step
    # transforms to condition_id.
    duck_conn.execute("DROP TABLE IF EXISTS ref._condition_dictionary_staging")
    duck_conn.execute("""
        CREATE TABLE ref._condition_dictionary_staging (
            condition_name  VARCHAR NOT NULL,
            canonical_term  VARCHAR NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)

    # Preserve non-automated rows in the live dictionary
    duck_conn.execute("""
        DELETE FROM ref.condition_dictionary
        WHERE mapping_method IN (SELECT unnest(?))
    """, [list(AUTOMATED_METHODS)])

    # Layer 1: Exact match
    duck_conn.execute("""
        INSERT INTO ref._condition_dictionary_staging
        SELECT DISTINCT
            c.downcase_name AS condition_name,
            bc.mesh_term    AS canonical_term,
            'exact'         AS mapping_method,
            'high'          AS confidence
        FROM raw.conditions c
        INNER JOIN raw.browse_conditions bc
            ON c.nct_id = bc.nct_id
            AND c.downcase_name = bc.downcase_mesh_term
        WHERE bc.mesh_type = 'mesh-list'
    """)
    layer1_count = duck_conn.execute(
        "SELECT COUNT(*) FROM ref._condition_dictionary_staging WHERE mapping_method = 'exact'"
    ).fetchone()[0]
    logger.info(f"  Layer 1 (exact): {layer1_count:,} condition mappings")

    # Layer 2: 1:1 study match
    duck_conn.execute("""
        INSERT INTO ref._condition_dictionary_staging
        SELECT DISTINCT
            c.downcase_name AS condition_name,
            bc.mesh_term    AS canonical_term,
            '1:1-study'     AS mapping_method,
            'high'          AS confidence
        FROM raw.conditions c
        INNER JOIN raw.browse_conditions bc
            ON c.nct_id = bc.nct_id
        WHERE bc.mesh_type = 'mesh-list'
        AND c.nct_id IN (
            SELECT nct_id FROM raw.conditions GROUP BY nct_id HAVING COUNT(DISTINCT name) = 1
            INTERSECT
            SELECT nct_id FROM raw.browse_conditions WHERE mesh_type = 'mesh-list'
            GROUP BY nct_id HAVING COUNT(DISTINCT mesh_term) = 1
        )
        AND c.downcase_name NOT IN (
            SELECT condition_name FROM ref._condition_dictionary_staging
        )
        AND c.downcase_name NOT IN (
            SELECT condition_name FROM ref.condition_dictionary
        )
    """)
    layer2_count = duck_conn.execute(
        "SELECT COUNT(*) FROM ref._condition_dictionary_staging WHERE mapping_method = '1:1-study'"
    ).fetchone()[0]
    logger.info(f"  Layer 2 (1:1-study): {layer2_count:,} condition mappings")

    # Layer 3: Co-occurrence dominance
    duck_conn.execute("""
        WITH co_occur AS (
            SELECT
                c.downcase_name AS condition_name,
                bc.mesh_term    AS canonical_term,
                COUNT(DISTINCT c.nct_id) AS n
            FROM raw.conditions c
            INNER JOIN raw.browse_conditions bc
                ON c.nct_id = bc.nct_id
            WHERE bc.mesh_type = 'mesh-list'
            AND c.downcase_name NOT IN (
                SELECT condition_name FROM ref._condition_dictionary_staging
            )
            AND c.downcase_name NOT IN (
                SELECT condition_name FROM ref.condition_dictionary
            )
            GROUP BY c.downcase_name, bc.mesh_term
            HAVING COUNT(DISTINCT c.nct_id) >= 3
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (PARTITION BY condition_name ORDER BY n DESC) AS rn,
                LEAD(n) OVER (PARTITION BY condition_name ORDER BY n DESC) AS runner_up_n
            FROM co_occur
        )
        INSERT INTO ref._condition_dictionary_staging
        SELECT DISTINCT
            condition_name,
            canonical_term,
            'co-occurrence' AS mapping_method,
            CASE
                WHEN n >= 10 AND (runner_up_n IS NULL OR n >= 2 * runner_up_n) THEN 'high'
                ELSE 'medium'
            END AS confidence
        FROM ranked
        WHERE rn = 1
        AND (runner_up_n IS NULL OR n >= 2 * runner_up_n)
    """)
    layer3_count = duck_conn.execute(
        "SELECT COUNT(*) FROM ref._condition_dictionary_staging WHERE mapping_method = 'co-occurrence'"
    ).fetchone()[0]
    logger.info(f"  Layer 3 (co-occurrence): {layer3_count:,} condition mappings")

    # Layer 4: Cancer synonym expansion
    _build_cancer_synonym_mappings(duck_conn)

    # Resolve canonical_term strings to condition_ids via entities.condition.
    _resolve_condition_entities(duck_conn)

    duck_conn.execute("DROP TABLE ref._condition_dictionary_staging")

    total = duck_conn.execute(
        "SELECT COUNT(*) FROM ref.condition_dictionary"
    ).fetchone()[0]
    logger.info(f"Condition dictionary: {total:,} total entries")
    return total


def _resolve_condition_entities(duck_conn):
    """Resolve each staging row's canonical_term to an entities.condition.condition_id.

    entities.condition is pre-seeded with all MeSH descriptors; the vast
    majority of lookups hit directly. If a layer produces a canonical_term
    not in MeSH (rare but possible with cancer-synonym), upsert_condition
    creates the row with origin='mesh' (treated as a MeSH-named concept
    even if not seeded) — flag it via review later if needed.
    """
    # Dedup on condition_name — the same raw condition can match multiple
    # MeSH terms within a layer (different studies, multiple mesh-list entries).
    # First-writer-wins on insertion order (layer priority is preserved via
    # the NOT IN guards between layers).
    staging = duck_conn.execute("""
        WITH ordered AS (
            SELECT condition_name, canonical_term, mapping_method, confidence,
                   ROW_NUMBER() OVER (PARTITION BY condition_name ORDER BY rowid) AS rn
            FROM ref._condition_dictionary_staging
        )
        SELECT condition_name, canonical_term, mapping_method, confidence
        FROM ordered WHERE rn = 1
    """).fetchall()

    inserts = []
    for condition_name, canonical_term, mapping_method, confidence in staging:
        condition_id = entities.upsert_condition(
            duck_conn,
            canonical_term=canonical_term,
            origin="mesh",
        )
        inserts.append((condition_name, condition_id, mapping_method, confidence))

    if inserts:
        duck_conn.executemany(
            """
            INSERT INTO ref.condition_dictionary
                (condition_name, condition_id, mapping_method, confidence)
            VALUES (?, ?, ?, ?)
            """,
            inserts,
        )
    logger.info(f"  resolved {len(inserts):,} dictionary entries to entities.condition")


def _build_cancer_synonym_mappings(duck_conn):
    """Map '[Site] Cancer' → '[Site] Neoplasms' against known mesh-list terms.

    Handles cancer, carcinoma, tumor, sarcoma, lymphoma, leukemia, etc.
    """
    # Get all mesh-list terms as match targets
    mesh_terms = duck_conn.execute("""
        SELECT DISTINCT mesh_term
        FROM raw.browse_conditions
        WHERE mesh_type = 'mesh-list'
    """).fetchall()
    mesh_set = {row[0] for row in mesh_terms}
    mesh_lower = {t.lower(): t for t in mesh_set}

    # Get unmapped condition names — skip any already in staging or the live dictionary
    existing_rows = duck_conn.execute("""
        SELECT condition_name FROM ref._condition_dictionary_staging
        UNION
        SELECT condition_name FROM ref.condition_dictionary
    """).fetchall()
    existing_names = {row[0] for row in existing_rows}

    unmapped = duck_conn.execute("""
        SELECT DISTINCT LOWER(name) AS condition_name
        FROM raw.conditions
        WHERE LOWER(name) NOT IN (
            SELECT condition_name FROM ref._condition_dictionary_staging
        )
        AND LOWER(name) NOT IN (
            SELECT condition_name FROM ref.condition_dictionary
        )
    """).fetchall()

    mappings = []
    for (cond_name,) in unmapped:
        if cond_name in existing_names:
            continue
        # Strip qualifiers first
        normalized = normalize_condition(cond_name)
        # Try each cancer synonym
        for cancer_term, mesh_suffix in CANCER_SYNONYMS.items():
            # Match "[site] cancer" pattern
            pattern = rf'^(.+?)\s+{re.escape(cancer_term)}s?$'
            m = re.match(pattern, normalized, re.IGNORECASE)
            if m:
                site = m.group(1).strip()
                # Try "[Site] Neoplasms" (most common MeSH pattern)
                candidate = f"{site} {mesh_suffix}"
                if candidate.lower() in mesh_lower:
                    mappings.append((cond_name, mesh_lower[candidate.lower()]))
                    existing_names.add(cond_name)
                    break
                # Try "{Site} Neoplasms" with title case
                candidate_title = candidate.title()
                if candidate_title in mesh_set:
                    mappings.append((cond_name, candidate_title))
                    existing_names.add(cond_name)
                    break

    if mappings:
        import pandas as pd
        df = pd.DataFrame(mappings, columns=["condition_name", "canonical_term"])
        df["mapping_method"] = "cancer-synonym"
        df["confidence"] = "high"
        duck_conn.execute(
            "INSERT INTO ref._condition_dictionary_staging SELECT * FROM df"
        )

    logger.info(f"  Layer 4 (cancer-synonym): {len(mappings):,} condition mappings")


def generate_fuzzy_candidates(duck_conn):
    """Generate fuzzy match candidates for human review.

    Matches unmapped conditions against mesh-list terms using rapidfuzz.
    Storage is delegated to src.hitl (ref.mapping_candidates with
    domain='condition', source='fuzzy'). Approved/rejected decisions
    persist across regenerations.

    Returns a pandas DataFrame of the generated candidates with columns
    [condition_name, canonical_term, score, study_count, status] for
    backward compatibility with the enrichment notebook.
    """
    import pandas as pd
    from rapidfuzz import fuzz, process

    from src import hitl

    hitl.ensure_candidates_table(duck_conn)

    mesh_terms = duck_conn.execute("""
        SELECT DISTINCT mesh_term
        FROM raw.browse_conditions
        WHERE mesh_type = 'mesh-list'
    """).fetchall()
    mesh_list = [row[0] for row in mesh_terms]

    unmapped = duck_conn.execute("""
        SELECT LOWER(c.name) AS condition_name,
               COUNT(DISTINCT c.nct_id) AS study_count
        FROM raw.conditions c
        WHERE LOWER(c.name) NOT IN (SELECT condition_name FROM ref.condition_dictionary)
        GROUP BY LOWER(c.name)
    """).fetchdf()

    candidates = []
    for _, row in unmapped.iterrows():
        cond_name = row["condition_name"]
        study_count = int(row["study_count"])

        if is_non_condition(cond_name):
            continue

        normalized = normalize_condition(cond_name)
        if len(normalized) < 3:
            continue

        result = process.extractOne(
            normalized, mesh_list, scorer=fuzz.token_sort_ratio, score_cutoff=75
        )
        if result is None:
            continue

        match_term, score, _ = result
        candidates.append({
            "source_value": cond_name,
            "canonical_term": match_term,
            "score": float(score),
            "study_count": study_count,
        })

    df = pd.DataFrame(candidates) if candidates else pd.DataFrame(
        columns=["source_value", "canonical_term", "score", "study_count"]
    )
    hitl.insert_candidates(duck_conn, "condition", df, source="fuzzy")

    logger.info(f"Generated {len(df):,} fuzzy candidates for review")

    # Return in legacy column naming for the enrichment notebook
    return df.rename(columns={"source_value": "condition_name"}).assign(status="pending")


def promote_candidates(duck_conn, approved_df):
    """Promote approved candidates into the condition dictionary as manual entries.

    approved_df: DataFrame with columns [condition_name, canonical_term].
    Returns number promoted. Thin wrapper around src.hitl.promote_candidates.
    """
    from src import hitl

    if approved_df is None or approved_df.empty:
        return 0
    normalized_df = approved_df.rename(columns={"condition_name": "source_value"})
    return hitl.promote_candidates(duck_conn, "condition", normalized_df)


def export_candidates_csv(duck_conn, output_path=None):
    """Export pending condition candidates to CSV for offline review."""
    from src import hitl

    if output_path is None:
        output_path = os.path.join(
            os.path.dirname(os.path.dirname(__file__)),
            "data", "reference", "condition_candidates.csv",
        )
    return hitl.export_candidates_csv(duck_conn, "condition", output_path)


def import_reviewed_csv(duck_conn, csv_path):
    """Import a reviewed condition-candidates CSV.

    Accepts both legacy (`condition_name`) and new (`source_value`) column
    names for backward compatibility.
    """
    import pandas as pd

    from src import hitl

    reviewed = pd.read_csv(csv_path)
    if "condition_name" in reviewed.columns and "source_value" not in reviewed.columns:
        # Legacy CSV — translate and write a temp file
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w")
        reviewed.rename(columns={"condition_name": "source_value"}).to_csv(
            tmp.name, index=False
        )
        tmp.close()
        return hitl.import_reviewed_csv(duck_conn, "condition", tmp.name)
    return hitl.import_reviewed_csv(duck_conn, "condition", csv_path)


def create_study_conditions(duck_conn):
    """Join raw.conditions to ref.condition_dictionary, creating norm.study_conditions.

    Every row in raw.conditions gets a row here. Unmapped conditions have
    NULL canonical_term. The dictionary is the single source of truth —
    manual or QuickUMLS entries added to the dictionary are automatically
    picked up on the next run.

    Returns the number of rows created.
    """
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS norm")
    duck_conn.execute("DROP TABLE IF EXISTS norm.study_conditions")

    duck_conn.execute("""
        CREATE TABLE norm.study_conditions AS
        SELECT
            c.nct_id,
            c.name           AS condition_name,
            d.condition_id,
            d.mapping_method,
            d.confidence
        FROM raw.conditions c
        LEFT JOIN ref.condition_dictionary d
            ON LOWER(c.name) = d.condition_name
    """)

    row_count = duck_conn.execute(
        "SELECT COUNT(*) FROM norm.study_conditions"
    ).fetchone()[0]
    mapped = duck_conn.execute(
        "SELECT COUNT(*) FROM norm.study_conditions WHERE condition_id IS NOT NULL"
    ).fetchone()[0]
    logger.info(
        f"Created norm.study_conditions: {row_count:,} rows "
        f"({mapped:,} mapped, {row_count - mapped:,} unmapped)"
    )
    return row_count


def get_coverage_stats(duck_conn):
    """Compute coverage statistics for condition mapping and TA assignment.

    Returns a dict with condition-level and study-level stats.
    """
    total_studies = duck_conn.execute(
        "SELECT COUNT(DISTINCT nct_id) FROM raw.studies"
    ).fetchone()[0]

    # Condition dictionary stats
    dict_stats = duck_conn.execute("""
        SELECT mapping_method, confidence, COUNT(*) as cnt
        FROM ref.condition_dictionary
        GROUP BY mapping_method, confidence
        ORDER BY mapping_method, confidence
    """).fetchdf()

    # Study-level condition mapping
    studies_with_canonical = duck_conn.execute(
        "SELECT COUNT(DISTINCT nct_id) FROM norm.study_conditions WHERE condition_id IS NOT NULL"
    ).fetchone()[0]

    # TA coverage
    studies_with_ta = duck_conn.execute(
        "SELECT COUNT(DISTINCT nct_id) FROM norm.study_therapeutic_areas"
    ).fetchone()[0]

    ta_pct = round(100 * studies_with_ta / total_studies, 1) if total_studies > 0 else 0
    cond_pct = round(100 * studies_with_canonical / total_studies, 1) if total_studies > 0 else 0

    stats = {
        "total_studies": total_studies,
        "studies_with_canonical": studies_with_canonical,
        "condition_coverage_pct": cond_pct,
        "studies_with_ta": studies_with_ta,
        "ta_coverage_pct": ta_pct,
        "dictionary_stats": dict_stats,
    }

    logger.info(
        f"Condition coverage: {studies_with_canonical:,}/{total_studies:,} ({cond_pct}%) "
        f"studies have canonical conditions"
    )
    logger.info(
        f"TA coverage: {studies_with_ta:,}/{total_studies:,} ({ta_pct}%) "
        f"studies have TA assignment"
    )
    return stats


def run_normalization_pipeline(duck_conn=None):
    """Run the full condition normalization pipeline.

    Builds the condition dictionary, then creates norm.study_conditions
    via dictionary lookup. Returns coverage stats dict.
    """
    close_conn = duck_conn is None
    duck_conn = duck_conn or get_duckdb_connection()

    try:
        logger.info("Building condition dictionary...")
        build_condition_dictionary(duck_conn)
        create_study_conditions(duck_conn)
        return duck_conn
    finally:
        if close_conn:
            duck_conn.close()


if __name__ == "__main__":
    from src.logging_config import setup_logging

    setup_logging()
    run_normalization_pipeline()
