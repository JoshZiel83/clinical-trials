"""Map intervention names to canonical drug identifiers via MeSH and ChEMBL."""

import re
from pathlib import Path

from config.settings import get_duckdb_connection
from src.logging_config import get_logger

logger = get_logger("normalize_drugs")

AUTOMATED_METHODS = ("mesh-exact", "control-map", "chembl-synonym")

CHEMBL_SYNONYMS_PATH = Path(__file__).parent.parent / "data" / "reference" / "chembl_synonyms.parquet"

# Dosage patterns: numbers followed by units
DOSAGE_RE = re.compile(
    r'\b\d+[\d.,/]*\s*'
    r'(?:mg|mcg|ug|µg|ml|g|kg|iu|units?|mmol|meq|%|cc)'
    r'(?:/(?:m[l2]|kg|d(?:ay)?|h(?:our|r)?|dose|min))?\b',
    re.IGNORECASE,
)

# Route/formulation terms to strip
ROUTE_FORMULATION_RE = re.compile(
    r'\b(?:'
    r'intravenous|iv|oral|topical|subcutaneous|sc|sq|intramuscular|im|'
    r'injection|infusion|tablets?|capsules?|solution|suspension|cream|'
    r'ointment|gel|patch|spray|drops?|inhaler|inhalation|suppository|'
    r'ophthalmic|nasal|rectal|transdermal|sublingual|'
    r'extended[\s-]?release|immediate[\s-]?release|'
    r'modified[\s-]?release|sustained[\s-]?release|'
    r'controlled[\s-]?release|delayed[\s-]?release|'
    r'lyophilized|reconstituted|powder|vial|syringe|'
    r'film[\s-]?coated|enteric[\s-]?coated'
    r')\b',
    re.IGNORECASE,
)

# Canonical mapping for control/comparator terms.
# Each entry: (compiled regex on normalized name, canonical_name)
# Order matters — first match wins.
CONTROL_MAPPINGS = [
    (re.compile(p, re.IGNORECASE), canonical) for p, canonical in [
        (r'saline|normal\s+saline|isotonic\s+saline|saline\s+0\.9', "Saline"),
        (r'vehicle\s+cream', "Vehicle Cream"),
        (r'vehicle', "Vehicle"),
        (r'sham', "Sham Comparator"),
        (r'placebo', "Placebo"),
        (r'matching\s+placebo', "Placebo"),
        (r'dummy', "Placebo"),
        (r'standard\s+(of\s+)?care|usual\s+care|standard\s+care', "Standard of Care"),
        (r'best\s+supportive\s+care', "Best Supportive Care"),
        (r'no\s+(intervention|treatment)', "No Treatment"),
        (r'^control\s*group$|^control$|^active\s+comparator$|^positive\s+control$|^blank\s+control$', "Control"),
        (r'rescue\s+medication', "Rescue Medication"),
    ]
]


def normalize_drug_name(name):
    """Normalize an intervention name for dictionary matching.

    Strips dosage, route/formulation info, parenthetical content,
    and normalizes casing and whitespace.
    """
    if not name:
        return ""
    s = name.lower()
    # Remove parenthetical content
    s = re.sub(r'\([^)]*\)', '', s)
    # Remove dosage patterns
    s = DOSAGE_RE.sub('', s)
    # Remove route/formulation terms
    s = ROUTE_FORMULATION_RE.sub('', s)
    # Collapse whitespace
    s = re.sub(r'\s+', ' ', s).strip()
    # Strip leading/trailing punctuation
    s = s.strip(' ,-/:;.')
    return s


def classify_control(name):
    """Check if a normalized name is a control/comparator term.

    Returns the canonical control name (e.g., "Placebo", "Vehicle") or None.
    """
    for pattern, canonical in CONTROL_MAPPINGS:
        if pattern.search(name):
            return canonical
    return None


def is_non_drug(name):
    """Check if an intervention name is a placebo/control/non-drug term."""
    return classify_control(name) is not None


def _load_chembl_synonyms():
    """Load the ChEMBL synonym lookup from Parquet into a dict.

    Returns {lowercase_synonym: (pref_name, chembl_id)}.
    """
    import pandas as pd

    if not CHEMBL_SYNONYMS_PATH.exists():
        logger.warning(f"ChEMBL synonyms file not found: {CHEMBL_SYNONYMS_PATH}")
        return {}

    df = pd.read_parquet(CHEMBL_SYNONYMS_PATH)
    return {
        row["synonym"]: (row["pref_name"], row["chembl_id"])
        for _, row in df.iterrows()
    }


def _register_udf(duck_conn):
    """Register normalize_drug_name as a DuckDB UDF (idempotent)."""
    try:
        duck_conn.create_function("normalize_drug_name", normalize_drug_name, [str], str)
    except Exception:
        # Already registered on this connection
        pass


def build_drug_dictionary(duck_conn, skip_chembl=False):
    """Build ref.drug_dictionary from layered matching.

    Layers (in priority order):
    1. Control/comparator: regex mapping to canonical control terms (first, so
       placebo/vehicle/saline are excluded from MeSH layers)
    2. MeSH exact: normalized intervention name matches browse_interventions mesh term
    3. ChEMBL synonym: local lookup against ChEMBL synonym database

    Preserves any manual rows already in the dictionary.
    Returns the total number of dictionary entries.
    """
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS ref")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS ref.drug_dictionary (
            source_name     VARCHAR NOT NULL,
            canonical_name  VARCHAR NOT NULL,
            canonical_id    VARCHAR,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)

    # Preserve manual entries, rebuild automated layers
    # Also clean up deprecated 'mesh-cooccurrence' entries from prior runs
    methods_to_delete = list(AUTOMATED_METHODS) + ["mesh-cooccurrence"]
    duck_conn.execute("""
        DELETE FROM ref.drug_dictionary
        WHERE mapping_method IN (SELECT unnest(?))
    """, [methods_to_delete])

    # Register the normalize function for use in SQL
    _register_udf(duck_conn)

    # Layer 1: Control/comparator canonical mapping
    # Runs first so placebo/vehicle/saline are excluded from MeSH layers
    _build_control_mappings(duck_conn)

    # Layer 2: MeSH exact match
    # Match normalized intervention name against downcase_mesh_term (same study)
    duck_conn.execute("""
        INSERT INTO ref.drug_dictionary
        SELECT DISTINCT
            normalize_drug_name(i.name)  AS source_name,
            bi.mesh_term                 AS canonical_name,
            NULL                         AS canonical_id,
            'mesh-exact'                 AS mapping_method,
            'high'                       AS confidence
        FROM raw.interventions i
        INNER JOIN raw.browse_interventions bi
            ON i.nct_id = bi.nct_id
            AND normalize_drug_name(i.name) = bi.downcase_mesh_term
        WHERE i.intervention_type IN ('DRUG', 'BIOLOGICAL')
        AND bi.mesh_type = 'mesh-list'
        AND normalize_drug_name(i.name) != ''
        AND normalize_drug_name(i.name) NOT IN (
            SELECT source_name FROM ref.drug_dictionary
        )
    """)
    layer2_count = duck_conn.execute(
        "SELECT COUNT(*) FROM ref.drug_dictionary WHERE mapping_method = 'mesh-exact'"
    ).fetchone()[0]
    logger.info(f"  Layer 2 (mesh-exact): {layer2_count:,} drug mappings")

    # Layer 3: ChEMBL local synonym lookup
    if not skip_chembl:
        _build_chembl_mappings(duck_conn)
        # Backfill ChEMBL IDs for MeSH/control entries
        _backfill_chembl_ids(duck_conn)

    total = duck_conn.execute(
        "SELECT COUNT(*) FROM ref.drug_dictionary"
    ).fetchone()[0]
    logger.info(f"Drug dictionary: {total:,} total entries")
    return total


def _build_control_mappings(duck_conn):
    """Map placebo/vehicle/control intervention names to canonical terms."""
    # Get unmatched normalized drug names
    unmatched = duck_conn.execute("""
        SELECT DISTINCT normalize_drug_name(i.name) AS norm_name
        FROM raw.interventions i
        WHERE i.intervention_type IN ('DRUG', 'BIOLOGICAL')
        AND normalize_drug_name(i.name) NOT IN (
            SELECT source_name FROM ref.drug_dictionary
        )
        AND normalize_drug_name(i.name) != ''
    """).fetchall()

    import pandas as pd
    mappings = []
    for (norm_name,) in unmatched:
        canonical = classify_control(norm_name)
        if canonical:
            mappings.append((norm_name, canonical))

    if mappings:
        df = pd.DataFrame(mappings, columns=["source_name", "canonical_name"])
        df["canonical_id"] = None
        df["mapping_method"] = "control-map"
        df["confidence"] = "high"
        duck_conn.execute("""
            INSERT INTO ref.drug_dictionary
            SELECT source_name, canonical_name, canonical_id, mapping_method, confidence
            FROM df
        """)

    logger.info(f"  Layer 1 (control-map): {len(mappings):,} control/comparator mappings")


def _build_chembl_mappings(duck_conn):
    """Match unmatched drug names against the local ChEMBL synonym lookup.

    Requires data/reference/chembl_synonyms.parquet (built from ChEMBL SQLite).
    """
    import pandas as pd

    synonyms = _load_chembl_synonyms()
    if not synonyms:
        logger.warning("  Skipping ChEMBL layer — synonym file not available")
        return

    # Get unique unmatched normalized drug names
    unmatched = duck_conn.execute("""
        SELECT DISTINCT normalize_drug_name(i.name) AS norm_name
        FROM raw.interventions i
        WHERE i.intervention_type IN ('DRUG', 'BIOLOGICAL')
        AND normalize_drug_name(i.name) NOT IN (
            SELECT source_name FROM ref.drug_dictionary
        )
        AND normalize_drug_name(i.name) != ''
    """).fetchall()

    mappings = []
    for (norm_name,) in unmatched:
        if is_non_drug(norm_name) or len(norm_name) < 2:
            continue
        match = synonyms.get(norm_name)
        if match:
            pref_name, chembl_id = match
            mappings.append((norm_name, pref_name, chembl_id))

    logger.info(f"  ChEMBL local lookup: {len(unmatched):,} candidates, {len(mappings):,} matched")

    if mappings:
        df = pd.DataFrame(mappings, columns=["source_name", "canonical_name", "canonical_id"])
        df["mapping_method"] = "chembl-synonym"
        df["confidence"] = "high"
        duck_conn.execute("""
            INSERT INTO ref.drug_dictionary
            SELECT source_name, canonical_name, canonical_id, mapping_method, confidence
            FROM df
        """)


def _backfill_chembl_ids(duck_conn):
    """Backfill canonical_id for dictionary entries matched via MeSH or control mapping.

    Tries two lookups against the ChEMBL synonym table:
    1. canonical_name (the MeSH term) — works when MeSH term is a drug name
    2. source_name (the normalized intervention name) — catches cases where
       the original name is a known drug but MeSH mapped to a generic class
    """
    synonyms = _load_chembl_synonyms()
    if not synonyms:
        return

    # Get entries missing a ChEMBL ID
    missing = duck_conn.execute("""
        SELECT source_name, canonical_name
        FROM ref.drug_dictionary
        WHERE canonical_id IS NULL
    """).fetchall()

    updated = 0
    for source_name, canonical_name in missing:
        # Try canonical_name first (MeSH term), then source_name (original drug name)
        match = synonyms.get(canonical_name.lower()) or synonyms.get(source_name)
        if match:
            _, chembl_id = match
            duck_conn.execute("""
                UPDATE ref.drug_dictionary
                SET canonical_id = ?
                WHERE source_name = ? AND canonical_id IS NULL
            """, [chembl_id, source_name])
            updated += 1

    logger.info(f"  ChEMBL ID backfill: {updated:,}/{len(missing):,} entries enriched")


def create_study_drugs(duck_conn):
    """Join raw.interventions to ref.drug_dictionary, creating norm.study_drugs.

    Only includes Drug and Biological intervention types.
    Returns the number of rows created.
    """
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS norm")
    duck_conn.execute("DROP TABLE IF EXISTS norm.study_drugs")

    _register_udf(duck_conn)

    duck_conn.execute("""
        CREATE TABLE norm.study_drugs AS
        SELECT
            i.nct_id,
            i.intervention_type,
            i.name                                      AS intervention_name,
            d.canonical_name,
            d.canonical_id,
            COALESCE(d.mapping_method, 'unmatched')     AS mapping_method,
            d.confidence
        FROM raw.interventions i
        LEFT JOIN ref.drug_dictionary d
            ON normalize_drug_name(i.name) = d.source_name
        WHERE i.intervention_type IN ('DRUG', 'BIOLOGICAL')
    """)

    row_count = duck_conn.execute(
        "SELECT COUNT(*) FROM norm.study_drugs"
    ).fetchone()[0]
    matched = duck_conn.execute(
        "SELECT COUNT(*) FROM norm.study_drugs WHERE canonical_name IS NOT NULL"
    ).fetchone()[0]
    logger.info(
        f"Created norm.study_drugs: {row_count:,} rows "
        f"({matched:,} mapped, {row_count - matched:,} unmapped)"
    )
    return row_count


def get_coverage_stats(duck_conn):
    """Compute coverage statistics for drug normalization.

    Returns a dict with intervention-level and study-level stats.
    """
    total_interventions = duck_conn.execute(
        "SELECT COUNT(*) FROM norm.study_drugs"
    ).fetchone()[0]

    matched_interventions = duck_conn.execute(
        "SELECT COUNT(*) FROM norm.study_drugs WHERE canonical_name IS NOT NULL"
    ).fetchone()[0]

    total_studies = duck_conn.execute(
        "SELECT COUNT(DISTINCT nct_id) FROM norm.study_drugs"
    ).fetchone()[0]

    matched_studies = duck_conn.execute(
        "SELECT COUNT(DISTINCT nct_id) FROM norm.study_drugs WHERE canonical_name IS NOT NULL"
    ).fetchone()[0]

    method_breakdown = duck_conn.execute("""
        SELECT mapping_method, COUNT(*) as cnt
        FROM norm.study_drugs
        GROUP BY mapping_method
        ORDER BY cnt DESC
    """).fetchdf()

    dict_stats = duck_conn.execute("""
        SELECT mapping_method, confidence, COUNT(*) as cnt
        FROM ref.drug_dictionary
        GROUP BY mapping_method, confidence
        ORDER BY mapping_method, confidence
    """).fetchdf()

    int_pct = round(100 * matched_interventions / total_interventions, 1) if total_interventions > 0 else 0
    study_pct = round(100 * matched_studies / total_studies, 1) if total_studies > 0 else 0

    stats = {
        "total_interventions": total_interventions,
        "matched_interventions": matched_interventions,
        "intervention_coverage_pct": int_pct,
        "total_studies": total_studies,
        "matched_studies": matched_studies,
        "study_coverage_pct": study_pct,
        "method_breakdown": method_breakdown,
        "dictionary_stats": dict_stats,
    }

    logger.info(
        f"Drug coverage: {matched_interventions:,}/{total_interventions:,} "
        f"({int_pct}%) interventions mapped"
    )
    logger.info(
        f"Drug coverage: {matched_studies:,}/{total_studies:,} "
        f"({study_pct}%) studies have ≥1 mapped drug"
    )
    return stats


def run_normalization_pipeline(duck_conn=None, skip_chembl=False):
    """Run the full drug normalization pipeline.

    Builds the drug dictionary, then creates norm.study_drugs.
    Set skip_chembl=True to skip the ChEMBL layer (MeSH + control only).
    Returns coverage stats dict.
    """
    close_conn = duck_conn is None
    duck_conn = duck_conn or get_duckdb_connection()

    try:
        logger.info("Building drug dictionary...")
        build_drug_dictionary(duck_conn, skip_chembl=skip_chembl)
        create_study_drugs(duck_conn)
        stats = get_coverage_stats(duck_conn)
        return stats
    finally:
        if close_conn:
            duck_conn.close()


if __name__ == "__main__":
    from src.logging_config import setup_logging

    setup_logging()
    run_normalization_pipeline()
