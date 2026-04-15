"""Map intervention names to canonical drug identifiers via MeSH and ChEMBL."""

import re
from pathlib import Path

from config.settings import get_duckdb_connection
from src import entities, reference_sources
from src.logging_config import get_logger

logger = get_logger("normalize_drugs")

AUTOMATED_METHODS = ("mesh-exact", "control-map", "chembl-synonym")


def _chembl_synonyms_path(duck_conn) -> Path:
    """Resolve the active ChEMBL synonyms parquet path via meta.reference_sources."""
    return Path(reference_sources.get_active_path(duck_conn, "chembl"))

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


def _load_chembl_synonyms(duck_conn):
    """Load the ChEMBL synonym lookup from Parquet into a dict.

    Returns {lowercase_synonym: (pref_name, chembl_id)}.
    """
    import pandas as pd

    try:
        path = _chembl_synonyms_path(duck_conn)
    except LookupError as e:
        logger.warning(f"ChEMBL synonyms not registered: {e}")
        return {}
    if not path.exists():
        logger.warning(f"ChEMBL synonyms file not found: {path}")
        return {}

    df = pd.read_parquet(path)
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
    """Build ref.drug_dictionary, FK'd to entities.drug (Phase 7B).

    Layers (in priority order):
    1. Control/comparator: regex mapping to canonical control terms (first, so
       placebo/vehicle/saline are excluded from MeSH layers)
    2. MeSH exact: normalized intervention name matches browse_interventions mesh term
    3. ChEMBL synonym: local lookup against ChEMBL synonym database

    Writes each layer to a staging table, then resolves every
    (canonical_name, canonical_id) pair against entities.drug (creating rows
    as needed via entities.upsert_drug) and inserts the final dictionary rows
    as (source_name, drug_id, mapping_method, confidence).

    Preserves any manual entries already in ref.drug_dictionary.
    Returns the total number of dictionary entries.
    """
    entities.ensure_schema(duck_conn)

    # Seed ChEMBL entities once so the mesh-exact/control layers can resolve
    # canonical_name collisions against them (e.g., MeSH term "Aspirin" should
    # reuse the ChEMBL drug_id, not create a duplicate entity).
    if not skip_chembl:
        entities.seed_drugs_from_chembl(duck_conn)

    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS ref")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS ref.drug_dictionary (
            source_name     VARCHAR PRIMARY KEY,
            drug_id         BIGINT NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)

    # Staging table — layers write raw matches here with canonical strings;
    # resolve step transforms to (source_name, drug_id).
    duck_conn.execute("DROP TABLE IF EXISTS ref._drug_dictionary_staging")
    duck_conn.execute("""
        CREATE TABLE ref._drug_dictionary_staging (
            source_name     VARCHAR NOT NULL,
            canonical_name  VARCHAR NOT NULL,
            canonical_id    VARCHAR,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)

    # Preserve manual entries; rebuild automated layers.
    methods_to_delete = list(AUTOMATED_METHODS) + ["mesh-cooccurrence"]
    duck_conn.execute("""
        DELETE FROM ref.drug_dictionary
        WHERE mapping_method IN (SELECT unnest(?))
    """, [methods_to_delete])

    _register_udf(duck_conn)

    # Layer 1: Control/comparator
    _build_control_mappings(duck_conn)

    # Layer 2: MeSH exact match
    duck_conn.execute("""
        INSERT INTO ref._drug_dictionary_staging
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
            SELECT source_name FROM ref._drug_dictionary_staging
        )
        AND normalize_drug_name(i.name) NOT IN (
            SELECT source_name FROM ref.drug_dictionary
        )
    """)
    layer2_count = duck_conn.execute(
        "SELECT COUNT(*) FROM ref._drug_dictionary_staging WHERE mapping_method = 'mesh-exact'"
    ).fetchone()[0]
    logger.info(f"  Layer 2 (mesh-exact): {layer2_count:,} drug mappings")

    # Layer 3: ChEMBL local synonym lookup
    if not skip_chembl:
        _build_chembl_mappings(duck_conn)

    # Resolve canonical strings to drug_ids and emit final dictionary rows.
    _resolve_drug_entities(duck_conn)

    duck_conn.execute("DROP TABLE ref._drug_dictionary_staging")

    total = duck_conn.execute(
        "SELECT COUNT(*) FROM ref.drug_dictionary"
    ).fetchone()[0]
    logger.info(f"Drug dictionary: {total:,} total entries")
    return total


def _resolve_drug_entities(duck_conn):
    """Join staging → entities.drug (creating rows as needed), emit dictionary.

    Dedups on source_name — within a layer the same normalized name can map
    to multiple canonicals (e.g. mesh-exact where one study has multiple
    matching browse_interventions). First-writer-wins on insertion order.
    """
    staging = duck_conn.execute("""
        WITH ordered AS (
            SELECT source_name, canonical_name, canonical_id, mapping_method, confidence,
                   ROW_NUMBER() OVER (PARTITION BY source_name ORDER BY rowid) AS rn
            FROM ref._drug_dictionary_staging
        )
        SELECT source_name, canonical_name, canonical_id, mapping_method, confidence
        FROM ordered WHERE rn = 1
    """).fetchall()

    inserts = []
    for source_name, canonical_name, canonical_id, mapping_method, confidence in staging:
        origin = {
            "chembl-synonym": "chembl",
            "mesh-exact": "mesh",
            "control-map": "manual",
        }.get(mapping_method, "manual")
        drug_id = entities.upsert_drug(
            duck_conn,
            canonical_name=canonical_name,
            origin=origin,
            chembl_id=canonical_id,
        )
        inserts.append((source_name, drug_id, mapping_method, confidence))

    if inserts:
        duck_conn.executemany(
            """
            INSERT INTO ref.drug_dictionary (source_name, drug_id, mapping_method, confidence)
            VALUES (?, ?, ?, ?)
            """,
            inserts,
        )
    logger.info(f"  resolved {len(inserts):,} dictionary entries to entities.drug")


def _build_control_mappings(duck_conn):
    """Map placebo/vehicle/control intervention names to canonical terms."""
    unmatched = duck_conn.execute("""
        SELECT DISTINCT normalize_drug_name(i.name) AS norm_name
        FROM raw.interventions i
        WHERE i.intervention_type IN ('DRUG', 'BIOLOGICAL')
        AND normalize_drug_name(i.name) NOT IN (
            SELECT source_name FROM ref.drug_dictionary
        )
        AND normalize_drug_name(i.name) NOT IN (
            SELECT source_name FROM ref._drug_dictionary_staging
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
            INSERT INTO ref._drug_dictionary_staging
            SELECT source_name, canonical_name, canonical_id, mapping_method, confidence
            FROM df
        """)

    logger.info(f"  Layer 1 (control-map): {len(mappings):,} control/comparator mappings")


def _build_chembl_mappings(duck_conn):
    """Match unmatched drug names against the local ChEMBL synonym lookup.

    Writes to ref._drug_dictionary_staging. The backfill step that existed
    in the previous schema is no longer needed — entities.drug already carries
    authoritative chembl_ids for seeded drugs, so resolving canonical_name
    collisions in _resolve_drug_entities() naturally picks up the correct id.
    """
    import pandas as pd

    synonyms = _load_chembl_synonyms(duck_conn)
    if not synonyms:
        logger.warning("  Skipping ChEMBL layer — synonym file not available")
        return

    unmatched = duck_conn.execute("""
        SELECT DISTINCT normalize_drug_name(i.name) AS norm_name
        FROM raw.interventions i
        WHERE i.intervention_type IN ('DRUG', 'BIOLOGICAL')
        AND normalize_drug_name(i.name) NOT IN (
            SELECT source_name FROM ref.drug_dictionary
        )
        AND normalize_drug_name(i.name) NOT IN (
            SELECT source_name FROM ref._drug_dictionary_staging
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
            INSERT INTO ref._drug_dictionary_staging
            SELECT source_name, canonical_name, canonical_id, mapping_method, confidence
            FROM df
        """)


def create_study_drugs(duck_conn):
    """Join raw.interventions to ref.drug_dictionary → norm.study_drugs.

    Only includes Drug and Biological intervention types. The new schema
    projects drug_id (FK into entities.drug) — labels/chembl_ids are looked
    up at view time via joins through entities.drug.
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
            d.drug_id,
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
        "SELECT COUNT(*) FROM norm.study_drugs WHERE drug_id IS NOT NULL"
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
        "SELECT COUNT(*) FROM norm.study_drugs WHERE drug_id IS NOT NULL"
    ).fetchone()[0]

    total_studies = duck_conn.execute(
        "SELECT COUNT(DISTINCT nct_id) FROM norm.study_drugs"
    ).fetchone()[0]

    matched_studies = duck_conn.execute(
        "SELECT COUNT(DISTINCT nct_id) FROM norm.study_drugs WHERE drug_id IS NOT NULL"
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


def generate_drug_fuzzy_candidates(duck_conn, score_cutoff=88, top_n=2000):
    """Propose canonical mappings for unmatched drug interventions.

    Pulls top-impact unmatched intervention names (by study count),
    normalizes them via `normalize_drug_name`, scores rapidfuzz WRatio
    against (a) ChEMBL synonyms and (b) MeSH intervention terms. Writes
    best proposals (score ≥ cutoff) into `ref.mapping_candidates` with
    domain='drug', source='fuzzy'.

    `top_n` caps the candidate set to keep the N×M score calc tractable.
    """
    import pandas as pd
    from rapidfuzz import fuzz, process

    from src import hitl

    unmatched = duck_conn.execute("""
        SELECT intervention_name AS raw_name,
               COUNT(DISTINCT nct_id) AS study_count
        FROM norm.study_drugs
        WHERE mapping_method = 'unmatched'
        GROUP BY intervention_name
        ORDER BY study_count DESC
        LIMIT ?
    """, [top_n]).fetchdf()

    if unmatched.empty:
        logger.info("[drug] no unmatched interventions; nothing to fuzzy-match")
        hitl.insert_candidates(
            duck_conn, "drug",
            pd.DataFrame(columns=["source_value", "canonical_term", "score", "study_count"]),
            source="fuzzy",
        )
        return 0

    # Build match targets from ChEMBL synonyms + MeSH intervention terms
    targets = {}  # normalized_target -> (canonical_name, canonical_id)
    try:
        chembl_path = _chembl_synonyms_path(duck_conn)
    except LookupError:
        chembl_path = None
    if chembl_path is not None and chembl_path.exists():
        chembl = pd.read_parquet(chembl_path).dropna(subset=["synonym", "pref_name"])
        for _, r in chembl.iterrows():
            syn = str(r["synonym"]).lower().strip()
            pref = str(r["pref_name"]).strip()
            # Skip junk synonyms that produce false positives: too short,
            # purely numeric, or single-token punctuation.
            if len(syn) < 4 or syn.isdigit() or syn.replace("-", "").isdigit():
                continue
            if syn and pref and syn not in targets:
                targets[syn] = (pref, r["chembl_id"])
    mesh = duck_conn.execute("""
        SELECT DISTINCT downcase_mesh_term AS term
        FROM raw.browse_interventions
        WHERE mesh_type = 'mesh-list'
    """).fetchdf()
    for term in mesh["term"]:
        t = str(term).strip()
        if t and t not in targets:
            targets[t] = (t, None)

    target_list = list(targets.keys())
    logger.info(
        f"[drug] fuzzy-matching {len(unmatched):,} unmatched names "
        f"against {len(target_list):,} targets (cutoff={score_cutoff})"
    )

    proposals = []
    for _, row in unmatched.iterrows():
        raw_name = row["raw_name"]
        study_count = int(row["study_count"])
        normalized = normalize_drug_name(raw_name)
        if not normalized or len(normalized) < 3:
            continue
        result = process.extractOne(
            normalized, target_list, scorer=fuzz.WRatio, score_cutoff=score_cutoff
        )
        if result is None:
            continue
        match_key, score, _ = result
        canonical_name, canonical_id = targets[match_key]
        proposals.append({
            "source_value": normalized,
            "canonical_term": canonical_name,
            "canonical_id": canonical_id,
            "score": float(score),
            "study_count": study_count,
        })

    df = pd.DataFrame(proposals) if proposals else pd.DataFrame(
        columns=["source_value", "canonical_term", "canonical_id", "score", "study_count"]
    )
    # Collapse duplicates (multiple raw names may normalize to the same source_value
    # and point at the same canonical — keep the highest-impact row).
    if not df.empty:
        df = (
            df.sort_values(["source_value", "canonical_term", "study_count"],
                           ascending=[True, True, False])
              .drop_duplicates(subset=["source_value", "canonical_term"], keep="first")
              .reset_index(drop=True)
        )
    hitl.insert_candidates(duck_conn, "drug", df, source="fuzzy")
    logger.info(f"[drug] generated {len(df):,} fuzzy candidates")
    return len(df)


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
        generate_drug_fuzzy_candidates(duck_conn)
        stats = get_coverage_stats(duck_conn)
        return stats
    finally:
        if close_conn:
            duck_conn.close()


if __name__ == "__main__":
    from src.logging_config import setup_logging

    setup_logging()
    run_normalization_pipeline()
