"""Seed a standalone DuckDB for Phase 7D Shiny UI verification.

Builds `data/demo_7d.duckdb` using real canonical names + study counts drawn
from the live DB, so reviewers can eyeball the sponsor tab against realistic
data without touching production state or spending API budget.

The demo DB seeds:
  * entities.sponsor — anchors + children covering Novartis, Pfizer, BMS,
    Sanofi, J&J (including Janssen long-tail), and Merck & Co. (including
    MSD LLC + Rahway-subsidiary rows). Plus a handful of distinct
    institutions so the tab isn't monolithic.
  * meta.sponsor_anchor_set — 15 anchors matching the curation file.
  * ref.sponsor_dictionary / norm.study_sponsors — plausible row volumes
    per sponsor so study_count sorting in the UI works realistically.
  * ref.mapping_candidates — three UI states:
      - MERGE proposals (anchor_sponsor_id populated) → "Merge into anchor"
        button state. Includes realistic rationales + fabricated tool_trace
        that looks like actual agent output.
      - MAPPING proposals (anchor_sponsor_id NULL) → "Approve mapping"
        button state.
      - One already-rejected row → status styling verification.
  * A handful of condition + drug candidates so the other tabs render.

Run:
    python -m scripts.seed_phase_7d_demo

Then launch Shiny against the demo DB:
    CLINICAL_TRIALS_DB_PATH=$(pwd)/data/demo_7d.duckdb \\
    Rscript -e 'shiny::runApp("apps/review", launch.browser=TRUE)'

Teardown:
    rm data/demo_7d.duckdb
"""

from __future__ import annotations

import json
from pathlib import Path

import duckdb

from config.settings import PROJECT_ROOT
from src import entities, hitl
from src.logging_config import get_logger, setup_logging

logger = get_logger("seed_phase_7d_demo")

DEMO_DB_PATH = PROJECT_ROOT / "data" / "demo_7d.duckdb"


# ----------------------------------------------------------------------------
# Source-of-truth data for the demo. Structure:
#   ANCHORS: [(canonical_name, approx_study_count), ...]
#     — seeded as entities.sponsor + meta.sponsor_anchor_set + ref dictionary
#   MERGE_PROPOSALS: [(child_name, parent_canonical, score, rationale, tool_trace), ...]
#     — seeded as entities.sponsor children + ref.sponsor_dictionary entries
#     + a pending ref.mapping_candidates row with anchor_sponsor_id populated
#   MAPPING_PROPOSALS: [(source_value, canonical_term, score, rationale), ...]
#     — pending candidates with anchor_sponsor_id NULL (pure mapping, no merge)
#   REJECTED_ROW: single already-rejected row to prove status styling
# ----------------------------------------------------------------------------

ANCHORS = [
    ("National Cancer Institute (NCI)", 3040),
    ("Assiut University", 1331),
    ("M.D. Anderson Cancer Center", 1144),
    ("Cairo University", 1092),
    ("Assistance Publique - Hôpitaux de Paris", 981),
    ("Mayo Clinic", 963),
    ("AstraZeneca", 784),
    ("Massachusetts General Hospital", 749),
    ("Memorial Sloan Kettering Cancer Center", 745),
    ("Merck & Co., Inc.", 0),  # curated — no AACT rows point here yet
    ("Pfizer", 437),
    ("Bristol-Myers Squibb", 434),
    ("Sanofi", 258),
    ("Novartis", 118),
    ("Johnson & Johnson", 21),
    ("Merck KGaA, Darmstadt, Germany", 31),
]


MERGE_PROPOSALS = [
    # ---------- Novartis ----------
    (
        "Novartis Pharmaceuticals", "Novartis", 98.0, 408,
        "ROR confirms 'Novartis Pharmaceuticals Corporation' is the US "
        "operating subsidiary of Novartis AG (parent ror_id=02f9zrr09); "
        "sponsor_co_occurrence shows 47 shared trials between the two names.",
        [
            {"tool": "sponsor_anchor_lookup", "input": {"text": "Novartis Pharmaceuticals"},
             "result": [{"canonical_term": "Novartis", "anchor_sponsor_id": None,
                         "score": 98.0, "study_count": 118}]},
            {"tool": "sponsor_ror_api", "input": {"text": "Novartis Pharmaceuticals"},
             "result": [{"canonical_name": "Novartis Pharmaceuticals Corporation",
                         "ror_id": "0206s7b45", "country": "United States",
                         "parent": {"name": "Novartis", "ror_id": "02f9zrr09"}}]},
            {"tool": "sponsor_co_occurrence", "input": {"text": "Novartis Pharmaceuticals"},
             "result": [{"canonical_name": "Novartis", "shared_studies": 47}]},
        ],
    ),

    # ---------- Pfizer ----------
    (
        "Seagen, a wholly owned subsidiary of Pfizer", "Pfizer", 100.0, 21,
        "Name explicitly states 'wholly owned subsidiary of Pfizer'; ROR "
        "confirms acquisition completed 2023-12; Seagen ror_id=02s75xr81 "
        "lists Pfizer as parent.",
        [
            {"tool": "sponsor_anchor_lookup", "input": {"text": "Seagen, a wholly owned subsidiary of Pfizer"},
             "result": [{"canonical_term": "Pfizer", "anchor_sponsor_id": None,
                         "score": 100.0, "study_count": 437}]},
            {"tool": "sponsor_ror_api", "input": {"text": "Seagen"},
             "result": [{"canonical_name": "Seagen",
                         "ror_id": "02s75xr81", "country": "United States",
                         "parent": {"name": "Pfizer", "ror_id": "014gs0g03"}}]},
        ],
    ),
    (
        "Metsera, a wholly owned subsidiary of Pfizer", "Pfizer", 100.0, 6,
        "Name explicitly states 'wholly owned subsidiary of Pfizer'. Metsera "
        "acquired by Pfizer; no standalone ROR record yet.",
        [
            {"tool": "sponsor_anchor_lookup", "input": {"text": "Metsera, a wholly owned subsidiary of Pfizer"},
             "result": [{"canonical_term": "Pfizer", "anchor_sponsor_id": None,
                         "score": 100.0, "study_count": 437}]},
        ],
    ),

    # ---------- Bristol-Myers Squibb ----------
    (
        "Juno Therapeutics, Inc., a Bristol-Myers Squibb Company",
        "Bristol-Myers Squibb", 100.0, 16,
        "Name explicitly states 'a Bristol-Myers Squibb Company'; ROR lists "
        "BMS as parent. Acquired via Celgene merger 2019.",
        [
            {"tool": "sponsor_anchor_lookup",
             "input": {"text": "Juno Therapeutics, Inc., a Bristol-Myers Squibb Company"},
             "result": [{"canonical_term": "Bristol-Myers Squibb",
                         "anchor_sponsor_id": None, "score": 100.0, "study_count": 434}]},
            {"tool": "sponsor_ror_api", "input": {"text": "Juno Therapeutics"},
             "result": [{"canonical_name": "Juno Therapeutics", "ror_id": "05mhwyh95",
                         "parent": {"name": "Bristol-Myers Squibb", "ror_id": "00px1kj14"}}]},
        ],
    ),

    # ---------- Sanofi ----------
    (
        "Genzyme, a Sanofi Company", "Sanofi", 100.0, 23,
        "Name explicitly states 'a Sanofi Company'; ROR confirms Sanofi as "
        "parent (acquired 2011).",
        [
            {"tool": "sponsor_anchor_lookup", "input": {"text": "Genzyme, a Sanofi Company"},
             "result": [{"canonical_term": "Sanofi", "anchor_sponsor_id": None,
                         "score": 100.0, "study_count": 258}]},
            {"tool": "sponsor_ror_api", "input": {"text": "Genzyme"},
             "result": [{"canonical_name": "Genzyme", "ror_id": "04x6h4y78",
                         "parent": {"name": "Sanofi", "ror_id": "02pmzjv68"}}]},
        ],
    ),
    (
        "Sanofi Pasteur, a Sanofi Company", "Sanofi", 100.0, 17,
        "Name explicitly states 'a Sanofi Company'. Sanofi Pasteur is the "
        "vaccines division.",
        [
            {"tool": "sponsor_anchor_lookup", "input": {"text": "Sanofi Pasteur, a Sanofi Company"},
             "result": [{"canonical_term": "Sanofi", "anchor_sponsor_id": None,
                         "score": 100.0, "study_count": 258}]},
        ],
    ),

    # ---------- Johnson & Johnson (the big Janssen rollup) ----------
    (
        "Janssen Research & Development, LLC", "Johnson & Johnson", 82.0, 185,
        "Janssen is the pharmaceutical division of Johnson & Johnson. ROR "
        "confirms J&J as parent (Janssen R&D ror_id=02jndh738, parent "
        "ror_id=01phw5r37). Shared studies count with J&J: 8 (relatively low "
        "because J&J itself rarely lists as a sponsor — Janssen is the "
        "outward-facing trial sponsor).",
        [
            {"tool": "sponsor_anchor_lookup", "input": {"text": "Janssen Research & Development, LLC"},
             "result": []},  # low string similarity — anchor_lookup misses
            {"tool": "sponsor_ror_api", "input": {"text": "Janssen Research & Development"},
             "result": [{"canonical_name": "Janssen Research & Development",
                         "ror_id": "02jndh738", "country": "United States",
                         "parent": {"name": "Johnson & Johnson", "ror_id": "01phw5r37"}}]},
            {"tool": "sponsor_co_occurrence", "input": {"text": "Janssen Research & Development, LLC"},
             "result": [{"canonical_name": "Johnson & Johnson", "shared_studies": 8}]},
        ],
    ),
    (
        "Janssen Scientific Affairs, LLC", "Johnson & Johnson", 78.0, 38,
        "Medical affairs arm of J&J's Janssen division. ROR confirms J&J "
        "as parent via corporate-family lookup.",
        [
            {"tool": "sponsor_ror_api", "input": {"text": "Janssen Scientific Affairs"},
             "result": [{"canonical_name": "Janssen Scientific Affairs",
                         "ror_id": "02jndh738", "parent": {"name": "Johnson & Johnson",
                                                            "ror_id": "01phw5r37"}}]},
        ],
    ),
    (
        "Janssen, LP", "Johnson & Johnson", 80.0, 37,
        "Janssen L.P. is the US marketing/distribution LP for J&J "
        "pharmaceuticals. Same corporate family.",
        [
            {"tool": "sponsor_ror_api", "input": {"text": "Janssen LP"},
             "result": [{"canonical_name": "Janssen Pharmaceuticals",
                         "parent": {"name": "Johnson & Johnson", "ror_id": "01phw5r37"}}]},
        ],
    ),
    (
        "Janssen Pharmaceuticals", "Johnson & Johnson", 88.0, 26,
        "Generic name for the J&J pharma division. ROR parent = J&J.",
        [
            {"tool": "sponsor_ror_api", "input": {"text": "Janssen Pharmaceuticals"},
             "result": [{"canonical_name": "Janssen Pharmaceuticals",
                         "ror_id": "02jndh738", "parent": {"name": "Johnson & Johnson",
                                                            "ror_id": "01phw5r37"}}]},
        ],
    ),
    (
        "Janssen-Cilag Ltd.", "Johnson & Johnson", 75.0, 19,
        "Janssen-Cilag is J&J's European/international pharma brand (the "
        "'Cilag' suffix is a legacy Swiss acquisition). Multiple ROR records "
        "by country all list Johnson & Johnson as ultimate parent.",
        [
            {"tool": "sponsor_ror_api", "input": {"text": "Janssen-Cilag Ltd"},
             "result": [{"canonical_name": "Janssen-Cilag",
                         "parent": {"name": "Johnson & Johnson", "ror_id": "01phw5r37"}}]},
        ],
    ),
    (
        "Janssen Pharmaceutica N.V., Belgium", "Johnson & Johnson", 74.0, 13,
        "Original Belgian Janssen entity (founded by Dr. Paul Janssen, acquired "
        "by J&J 1961). Parent ultimate: Johnson & Johnson.",
        [
            {"tool": "sponsor_ror_api", "input": {"text": "Janssen Pharmaceutica N.V."},
             "result": [{"canonical_name": "Janssen Pharmaceutica",
                         "ror_id": "039c2bj29", "country": "Belgium",
                         "parent": {"name": "Johnson & Johnson", "ror_id": "01phw5r37"}}]},
        ],
    ),

    # ---------- Merck & Co., Inc. ----------
    (
        "Merck Sharp & Dohme LLC", "Merck & Co., Inc.", 85.0, 683,
        "MSD LLC is the US pharma operating subsidiary of Merck & Co., Inc. "
        "(Rahway, NJ). ROR confirms parent relationship. The 'MSD' branding "
        "is used outside North America to avoid confusion with the unrelated "
        "German Merck KGaA.",
        [
            {"tool": "sponsor_anchor_lookup", "input": {"text": "Merck Sharp & Dohme LLC"},
             "result": [{"canonical_term": "Merck & Co., Inc.",
                         "anchor_sponsor_id": None, "score": 85.0, "study_count": 0}]},
            {"tool": "sponsor_ror_api", "input": {"text": "Merck Sharp & Dohme"},
             "result": [{"canonical_name": "Merck Sharp & Dohme",
                         "ror_id": "02n7qx107", "country": "United States",
                         "parent": {"name": "Merck & Co.", "ror_id": "02vyp7w63"}}]},
        ],
    ),
    (
        "Peloton Therapeutics, Inc., a subsidiary of Merck & Co., Inc. (Rahway, New Jersey USA)",
        "Merck & Co., Inc.", 100.0, 4,
        "Name explicitly states 'a subsidiary of Merck & Co., Inc.'. Direct "
        "text match.",
        [
            {"tool": "sponsor_anchor_lookup",
             "input": {"text": "Peloton Therapeutics, Inc., a subsidiary of Merck & Co., Inc. (Rahway, New Jersey USA)"},
             "result": [{"canonical_term": "Merck & Co., Inc.",
                         "anchor_sponsor_id": None, "score": 100.0, "study_count": 0}]},
        ],
    ),
    (
        "Acceleron Pharma, Inc., a wholly-owned subsidiary of Merck & Co., Inc., Rahway, NJ USA",
        "Merck & Co., Inc.", 100.0, 3,
        "Name explicitly states 'a wholly-owned subsidiary of Merck & Co., Inc.'. "
        "Direct text match.",
        [
            {"tool": "sponsor_anchor_lookup",
             "input": {"text": "Acceleron Pharma, Inc., a wholly-owned subsidiary of Merck & Co., Inc., Rahway, NJ USA"},
             "result": [{"canonical_term": "Merck & Co., Inc.",
                         "anchor_sponsor_id": None, "score": 100.0, "study_count": 0}]},
        ],
    ),
    (
        "Merck Canada Inc.", "Merck & Co., Inc.", 92.0, 3,
        "Canadian operating arm of US Merck. ROR confirms parent relationship.",
        [
            {"tool": "sponsor_ror_api", "input": {"text": "Merck Canada Inc"},
             "result": [{"canonical_name": "Merck Canada",
                         "country": "Canada",
                         "parent": {"name": "Merck & Co.", "ror_id": "02vyp7w63"}}]},
        ],
    ),

    # ---------- Merck KGaA, Darmstadt, Germany ----------
    (
        "Merck Healthcare KGaA, Darmstadt, Germany, an affiliate of Merck KGaA, Darmstadt, Germany",
        "Merck KGaA, Darmstadt, Germany", 100.0, 19,
        "Name explicitly states 'affiliate of Merck KGaA, Darmstadt, Germany'. "
        "DISTINCT from US Merck & Co. — German Merck is legally and "
        "operationally separate (the two companies share a name by historical "
        "accident, not corporate structure).",
        [
            {"tool": "sponsor_anchor_lookup",
             "input": {"text": "Merck Healthcare KGaA, Darmstadt, Germany, an affiliate of Merck KGaA, Darmstadt, Germany"},
             "result": [{"canonical_term": "Merck KGaA, Darmstadt, Germany",
                         "anchor_sponsor_id": None, "score": 100.0, "study_count": 31}]},
        ],
    ),

    # ---------- Bristol-Myers Squibb second case ----------
    (
        "Johnson & Johnson Enterprise Innovation Inc.", "Johnson & Johnson", 95.0, 5,
        "Corporate subsidiary of Johnson & Johnson (J&J's venture arm).",
        [
            {"tool": "sponsor_anchor_lookup",
             "input": {"text": "Johnson & Johnson Enterprise Innovation Inc."},
             "result": [{"canonical_term": "Johnson & Johnson",
                         "anchor_sponsor_id": None, "score": 95.0, "study_count": 21}]},
        ],
    ),
]


# Pure mapping proposals — no anchor_sponsor_id. Button state: "Approve mapping".
MAPPING_PROPOSALS = [
    (
        "Random Hospital Sys.", "Random Hospital System", 91.0, 4,
        "Exact match after legal-suffix stripping; dictionary lookup found "
        "'Random Hospital System' with 'exact-after-normalize' method.",
        [
            {"tool": "sponsor_anchor_lookup", "input": {"text": "Random Hospital Sys."},
             "result": []},
            {"tool": "fuzzy_sponsor", "input": {"text": "Random Hospital Sys."},
             "result": [{"canonical_term": "Random Hospital System", "score": 91.0}]},
        ],
    ),
    (
        "Acme Biotech Ltd", "Acme Biotech", 93.0, 3,
        "Legal-suffix variant; fuzzy match high. Not an anchor — plain mapping "
        "into existing canonical.",
        [
            {"tool": "fuzzy_sponsor", "input": {"text": "Acme Biotech Ltd"},
             "result": [{"canonical_term": "Acme Biotech", "score": 93.0}]},
        ],
    ),
]


# An already-rejected row — verifies status-styling visual.
REJECTED_ROW = {
    "source_value": "Hunan Cancer Hospital",
    "canonical_term": "Hunan Provincial People's Hospital",
    "anchor_sponsor_id": None,
    "score": 87.0,
    "study_count": 12,
    "source": "fuzzy",
    "rationale": None,
    "tool_trace": None,
    "status": "rejected",
}


# Non-merge/non-mapping targets so the other tabs aren't empty.
CONDITION_CANDIDATES = [
    ("breast cancer nos", "Breast Neoplasms", 78.0, 15,
     "fuzzy MeSH match; QuickUMLS C0006142 score=1.0"),
    ("type ii dm", "Diabetes Mellitus, Type 2", 72.0, 8,
     "QuickUMLS C0011860 score=1.0; common abbreviation"),
]
DRUG_CANDIDATES = [
    ("asa 81mg", "Aspirin", 85.0, 20,
     "Normalized drug name 'asa' matches ChEMBL synonym; canonical=Aspirin (CHEMBL25)"),
]


# ============================================================================


def _init_schemas(conn):
    for schema in ("raw", "ref", "norm", "class", "meta", "entities", "enriched", "views"):
        conn.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    entities.ensure_schema(conn)
    hitl.ensure_candidates_table(conn)


def _init_supporting_tables(conn):
    """Create stub dictionaries / norm tables and Phase 7D anchor table."""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ref.sponsor_dictionary (
            source_name     VARCHAR PRIMARY KEY,
            sponsor_id      BIGINT NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ref.condition_dictionary (
            condition_name  VARCHAR PRIMARY KEY,
            condition_id    BIGINT NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ref.drug_dictionary (
            source_name     VARCHAR PRIMARY KEY,
            drug_id         BIGINT NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS norm.study_sponsors (
            nct_id               VARCHAR,
            original_name        VARCHAR,
            sponsor_id           BIGINT,
            agency_class         VARCHAR,
            lead_or_collaborator VARCHAR
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.sponsor_anchor_set (
            sponsor_id      BIGINT PRIMARY KEY,
            canonical_name  VARCHAR NOT NULL,
            study_count     INTEGER NOT NULL,
            origin          VARCHAR NOT NULL,
            built_at        TIMESTAMP DEFAULT current_timestamp
        )
    """)


def _seed_anchor(conn, name, study_count, origin="auto"):
    sid = entities.upsert_sponsor(conn, canonical_name=name, origin="aact")
    conn.execute(
        "INSERT INTO ref.sponsor_dictionary "
        "(source_name, sponsor_id, mapping_method, confidence) "
        "VALUES (?, ?, 'exact-after-normalize', 'high') "
        "ON CONFLICT DO NOTHING",
        [name.lower(), sid],
    )
    conn.execute(
        "INSERT INTO meta.sponsor_anchor_set "
        "(sponsor_id, canonical_name, study_count, origin) VALUES (?, ?, ?, ?) "
        "ON CONFLICT DO NOTHING",
        [sid, name, study_count, origin],
    )
    # Seed plausible norm.study_sponsors rows so study_count is reflected.
    for i in range(min(study_count, 20)):   # capped for demo size
        conn.execute(
            "INSERT INTO norm.study_sponsors VALUES (?, ?, ?, 'Industry', 'Lead')",
            [f"NCT{sid:04d}{i:03d}", name, sid],
        )
    return sid


def _seed_child(conn, name, study_count):
    sid = entities.upsert_sponsor(conn, canonical_name=name, origin="aact")
    conn.execute(
        "INSERT INTO ref.sponsor_dictionary "
        "(source_name, sponsor_id, mapping_method, confidence) "
        "VALUES (?, ?, 'exact-after-normalize', 'high') "
        "ON CONFLICT DO NOTHING",
        [name.lower(), sid],
    )
    for i in range(min(study_count, 15)):
        conn.execute(
            "INSERT INTO norm.study_sponsors VALUES (?, ?, ?, 'Industry', 'Lead')",
            [f"NCT9{sid:03d}{i:03d}", name, sid],
        )
    return sid


def seed_demo(db_path: Path = DEMO_DB_PATH) -> None:
    if db_path.exists():
        db_path.unlink()
    conn = duckdb.connect(str(db_path))
    try:
        _init_schemas(conn)
        _init_supporting_tables(conn)

        # 1. Anchors
        anchor_ids: dict[str, int] = {}
        for canonical_name, study_count in ANCHORS:
            origin = "curated_include" if canonical_name in {
                "Novartis", "Johnson & Johnson",
                "Merck & Co., Inc.", "Merck KGaA, Darmstadt, Germany"
            } else "auto"
            anchor_ids[canonical_name] = _seed_anchor(
                conn, canonical_name, study_count, origin=origin,
            )
        logger.info(f"seeded {len(anchor_ids)} anchors")

        # 2. Merge children + their pending candidates
        for (child_name, parent_name, score, study_count, rationale, tool_trace) in MERGE_PROPOSALS:
            if parent_name not in anchor_ids:
                logger.warning(f"merge proposal references unknown anchor {parent_name!r}; skipping")
                continue
            _seed_child(conn, child_name, study_count)
            conn.execute(
                """
                INSERT INTO ref.mapping_candidates
                    (domain, source_value, canonical_term, canonical_id,
                     score, study_count, source, rationale, tool_trace,
                     anchor_sponsor_id, status)
                VALUES ('sponsor', ?, ?, NULL, ?, ?, 'agent', ?, ?, ?, 'pending')
                """,
                [child_name, parent_name, score, study_count, rationale,
                 json.dumps(tool_trace), anchor_ids[parent_name]],
            )
        logger.info(f"seeded {len(MERGE_PROPOSALS)} pending sponsor merge proposals")

        # 3. Mapping-only proposals (no anchor)
        for (source_value, canonical_term, score, study_count, rationale, tool_trace) in MAPPING_PROPOSALS:
            # Ensure the "canonical" target sponsor exists so the demo can
            # show realistic data, but don't anchor it.
            entities.upsert_sponsor(conn, canonical_name=canonical_term, origin="aact")
            conn.execute(
                "INSERT INTO ref.sponsor_dictionary "
                "(source_name, sponsor_id, mapping_method, confidence) "
                "SELECT ?, sponsor_id, 'exact-after-normalize', 'high' "
                "FROM entities.sponsor WHERE canonical_name = ? "
                "ON CONFLICT DO NOTHING",
                [canonical_term.lower(), canonical_term],
            )
            conn.execute(
                """
                INSERT INTO ref.mapping_candidates
                    (domain, source_value, canonical_term, canonical_id,
                     score, study_count, source, rationale, tool_trace,
                     anchor_sponsor_id, status)
                VALUES ('sponsor', ?, ?, NULL, ?, ?, 'fuzzy', ?, ?, NULL, 'pending')
                """,
                [source_value, canonical_term, score, study_count, rationale,
                 json.dumps(tool_trace)],
            )
        logger.info(f"seeded {len(MAPPING_PROPOSALS)} pending sponsor mapping proposals")

        # 4. Rejected row
        conn.execute(
            """
            INSERT INTO ref.mapping_candidates
                (domain, source_value, canonical_term, canonical_id,
                 score, study_count, source, rationale, tool_trace,
                 anchor_sponsor_id, status)
            VALUES ('sponsor', ?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
            """,
            [REJECTED_ROW["source_value"], REJECTED_ROW["canonical_term"],
             REJECTED_ROW["score"], REJECTED_ROW["study_count"],
             REJECTED_ROW["source"], REJECTED_ROW["rationale"],
             REJECTED_ROW["tool_trace"], REJECTED_ROW["anchor_sponsor_id"],
             REJECTED_ROW["status"]],
        )
        logger.info("seeded 1 rejected sponsor row")

        # 5. A few non-sponsor candidates so other tabs render
        for (source_value, canonical_term, score, study_count, rationale) in CONDITION_CANDIDATES:
            conn.execute(
                """
                INSERT INTO ref.mapping_candidates
                    (domain, source_value, canonical_term, canonical_id,
                     score, study_count, source, rationale, tool_trace,
                     anchor_sponsor_id, status)
                VALUES ('condition', ?, ?, NULL, ?, ?, 'agent', ?, NULL, NULL, 'pending')
                """,
                [source_value, canonical_term, score, study_count, rationale],
            )
        for (source_value, canonical_term, score, study_count, rationale) in DRUG_CANDIDATES:
            conn.execute(
                """
                INSERT INTO ref.mapping_candidates
                    (domain, source_value, canonical_term, canonical_id,
                     score, study_count, source, rationale, tool_trace,
                     anchor_sponsor_id, status)
                VALUES ('drug', ?, ?, NULL, ?, ?, 'agent', ?, NULL, NULL, 'pending')
                """,
                [source_value, canonical_term, score, study_count, rationale],
            )

        # Final report
        counts = conn.execute("""
            SELECT domain, status, COUNT(*)
            FROM ref.mapping_candidates
            GROUP BY domain, status
            ORDER BY domain, status
        """).fetchall()
        logger.info("demo ref.mapping_candidates rows:")
        for dom, st, n in counts:
            logger.info(f"  {dom:10} {st:10} {n}")

        logger.info(f"demo DB written to {db_path}")
    finally:
        conn.close()


if __name__ == "__main__":
    setup_logging()
    seed_demo()
