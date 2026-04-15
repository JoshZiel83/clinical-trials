"""Sponsor normalization (Phase 6B).

Deterministic case-folding + legal-suffix stripping builds
`ref.sponsor_dictionary`; `norm.study_sponsors` joins raw sponsors to
the canonical names; fuzzy candidates for near-duplicates flow into
`ref.mapping_candidates` for HITL review.
"""

import re

import json

import pandas as pd

from config.settings import get_duckdb_connection
from src import entities, hitl
from src.logging_config import get_logger

logger = get_logger("normalize_sponsors")

AUTOMATED_METHODS = ("exact-after-normalize",)

# Legal / corporate suffixes, stripped iteratively from the right
# Order-insensitive; we keep looping until nothing strips.
_SUFFIX_TOKENS = (
    r"inc", r"inc\.", r"incorporated",
    r"ltd", r"ltd\.", r"limited",
    r"llc", r"l\.l\.c\.",
    r"llp", r"l\.l\.p\.",
    r"lp", r"l\.p\.",
    r"corp", r"corp\.", r"corporation",
    r"co", r"co\.", r"company",
    r"plc",
    r"ag", r"a\.g\.",
    r"sa", r"s\.a\.", r"s\.a",
    r"spa", r"s\.p\.a\.",
    r"gmbh",
    r"bv", r"b\.v\.",
    r"nv", r"n\.v\.",
    r"pty", r"pty\.",
    r"oy",
    r"kft",
    r"ab",
    r"aps",
    r"kk", r"k\.k\.",
)
_SUFFIX_RE = re.compile(
    r"[,.\s]*\b(?:" + "|".join(_SUFFIX_TOKENS) + r")\b[,.\s]*$",
    re.IGNORECASE,
)
_THE_PREFIX_RE = re.compile(r"^the\s+", re.IGNORECASE)
_WS_RE = re.compile(r"\s+")
_PUNCT_STRIP = " ,.-/:;()\"'"


def normalize_sponsor_name(name):
    """Normalize a sponsor name into a canonical-match key.

    - Strip leading 'The '
    - Iteratively strip legal suffixes (Inc, LLC, Corp, GmbH, ...)
    - Case-fold
    - Collapse whitespace
    - Strip surrounding punctuation
    """
    if name is None:
        return ""
    s = str(name).strip()
    if not s:
        return ""
    s = _THE_PREFIX_RE.sub("", s)
    # Iteratively strip legal suffixes (handles "Foo Co., Inc." → "foo")
    prev = None
    while prev != s:
        prev = s
        s = _SUFFIX_RE.sub("", s).strip(_PUNCT_STRIP)
    s = s.lower()
    s = _WS_RE.sub(" ", s).strip()
    return s


def build_sponsor_dictionary(duck_conn):
    """Build ref.sponsor_dictionary, FK'd to entities.sponsor (Phase 7B).

    Layer 1 (`exact-after-normalize`): group raw sponsor names by their
    normalized form; canonical_name is the most frequent original form
    in each group (tiebreak: alphabetical). Seeds entities.sponsor with
    origin='aact' for each distinct canonical name, then writes dict
    entries keyed on sponsor_id. Manual entries are preserved.
    """
    entities.ensure_schema(duck_conn)
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS ref")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS ref.sponsor_dictionary (
            source_name     VARCHAR PRIMARY KEY,
            sponsor_id      BIGINT NOT NULL,
            mapping_method  VARCHAR NOT NULL,
            confidence      VARCHAR NOT NULL
        )
    """)

    duck_conn.execute(
        "DELETE FROM ref.sponsor_dictionary WHERE mapping_method IN (SELECT unnest(?))",
        [list(AUTOMATED_METHODS)],
    )

    raw_counts = duck_conn.execute("""
        SELECT name, COUNT(*) AS freq
        FROM raw.sponsors
        WHERE name IS NOT NULL AND TRIM(name) <> ''
        GROUP BY name
    """).fetchdf()

    if raw_counts.empty:
        logger.info("no raw.sponsors rows; sponsor dictionary left empty")
        return 0

    raw_counts["normalized"] = raw_counts["name"].map(normalize_sponsor_name)
    raw_counts = raw_counts[raw_counts["normalized"] != ""]

    raw_counts_sorted = raw_counts.sort_values(
        ["normalized", "freq", "name"], ascending=[True, False, True]
    )
    canonicals = raw_counts_sorted.drop_duplicates("normalized", keep="first")[
        ["normalized", "name"]
    ].rename(columns={"name": "canonical_name"})

    mapped = raw_counts.merge(canonicals, on="normalized", how="left")

    manual_keys = duck_conn.execute(
        "SELECT source_name FROM ref.sponsor_dictionary WHERE mapping_method = 'manual'"
    ).fetchdf()
    manual_set = set(manual_keys["source_name"]) if not manual_keys.empty else set()

    # Seed entities.sponsor for each distinct canonical name; remember the ids.
    source_versions = json.dumps({"aact_sponsors": "raw.sponsors"})
    sponsor_id_by_canonical = {}
    for canonical in canonicals["canonical_name"].unique():
        sponsor_id_by_canonical[canonical] = entities.upsert_sponsor(
            duck_conn,
            canonical_name=canonical,
            origin="aact",
            source_versions=json.loads(source_versions),
        )

    mapped["sponsor_id"] = mapped["canonical_name"].map(sponsor_id_by_canonical)
    insert_df = pd.DataFrame({
        "source_name": mapped["name"].str.lower(),
        "sponsor_id": mapped["sponsor_id"].astype("Int64"),
        "mapping_method": "exact-after-normalize",
        "confidence": "high",
    })
    insert_df = insert_df[~insert_df["source_name"].isin(manual_set)]
    insert_df = insert_df.drop_duplicates("source_name")

    if insert_df.empty:
        return 0

    duck_conn.register("_sponsor_dict_df", insert_df)
    try:
        duck_conn.execute("""
            INSERT INTO ref.sponsor_dictionary
                (source_name, sponsor_id, mapping_method, confidence)
            SELECT source_name, sponsor_id, mapping_method, confidence
            FROM _sponsor_dict_df
        """)
    finally:
        duck_conn.unregister("_sponsor_dict_df")

    total = duck_conn.execute(
        "SELECT COUNT(*) FROM ref.sponsor_dictionary"
    ).fetchone()[0]
    distinct_entities = duck_conn.execute(
        "SELECT COUNT(*) FROM entities.sponsor"
    ).fetchone()[0]
    logger.info(
        f"ref.sponsor_dictionary: {total:,} rows, "
        f"{distinct_entities:,} entities.sponsor rows"
    )
    return total


def create_study_sponsors(duck_conn):
    """Create norm.study_sponsors by joining raw.sponsors → ref.sponsor_dictionary.

    Projects sponsor_id (FK into entities.sponsor). Canonical labels are
    looked up at view time via entities.sponsor. Unmapped rows have NULL
    sponsor_id (rare — dictionary covers every non-empty raw name).
    """
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS norm")
    duck_conn.execute("DROP TABLE IF EXISTS norm.study_sponsors")
    duck_conn.execute("""
        CREATE TABLE norm.study_sponsors AS
        SELECT
            s.nct_id,
            s.name AS original_name,
            d.sponsor_id,
            s.agency_class,
            s.lead_or_collaborator
        FROM raw.sponsors s
        LEFT JOIN ref.sponsor_dictionary d
          ON LOWER(s.name) = d.source_name
    """)
    row_count = duck_conn.execute(
        "SELECT COUNT(*) FROM norm.study_sponsors"
    ).fetchone()[0]
    mapped = duck_conn.execute(
        "SELECT COUNT(*) FROM norm.study_sponsors WHERE sponsor_id IS NOT NULL"
    ).fetchone()[0]
    logger.info(
        f"norm.study_sponsors: {row_count:,} rows, {mapped:,} mapped to entities.sponsor"
    )
    return row_count


def generate_sponsor_fuzzy_candidates(duck_conn, score_cutoff=88, top_n=2000):
    """Propose near-duplicate sponsor mergers via rapidfuzz.

    To stay tractable at scale (~38K canonicals → 1.4B pairs at full N²),
    we only compare the top `top_n` canonicals by study impact. This
    captures the high-value merges; long-tail dupes can be handled by
    the enrichment agent. Pushes non-self score ≥ cutoff matches into
    `ref.mapping_candidates` with domain='sponsor', source='fuzzy'.
    """
    from rapidfuzz import fuzz, process

    canonicals = duck_conn.execute("""
        SELECT e.canonical_name AS canonical,
               COUNT(DISTINCT s.nct_id) AS study_count
        FROM ref.sponsor_dictionary d
        JOIN entities.sponsor e ON d.sponsor_id = e.sponsor_id
        LEFT JOIN raw.sponsors s ON d.source_name = LOWER(s.name)
        WHERE d.mapping_method IN ('exact-after-normalize', 'manual')
        GROUP BY e.canonical_name
        ORDER BY study_count DESC
        LIMIT ?
    """, [top_n]).fetchdf()

    if canonicals.empty:
        logger.info("[sponsor] no canonicals; nothing to fuzzy-match")
        return 0

    names = list(canonicals["canonical"])
    counts = dict(zip(canonicals["canonical"], canonicals["study_count"]))
    logger.info(f"[sponsor] fuzzy-matching top {len(names):,} canonicals (cutoff={score_cutoff})")

    proposals = []
    seen_pairs = set()
    for name in names:
        matches = process.extract(
            name, names, scorer=fuzz.WRatio,
            limit=3, score_cutoff=score_cutoff,
        )
        for match_name, score, _ in matches:
            if match_name == name:
                continue
            # Canonical direction: smaller impact → larger impact
            small, large = sorted([name, match_name], key=lambda n: (counts[n], n))
            pair = (small, large)
            if pair in seen_pairs:
                continue
            seen_pairs.add(pair)
            proposals.append({
                "source_value": small.lower(),
                "canonical_term": large,
                "score": float(score),
                "study_count": int(counts[small]),
            })

    df = pd.DataFrame(proposals) if proposals else pd.DataFrame(
        columns=["source_value", "canonical_term", "score", "study_count"]
    )
    hitl.insert_candidates(duck_conn, "sponsor", df, source="fuzzy")
    logger.info(f"[sponsor] generated {len(df):,} fuzzy merge candidates")
    return len(df)


def run_sponsor_pipeline(duck_conn=None):
    close_conn = duck_conn is None
    duck_conn = duck_conn or get_duckdb_connection()
    try:
        build_sponsor_dictionary(duck_conn)
        create_study_sponsors(duck_conn)
        generate_sponsor_fuzzy_candidates(duck_conn)
        return duck_conn
    finally:
        if close_conn:
            duck_conn.close()


if __name__ == "__main__":
    from src.logging_config import setup_logging

    setup_logging()
    run_sponsor_pipeline()
