"""Reference data provenance (Phase 7E).

Tracks versioned external reference datasets (ChEMBL, UMLS, MeSH, MeSH TA
mapping) in `meta.reference_sources`. Data files live on disk under
`data/reference/<source>/<version>/...`; this module records which version
is active and returns its path to loaders.

Exactly one row per `source_name` may have `is_active = TRUE`.
"""

import hashlib
import os
from pathlib import Path

from src.logging_config import get_logger

logger = get_logger("reference_sources")


KNOWN_SOURCES = ("chembl", "umls", "mesh", "mesh_ta_mapping")


def ensure_table(duck_conn):
    """Create meta.reference_sources if it doesn't exist."""
    duck_conn.execute("CREATE SCHEMA IF NOT EXISTS meta")
    duck_conn.execute("""
        CREATE TABLE IF NOT EXISTS meta.reference_sources (
            source_name  VARCHAR   NOT NULL,
            version      VARCHAR   NOT NULL,
            acquired_at  TIMESTAMP,
            built_at     TIMESTAMP DEFAULT current_timestamp,
            path         VARCHAR   NOT NULL,
            checksum     VARCHAR,
            is_active    BOOLEAN   NOT NULL DEFAULT FALSE,
            notes        VARCHAR,
            PRIMARY KEY (source_name, version)
        )
    """)


def compute_checksum(path, chunk_size=1 << 20):
    """sha256 of a file, or of a manifest of files if `path` is a directory.

    For directories, we hash the relative-path + size + mtime of each file
    in sorted order. Fast, stable, and good enough to detect "did this index
    change" without re-hashing multi-GB binary blobs.
    """
    p = Path(path)
    if p.is_file():
        h = hashlib.sha256()
        with open(p, "rb") as f:
            while chunk := f.read(chunk_size):
                h.update(chunk)
        return h.hexdigest()
    if p.is_dir():
        h = hashlib.sha256()
        for sub in sorted(p.rglob("*")):
            if sub.is_file():
                rel = sub.relative_to(p).as_posix()
                st = sub.stat()
                h.update(f"{rel}\0{st.st_size}\0{int(st.st_mtime)}\n".encode())
        return h.hexdigest()
    raise FileNotFoundError(path)


def register_source(
    duck_conn,
    source_name,
    version,
    path,
    notes=None,
    make_active=True,
    acquired_at=None,
    checksum=None,
):
    """Insert-or-replace a (source_name, version) row. If make_active, flips
    is_active=FALSE on all other versions of the same source first.

    `path` is stored as a project-root-relative string when possible.
    """
    ensure_table(duck_conn)

    if not os.path.exists(path):
        raise FileNotFoundError(f"reference path does not exist: {path}")
    if checksum is None:
        checksum = compute_checksum(path)

    if make_active:
        duck_conn.execute(
            "UPDATE meta.reference_sources SET is_active = FALSE WHERE source_name = ?",
            [source_name],
        )

    duck_conn.execute("""
        INSERT OR REPLACE INTO meta.reference_sources
            (source_name, version, acquired_at, path, checksum, is_active, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, [source_name, version, acquired_at, str(path), checksum, make_active, notes])

    logger.info(
        f"registered reference source: {source_name}@{version} "
        f"(active={make_active}, path={path})"
    )


def _table_exists(duck_conn):
    return duck_conn.execute("""
        SELECT COUNT(*) FROM information_schema.tables
        WHERE table_schema = 'meta' AND table_name = 'reference_sources'
    """).fetchone()[0] > 0


def get_active_path(duck_conn, source_name):
    """Return the filesystem path for the active version of `source_name`.

    Raises LookupError if no active row is registered.
    """
    if not _table_exists(duck_conn):
        raise LookupError(
            f"meta.reference_sources does not exist; "
            f"run scripts/bootstrap_reference_sources.py"
        )
    row = duck_conn.execute(
        """
        SELECT path FROM meta.reference_sources
        WHERE source_name = ? AND is_active = TRUE
        """,
        [source_name],
    ).fetchone()
    if row is None:
        raise LookupError(
            f"no active reference source registered for {source_name!r}; "
            f"run scripts/bootstrap_reference_sources.py"
        )
    return row[0]


def get_active_version(duck_conn, source_name):
    """Return the version string for the active row of `source_name`."""
    if not _table_exists(duck_conn):
        raise LookupError(f"meta.reference_sources does not exist")
    row = duck_conn.execute(
        """
        SELECT version FROM meta.reference_sources
        WHERE source_name = ? AND is_active = TRUE
        """,
        [source_name],
    ).fetchone()
    if row is None:
        raise LookupError(f"no active reference source registered for {source_name!r}")
    return row[0]


def active_versions_snapshot(duck_conn, source_names=None):
    """Return a `{source_name: version}` dict for the active row of each
    requested source. Used to stamp `source_versions` JSON on entity rows.
    """
    if not _table_exists(duck_conn):
        return {}
    sources = source_names or KNOWN_SOURCES
    rows = duck_conn.execute(
        """
        SELECT source_name, version FROM meta.reference_sources
        WHERE is_active = TRUE AND source_name IN (SELECT unnest(?))
        """,
        [list(sources)],
    ).fetchall()
    return {name: version for name, version in rows}
