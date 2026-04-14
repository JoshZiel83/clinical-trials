"""One-shot: build a QuickUMLS index from the UMLS Metathesaurus full zip.

Extracts only MRCONSO.RRF + MRSTY.RRF (~2.4GB combined; saves 50GB
vs. extracting the whole archive), then invokes the QuickUMLS install
script to build the index at data/reference/umls/quickumls_index/.

Inputs:
  data/umls-YYYYxx-metathesaurus-full.zip  (UMLS release zip)

Outputs:
  data/raw/umls/2025AB/META/MRCONSO.RRF
  data/raw/umls/2025AB/META/MRSTY.RRF
  data/reference/umls/quickumls_index/

Run: python -m scripts.build_quickumls_index [path/to/zip]

This is NOT part of the recurring pipeline — run once per UMLS release.
Expect ~30-60 minutes and ~5GB of disk for the index.
"""

import subprocess
import sys
import zipfile
from pathlib import Path

from src.logging_config import get_logger, setup_logging

logger = get_logger("build_quickumls_index")

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DEFAULT_ZIP = PROJECT_ROOT / "data" / "raw" / "umls" / "metathesaurus-2025AB.zip"
EXTRACT_ROOT = PROJECT_ROOT / "data" / "raw" / "umls"
INDEX_DIR = PROJECT_ROOT / "data" / "reference" / "umls" / "quickumls_index"

REQUIRED_FILES = ("MRCONSO.RRF", "MRSTY.RRF")


def extract_required(zip_path: Path, extract_root: Path) -> Path:
    """Extract only MRCONSO.RRF and MRSTY.RRF into extract_root.

    Returns the path containing the META/ directory that QuickUMLS expects.
    """
    extract_root.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        members = zf.namelist()
        picks = [m for m in members if Path(m).name in REQUIRED_FILES]
        if len(picks) < 2:
            raise RuntimeError(
                f"zip missing required files {REQUIRED_FILES}: found {picks!r}"
            )
        logger.info(f"extracting {len(picks)} files to {extract_root}")
        for m in picks:
            target = extract_root / m
            if target.exists() and target.stat().st_size > 0:
                logger.info(f"  skip (exists): {m}")
                continue
            logger.info(f"  extract: {m}")
            zf.extract(m, extract_root)

    # Find the directory containing META/MRCONSO.RRF
    for mrconso in extract_root.rglob("MRCONSO.RRF"):
        return mrconso.parent
    raise RuntimeError("MRCONSO.RRF not found after extraction")


def build_index(meta_dir: Path, index_dir: Path) -> None:
    if index_dir.exists() and any(index_dir.iterdir()):
        logger.info(f"index already exists at {index_dir}; skipping build")
        return
    index_dir.mkdir(parents=True, exist_ok=True)
    logger.info(f"building QuickUMLS index: meta={meta_dir} → {index_dir}")
    cmd = [
        sys.executable, "-m", "quickumls.install",
        str(meta_dir), str(index_dir),
    ]
    logger.info(f"running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    logger.info("QuickUMLS index build complete")


def main():
    zip_path = Path(sys.argv[1]) if len(sys.argv) > 1 else DEFAULT_ZIP
    if not zip_path.exists():
        raise SystemExit(f"UMLS zip not found: {zip_path}")

    meta_dir = extract_required(zip_path, EXTRACT_ROOT)
    build_index(meta_dir, INDEX_DIR)
    logger.info(f"done. QUICKUMLS_INDEX_PATH={INDEX_DIR}")


if __name__ == "__main__":
    setup_logging()
    main()
