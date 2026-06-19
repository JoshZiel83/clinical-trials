#!/usr/bin/env bash
#
# Install the optional QuickUMLS dependency (Phase 6D/6E condition enrichment).
#
# QuickUMLS is NOT in environment.yml on purpose: it is pip-only, optional, and
# `quickumls-simstring` needs a macOS-specific dylib relink that a conda/pip
# manifest cannot express. This script installs both packages into the ACTIVE
# conda env and applies the fixup. Idempotent — safe to re-run.
#
# Prereq: `conda activate clinical_trials_env` first.
# After this, build the index once: `python -m scripts.build_quickumls_index <umls.zip>`
set -euo pipefail

if [[ -z "${CONDA_PREFIX:-}" ]]; then
  echo "error: no active conda env (CONDA_PREFIX unset). Run: conda activate clinical_trials_env" >&2
  exit 1
fi
echo "Installing QuickUMLS into: ${CONDA_PREFIX}"

python -m pip install quickumls quickumls-simstring

# macOS only: the quickumls-simstring wheel links _simstring.so against
# /usr/lib/libiconv.2.dylib, which symbol-errors on modern macOS. Repoint it at
# the conda-provided libiconv. No-op on Linux and harmless if already patched.
if [[ "$(uname -s)" == "Darwin" ]]; then
  SO_DIR="$(python -c 'import os, quickumls_simstring as q; print(os.path.dirname(q.__file__))')"
  SO_PATH="${SO_DIR}/_simstring.so"
  if [[ -f "${SO_PATH}" ]] && otool -L "${SO_PATH}" | grep -q '/usr/lib/libiconv.2.dylib'; then
    echo "Patching libiconv linkage: ${SO_PATH}"
    install_name_tool -change /usr/lib/libiconv.2.dylib \
      "${CONDA_PREFIX}/lib/libiconv.2.dylib" \
      "${SO_PATH}"
  else
    echo "libiconv linkage already correct (or _simstring.so not found) — skipping patch"
  fi
fi

# Verify the native extension actually loads (this is what the dylib bug breaks).
python -c "import simstring; from quickumls import QuickUMLS; print('QuickUMLS import OK')"
echo "Done. Next: build the index with scripts.build_quickumls_index (~5GB, one-time)."
