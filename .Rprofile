source("renv/activate.R")

# --- R <-> Python bridge (point-only) -------------------------------------
# conda owns Python entirely via environment.yml; reticulate only LOCATES the
# interpreter so R and Python can share data in-process. Do NOT call
# renv::use_python() here -- that would hand Python package management to renv
# and fight conda. We resolve the conda env BY NAME so the path stays portable
# across machines (no hard-coded Caskroom path). See
# refs-and-resources/r-coding/managing-multilingual-projects.md, Part 6.
local({
  # CONDA_EXE is exported by `conda activate` and by any conda-launched process
  # (e.g. the Jupyter kernel, which is started from this env). Feed it to
  # reticulate so conda is found even in non-login R sessions.
  conda_exe <- Sys.getenv("CONDA_EXE", unset = NA_character_)
  if (!is.na(conda_exe) && file.exists(conda_exe))
    options(reticulate.conda_binary = conda_exe)
  py <- tryCatch(reticulate::conda_python("clinical_trials_env"),
                 error = function(e) NA_character_)
  if (!is.na(py) && file.exists(py)) Sys.setenv(RETICULATE_PYTHON = py)
})
