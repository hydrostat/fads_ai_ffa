# =========================================================
# 001_create_project_structure.R
# Create and verify FADS_AI public repository structure
# =========================================================
#
# This script verifies the public repository structure and
# creates generated-output folders under outputs/.
#
# Public repository convention:
#   Run this script from the repository root:
#
#     fads_ai_ffa/
#
# Precomputed outputs are not distributed. Generated files
# are written under outputs/.
# =========================================================

cat("\n[001] Creating/verifying FADS_AI project structure...\n")


# ---------------------------------------------------------
# Helper
# ---------------------------------------------------------

create_dir <- function(path) {
  if (!dir.exists(path)) {
    ok <- dir.create(path, recursive = TRUE, showWarnings = FALSE)
    
    if (!ok && !dir.exists(path)) {
      stop("[001] Could not create directory: ", path)
    }
    
    cat("[001] Created:", path, "\n")
  } else {
    cat("[001] Exists :", path, "\n")
  }
  
  invisible(normalizePath(path, winslash = "/", mustWork = FALSE))
}


# ---------------------------------------------------------
# Repository root
# ---------------------------------------------------------

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

required_root_items <- c("R", "scripts", "data", "README.md")

missing_root_items <- required_root_items[
  !file.exists(file.path(root_dir, required_root_items))
]

if (length(missing_root_items) > 0) {
  stop(
    "[001] The working directory does not appear to be the FADS_AI repository root.\n",
    "Current working directory: ", root_dir, "\n",
    "Missing expected items: ", paste(missing_root_items, collapse = ", "), "\n",
    "Please set the working directory to the root folder of the repository."
  )
}

cat("[001] Repository root detected:", root_dir, "\n")


# ---------------------------------------------------------
# Fixed public repository directories
# ---------------------------------------------------------

static_dirs <- c(
  file.path(root_dir, "R"),
  file.path(root_dir, "scripts"),
  file.path(root_dir, "scripts", "supplementary_analysis"),
  file.path(root_dir, "data"),
  file.path(root_dir, "data", "input_data"),
  file.path(root_dir, "data", "sample_inputs"),
  file.path(root_dir, "examples"),
  file.path(root_dir, "models"),
  file.path(root_dir, "paper_outputs"),
  file.path(root_dir, "paper_outputs", "tables"),
  file.path(root_dir, "paper_outputs", "tables", "scripts"),
  file.path(root_dir, "paper_outputs", "figures"),
  file.path(root_dir, "paper_outputs", "figures", "scripts"),
  file.path(root_dir, "paper_outputs", "quantile_consequence"),
  file.path(root_dir, "paper_outputs", "quantile_consequence", "scripts"),
  file.path(root_dir, "tests")
)

invisible(lapply(static_dirs, create_dir))


# ---------------------------------------------------------
# Generated-output directories
# ---------------------------------------------------------

output_dirs <- c(
  file.path(root_dir, "outputs"),
  file.path(root_dir, "outputs", "config"),
  file.path(root_dir, "outputs", "simulated_samples"),
  file.path(root_dir, "outputs", "feature_tables"),
  file.path(root_dir, "outputs", "gof_tables"),
  file.path(root_dir, "outputs", "datasets"),
  file.path(root_dir, "outputs", "ml_models"),
  file.path(root_dir, "outputs", "evaluation"),
  file.path(root_dir, "outputs", "run_logs"),
  file.path(root_dir, "outputs", "paper_output")
)

invisible(lapply(output_dirs, create_dir))


# ---------------------------------------------------------
# Check fixed input file
# ---------------------------------------------------------

polygons_path <- file.path(root_dir, "data", "input_data", "poligonos_pre.rds")

if (!file.exists(polygons_path)) {
  stop(
    "[001] Required fixed input file not found: ",
    polygons_path,
    "\nThe file poligonos_pre.rds must be present before running the pipeline."
  )
}

cat("[001] Fixed input found:", polygons_path, "\n")


# ---------------------------------------------------------
# Write session info for this step
# ---------------------------------------------------------

session_dir <- file.path(root_dir, "outputs", "run_logs")
create_dir(session_dir)

session_file <- file.path(
  session_dir,
  paste0("session_info_001_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".txt")
)

writeLines(capture.output(sessionInfo()), session_file)

cat("[001] Session info saved to:", session_file, "\n")

cat("[001] Directory structure ready.\n\n")