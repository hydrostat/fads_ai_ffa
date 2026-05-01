# =========================================================
# check_repository_integrity.R
# Basic integrity checks for the public FADS_AI repository
# =========================================================
#
# Run from the repository root:
#
#   source("tests/check_repository_integrity.R")
#
# This script does not run the full Monte Carlo pipeline.
# It checks:
#   - expected directory structure;
#   - required files;
#   - R syntax of scripts and helper functions;
#   - presence of fixed input data;
#   - key documentation files.
# =========================================================

setwd("C:/Users/wilso/OneDrive/My papers/SEAF_AI/github_zenodo/fads_ai_ffa")

# ---------------------------------------------------------
# Repository root
# ---------------------------------------------------------

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

required_root_items <- c(
  "R",
  "scripts",
  "data",
  "data/input_data",
  "tests",
  "README.md",
  "LICENSE",
  "CITATION.cff",
  "data-availability.md",
  "reproducibility.md"
)

missing_root_items <- required_root_items[
  !file.exists(file.path(root_dir, required_root_items))
]

if (length(missing_root_items) > 0) {
  stop(
    "Repository integrity check failed. Missing root items: ",
    paste(missing_root_items, collapse = ", ")
  )
}

cat("[OK] Repository structure exists.\n")


# ---------------------------------------------------------
# Required helper functions
# ---------------------------------------------------------

required_R_files <- c(
  "fun_bayes.R",
  "fun_distributions.R",
  "fun_gof.R",
  "fun_gof_chunk.R",
  "fun_io.R",
  "fun_lmom.R",
  "fun_lmrd.R",
  "fun_lmrd_preprocess.R",
  "fun_ml.R",
  "fun_pdf.R"
)

missing_R_files <- required_R_files[
  !file.exists(file.path(root_dir, "R", required_R_files))
]

if (length(missing_R_files) > 0) {
  stop(
    "Missing required R helper files: ",
    paste(missing_R_files, collapse = ", ")
  )
}

cat("[OK] Required R helper files exist.\n")


# ---------------------------------------------------------
# Required pipeline scripts
# ---------------------------------------------------------

required_script_files <- c(
  "000_config.R",
  "001_create_project_structure.R",
  "010_simulate_samples.R",
  "020_extract_features.R",
  "021_extract_gof_features.R",
  "022_combine_gof_chunks.R",
  "030_build_datasets.R",
  "040_train_models.R",
  "050_evaluate_models.R",
  "900_run_full_pipeline.R"
)

missing_script_files <- required_script_files[
  !file.exists(file.path(root_dir, "scripts", required_script_files))
]

if (length(missing_script_files) > 0) {
  stop(
    "Missing required pipeline scripts: ",
    paste(missing_script_files, collapse = ", ")
  )
}

cat("[OK] Required pipeline scripts exist.\n")


# ---------------------------------------------------------
# Fixed input file
# ---------------------------------------------------------

polygon_path <- file.path(root_dir, "data", "input_data", "poligonos_pre.rds")

if (!file.exists(polygon_path)) {
  stop("Required fixed input file missing: ", polygon_path)
}

polygon_obj <- tryCatch(
  readRDS(polygon_path),
  error = function(e) e
)

if (inherits(polygon_obj, "error")) {
  stop("Could not read poligonos_pre.rds: ", conditionMessage(polygon_obj))
}

if (!is.list(polygon_obj)) {
  stop("poligonos_pre.rds must contain a list object.")
}

if (!all(c("polygons", "tol") %in% names(polygon_obj))) {
  stop("poligonos_pre.rds must contain elements 'polygons' and 'tol'.")
}

cat("[OK] Fixed LMRD input file exists and has expected structure.\n")


# ---------------------------------------------------------
# Syntax check for all R files
# ---------------------------------------------------------

r_files <- sort(unique(c(
  list.files(file.path(root_dir, "R"), pattern = "\\.R$", full.names = TRUE),
  list.files(file.path(root_dir, "scripts"), pattern = "\\.R$", recursive = TRUE, full.names = TRUE),
  list.files(file.path(root_dir, "paper_outputs"), pattern = "\\.R$", recursive = TRUE, full.names = TRUE),
  list.files(file.path(root_dir, "examples"), pattern = "\\.R$", recursive = TRUE, full.names = TRUE),
  list.files(file.path(root_dir, "tests"), pattern = "\\.R$", recursive = TRUE, full.names = TRUE)
)))

syntax_results <- lapply(r_files, function(path) {
  out <- tryCatch(
    {
      parse(path)
      data.frame(
        file = normalizePath(path, winslash = "/", mustWork = FALSE),
        syntax_ok = TRUE,
        error = NA_character_,
        stringsAsFactors = FALSE
      )
    },
    error = function(e) {
      data.frame(
        file = normalizePath(path, winslash = "/", mustWork = FALSE),
        syntax_ok = FALSE,
        error = conditionMessage(e),
        stringsAsFactors = FALSE
      )
    }
  )
  
  out
})

syntax_tbl <- do.call(rbind, syntax_results)

out_dir <- file.path(root_dir, "tests", "integrity_check")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

write.csv(
  syntax_tbl,
  file.path(out_dir, "syntax_check_results.csv"),
  row.names = FALSE
)

if (!all(syntax_tbl$syntax_ok)) {
  print(syntax_tbl[!syntax_tbl$syntax_ok, ])
  stop("Syntax check failed. See tests/integrity_check/syntax_check_results.csv")
}

cat("[OK] All R files passed syntax check.\n")


# ---------------------------------------------------------
# Package availability check
# ---------------------------------------------------------

required_packages <- c(
  "data.table",
  "dplyr",
  "purrr",
  "readr",
  "arrow",
  "future",
  "future.apply",
  "yardstick",
  "nnet",
  "ranger",
  "xgboost",
  "kernlab",
  "goftest",
  "lmom",
  "tidyr",
  "tibble",
  "rpart",
  "Rcpp",
  "recipes",
  "parsnip",
  "workflows",
  "tidymodels"
)

package_tbl <- data.frame(
  package = required_packages,
  installed = vapply(required_packages, requireNamespace, logical(1), quietly = TRUE),
  stringsAsFactors = FALSE
)

write.csv(
  package_tbl,
  file.path(out_dir, "package_check_results.csv"),
  row.names = FALSE
)

if (!all(package_tbl$installed)) {
  cat("\nMissing packages:\n")
  print(package_tbl[!package_tbl$installed, ])
  warning("Some required packages are not installed.")
} else {
  cat("[OK] All required packages are installed.\n")
}


# ---------------------------------------------------------
# Final message
# ---------------------------------------------------------

cat("\nRepository integrity check completed.\n")
cat("Results written to:\n")
cat(out_dir, "\n")