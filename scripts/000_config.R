# =========================================================
# 000_config.R
# Central project configuration for FADS_AI
# =========================================================
#
# This script defines the main configuration object used by
# the reproducible FADS_AI pipeline.
#
# Public repository convention:
#   Run scripts from the root of the repository:
#
#     fads_ai_ffa/
#
# The script creates generated-output folders under:
#
#     outputs/
#
# Precomputed outputs are not distributed with the repository.
# =========================================================

cat("\n[000] Starting configuration...\n")


# =========================================================
# 1. DEFINE ROOT DIRECTORY
# =========================================================

# In the public repository, the recommended workflow is to run
# scripts from the repository root. Therefore, getwd() should
# correspond to the fads_ai_ffa folder.

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

required_root_items <- c("R", "scripts", "data", "README.md")

missing_root_items <- required_root_items[
  !file.exists(file.path(root_dir, required_root_items))
]

if (length(missing_root_items) > 0) {
  stop(
    "The working directory does not appear to be the FADS_AI repository root.\n",
    "Current working directory: ", root_dir, "\n",
    "Missing expected items: ", paste(missing_root_items, collapse = ", "), "\n",
    "Please set the working directory to the root folder of the repository."
  )
}

cat("[000] Repository root detected:", root_dir, "\n")


# =========================================================
# 2. LOAD REQUIRED PACKAGES
# =========================================================

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
  "tidyselect",
  "rpart",
  "Rcpp",
  "recipes",
  "parsnip",
  "workflows"
)

missing_pkgs <- required_packages[
  !sapply(required_packages, requireNamespace, quietly = TRUE)
]

if (length(missing_pkgs) > 0) {
  stop(
    "Missing required packages: ",
    paste(missing_pkgs, collapse = ", "),
    "\nPlease install the missing packages before running the pipeline."
  )
}

cat("[000] All required packages are available.\n")


# =========================================================
# 3. DEFINE PATHS
# =========================================================

paths <- list(
  root   = root_dir,
  R      = file.path(root_dir, "R"),
  script = file.path(root_dir, "scripts"),
  input  = file.path(root_dir, "data", "input_data"),
  output = file.path(root_dir, "outputs"),
  
  # fixed input
  polygons_rds = file.path(root_dir, "data", "input_data", "poligonos_pre.rds"),
  
  # generated outputs
  config   = file.path(root_dir, "outputs", "config"),
  sim      = file.path(root_dir, "outputs", "simulated_samples"),
  features = file.path(root_dir, "outputs", "feature_tables"),
  gof      = file.path(root_dir, "outputs", "gof_tables"),
  datasets = file.path(root_dir, "outputs", "datasets"),
  models   = file.path(root_dir, "outputs", "ml_models"),
  eval     = file.path(root_dir, "outputs", "evaluation"),
  logs     = file.path(root_dir, "outputs", "run_logs"),
  paper    = file.path(root_dir, "outputs", "paper_output")
)

if (!file.exists(paths$polygons_rds)) {
  stop(
    "Required fixed input file not found: ",
    paths$polygons_rds,
    "\nThe file data/input_data/poligonos_pre.rds must be present."
  )
}


# =========================================================
# 4. CREATE GENERATED-OUTPUT DIRECTORIES
# =========================================================

output_dirs <- c(
  paths$output,
  paths$config,
  paths$sim,
  paths$features,
  paths$gof,
  paths$datasets,
  paths$models,
  paths$eval,
  paths$logs,
  paths$paper
)

for (dir_i in output_dirs) {
  dir.create(dir_i, recursive = TRUE, showWarnings = FALSE)
}

cat("[000] Generated-output directory structure verified.\n")


# =========================================================
# 5. EXPERIMENT CONFIGURATION
# =========================================================

experiment <- list(
  seed_master = 20260324L,
  families = c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM"),
  sample_sizes = c(10, 15, 20, 25, 30, 50, 60, 75, 100, 125, 150),
  n_rep_mc = 10000L
)


# =========================================================
# 6. DATA SPLIT
# =========================================================

split <- list(
  train = 0.60,
  valid = 0.20,
  test  = 0.20
)

if (abs(split$train + split$valid + split$test - 1) > 1e-8) {
  stop("Train/validation/test split proportions must sum to 1.")
}


# =========================================================
# 7. ML CONFIGURATION
# =========================================================

ml <- list(
  models = c("xgb"),
  folds = 4L,
  metric_primary = "accuracy",
  save_validation_predictions = FALSE
)


# =========================================================
# 8. FEATURE EXTRACTION CONFIGURATION
# =========================================================

features <- list(
  chunk_size = 4000L,
  progress_step = 1000L,
  keep_feature_shards = TRUE
)


# =========================================================
# 9. GOF CONFIGURATION
# =========================================================

gof <- list(
  chunk_size = 1000L,
  progress_step = 500L,
  parallel = TRUE,
  workers = 12L,
  
  combine_batch_size = 40L,
  cleanup_feature_shards_after_gof = FALSE,
  cleanup_chunks_after_combine = FALSE
)


# =========================================================
# 10. DATASET BUILD CONFIGURATION
# =========================================================

datasets <- list(
  save_full_dataset = TRUE
)


# =========================================================
# 11. GLOBAL CONFIG OBJECT
# =========================================================

config <- list(
  paths = paths,
  experiment = experiment,
  split = split,
  ml = ml,
  features = features,
  gof = gof,
  datasets = datasets,
  families = experiment$families,
  sample_sizes = experiment$sample_sizes,
  n_rep_mc = experiment$n_rep_mc
)


# =========================================================
# 12. SAVE CONFIG SNAPSHOT
# =========================================================

config_path <- file.path(paths$config, "config_main.rds")
saveRDS(config, config_path)

cat("[000] Config saved to:", config_path, "\n")


# =========================================================
# 13. SESSION INFO
# =========================================================

session_file <- file.path(
  paths$logs,
  paste0("session_info_000_", format(Sys.time(), "%Y%m%d_%H%M%S"), ".txt")
)

writeLines(capture.output(sessionInfo()), session_file)

cat("[000] Session info saved to:", session_file, "\n")


# =========================================================
# 14. QUICK SUMMARY
# =========================================================

cat("[000] Experiment summary:\n")
cat("       Families: ", paste(experiment$families, collapse = ", "), "\n", sep = "")
cat("       Sample sizes: ", paste(experiment$sample_sizes, collapse = ", "), "\n", sep = "")
cat("       n_rep_mc: ", experiment$n_rep_mc, "\n", sep = "")
cat("       Active ML models: ", paste(ml$models, collapse = ", "), "\n", sep = "")
cat("       GOF workers: ", gof$workers, "\n", sep = "")
cat("       Feature chunk_size: ", features$chunk_size, "\n", sep = "")
cat("       GOF chunk_size: ", gof$chunk_size, "\n", sep = "")
cat("       Save full dataset: ", datasets$save_full_dataset, "\n", sep = "")
cat("       Save validation predictions: ", ml$save_validation_predictions, "\n", sep = "")

cat("[000] Configuration completed successfully.\n\n")