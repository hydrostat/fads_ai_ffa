# =========================================================
# export_application_model.R
# Export application-ready FADS_AI final model
# =========================================================
#
# This script rebuilds the final application model:
#   scenario = classical
#   model    = xgb
#
# using the 10K training dataset from the original experiment,
# and saves an application-ready model object under models/.
# =========================================================

library(dplyr)
library(tibble)
library(arrow)
library(jsonlite)

# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

repo_root <- "C:/Users/wilso/OneDrive/My papers/SEAF_AI/github_zenodo/fads_ai_ffa"
base_10k  <- "C:/Users/wilso/OneDrive/My papers/SEAF_AI/code/code_output_10K"

setwd(repo_root)

source(file.path(repo_root, "R", "fun_io.R"))
source(file.path(repo_root, "R", "fun_ml.R"))

train_path <- file.path(base_10k, "datasets", "train.parquet")
scenario_path <- file.path(base_10k, "datasets", "feature_scenarios.rds")
metrics_path <- file.path(base_10k, "ml_models", "validation_metrics_round1.parquet")
registry_path <- file.path(base_10k, "ml_models", "training_registry_round1.parquet")

if (!file.exists(train_path)) stop("Missing train dataset: ", train_path)
if (!file.exists(scenario_path)) stop("Missing feature scenarios: ", scenario_path)
if (!file.exists(metrics_path)) stop("Missing validation metrics: ", metrics_path)
if (!file.exists(registry_path)) stop("Missing training registry: ", registry_path)

models_dir <- file.path(repo_root, "models")
ensure_dir(models_dir)

# ---------------------------------------------------------
# Load training data and scenario
# ---------------------------------------------------------

cat("\nReading 10K training dataset...\n")
train_tbl <- arrow::read_parquet(train_path)

cat("Reading feature scenarios...\n")
feature_scenarios <- readRDS(scenario_path)

scenario_name <- "classical"
model_name <- "xgb"

feature_columns <- feature_scenarios[[scenario_name]]

if (is.null(feature_columns)) {
  stop("Scenario not found in feature_scenarios: ", scenario_name)
}

expected_features <- c("lmom_l1", "lmom_l2", "lmom_l3", "lmom_l4", "lmom_t3", "lmom_t4")

if (!identical(feature_columns, expected_features)) {
  stop(
    "Unexpected classical feature columns.\n",
    "Observed: ", paste(feature_columns, collapse = ", "), "\n",
    "Expected: ", paste(expected_features, collapse = ", ")
  )
}

missing_features <- setdiff(feature_columns, names(train_tbl))

if (length(missing_features) > 0) {
  stop("Training dataset is missing features: ", paste(missing_features, collapse = ", "))
}

if (!"true_family" %in% names(train_tbl)) {
  stop("Training dataset must contain true_family.")
}

# ---------------------------------------------------------
# Train final application model
# ---------------------------------------------------------

cat("\nTraining final application model...\n")
cat("Scenario:", scenario_name, "\n")
cat("Model   :", model_name, "\n")
cat("Features:", paste(feature_columns, collapse = ", "), "\n")

set.seed(20260324L)

fit_out <- fit_one_model(
  train_tbl = train_tbl,
  scenario_cols = feature_columns,
  model_name = model_name
)

# ---------------------------------------------------------
# Build model object
# ---------------------------------------------------------

validation_metrics <- arrow::read_parquet(metrics_path)
training_registry <- arrow::read_parquet(registry_path)

validation_row <- validation_metrics %>%
  filter(
    scenario == scenario_name,
    model == model_name,
    .metric == "accuracy",
    status == "ok"
  ) %>%
  arrange(desc(.estimate)) %>%
  slice(1)

if (nrow(validation_row) == 0) {
  warning("Could not find validation accuracy row for classical XGB.")
}

application_model <- list(
  model_name = "FADS_AI final application model",
  model_version = "1.0.0",
  scenario = scenario_name,
  algorithm = model_name,
  candidate_distributions = c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM"),
  feature_columns = feature_columns,
  workflow = fit_out$workflow,
  fit = fit_out$fit,
  training_source = list(
    experiment_size = "10K Monte Carlo replicates per configuration",
    train_file = train_path,
    feature_scenario_file = scenario_path,
    validation_metrics_file = metrics_path,
    training_registry_file = registry_path,
    seed = 20260324L
  ),
  validation_summary = if (nrow(validation_row) > 0) validation_row else NULL,
  intended_use = paste(
    "Decision support for probability-distribution selection",
    "in Flood Frequency Analysis using sample L-moment descriptors."
  ),
  limitations = paste(
    "The model was trained under controlled Monte Carlo conditions.",
    "In real applications, predictions should be interpreted as support",
    "for candidate families, not as definitive identification of the true distribution."
  ),
  created_at = as.character(Sys.time())
)

# ---------------------------------------------------------
# Save application-ready model
# ---------------------------------------------------------

model_out <- file.path(models_dir, "fads_ai_final_model.rds")
feature_out <- file.path(models_dir, "fads_ai_feature_columns.rds")
candidate_out <- file.path(models_dir, "fads_ai_candidate_distributions.csv")
metadata_out <- file.path(models_dir, "fads_ai_model_metadata.json")
dictionary_out <- file.path(models_dir, "fads_ai_feature_dictionary.csv")

saveRDS(application_model, model_out, compress = "xz")
saveRDS(feature_columns, feature_out, compress = "xz")

readr::write_csv(
  tibble(
    family = c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM"),
    description = c(
      "Generalized Extreme Value",
      "Generalized Pareto",
      "Pearson type III",
      "Two-parameter lognormal",
      "Three-parameter lognormal",
      "Gumbel"
    )
  ),
  candidate_out
)

readr::write_csv(
  tibble(
    feature_name = feature_columns,
    description = c(
      "First sample L-moment",
      "Second sample L-moment",
      "Third sample L-moment",
      "Fourth sample L-moment",
      "Sample L-skewness",
      "Sample L-kurtosis"
    ),
    feature_group = "classical_lmom",
    required_for_application = TRUE
  ),
  dictionary_out
)

metadata <- list(
  model_name = "FADS_AI final application model",
  model_version = "1.0.0",
  scenario = scenario_name,
  algorithm = model_name,
  candidate_distributions = c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM"),
  feature_columns = feature_columns,
  training_source = "10K Monte Carlo experiment",
  application_features = "sample L-moment descriptors only",
  output_type = "predicted class and class probabilities when available",
  intended_use = "Decision support for probability-distribution selection in Flood Frequency Analysis",
  limitations = "Predictions should be interpreted as model-based support for candidate families, not as proof of the true generating distribution.",
  created_at = as.character(Sys.time())
)

jsonlite::write_json(
  metadata,
  metadata_out,
  pretty = TRUE,
  auto_unbox = TRUE
)

cat("\nApplication-ready model exported successfully.\n")
cat("Model file:      ", model_out, "\n")
cat("Feature columns: ", feature_out, "\n")
cat("Metadata:        ", metadata_out, "\n")
cat("Dictionary:      ", dictionary_out, "\n")
cat("Candidates:      ", candidate_out, "\n")