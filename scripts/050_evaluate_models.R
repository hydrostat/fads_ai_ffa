# =========================================================
# 050_evaluate_models.R
# Evaluate validation-selected models on the held-out test set
# =========================================================
#
# This script:
#   - reads training and test datasets;
#   - reads validation metrics and training registry;
#   - selects the best model overall and the best model within
#     each feature scenario based only on validation performance;
#   - rebuilds models in memory when minimal model markers are
#     detected;
#   - evaluates selected models on the held-out test set;
#   - saves final test metrics, predictions, and summaries.
#
# Important:
#   The test set is not used for model selection. It is used only
#   for final evaluation of validation-selected models.
#
# Required previous steps:
#   scripts/000_config.R
#   scripts/030_build_datasets.R
#   scripts/040_train_models.R
# =========================================================

library(dplyr)
library(tibble)
library(arrow)
library(yardstick)
library(readr)


# ---------------------------------------------------------
# Repository root check
# ---------------------------------------------------------

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

config_path <- file.path(root_dir, "outputs", "config", "config_main.rds")

if (!file.exists(file.path(root_dir, "R")) ||
    !file.exists(file.path(root_dir, "scripts")) ||
    !file.exists(config_path)) {
  stop(
    "[050] This script must be run from the FADS_AI repository root ",
    "after scripts/000_config.R has been executed.\n",
    "Current working directory: ", root_dir
  )
}


# ---------------------------------------------------------
# Load helper functions and configuration
# ---------------------------------------------------------

source(file.path(root_dir, "R", "fun_io.R"))
source(file.path(root_dir, "R", "fun_ml.R"))

config <- read_rds(config_path)
paths  <- config$paths


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

dataset_dir <- paths$datasets
model_dir   <- paths$models
eval_dir    <- paths$eval

ensure_dir(eval_dir)

overall_shard_dir  <- file.path(eval_dir, "shards_best_overall")
scenario_shard_dir <- file.path(eval_dir, "shards_best_by_scenario")

ensure_dir(overall_shard_dir)
ensure_dir(scenario_shard_dir)


# ---------------------------------------------------------
# Required inputs
# ---------------------------------------------------------

train_path <- file.path(dataset_dir, "train.parquet")
test_path  <- file.path(dataset_dir, "test.parquet")

metrics_path  <- file.path(model_dir, "validation_metrics_round1.parquet")
registry_path <- file.path(model_dir, "training_registry_round1.parquet")

if (!file.exists(train_path)) {
  stop("[050] Training dataset not found: ", train_path, "\nRun scripts/030_build_datasets.R first.")
}

if (!file.exists(test_path)) {
  stop("[050] Test dataset not found: ", test_path, "\nRun scripts/030_build_datasets.R first.")
}

if (!file.exists(metrics_path)) {
  stop("[050] Validation metrics not found: ", metrics_path, "\nRun scripts/040_train_models.R first.")
}

if (!file.exists(registry_path)) {
  stop("[050] Training registry not found: ", registry_path, "\nRun scripts/040_train_models.R first.")
}


# ---------------------------------------------------------
# Helper paths
# ---------------------------------------------------------

shard_path <- function(base_dir, prefix, scenario, model) {
  file.path(
    base_dir,
    paste0(prefix, "__", scenario, "__", model, ".parquet")
  )
}

model_file_path <- function(scenario, model, fitted_dir) {
  file.path(fitted_dir, paste0("model_", scenario, "__", model, ".rds"))
}


# ---------------------------------------------------------
# Save one shard
# ---------------------------------------------------------

save_one_shard <- function(df, path) {
  if (file.exists(path)) {
    file.remove(path)
  }
  
  save_parquet(df, path)
  
  invisible(path)
}


# ---------------------------------------------------------
# Combine shard files
# ---------------------------------------------------------

combine_shards <- function(files) {
  if (length(files) == 0) {
    return(tibble())
  }
  
  bind_rows(lapply(files, read_parquet))
}


# ---------------------------------------------------------
# Cleanup objects
# ---------------------------------------------------------

cleanup_objects <- function(...) {
  objs <- as.character(substitute(list(...)))[-1L]
  rm(list = objs, envir = parent.frame())
  invisible(gc())
}


# ---------------------------------------------------------
# Clear shard directory
# ---------------------------------------------------------

clear_shard_dir <- function(dir_path) {
  if (!dir.exists(dir_path)) {
    return(invisible(NULL))
  }
  
  old_files <- list.files(dir_path, full.names = TRUE, recursive = FALSE)
  
  if (length(old_files) > 0) {
    unlink(old_files, recursive = TRUE, force = TRUE)
  }
  
  invisible(NULL)
}


# ---------------------------------------------------------
# Minimal model marker helpers
# ---------------------------------------------------------

is_minimal_marker_fit <- function(fit_obj) {
  is.list(fit_obj) &&
    !is.null(fit_obj$compact_type) &&
    grepl("_minimal$", fit_obj$compact_type)
}


is_valid_saved_model_file <- function(path) {
  
  if (!file.exists(path)) {
    return(FALSE)
  }
  
  out <- tryCatch(
    {
      obj <- read_rds(path)
      
      is.list(obj) &&
        all(c("scenario", "model", "feature_columns", "workflow", "fit") %in% names(obj)) &&
        length(obj$feature_columns) > 0 &&
        !is.null(obj$fit)
    },
    error = function(e) FALSE
  )
  
  isTRUE(out)
}


# ---------------------------------------------------------
# Resolve model file path
#
# The registry may contain an absolute model_file path. If the
# repository was moved after training, this function falls back
# to the expected path inside outputs/ml_models/fitted_models_round1.
# ---------------------------------------------------------

resolve_model_file <- function(scenario, model, model_file, model_dir) {
  
  if (!is.na(model_file) && nzchar(model_file) && file.exists(model_file)) {
    return(model_file)
  }
  
  fallback <- model_file_path(
    scenario = scenario,
    model = model,
    fitted_dir = file.path(model_dir, "fitted_models_round1")
  )
  
  if (file.exists(fallback)) {
    return(fallback)
  }
  
  NA_character_
}


# ---------------------------------------------------------
# Load selected model marker or full model object
# ---------------------------------------------------------

load_model <- function(scenario, model, registry_tbl, model_dir) {
  
  row <- registry_tbl %>%
    filter(scenario == !!scenario, model == !!model, status == "ok") %>%
    slice(1)
  
  if (nrow(row) == 0) {
    stop("[050] Model not found in registry: ", scenario, " | ", model)
  }
  
  resolved_file <- resolve_model_file(
    scenario = scenario,
    model = model,
    model_file = row$model_file[[1]],
    model_dir = model_dir
  )
  
  if (is.na(resolved_file) || !nzchar(resolved_file)) {
    stop("[050] Model file could not be resolved for: ", scenario, " | ", model)
  }
  
  if (!is_valid_saved_model_file(resolved_file)) {
    stop("[050] Model file is invalid: ", resolved_file)
  }
  
  read_rds(resolved_file)
}


# ---------------------------------------------------------
# Cleanup stale shards from previous evaluation runs
# ---------------------------------------------------------

log_msg("[050] Cleaning previous evaluation shards...")

clear_shard_dir(overall_shard_dir)
clear_shard_dir(scenario_shard_dir)


# ---------------------------------------------------------
# Read inputs
# ---------------------------------------------------------

log_msg("[050] Reading datasets...")

train_tbl <- read_parquet(train_path)
test_tbl  <- read_parquet(test_path)

log_msg("[050] Reading validation outputs...")

metrics_tbl  <- read_parquet(metrics_path)
registry_tbl <- read_parquet(registry_path)


# ---------------------------------------------------------
# Determine available selected model markers/files
# ---------------------------------------------------------

registry_available <- registry_tbl %>%
  filter(status == "ok", !is.na(model_file), nzchar(model_file)) %>%
  rowwise() %>%
  mutate(
    resolved_model_file = resolve_model_file(
      scenario = scenario,
      model = model,
      model_file = model_file,
      model_dir = model_dir
    ),
    model_file_exists = !is.na(resolved_model_file) &&
      is_valid_saved_model_file(resolved_model_file)
  ) %>%
  ungroup() %>%
  filter(model_file_exists) %>%
  mutate(model_file = resolved_model_file) %>%
  select(-resolved_model_file, -model_file_exists)

if (nrow(registry_available) == 0) {
  stop("[050] No valid selected model files available in registry for test evaluation.")
}

log_msg("[050] Available selected model files:")
print(
  registry_available %>%
    select(scenario, model, model_file) %>%
    distinct()
)


# ---------------------------------------------------------
# Primary metric from validation
# ---------------------------------------------------------

metric_primary <- config$ml$metric_primary

log_msg("[050] Primary metric for model selection: ", metric_primary)

valid_metrics_ok <- metrics_tbl %>%
  filter(.metric == metric_primary, status == "ok") %>%
  semi_join(
    registry_available %>% distinct(scenario, model),
    by = c("scenario", "model")
  )

if (nrow(valid_metrics_ok) == 0) {
  stop(
    "[050] No successful validation results available for metric among models present on disk: ",
    metric_primary
  )
}


# ---------------------------------------------------------
# Select best models among available selected models
# ---------------------------------------------------------

best_overall <- valid_metrics_ok %>%
  arrange(desc(.estimate)) %>%
  slice(1)

best_by_scenario <- valid_metrics_ok %>%
  group_by(scenario) %>%
  slice_max(.estimate, n = 1, with_ties = FALSE) %>%
  ungroup()

log_msg("[050] Best overall model selected from validation:")
print(best_overall)

log_msg("[050] Best model by scenario selected from validation:")
print(best_by_scenario)


# ---------------------------------------------------------
# Save model selection tables
# ---------------------------------------------------------

save_parquet(
  best_overall,
  file.path(eval_dir, "best_model_overall_from_validation.parquet")
)

save_parquet(
  best_by_scenario,
  file.path(eval_dir, "best_models_by_scenario_from_validation.parquet")
)


# ---------------------------------------------------------
# Get predict-ready fit
#
# Compatibility rule:
#   - minimal marker (*_minimal): retrain in memory using train_tbl;
#   - full saved fit: use saved fit directly.
# ---------------------------------------------------------

get_predict_ready_fit <- function(model_obj, train_tbl) {
  
  sc <- model_obj$scenario
  mdl <- model_obj$model
  feature_cols <- model_obj$feature_columns
  
  if (is_minimal_marker_fit(model_obj$fit)) {
    
    log_msg(
      "[050] Minimal model marker detected; retraining in memory for prediction: ",
      sc,
      " | ",
      mdl
    )
    
    train_sc <- select_scenario_dataset(train_tbl, feature_cols)
    
    fit_out <- tryCatch(
      fit_one_model(
        train_tbl = train_sc,
        scenario_cols = feature_cols,
        model_name = mdl
      ),
      error = function(e) e
    )
    
    if (inherits(fit_out, "error")) {
      stop(
        "[050] Failed to rebuild minimal model for prediction: ",
        sc,
        " | ",
        mdl,
        " | error = ",
        conditionMessage(fit_out)
      )
    }
    
    fit_ready <- fit_out$fit
    
    cleanup_objects(train_sc, fit_out)
    
    return(
      list(
        fit = fit_ready,
        evaluation_source = "retrained_from_minimal"
      )
    )
  }
  
  list(
    fit = model_obj$fit,
    evaluation_source = "saved_fit"
  )
}


# ---------------------------------------------------------
# Evaluate one selected model and save shards immediately
# ---------------------------------------------------------

evaluate_and_save_one <- function(model_info_row, train_tbl, test_tbl, registry_tbl, shard_dir) {
  
  sc <- model_info_row$scenario[[1]]
  mdl <- model_info_row$model[[1]]
  
  log_msg("[050] Evaluating on test: ", sc, " | ", mdl)
  
  model_obj <- load_model(
    scenario = sc,
    model = mdl,
    registry_tbl = registry_tbl,
    model_dir = model_dir
  )
  
  feature_cols <- model_obj$feature_columns
  
  fit_ready_info <- get_predict_ready_fit(
    model_obj = model_obj,
    train_tbl = train_tbl
  )
  
  fit_ready <- fit_ready_info$fit
  evaluation_source <- fit_ready_info$evaluation_source
  
  test_sc <- select_scenario_dataset(test_tbl, feature_cols)
  
  pred_tbl <- predict_multiclass(
    fit_obj = fit_ready,
    new_data = test_sc
  ) %>%
    mutate(
      scenario = sc,
      model = mdl,
      evaluation_source = evaluation_source
    )
  
  metrics_out <- evaluate_predictions(pred_tbl)
  
  metrics_tbl_i <- metrics_out$metrics %>%
    mutate(
      scenario = sc,
      model = mdl,
      evaluation_source = evaluation_source
    )
  
  by_class_tbl <- pred_tbl %>%
    group_by(true_family) %>%
    summarise(
      n_samples = n(),
      accuracy = mean(.pred_class == true_family),
      .groups = "drop"
    ) %>%
    mutate(
      scenario = sc,
      model = mdl,
      evaluation_source = evaluation_source
    )
  
  by_n_tbl <- pred_tbl %>%
    group_by(n) %>%
    summarise(
      n_samples = n(),
      accuracy = mean(.pred_class == true_family),
      .groups = "drop"
    ) %>%
    mutate(
      scenario = sc,
      model = mdl,
      evaluation_source = evaluation_source
    )
  
  precision_macro_tbl <- precision(
    pred_tbl,
    truth = true_family,
    estimate = .pred_class,
    estimator = "macro"
  ) %>%
    mutate(
      scenario = sc,
      model = mdl,
      evaluation_source = evaluation_source
    )
  
  recall_macro_tbl <- recall(
    pred_tbl,
    truth = true_family,
    estimate = .pred_class,
    estimator = "macro"
  ) %>%
    mutate(
      scenario = sc,
      model = mdl,
      evaluation_source = evaluation_source
    )
  
  f1_macro_tbl <- f_meas(
    pred_tbl,
    truth = true_family,
    estimate = .pred_class,
    estimator = "macro"
  ) %>%
    mutate(
      scenario = sc,
      model = mdl,
      evaluation_source = evaluation_source
    )
  
  save_one_shard(pred_tbl,            shard_path(shard_dir, "predictions",       sc, mdl))
  save_one_shard(metrics_tbl_i,       shard_path(shard_dir, "metrics",           sc, mdl))
  save_one_shard(by_class_tbl,        shard_path(shard_dir, "by_class",          sc, mdl))
  save_one_shard(by_n_tbl,            shard_path(shard_dir, "by_n",              sc, mdl))
  save_one_shard(precision_macro_tbl, shard_path(shard_dir, "precision_macro",   sc, mdl))
  save_one_shard(recall_macro_tbl,    shard_path(shard_dir, "recall_macro",      sc, mdl))
  save_one_shard(f1_macro_tbl,        shard_path(shard_dir, "f1_macro",          sc, mdl))
  
  cleanup_objects(
    model_obj,
    feature_cols,
    fit_ready_info,
    fit_ready,
    test_sc,
    pred_tbl,
    metrics_out,
    metrics_tbl_i,
    by_class_tbl,
    by_n_tbl,
    precision_macro_tbl,
    recall_macro_tbl,
    f1_macro_tbl
  )
  
  invisible(NULL)
}


# ---------------------------------------------------------
# Evaluate best overall
# ---------------------------------------------------------

evaluate_and_save_one(
  model_info_row = best_overall,
  train_tbl = train_tbl,
  test_tbl = test_tbl,
  registry_tbl = registry_available,
  shard_dir = overall_shard_dir
)


# ---------------------------------------------------------
# Evaluate best model per scenario
# ---------------------------------------------------------

for (i in seq_len(nrow(best_by_scenario))) {
  evaluate_and_save_one(
    model_info_row = best_by_scenario[i, , drop = FALSE],
    train_tbl = train_tbl,
    test_tbl = test_tbl,
    registry_tbl = registry_available,
    shard_dir = scenario_shard_dir
  )
}

cleanup_objects(
  train_tbl,
  test_tbl,
  metrics_tbl,
  registry_tbl,
  registry_available,
  valid_metrics_ok,
  best_overall,
  best_by_scenario
)


# ---------------------------------------------------------
# Combine overall shards
# ---------------------------------------------------------

overall_predictions <- combine_shards(
  list.files(overall_shard_dir, pattern = "^predictions__.*\\.parquet$", full.names = TRUE)
)

overall_metrics <- combine_shards(
  list.files(overall_shard_dir, pattern = "^metrics__.*\\.parquet$", full.names = TRUE)
)

overall_by_class <- combine_shards(
  list.files(overall_shard_dir, pattern = "^by_class__.*\\.parquet$", full.names = TRUE)
)

overall_by_n <- combine_shards(
  list.files(overall_shard_dir, pattern = "^by_n__.*\\.parquet$", full.names = TRUE)
)

overall_precision_macro <- combine_shards(
  list.files(overall_shard_dir, pattern = "^precision_macro__.*\\.parquet$", full.names = TRUE)
)

overall_recall_macro <- combine_shards(
  list.files(overall_shard_dir, pattern = "^recall_macro__.*\\.parquet$", full.names = TRUE)
)

overall_f1_macro <- combine_shards(
  list.files(overall_shard_dir, pattern = "^f1_macro__.*\\.parquet$", full.names = TRUE)
)


# ---------------------------------------------------------
# Combine scenario shards
# ---------------------------------------------------------

scenario_predictions <- combine_shards(
  list.files(scenario_shard_dir, pattern = "^predictions__.*\\.parquet$", full.names = TRUE)
)

scenario_metrics <- combine_shards(
  list.files(scenario_shard_dir, pattern = "^metrics__.*\\.parquet$", full.names = TRUE)
)

scenario_by_class <- combine_shards(
  list.files(scenario_shard_dir, pattern = "^by_class__.*\\.parquet$", full.names = TRUE)
)

scenario_by_n <- combine_shards(
  list.files(scenario_shard_dir, pattern = "^by_n__.*\\.parquet$", full.names = TRUE)
)

scenario_precision_macro <- combine_shards(
  list.files(scenario_shard_dir, pattern = "^precision_macro__.*\\.parquet$", full.names = TRUE)
)

scenario_recall_macro <- combine_shards(
  list.files(scenario_shard_dir, pattern = "^recall_macro__.*\\.parquet$", full.names = TRUE)
)

scenario_f1_macro <- combine_shards(
  list.files(scenario_shard_dir, pattern = "^f1_macro__.*\\.parquet$", full.names = TRUE)
)


# ---------------------------------------------------------
# Save final outputs
# ---------------------------------------------------------

save_parquet(
  overall_predictions,
  file.path(eval_dir, "test_predictions_best_overall.parquet")
)

save_parquet(
  overall_metrics,
  file.path(eval_dir, "test_metrics_best_overall.parquet")
)

save_parquet(
  overall_by_class,
  file.path(eval_dir, "test_by_class_best_overall.parquet")
)

save_parquet(
  overall_by_n,
  file.path(eval_dir, "test_by_n_best_overall.parquet")
)

save_parquet(
  overall_precision_macro,
  file.path(eval_dir, "test_precision_macro_best_overall.parquet")
)

save_parquet(
  overall_recall_macro,
  file.path(eval_dir, "test_recall_macro_best_overall.parquet")
)

save_parquet(
  overall_f1_macro,
  file.path(eval_dir, "test_f1_macro_best_overall.parquet")
)

save_parquet(
  scenario_predictions,
  file.path(eval_dir, "test_predictions_best_by_scenario.parquet")
)

save_parquet(
  scenario_metrics,
  file.path(eval_dir, "test_metrics_best_by_scenario.parquet")
)

save_parquet(
  scenario_by_class,
  file.path(eval_dir, "test_by_class_best_by_scenario.parquet")
)

save_parquet(
  scenario_by_n,
  file.path(eval_dir, "test_by_n_best_by_scenario.parquet")
)

save_parquet(
  scenario_precision_macro,
  file.path(eval_dir, "test_precision_macro_best_by_scenario.parquet")
)

save_parquet(
  scenario_recall_macro,
  file.path(eval_dir, "test_recall_macro_best_by_scenario.parquet")
)

save_parquet(
  scenario_f1_macro,
  file.path(eval_dir, "test_f1_macro_best_by_scenario.parquet")
)


# ---------------------------------------------------------
# Final diagnostics
# ---------------------------------------------------------

log_msg("[050] Saved best-overall and best-by-scenario evaluation outputs.")

log_msg("[050] Best overall test metrics:")
print(overall_metrics)

log_msg("[050] Best-by-scenario test metrics:")
print(scenario_metrics)

save_session_info(
  file.path(eval_dir, "session_info_050.txt")
)

log_msg("[050] 050_evaluate_models.R finished successfully.")