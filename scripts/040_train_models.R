# =========================================================
# 040_train_models.R
# Train multiclass models across feature scenarios
# =========================================================
#
# This script:
#   - reads train and validation datasets;
#   - trains active ML models across feature scenarios;
#   - evaluates validation performance;
#   - selects the best model overall and the best model within
#     each scenario based on validation metrics;
#   - saves validation metrics and a training registry;
#   - saves minimal model markers for selected models.
#
# Important:
#   Model files saved by this script are minimal markers, not
#   full fitted model objects. The final evaluation script
#   (050_evaluate_models.R) must detect these markers and
#   retrain selected models in memory before test evaluation.
#
# Required previous steps:
#   scripts/000_config.R
#   scripts/030_build_datasets.R
# =========================================================

library(dplyr)
library(tibble)
library(arrow)
library(tidymodels)
library(workflows)
library(parsnip)
library(recipes)
library(yardstick)
library(tidyr)


# ---------------------------------------------------------
# Repository root check
# ---------------------------------------------------------

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

config_path <- file.path(root_dir, "outputs", "config", "config_main.rds")

if (!file.exists(file.path(root_dir, "R")) ||
    !file.exists(file.path(root_dir, "scripts")) ||
    !file.exists(config_path)) {
  stop(
    "[040] This script must be run from the FADS_AI repository root ",
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
fitted_dir  <- file.path(model_dir, "fitted_models_round1")

metric_shard_dir   <- file.path(model_dir, "validation_metric_shards")
pred_shard_dir     <- file.path(model_dir, "validation_prediction_shards")
registry_shard_dir <- file.path(model_dir, "training_registry_shards")

ensure_dir(model_dir)
ensure_dir(fitted_dir)
ensure_dir(metric_shard_dir)
ensure_dir(pred_shard_dir)
ensure_dir(registry_shard_dir)


# ---------------------------------------------------------
# Required inputs
# ---------------------------------------------------------

train_path <- file.path(dataset_dir, "train.parquet")
valid_path <- file.path(dataset_dir, "valid.parquet")
scenario_path <- file.path(dataset_dir, "feature_scenarios.rds")

if (!file.exists(train_path)) {
  stop("[040] Training dataset not found: ", train_path, "\nRun scripts/030_build_datasets.R first.")
}

if (!file.exists(valid_path)) {
  stop("[040] Validation dataset not found: ", valid_path, "\nRun scripts/030_build_datasets.R first.")
}

if (!file.exists(scenario_path)) {
  stop("[040] Feature scenario object not found: ", scenario_path, "\nRun scripts/030_build_datasets.R first.")
}


# ---------------------------------------------------------
# Runtime controls
# ---------------------------------------------------------

save_validation_predictions <- isTRUE(config$ml$save_validation_predictions)
compress_models <- "xz"

model_names_all <- config$ml$models
validate_model_names(model_names_all)


# ---------------------------------------------------------
# Optional subset execution
#
# Public option names:
#   options(fads.ml_subset = "xgb")
#   options(fads.ml_subset_clean = TRUE)
#
# Legacy option names are also accepted for compatibility:
#   options(seaf.ml_subset = "xgb")
#   options(seaf.ml_subset_clean = TRUE)
# ---------------------------------------------------------

normalize_ml_subset <- function(x) {
  
  if (is.null(x)) {
    return(character(0))
  }
  
  if (length(x) == 0) {
    return(character(0))
  }
  
  if (is.character(x) && length(x) == 1L) {
    x <- trimws(x)
    
    if (!nzchar(x)) {
      return(character(0))
    }
    
    x <- unlist(strsplit(x, ",", fixed = TRUE), use.names = FALSE)
  }
  
  x <- trimws(as.character(x))
  x <- x[nzchar(x)]
  
  unique(x)
}

ml_subset_raw <- getOption(
  "fads.ml_subset",
  default = getOption("seaf.ml_subset", default = NULL)
)

ml_subset <- normalize_ml_subset(ml_subset_raw)

if (length(ml_subset) > 0) {
  validate_model_names(ml_subset)
}

run_models <- if (length(ml_subset) == 0) {
  model_names_all
} else {
  ml_subset
}

subset_mode <- !setequal(sort(run_models), sort(model_names_all))

subset_clean <- isTRUE(
  getOption(
    "fads.ml_subset_clean",
    default = getOption("seaf.ml_subset_clean", default = FALSE)
  )
)

log_msg(
  "[040] Raw getOption(fads.ml_subset/seaf.ml_subset): ",
  if (is.null(ml_subset_raw)) "<NULL>" else paste(ml_subset_raw, collapse = ", ")
)

log_msg(
  "[040] Normalized ml_subset: ",
  if (length(ml_subset) == 0) "<empty>" else paste(ml_subset, collapse = ", ")
)

log_msg("[040] ML models declared in config: ", paste(model_names_all, collapse = ", "))
log_msg("[040] ML models in this execution: ", paste(run_models, collapse = ", "))
log_msg("[040] Selective model cleanup requested: ", subset_clean)
log_msg("[040] Subset mode active: ", subset_mode)

expected_models <- sort(unique(model_names_all))
observed_run_models <- sort(unique(run_models))

if (length(run_models) == 0) {
  stop("[040] run_models is empty after normalization.")
}

if (!subset_mode && !identical(expected_models, observed_run_models)) {
  stop(
    "[040] Full-run mode inconsistency: run_models does not match config$ml$models. ",
    "Expected: ", paste(expected_models, collapse = ", "),
    " | Observed: ", paste(observed_run_models, collapse = ", ")
  )
}


# ---------------------------------------------------------
# Read inputs
# ---------------------------------------------------------

log_msg("[040] Reading datasets...")

train_tbl <- read_parquet(train_path)
valid_tbl <- read_parquet(valid_path)
feature_scenarios <- read_rds(scenario_path)

if (is.null(config$ml) || is.null(config$ml$models)) {
  stop("[040] config$ml$models is not defined.")
}

log_msg("[040] Primary metric: ", config$ml$metric_primary)
log_msg("[040] CV folds declared in config: ", config$ml$folds)
log_msg("[040] Feature scenarios available: ", paste(names(feature_scenarios), collapse = ", "))
log_msg("[040] Save validation predictions: ", save_validation_predictions)
log_msg("[040] Model marker compression: ", compress_models)


# ---------------------------------------------------------
# Path helpers
# ---------------------------------------------------------

model_file_path <- function(scenario, model, fitted_dir) {
  file.path(fitted_dir, paste0("model_", scenario, "__", model, ".rds"))
}

metric_shard_path <- function(scenario, model, dir) {
  file.path(dir, paste0("metric_", scenario, "__", model, ".parquet"))
}

pred_shard_path <- function(scenario, model, dir) {
  file.path(dir, paste0("pred_", scenario, "__", model, ".parquet"))
}

registry_shard_path <- function(scenario, model, dir) {
  file.path(dir, paste0("registry_", scenario, "__", model, ".parquet"))
}


# ---------------------------------------------------------
# Minimal model marker validation
# ---------------------------------------------------------

is_valid_model_file <- function(path) {
  
  if (!file.exists(path)) {
    return(FALSE)
  }
  
  out <- tryCatch(
    {
      obj <- read_rds(path)
      
      is.list(obj) &&
        all(c("scenario", "model", "feature_columns", "workflow", "fit") %in% names(obj)) &&
        length(obj$feature_columns) > 0 &&
        is.null(obj$workflow) &&
        !is.null(obj$fit) &&
        is.list(obj$fit) &&
        !is.null(obj$fit$compact_type) &&
        identical(obj$fit$compact_type, paste0(obj$model, "_minimal"))
    },
    error = function(e) FALSE
  )
  
  isTRUE(out)
}


# ---------------------------------------------------------
# Safe readers/writers
# ---------------------------------------------------------

safe_read_parquet <- function(path) {
  
  if (!file.exists(path)) {
    return(NULL)
  }
  
  tryCatch(read_parquet(path), error = function(e) NULL)
}

save_one_shard <- function(df, path, max_tries = 3L, sleep_sec = 2) {
  
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  tmp_path <- paste0(
    path,
    ".tmp_",
    Sys.getpid(),
    "_",
    format(Sys.time(), "%Y%m%d%H%M%S"),
    "_",
    sample.int(1e6, 1)
  )
  
  last_err <- NULL
  
  for (k in seq_len(max_tries)) {
    
    try(unlink(tmp_path, force = TRUE), silent = TRUE)
    
    ok <- tryCatch(
      {
        arrow::write_parquet(df, sink = tmp_path)
        TRUE
      },
      error = function(e) {
        last_err <<- e
        FALSE
      }
    )
    
    if (ok && file.exists(tmp_path) && file.info(tmp_path)$size > 0) {
      
      if (file.exists(path)) {
        unlink(path, force = TRUE)
      }
      
      moved <- file.rename(tmp_path, path)
      
      if (!moved) {
        copied <- file.copy(tmp_path, path, overwrite = TRUE)
        
        if (copied) {
          unlink(tmp_path, force = TRUE)
          moved <- TRUE
        }
      }
      
      if (moved && file.exists(path) && file.info(path)$size > 0) {
        return(invisible(path))
      }
    }
    
    Sys.sleep(sleep_sec)
  }
  
  stop(
    "[040] Failed to save parquet after ",
    max_tries,
    " attempts: ",
    path,
    if (!is.null(last_err)) paste0(" | last error: ", conditionMessage(last_err)) else ""
  )
}


# ---------------------------------------------------------
# Save minimal model marker
# ---------------------------------------------------------

save_compact_model <- function(obj, path, compress = "xz", max_tries = 3L, sleep_sec = 2) {
  
  dir.create(dirname(path), recursive = TRUE, showWarnings = FALSE)
  
  make_tmp_path <- function(base_dir, filename) {
    file.path(
      base_dir,
      paste0(
        filename,
        ".tmp_",
        Sys.getpid(),
        "_",
        format(Sys.time(), "%Y%m%d%H%M%S"),
        "_",
        sample.int(1e6, 1)
      )
    )
  }
  
  compact_fit_for_storage <- function(obj) {
    fit_out <- list(
      compact_type = paste0(obj$model, "_minimal"),
      model = obj$model,
      scenario = obj$scenario
    )
    
    list(
      scenario = obj$scenario,
      model = obj$model,
      feature_columns = obj$feature_columns,
      workflow = NULL,
      fit = fit_out
    )
  }
  
  compact_obj <- compact_fit_for_storage(obj)
  last_err <- NULL
  
  obj_size_mb <- tryCatch(
    as.numeric(object.size(compact_obj)) / 1024^2,
    error = function(e) NA_real_
  )
  
  tmp_dir <- if (dir.exists("/tmp")) "/tmp" else dirname(path)
  tmp_path <- make_tmp_path(tmp_dir, basename(path))
  
  for (k in seq_len(max_tries)) {
    
    try(unlink(tmp_path, force = TRUE), silent = TRUE)
    
    ok <- tryCatch(
      {
        log_msg(
          "[040] About to save minimal model marker | scenario = ", obj$scenario,
          " | model = ", obj$model,
          " | compress = ", compress,
          " | size_mb = ", round(obj_size_mb, 4)
        )
        
        saveRDS(compact_obj, tmp_path, compress = compress)
        TRUE
      },
      error = function(e) {
        last_err <<- e
        FALSE
      }
    )
    
    if (ok && file.exists(tmp_path)) {
      
      tmp_info <- file.info(tmp_path)
      
      if (isTRUE(is.finite(tmp_info$size)) && tmp_info$size > 0) {
        
        if (file.exists(path)) {
          unlink(path, force = TRUE)
        }
        
        moved <- tryCatch(file.rename(tmp_path, path), error = function(e) FALSE)
        
        if (!isTRUE(moved)) {
          copied <- tryCatch(file.copy(tmp_path, path, overwrite = TRUE), error = function(e) FALSE)
          
          if (isTRUE(copied)) {
            unlink(tmp_path, force = TRUE)
            moved <- TRUE
          }
        }
        
        if (isTRUE(moved) && file.exists(path)) {
          out_info <- file.info(path)
          
          if (isTRUE(is.finite(out_info$size)) && out_info$size > 0) {
            return(invisible(path))
          }
        }
      }
    }
    
    Sys.sleep(sleep_sec)
  }
  
  stop(
    "[040] Failed to save minimal model marker after ",
    max_tries,
    " attempts: ",
    path,
    " | model = ",
    obj$model,
    " | scenario = ",
    obj$scenario,
    " | compact_obj_size_mb = ",
    round(obj_size_mb, 4),
    if (!is.null(last_err)) paste0(" | last error: ", conditionMessage(last_err)) else ""
  )
}


# ---------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------

cleanup_objects <- function(...) {
  objs <- as.character(substitute(list(...)))[-1L]
  rm(list = objs, envir = parent.frame())
  invisible(gc())
}

merge_replace_by_model <- function(old_tbl, new_tbl, run_models) {
  
  if (is.null(old_tbl) || nrow(old_tbl) == 0) {
    return(new_tbl)
  }
  
  if (is.null(new_tbl) || nrow(new_tbl) == 0) {
    return(old_tbl)
  }
  
  old_keep <- old_tbl %>%
    filter(!model %in% run_models)
  
  bind_rows(old_keep, new_tbl)
}

read_subset_shards <- function(shard_dir, run_models) {
  
  files_i <- list.files(shard_dir, pattern = "\\.parquet$", full.names = TRUE)
  
  if (length(files_i) == 0) {
    return(tibble())
  }
  
  matched <- files_i[
    vapply(
      files_i,
      function(p) {
        bn <- basename(p)
        any(grepl(paste0("__", run_models, "\\.parquet$"), bn))
      },
      logical(1)
    )
  ]
  
  if (length(matched) == 0) {
    return(tibble())
  }
  
  bind_rows(lapply(matched, read_parquet))
}

remove_model_artifacts <- function(model_name) {
  
  unlink(
    list.files(
      metric_shard_dir,
      pattern = paste0("__", model_name, "\\.parquet$"),
      full.names = TRUE
    ),
    force = TRUE
  )
  
  unlink(
    list.files(
      registry_shard_dir,
      pattern = paste0("__", model_name, "\\.parquet$"),
      full.names = TRUE
    ),
    force = TRUE
  )
  
  unlink(
    list.files(
      pred_shard_dir,
      pattern = paste0("__", model_name, "\\.parquet$"),
      full.names = TRUE
    ),
    force = TRUE
  )
  
  unlink(
    list.files(
      fitted_dir,
      pattern = paste0("__", model_name, "\\.rds$"),
      full.names = TRUE
    ),
    force = TRUE
  )
}


# ---------------------------------------------------------
# Optional selective cleanup for current model subset
# ---------------------------------------------------------

if (subset_clean) {
  log_msg("[040] Cleaning artifacts only for current model subset...")
  
  for (mdl in run_models) {
    remove_model_artifacts(mdl)
    log_msg("[040] Removed old artifacts for model: ", mdl)
  }
}


# ---------------------------------------------------------
# Train/evaluate one scenario-model pair
# ---------------------------------------------------------

process_one_pair <- function(sc_name, sc_cols, mdl) {
  
  metric_path_i <- metric_shard_path(sc_name, mdl, metric_shard_dir)
  pred_path_i <- pred_shard_path(sc_name, mdl, pred_shard_dir)
  registry_path_i <- registry_shard_path(sc_name, mdl, registry_shard_dir)
  
  if (file.exists(metric_path_i) && file.exists(registry_path_i)) {
    log_msg(
      "[040] Skipping existing metric/registry shards for scenario = ",
      sc_name,
      " | model = ",
      mdl
    )
    return(invisible(NULL))
  }
  
  train_sc <- select_scenario_dataset(train_tbl, sc_cols)
  valid_sc <- select_scenario_dataset(valid_tbl, sc_cols)
  
  n_features_i <- length(sc_cols)
  n_train_i <- nrow(train_sc)
  n_valid_i <- nrow(valid_sc)
  elapsed_i <- NA_real_
  
  log_msg("[040] Training/evaluating scenario = ", sc_name, " | model = ", mdl)
  
  t0 <- Sys.time()
  
  fit_out <- tryCatch(
    fit_one_model(
      train_tbl = train_sc,
      scenario_cols = sc_cols,
      model_name = mdl
    ),
    error = function(e) e
  )
  
  t1 <- Sys.time()
  elapsed_i <- as.numeric(difftime(t1, t0, units = "secs"))
  
  if (inherits(fit_out, "error")) {
    
    metric_tbl <- tibble(
      .metric = c("accuracy", "kap"),
      .estimator = "multiclass",
      .estimate = NA_real_,
      scenario = sc_name,
      model = mdl,
      status = "fit_failed",
      note = conditionMessage(fit_out)
    )
    
    registry_tbl <- tibble(
      scenario = sc_name,
      model = mdl,
      status = "fit_failed",
      note = conditionMessage(fit_out),
      n_features = n_features_i,
      n_train = n_train_i,
      n_valid = n_valid_i,
      elapsed_sec = elapsed_i,
      model_file = NA_character_,
      reused_existing = FALSE,
      keep_model_file = FALSE
    )
    
    save_one_shard(metric_tbl, metric_path_i)
    save_one_shard(registry_tbl, registry_path_i)
    
    if (save_validation_predictions && file.exists(pred_path_i)) {
      file.remove(pred_path_i)
    }
    
    cleanup_objects(train_sc, valid_sc, fit_out, metric_tbl, registry_tbl)
    return(invisible(NULL))
  }
  
  pred_tbl <- tryCatch(
    predict_multiclass(
      fit_obj = fit_out$fit,
      new_data = valid_sc
    ),
    error = function(e) e
  )
  
  if (inherits(pred_tbl, "error")) {
    
    metric_tbl <- tibble(
      .metric = c("accuracy", "kap"),
      .estimator = "multiclass",
      .estimate = NA_real_,
      scenario = sc_name,
      model = mdl,
      status = "predict_failed",
      note = conditionMessage(pred_tbl)
    )
    
    registry_tbl <- tibble(
      scenario = sc_name,
      model = mdl,
      status = "predict_failed",
      note = conditionMessage(pred_tbl),
      n_features = n_features_i,
      n_train = n_train_i,
      n_valid = n_valid_i,
      elapsed_sec = elapsed_i,
      model_file = NA_character_,
      reused_existing = FALSE,
      keep_model_file = FALSE
    )
    
    save_one_shard(metric_tbl, metric_path_i)
    save_one_shard(registry_tbl, registry_path_i)
    
    if (save_validation_predictions && file.exists(pred_path_i)) {
      file.remove(pred_path_i)
    }
    
    cleanup_objects(train_sc, valid_sc, fit_out, pred_tbl, metric_tbl, registry_tbl)
    return(invisible(NULL))
  }
  
  eval_out <- tryCatch(
    evaluate_predictions(pred_tbl),
    error = function(e) e
  )
  
  if (inherits(eval_out, "error")) {
    
    metric_tbl <- tibble(
      .metric = c("accuracy", "kap"),
      .estimator = "multiclass",
      .estimate = NA_real_,
      scenario = sc_name,
      model = mdl,
      status = "eval_failed",
      note = conditionMessage(eval_out)
    )
    
    registry_tbl <- tibble(
      scenario = sc_name,
      model = mdl,
      status = "eval_failed",
      note = conditionMessage(eval_out),
      n_features = n_features_i,
      n_train = n_train_i,
      n_valid = n_valid_i,
      elapsed_sec = elapsed_i,
      model_file = NA_character_,
      reused_existing = FALSE,
      keep_model_file = FALSE
    )
    
    save_one_shard(metric_tbl, metric_path_i)
    save_one_shard(registry_tbl, registry_path_i)
    
    if (save_validation_predictions && file.exists(pred_path_i)) {
      file.remove(pred_path_i)
    }
    
    cleanup_objects(train_sc, valid_sc, fit_out, pred_tbl, eval_out, metric_tbl, registry_tbl)
    return(invisible(NULL))
  }
  
  metric_tbl <- eval_out$metrics %>%
    mutate(
      scenario = sc_name,
      model = mdl,
      status = "ok",
      note = NA_character_
    )
  
  registry_tbl <- tibble(
    scenario = sc_name,
    model = mdl,
    status = "ok",
    note = NA_character_,
    n_features = n_features_i,
    n_train = n_train_i,
    n_valid = n_valid_i,
    elapsed_sec = elapsed_i,
    model_file = NA_character_,
    reused_existing = FALSE,
    keep_model_file = FALSE
  )
  
  save_one_shard(metric_tbl, metric_path_i)
  save_one_shard(registry_tbl, registry_path_i)
  
  if (save_validation_predictions) {
    pred_tbl <- pred_tbl %>%
      mutate(
        scenario = sc_name,
        model = mdl
      )
    
    save_one_shard(pred_tbl, pred_path_i)
  } else {
    if (file.exists(pred_path_i)) {
      file.remove(pred_path_i)
    }
  }
  
  cleanup_objects(train_sc, valid_sc, fit_out, pred_tbl, eval_out, metric_tbl, registry_tbl)
  
  invisible(NULL)
}


# ---------------------------------------------------------
# Stage A: train/evaluate current model subset
# ---------------------------------------------------------

log_msg("[040] Stage A: train/evaluate current model subset, saving only shards...")

for (sc_name in names(feature_scenarios)) {
  
  sc_cols <- feature_scenarios[[sc_name]]
  
  for (mdl in run_models) {
    process_one_pair(
      sc_name = sc_name,
      sc_cols = sc_cols,
      mdl = mdl
    )
  }
}


# ---------------------------------------------------------
# Read current-run shards
# ---------------------------------------------------------

log_msg("[040] Reading current-run shards for models: ", paste(run_models, collapse = ", "))

metrics_new <- read_subset_shards(metric_shard_dir, run_models)
registry_new <- read_subset_shards(registry_shard_dir, run_models)

if (nrow(metrics_new) == 0) {
  stop("[040] No metric shards found for current model subset: ", paste(run_models, collapse = ", "))
}

if (nrow(registry_new) == 0) {
  stop("[040] No registry shards found for current model subset: ", paste(run_models, collapse = ", "))
}

if (save_validation_predictions) {
  predictions_new <- read_subset_shards(pred_shard_dir, run_models)
} else {
  predictions_new <- tibble()
}


# ---------------------------------------------------------
# Read old aggregate outputs if present
# ---------------------------------------------------------

metrics_old <- safe_read_parquet(file.path(model_dir, "validation_metrics_round1.parquet"))
registry_old <- safe_read_parquet(file.path(model_dir, "training_registry_round1.parquet"))

if (save_validation_predictions) {
  predictions_old <- safe_read_parquet(file.path(model_dir, "validation_predictions_round1.parquet"))
} else {
  predictions_old <- tibble()
}


# ---------------------------------------------------------
# Safe merge
# ---------------------------------------------------------

metrics_tbl <- merge_replace_by_model(metrics_old, metrics_new, run_models) %>%
  arrange(scenario, model, .metric)

registry_tbl <- merge_replace_by_model(registry_old, registry_new, run_models) %>%
  arrange(scenario, model)

if (save_validation_predictions) {
  predictions_tbl <- merge_replace_by_model(predictions_old, predictions_new, run_models) %>%
    arrange(scenario, model)
} else {
  predictions_tbl <- tibble()
}

registry_tbl <- registry_tbl %>%
  group_by(scenario, model) %>%
  slice_tail(n = 1) %>%
  ungroup() %>%
  arrange(scenario, model)


# ---------------------------------------------------------
# Select best models on merged validation results
# ---------------------------------------------------------

valid_metrics_ok <- metrics_tbl %>%
  filter(.metric == config$ml$metric_primary, status == "ok")

if (nrow(valid_metrics_ok) == 0) {
  stop("[040] No successful validation results available for metric: ", config$ml$metric_primary)
}

best_overall <- valid_metrics_ok %>%
  arrange(desc(.estimate)) %>%
  slice(1) %>%
  select(scenario, model)

best_by_scenario <- valid_metrics_ok %>%
  group_by(scenario) %>%
  slice_max(.estimate, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  select(scenario, model)

keep_tbl <- bind_rows(best_overall, best_by_scenario) %>%
  distinct() %>%
  mutate(keep_model_file = TRUE)

log_msg("[040] Selected models to keep on disk after merged validation results:")
print(keep_tbl)


# ---------------------------------------------------------
# Stage B: save minimal markers for selected models
# ---------------------------------------------------------

train_and_save_selected_model <- function(sc_name, mdl) {
  
  sc_cols <- feature_scenarios[[sc_name]]
  model_file_i <- model_file_path(sc_name, mdl, fitted_dir)
  
  if (is_valid_model_file(model_file_i)) {
    log_msg(
      "[040] Reusing existing minimal model marker for scenario = ",
      sc_name,
      " | model = ",
      mdl
    )
    return(invisible(model_file_i))
  }
  
  if (file.exists(model_file_i)) {
    file.remove(model_file_i)
    log_msg("[040] Removed stale/corrupted model marker file: ", model_file_i)
  }
  
  train_sc <- select_scenario_dataset(train_tbl, sc_cols)
  
  log_msg(
    "[040] Stage B retraining selected model for minimal marker: scenario = ",
    sc_name,
    " | model = ",
    mdl
  )
  
  fit_out <- tryCatch(
    fit_one_model(
      train_tbl = train_sc,
      scenario_cols = sc_cols,
      model_name = mdl
    ),
    error = function(e) e
  )
  
  if (inherits(fit_out, "error")) {
    stop(
      "[040] Failed to retrain selected model for scenario = ",
      sc_name,
      " | model = ",
      mdl,
      " | error = ",
      conditionMessage(fit_out)
    )
  }
  
  model_obj <- list(
    scenario = sc_name,
    model = mdl,
    feature_columns = sc_cols,
    workflow = fit_out$workflow,
    fit = fit_out$fit
  )
  
  save_compact_model(model_obj, model_file_i, compress = compress_models)
  
  cleanup_objects(train_sc, fit_out, model_obj)
  
  invisible(model_file_i)
}

log_msg("[040] Stage B: ensuring selected model markers are saved on disk...")

for (i in seq_len(nrow(keep_tbl))) {
  train_and_save_selected_model(
    sc_name = keep_tbl$scenario[[i]],
    mdl = keep_tbl$model[[i]]
  )
}


# ---------------------------------------------------------
# Update registry
# ---------------------------------------------------------

registry_tbl <- registry_tbl %>%
  select(-any_of("keep_model_file")) %>%
  left_join(keep_tbl, by = c("scenario", "model")) %>%
  mutate(keep_model_file = if_else(is.na(keep_model_file), FALSE, keep_model_file))

registry_tbl <- registry_tbl %>%
  mutate(
    model_file = if_else(
      keep_model_file & status == "ok",
      model_file_path(scenario, model, fitted_dir),
      NA_character_
    )
  )


# ---------------------------------------------------------
# Remove model marker files no longer selected
# ---------------------------------------------------------

log_msg("[040] Removing fitted model marker files that are no longer selected...")

all_model_files <- list.files(fitted_dir, pattern = "\\.rds$", full.names = TRUE)

if (length(all_model_files) > 0) {
  
  keep_paths <- registry_tbl %>%
    filter(status == "ok", keep_model_file, !is.na(model_file)) %>%
    pull(model_file) %>%
    unique()
  
  drop_paths <- setdiff(all_model_files, keep_paths)
  
  if (length(drop_paths) > 0) {
    unlink(drop_paths, force = TRUE)
    
    for (pp in drop_paths) {
      log_msg("[040] Removed non-selected model marker file: ", pp)
    }
  }
}


# ---------------------------------------------------------
# Save final aggregate outputs
# ---------------------------------------------------------

metrics_path <- file.path(model_dir, "validation_metrics_round1.parquet")
pred_path <- file.path(model_dir, "validation_predictions_round1.parquet")
registry_path <- file.path(model_dir, "training_registry_round1.parquet")

if (file.exists(metrics_path)) {
  file.remove(metrics_path)
}

if (file.exists(pred_path)) {
  file.remove(pred_path)
}

if (file.exists(registry_path)) {
  file.remove(registry_path)
}

save_one_shard(metrics_tbl, metrics_path)
save_one_shard(predictions_tbl, pred_path)
save_one_shard(registry_tbl, registry_path)

log_msg("[040] Saved merged validation metrics: ", metrics_path)
log_msg("[040] Saved merged validation predictions: ", pred_path)
log_msg("[040] Saved merged training registry: ", registry_path)


# ---------------------------------------------------------
# Cleanup current-run shard files only
# ---------------------------------------------------------

for (mdl in run_models) {
  
  unlink(
    list.files(
      metric_shard_dir,
      pattern = paste0("__", mdl, "\\.parquet$"),
      full.names = TRUE
    ),
    force = TRUE
  )
  
  unlink(
    list.files(
      registry_shard_dir,
      pattern = paste0("__", mdl, "\\.parquet$"),
      full.names = TRUE
    ),
    force = TRUE
  )
  
  unlink(
    list.files(
      pred_shard_dir,
      pattern = paste0("__", mdl, "\\.parquet$"),
      full.names = TRUE
    ),
    force = TRUE
  )
}

log_msg("[040] Temporary training shards cleaned up for current model subset.")


# ---------------------------------------------------------
# Diagnostics
# ---------------------------------------------------------

log_msg("[040] Training summary (merged):")
print(registry_tbl %>% count(status, keep_model_file, .drop = FALSE))

log_msg("[040] Kept fitted model markers after merge:")
print(
  registry_tbl %>%
    filter(keep_model_file, status == "ok") %>%
    select(scenario, model, model_file)
)


# ---------------------------------------------------------
# Session info
# ---------------------------------------------------------

save_session_info(file.path(model_dir, "session_info_040.txt"))

log_msg("[040] 040_train_models.R finished successfully.")