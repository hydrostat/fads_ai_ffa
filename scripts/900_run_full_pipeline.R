# =========================================================
# 900_run_full_pipeline.R
# Master runner for the full FADS_AI pipeline
# =========================================================
#
# This script runs the full reproducible pipeline from
# simulation metadata generation to final test evaluation.
#
# Public repository convention:
#   Run this script from the repository root:
#
#     fads_ai_ffa/
#
# Example:
#   source("scripts/900_run_full_pipeline.R")
#
# Runtime options:
#
#   options(fads.run_mode = "resume")      # default
#   options(fads.run_mode = "clean")       # remove generated outputs and rerun
#
#   options(fads.force_steps = c("040_train_models"))
#   options(fads.clean_steps = c("040_train_models"))
#
# Legacy option names with prefix "seaf." are also accepted
# for compatibility, but "fads." is preferred.
#
# Important:
#   Precomputed outputs are not distributed with this
#   repository. This runner regenerates outputs locally.
# =========================================================

library(dplyr)
library(tibble)
library(arrow)
library(readr)


# =========================================================
# 1. Repository root check
# =========================================================

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

required_root_items <- c("R", "scripts", "data", "README.md")

missing_root_items <- required_root_items[
  !file.exists(file.path(root_dir, required_root_items))
]

if (length(missing_root_items) > 0) {
  stop(
    "[900] The working directory does not appear to be the FADS_AI repository root.\n",
    "Current working directory: ", root_dir, "\n",
    "Missing expected items: ", paste(missing_root_items, collapse = ", "), "\n",
    "Please set the working directory to the root folder of the repository."
  )
}


# =========================================================
# 2. Runtime options
# =========================================================

get_fads_option <- function(name, default = NULL) {
  fads_name <- paste0("fads.", name)
  seaf_name <- paste0("seaf.", name)
  
  getOption(
    fads_name,
    default = getOption(seaf_name, default = default)
  )
}

normalize_step_vector <- function(x) {
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

run_mode <- get_fads_option("run_mode", default = "resume")
run_mode <- match.arg(run_mode, choices = c("resume", "clean"))

force_steps <- normalize_step_vector(
  get_fads_option("force_steps", default = character(0))
)

clean_steps <- normalize_step_vector(
  get_fads_option("clean_steps", default = character(0))
)


# =========================================================
# 3. Clean mode before configuration
# =========================================================

if (identical(run_mode, "clean")) {
  message("[900] Clean mode requested. Removing generated outputs folder.")
  
  outputs_dir <- file.path(root_dir, "outputs")
  
  if (dir.exists(outputs_dir)) {
    unlink(outputs_dir, recursive = TRUE, force = TRUE)
  }
}


# =========================================================
# 4. Source structure and configuration scripts
# =========================================================

message("[900] Verifying repository structure...")

source(
  file.path(root_dir, "scripts", "001_create_project_structure.R"),
  local = new.env(parent = globalenv())
)

message("[900] Creating configuration...")

source(
  file.path(root_dir, "scripts", "000_config.R"),
  local = new.env(parent = globalenv())
)

config_path <- file.path(root_dir, "outputs", "config", "config_main.rds")

if (!file.exists(config_path)) {
  stop("[900] Config file was not created: ", config_path)
}

config <- readRDS(config_path)
paths <- config$paths


# =========================================================
# 5. Load basic helper functions
# =========================================================

source(file.path(root_dir, "R", "fun_io.R"))


# =========================================================
# 6. Logging
# =========================================================

ensure_dir(paths$logs)

run_label <- format(Sys.time(), "%Y%m%d_%H%M%S")

log_file <- file.path(
  paths$logs,
  paste0("pipeline_run_", run_label, ".log")
)

timing_file <- file.path(
  paths$logs,
  paste0("pipeline_timing_", run_label, ".csv")
)

file_size_file <- file.path(
  paths$logs,
  paste0("pipeline_file_sizes_", run_label, ".csv")
)

summary_file <- file.path(
  paths$logs,
  paste0("pipeline_summary_", run_label, ".txt")
)

log_msg_900 <- function(...) {
  msg <- paste(..., collapse = " ")
  line <- sprintf("[%s] %s", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), msg)
  
  cat(line, "\n")
  cat(line, "\n", file = log_file, append = TRUE)
  
  invisible(NULL)
}

log_section <- function(title) {
  line <- paste0("\n", paste(rep("=", 70), collapse = ""), "\n",
                 title, "\n",
                 paste(rep("=", 70), collapse = ""), "\n")
  
  cat(line)
  cat(line, file = log_file, append = TRUE)
  
  invisible(NULL)
}

append_timing <- function(step, t0, t1, status, note = NA_character_) {
  row <- tibble::tibble(
    step = step,
    start_time = as.character(t0),
    end_time = as.character(t1),
    elapsed_sec = as.numeric(difftime(t1, t0, units = "secs")),
    status = status,
    note = note
  )
  
  if (!file.exists(timing_file)) {
    readr::write_csv(row, timing_file)
  } else {
    readr::write_csv(row, timing_file, append = TRUE)
  }
  
  invisible(NULL)
}

append_file_size <- function(step, path) {
  if (!file.exists(path)) {
    return(invisible(NULL))
  }
  
  info <- file.info(path)
  
  row <- tibble::tibble(
    step = step,
    path = normalizePath(path, winslash = "/", mustWork = FALSE),
    size_mb = as.numeric(info$size) / 1024^2
  )
  
  if (!file.exists(file_size_file)) {
    readr::write_csv(row, file_size_file)
  } else {
    readr::write_csv(row, file_size_file, append = TRUE)
  }
  
  invisible(NULL)
}


# =========================================================
# 7. Utility functions
# =========================================================

safe_read_parquet <- function(path) {
  if (!file.exists(path)) {
    return(NULL)
  }
  
  tryCatch(
    arrow::read_parquet(path),
    error = function(e) NULL
  )
}

safe_nrow_parquet <- function(path) {
  tbl <- safe_read_parquet(path)
  
  if (is.null(tbl)) {
    return(NA_integer_)
  }
  
  as.integer(nrow(tbl))
}

file_has_rows <- function(path) {
  n <- safe_nrow_parquet(path)
  is.finite(n) && !is.na(n) && n > 0
}

remove_if_exists <- function(path) {
  if (file.exists(path) || dir.exists(path)) {
    unlink(path, recursive = TRUE, force = TRUE)
  }
  
  invisible(NULL)
}

remove_paths <- function(paths_to_remove) {
  if (is.null(paths_to_remove) || length(paths_to_remove) == 0) {
    return(invisible(NULL))
  }
  
  for (p in paths_to_remove) {
    remove_if_exists(p)
  }
  
  invisible(NULL)
}

is_forced <- function(step_name) {
  step_name %in% force_steps
}


# =========================================================
# 8. Completion checks
# =========================================================

is_step_complete_010 <- function() {
  file_has_rows(file.path(paths$sim, "valid_parameter_registry.parquet")) &&
    file_has_rows(file.path(paths$sim, "simulation_grid.parquet")) &&
    file.exists(file.path(paths$config, "parameter_registry.rds"))
}

is_step_complete_020 <- function() {
  fams <- config$experiment$families
  
  all(vapply(
    fams,
    function(fam) {
      file_has_rows(file.path(paths$features, paste0("feature_table_", fam, ".parquet")))
    },
    logical(1)
  ))
}

is_step_complete_021 <- function() {
  fams <- config$experiment$families
  
  all(vapply(
    fams,
    function(fam) {
      files_i <- list.files(
        paths$gof,
        pattern = paste0("^gof_table_", fam, "_chunk_[0-9]{5}\\.parquet$"),
        full.names = TRUE
      )
      
      length(files_i) > 0 && all(vapply(files_i, file_has_rows, logical(1)))
    },
    logical(1)
  ))
}

is_step_complete_022 <- function() {
  fams <- config$experiment$families
  
  all(vapply(
    fams,
    function(fam) {
      file_has_rows(file.path(paths$gof, paste0("gof_table_", fam, ".parquet")))
    },
    logical(1)
  ))
}

is_step_complete_030 <- function() {
  file_has_rows(file.path(paths$datasets, "train.parquet")) &&
    file_has_rows(file.path(paths$datasets, "valid.parquet")) &&
    file_has_rows(file.path(paths$datasets, "test.parquet")) &&
    file.exists(file.path(paths$datasets, "feature_scenarios.rds")) &&
    file_has_rows(file.path(paths$datasets, "feature_scenario_info.parquet"))
}

is_step_complete_040 <- function() {
  metrics_path <- file.path(paths$models, "validation_metrics_round1.parquet")
  registry_path <- file.path(paths$models, "training_registry_round1.parquet")
  
  metrics_ok <- file_has_rows(metrics_path)
  registry_ok <- file_has_rows(registry_path)
  
  if (!metrics_ok || !registry_ok) {
    return(FALSE)
  }
  
  registry_tbl <- safe_read_parquet(registry_path)
  
  if (is.null(registry_tbl)) {
    return(FALSE)
  }
  
  if (!all(c("scenario", "model", "status", "keep_model_file", "model_file") %in% names(registry_tbl))) {
    return(FALSE)
  }
  
  kept_tbl <- registry_tbl %>%
    filter(status == "ok", keep_model_file)
  
  if (nrow(kept_tbl) == 0) {
    return(FALSE)
  }
  
  model_files_exist <- vapply(
    kept_tbl$model_file,
    function(p) !is.na(p) && nzchar(p) && file.exists(p),
    logical(1)
  )
  
  all(model_files_exist)
}

is_step_complete_050 <- function() {
  required_files <- c(
    "best_model_overall_from_validation.parquet",
    "best_models_by_scenario_from_validation.parquet",
    "test_predictions_best_overall.parquet",
    "test_metrics_best_overall.parquet",
    "test_by_class_best_overall.parquet",
    "test_by_n_best_overall.parquet",
    "test_predictions_best_by_scenario.parquet",
    "test_metrics_best_by_scenario.parquet",
    "test_by_class_best_by_scenario.parquet",
    "test_by_n_best_by_scenario.parquet"
  )
  
  all(vapply(
    required_files,
    function(f) {
      file_has_rows(file.path(paths$eval, f))
    },
    logical(1)
  ))
}


# =========================================================
# 9. Summaries
# =========================================================

summary_010 <- function() {
  log_msg_900("[010] valid_parameter_registry rows: ",
              safe_nrow_parquet(file.path(paths$sim, "valid_parameter_registry.parquet")))
  log_msg_900("[010] simulation_grid rows: ",
              safe_nrow_parquet(file.path(paths$sim, "simulation_grid.parquet")))
}

summary_020 <- function() {
  for (fam in config$experiment$families) {
    p <- file.path(paths$features, paste0("feature_table_", fam, ".parquet"))
    log_msg_900("[020] feature_table_", fam, " rows: ", safe_nrow_parquet(p))
  }
}

summary_021 <- function() {
  for (fam in config$experiment$families) {
    files_i <- list.files(
      paths$gof,
      pattern = paste0("^gof_table_", fam, "_chunk_[0-9]{5}\\.parquet$"),
      full.names = TRUE
    )
    
    n_chunks <- length(files_i)
    
    log_msg_900("[021] GOF chunks for ", fam, ": ", n_chunks)
    
    if (n_chunks > 0) {
      chunk_rows <- vapply(files_i, safe_nrow_parquet, integer(1))
      log_msg_900(
        "[021] GOF chunk rows for ", fam,
        " | total = ", sum(chunk_rows, na.rm = TRUE),
        " | min = ", min(chunk_rows, na.rm = TRUE),
        " | max = ", max(chunk_rows, na.rm = TRUE)
      )
    }
  }
}

summary_022 <- function() {
  for (fam in config$experiment$families) {
    p <- file.path(paths$gof, paste0("gof_table_", fam, ".parquet"))
    log_msg_900("[022] gof_table_", fam, " rows: ", safe_nrow_parquet(p))
  }
}

summary_030 <- function() {
  log_msg_900("[030] train rows: ",
              safe_nrow_parquet(file.path(paths$datasets, "train.parquet")))
  log_msg_900("[030] valid rows: ",
              safe_nrow_parquet(file.path(paths$datasets, "valid.parquet")))
  log_msg_900("[030] test rows: ",
              safe_nrow_parquet(file.path(paths$datasets, "test.parquet")))
  
  scenario_path <- file.path(paths$datasets, "feature_scenario_info.parquet")
  
  if (file.exists(scenario_path)) {
    scen_tbl <- arrow::read_parquet(scenario_path)
    capture.output(print(scen_tbl), file = log_file, append = TRUE)
  }
}

summary_040 <- function() {
  metrics_path <- file.path(paths$models, "validation_metrics_round1.parquet")
  registry_path <- file.path(paths$models, "training_registry_round1.parquet")
  
  log_msg_900("[040] validation_metrics exists: ", file.exists(metrics_path))
  log_msg_900("[040] training_registry exists: ", file.exists(registry_path))
  
  if (file.exists(metrics_path)) {
    metrics_tbl <- arrow::read_parquet(metrics_path)
    capture.output(print(metrics_tbl), file = log_file, append = TRUE)
  }
  
  if (file.exists(registry_path)) {
    registry_tbl <- arrow::read_parquet(registry_path)
    capture.output(print(registry_tbl), file = log_file, append = TRUE)
  }
}

summary_050 <- function() {
  metrics_overall <- file.path(paths$eval, "test_metrics_best_overall.parquet")
  metrics_by_scenario <- file.path(paths$eval, "test_metrics_best_by_scenario.parquet")
  
  log_msg_900("[050] test_metrics_best_overall exists: ", file.exists(metrics_overall))
  log_msg_900("[050] test_metrics_best_by_scenario exists: ", file.exists(metrics_by_scenario))
  
  if (file.exists(metrics_overall)) {
    tbl <- arrow::read_parquet(metrics_overall)
    capture.output(print(tbl), file = log_file, append = TRUE)
  }
  
  if (file.exists(metrics_by_scenario)) {
    tbl <- arrow::read_parquet(metrics_by_scenario)
    capture.output(print(tbl), file = log_file, append = TRUE)
  }
}


# =========================================================
# 10. Clean paths by step
# =========================================================

clean_010 <- c(
  file.path(paths$config, "parameter_registry.rds"),
  file.path(paths$sim, "valid_parameter_registry.parquet"),
  file.path(paths$sim, "invalid_parameter_registry.parquet"),
  file.path(paths$sim, "simulation_grid.parquet")
)

clean_020 <- c(
  file.path(paths$features, paste0("feature_table_", config$experiment$families, ".parquet")),
  list.files(paths$features, pattern = "^feature_table_.*_shards$", full.names = TRUE)
)

clean_021 <- c(
  list.files(paths$gof, pattern = "^gof_table_.*_chunk_[0-9]{5}\\.parquet$", full.names = TRUE),
  file.path(paths$gof, "session_info_021.txt")
)

clean_022 <- file.path(
  paths$gof,
  paste0("gof_table_", config$experiment$families, ".parquet")
)

clean_030 <- c(
  file.path(paths$datasets, "full_dataset.parquet"),
  file.path(paths$datasets, "train.parquet"),
  file.path(paths$datasets, "valid.parquet"),
  file.path(paths$datasets, "test.parquet"),
  file.path(paths$datasets, "feature_scenarios.rds"),
  file.path(paths$datasets, "feature_scenario_info.parquet"),
  file.path(paths$datasets, "train_shards"),
  file.path(paths$datasets, "valid_shards"),
  file.path(paths$datasets, "test_shards"),
  file.path(paths$datasets, "full_shards")
)

clean_040 <- c(
  file.path(paths$models, "validation_metrics_round1.parquet"),
  file.path(paths$models, "validation_predictions_round1.parquet"),
  file.path(paths$models, "training_registry_round1.parquet"),
  file.path(paths$models, "fitted_models_round1"),
  file.path(paths$models, "validation_metric_shards"),
  file.path(paths$models, "validation_prediction_shards"),
  file.path(paths$models, "training_registry_shards")
)

clean_050 <- c(
  file.path(paths$eval, "best_model_overall_from_validation.parquet"),
  file.path(paths$eval, "best_models_by_scenario_from_validation.parquet"),
  file.path(paths$eval, "test_predictions_best_overall.parquet"),
  file.path(paths$eval, "test_metrics_best_overall.parquet"),
  file.path(paths$eval, "test_by_class_best_overall.parquet"),
  file.path(paths$eval, "test_by_n_best_overall.parquet"),
  file.path(paths$eval, "test_predictions_best_by_scenario.parquet"),
  file.path(paths$eval, "test_metrics_best_by_scenario.parquet"),
  file.path(paths$eval, "test_by_class_best_by_scenario.parquet"),
  file.path(paths$eval, "test_by_n_best_by_scenario.parquet"),
  file.path(paths$eval, "shards_best_overall"),
  file.path(paths$eval, "shards_best_by_scenario")
)


# =========================================================
# 11. Step runner
# =========================================================

run_step_if_needed <- function(
    step_name,
    script_file,
    is_complete_fun,
    summary_fun = NULL,
    clean_paths = NULL,
    output_paths = NULL
) {
  
  script_path <- file.path(root_dir, "scripts", script_file)
  
  if (!file.exists(script_path)) {
    stop("[900] Step script not found: ", script_path)
  }
  
  already_done <- is_complete_fun()
  forced <- is_forced(step_name)
  
  log_section(paste("CHECKING", step_name))
  log_msg_900("Script: ", script_path)
  log_msg_900("Already complete: ", already_done)
  log_msg_900("Forced rerun: ", forced)
  
  if (already_done && !forced) {
    log_msg_900("Skipping ", step_name, " because outputs already exist and passed validation.")
    
    if (!is.null(summary_fun)) {
      try(summary_fun(), silent = FALSE)
    }
    
    append_timing(step_name, Sys.time(), Sys.time(), status = "SKIPPED", note = "Already complete")
    
    return(invisible(NULL))
  }
  
  if (step_name %in% clean_steps || forced) {
    log_msg_900("Cleaning outputs for step: ", step_name)
    remove_paths(clean_paths)
  }
  
  t0 <- Sys.time()
  
  res <- tryCatch(
    {
      source(script_path, local = new.env(parent = globalenv()))
      list(ok = TRUE, error = NULL)
    },
    error = function(e) {
      list(ok = FALSE, error = e)
    }
  )
  
  t1 <- Sys.time()
  
  if (isTRUE(res$ok)) {
    log_msg_900(step_name, " finished successfully.")
    append_timing(step_name, t0, t1, status = "OK")
    
    if (!is.null(summary_fun)) {
      try(summary_fun(), silent = FALSE)
    }
    
    if (!is.null(output_paths)) {
      for (p in output_paths) {
        append_file_size(step_name, p)
      }
    }
    
  } else {
    log_msg_900(step_name, " FAILED.")
    log_msg_900("Error: ", conditionMessage(res$error))
    
    append_timing(
      step_name,
      t0,
      t1,
      status = "FAILED",
      note = conditionMessage(res$error)
    )
    
    stop(res$error)
  }
  
  invisible(NULL)
}


# =========================================================
# 12. Run header
# =========================================================

log_section("FADS_AI PIPELINE RUN START")

log_msg_900("Run label: ", run_label)
log_msg_900("Repository root: ", root_dir)
log_msg_900("Run mode: ", run_mode)
log_msg_900("Force steps: ", if (length(force_steps) == 0) "<none>" else paste(force_steps, collapse = ", "))
log_msg_900("Clean steps: ", if (length(clean_steps) == 0) "<none>" else paste(clean_steps, collapse = ", "))
log_msg_900("Log file: ", log_file)

log_msg_900("Families: ", paste(config$experiment$families, collapse = ", "))
log_msg_900("Sample sizes: ", paste(config$experiment$sample_sizes, collapse = ", "))
log_msg_900("n_rep_mc: ", config$experiment$n_rep_mc)
log_msg_900("Active ML models: ", paste(config$ml$models, collapse = ", "))
log_msg_900("Primary metric: ", config$ml$metric_primary)


# =========================================================
# 13. Run pipeline
# =========================================================

run_step_if_needed(
  step_name = "010_simulate_samples",
  script_file = "010_simulate_samples.R",
  is_complete_fun = is_step_complete_010,
  summary_fun = summary_010,
  clean_paths = clean_010,
  output_paths = clean_010
)

run_step_if_needed(
  step_name = "020_extract_features",
  script_file = "020_extract_features.R",
  is_complete_fun = is_step_complete_020,
  summary_fun = summary_020,
  clean_paths = clean_020
)

run_step_if_needed(
  step_name = "021_extract_gof_features",
  script_file = "021_extract_gof_features.R",
  is_complete_fun = is_step_complete_021,
  summary_fun = summary_021,
  clean_paths = clean_021
)

run_step_if_needed(
  step_name = "022_combine_gof_chunks",
  script_file = "022_combine_gof_chunks.R",
  is_complete_fun = is_step_complete_022,
  summary_fun = summary_022,
  clean_paths = clean_022,
  output_paths = clean_022
)

run_step_if_needed(
  step_name = "030_build_datasets",
  script_file = "030_build_datasets.R",
  is_complete_fun = is_step_complete_030,
  summary_fun = summary_030,
  clean_paths = clean_030
)

run_step_if_needed(
  step_name = "040_train_models",
  script_file = "040_train_models.R",
  is_complete_fun = is_step_complete_040,
  summary_fun = summary_040,
  clean_paths = clean_040
)

run_step_if_needed(
  step_name = "050_evaluate_models",
  script_file = "050_evaluate_models.R",
  is_complete_fun = is_step_complete_050,
  summary_fun = summary_050,
  clean_paths = clean_050
)


# =========================================================
# 14. Final summary
# =========================================================

log_section("FADS_AI PIPELINE RUN FINISHED")

if (file.exists(timing_file)) {
  timing_tbl <- readr::read_csv(timing_file, show_col_types = FALSE)
  
  total_sec <- sum(
    timing_tbl$elapsed_sec[timing_tbl$status == "OK"],
    na.rm = TRUE
  )
  
  log_msg_900("Total elapsed seconds for executed steps: ", round(total_sec, 2))
  log_msg_900("Total elapsed hours for executed steps: ", round(total_sec / 3600, 2))
  
  capture.output(print(timing_tbl), file = log_file, append = TRUE)
  capture.output(print(timing_tbl), file = summary_file, append = FALSE)
}

if (file.exists(file_size_file)) {
  size_tbl <- readr::read_csv(file_size_file, show_col_types = FALSE)
  capture.output(print(size_tbl), file = log_file, append = TRUE)
}

log_msg_900("Summary file: ", summary_file)
log_msg_900("Timing file: ", timing_file)
log_msg_900("File size file: ", file_size_file)
log_msg_900("Log file: ", log_file)

log_msg_900("[900] Full FADS_AI pipeline completed.")