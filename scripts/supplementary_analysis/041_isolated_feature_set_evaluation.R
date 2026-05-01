
# =========================================================
# 041_isolated_feature_set_evaluation.R
# Evaluate isolated feature groups using the 10K datasets
#
# Purpose:
# Test the predictive contribution of feature groups when used
# in isolation, rather than nested on top of the classical set.
#
# Isolated feature sets created:
# - classical_only
# - lmrd_only
# - gof_ic_only
# - bayes_only
# - gof_ic_bayes_only
#
# Default model set:
# - xgb only
#
# Optional runtime control:
# options(seaf.ml_subset = c("xgb", "multinom"))
#
# Inputs:
# - code_output/datasets/train.parquet
# - code_output/datasets/valid.parquet
# - code_output/datasets/test.parquet
#
# Outputs:
# - code_output/ablation_isolated_scenarios/isolated_feature_sets.rds
# - code_output/ablation_isolated_scenarios/isolated_feature_set_info.csv
# - code_output/ablation_isolated_scenarios/validation_metrics.parquet
# - code_output/ablation_isolated_scenarios/test_metrics.parquet
# - code_output/ablation_isolated_scenarios/test_by_class.parquet
# - code_output/ablation_isolated_scenarios/test_by_n.parquet
# - code_output/ablation_isolated_scenarios/run_report.txt
# =========================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(tibble)
  library(readr)
  library(arrow)
  library(yardstick)
})

source("script/fun/fun_io.R")
source("script/fun/fun_ml.R")

config <- readRDS("code_output/config/config_main.rds")
paths <- config$paths

dataset_dir <- paths$datasets
out_dir <- file.path(paths$output, "ablation_isolated_scenarios")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# ---------------------------------------------------------
# 0. Runtime controls
# ---------------------------------------------------------
model_names_all <- c("xgb")
run_models <- getOption("seaf.ml_subset", default = model_names_all)

if (is.null(run_models) || length(run_models) == 0) {
  run_models <- model_names_all
}

run_models <- unique(as.character(run_models))
validate_model_names(run_models)

# ---------------------------------------------------------
# 1. Read datasets
# ---------------------------------------------------------
log_msg("Reading 10K datasets for isolated-scenario evaluation...")

train_tbl <- read_parquet(file.path(dataset_dir, "train.parquet"))
valid_tbl <- read_parquet(file.path(dataset_dir, "valid.parquet"))
test_tbl  <- read_parquet(file.path(dataset_dir, "test.parquet"))

all_names <- names(train_tbl)

# ---------------------------------------------------------
# 2. Build isolated feature sets
# Same naming logic already used in fun_ml.R / 030
# ---------------------------------------------------------
classical_cols <- intersect(
  c("lmom_l1", "lmom_l2", "lmom_l3", "lmom_l4", "lmom_t3", "lmom_t4"),
  all_names
)

lmrd_cols <- grep("^lmrd_", all_names, value = TRUE)

gof_stat_cols <- grep("^(KS_stat|AD_stat|CvM_stat)_", all_names, value = TRUE)
ic_cols <- grep("^(AIC|BIC)_", all_names, value = TRUE)
gof_ic_cols <- unique(c(gof_stat_cols, ic_cols))

bayes_cols <- grep(
  "^(KS_bfmin|KS_elogbf|AD_bfmin|AD_elogbf|CvM_bfmin|CvM_elogbf)_",
  all_names,
  value = TRUE
)

isolated_feature_sets <- list(
  classical_only = classical_cols,
  lmrd_only = lmrd_cols,
  gof_ic_only = gof_ic_cols,
  bayes_only = bayes_cols,
  gof_ic_bayes_only = unique(c(gof_ic_cols, bayes_cols))
)

# Keep only non-empty sets
isolated_feature_sets <- isolated_feature_sets[
  vapply(isolated_feature_sets, length, integer(1)) > 0
]

if (length(isolated_feature_sets) == 0) {
  stop("No isolated feature sets could be constructed from the dataset columns.")
}

feature_info <- tibble(
  scenario = names(isolated_feature_sets),
  n_features = vapply(isolated_feature_sets, length, integer(1)),
  feature_names = vapply(isolated_feature_sets, function(x) paste(x, collapse = "; "), character(1))
)

saveRDS(isolated_feature_sets, file.path(out_dir, "isolated_feature_sets.rds"))
write_csv(feature_info, file.path(out_dir, "isolated_feature_set_info.csv"))

# ---------------------------------------------------------
# 3. Train on train, evaluate on valid and test
# ---------------------------------------------------------
validation_metric_rows <- list()
test_metric_rows <- list()
test_by_class_rows <- list()
test_by_n_rows <- list()

counter <- 0L

for (sc_name in names(isolated_feature_sets)) {
  sc_cols <- isolated_feature_sets[[sc_name]]
  
  log_msg("Processing isolated scenario: ", sc_name,
          " | n_features = ", length(sc_cols))
  
  for (mdl in run_models) {
    counter <- counter + 1L
    
    log_msg("Training isolated scenario = ", sc_name, " | model = ", mdl)
    
    fit_out <- tryCatch(
      fit_one_model(
        train_tbl = train_tbl,
        scenario_cols = sc_cols,
        model_name = mdl
      ),
      error = function(e) e
    )
    
    if (inherits(fit_out, "error")) {
      validation_metric_rows[[length(validation_metric_rows) + 1L]] <- tibble(
        .metric = c("accuracy", "kap"),
        .estimator = "multiclass",
        .estimate = NA_real_,
        scenario = sc_name,
        model = mdl,
        stage = "validation",
        status = "fit_failed",
        note = conditionMessage(fit_out)
      )
      
      test_metric_rows[[length(test_metric_rows) + 1L]] <- tibble(
        .metric = c("accuracy", "kap"),
        .estimator = "multiclass",
        .estimate = NA_real_,
        scenario = sc_name,
        model = mdl,
        stage = "test",
        status = "fit_failed",
        note = conditionMessage(fit_out)
      )
      
      next
    }
    
    # ----------------------------
    # Validation
    # ----------------------------
    pred_valid <- tryCatch(
      predict_multiclass(
        fit_obj = fit_out$fit,
        new_data = select_scenario_dataset(valid_tbl, sc_cols)
      ),
      error = function(e) e
    )
    
    if (inherits(pred_valid, "error")) {
      validation_metric_rows[[length(validation_metric_rows) + 1L]] <- tibble(
        .metric = c("accuracy", "kap"),
        .estimator = "multiclass",
        .estimate = NA_real_,
        scenario = sc_name,
        model = mdl,
        stage = "validation",
        status = "predict_failed",
        note = conditionMessage(pred_valid)
      )
      
      test_metric_rows[[length(test_metric_rows) + 1L]] <- tibble(
        .metric = c("accuracy", "kap"),
        .estimator = "multiclass",
        .estimate = NA_real_,
        scenario = sc_name,
        model = mdl,
        stage = "test",
        status = "predict_failed",
        note = "Validation prediction failed; test skipped."
      )
      
      next
    }
    
    valid_eval <- tryCatch(
      evaluate_predictions(pred_valid),
      error = function(e) e
    )
    
    if (inherits(valid_eval, "error")) {
      validation_metric_rows[[length(validation_metric_rows) + 1L]] <- tibble(
        .metric = c("accuracy", "kap"),
        .estimator = "multiclass",
        .estimate = NA_real_,
        scenario = sc_name,
        model = mdl,
        stage = "validation",
        status = "eval_failed",
        note = conditionMessage(valid_eval)
      )
      
      test_metric_rows[[length(test_metric_rows) + 1L]] <- tibble(
        .metric = c("accuracy", "kap"),
        .estimator = "multiclass",
        .estimate = NA_real_,
        scenario = sc_name,
        model = mdl,
        stage = "test",
        status = "eval_failed",
        note = "Validation evaluation failed; test skipped."
      )
      
      next
    }
    
    validation_metric_rows[[length(validation_metric_rows) + 1L]] <- valid_eval$metrics %>%
      mutate(
        scenario = sc_name,
        model = mdl,
        stage = "validation",
        status = "ok",
        note = NA_character_
      )
    
    # ----------------------------
    # Test
    # ----------------------------
    pred_test <- tryCatch(
      predict_multiclass(
        fit_obj = fit_out$fit,
        new_data = select_scenario_dataset(test_tbl, sc_cols)
      ),
      error = function(e) e
    )
    
    if (inherits(pred_test, "error")) {
      test_metric_rows[[length(test_metric_rows) + 1L]] <- tibble(
        .metric = c("accuracy", "kap"),
        .estimator = "multiclass",
        .estimate = NA_real_,
        scenario = sc_name,
        model = mdl,
        stage = "test",
        status = "predict_failed",
        note = conditionMessage(pred_test)
      )
      
      next
    }
    
    test_eval <- tryCatch(
      evaluate_predictions(pred_test),
      error = function(e) e
    )
    
    if (inherits(test_eval, "error")) {
      test_metric_rows[[length(test_metric_rows) + 1L]] <- tibble(
        .metric = c("accuracy", "kap"),
        .estimator = "multiclass",
        .estimate = NA_real_,
        scenario = sc_name,
        model = mdl,
        stage = "test",
        status = "eval_failed",
        note = conditionMessage(test_eval)
      )
      
      next
    }
    
    test_metric_rows[[length(test_metric_rows) + 1L]] <- test_eval$metrics %>%
      mutate(
        scenario = sc_name,
        model = mdl,
        stage = "test",
        status = "ok",
        note = NA_character_
      )
    
    test_by_class_rows[[length(test_by_class_rows) + 1L]] <- pred_test %>%
      group_by(true_family) %>%
      summarise(
        accuracy = mean(.pred_class == true_family),
        .groups = "drop"
      ) %>%
      mutate(
        scenario = sc_name,
        model = mdl
      )
    
    test_by_n_rows[[length(test_by_n_rows) + 1L]] <- pred_test %>%
      group_by(n) %>%
      summarise(
        accuracy = mean(.pred_class == true_family),
        .groups = "drop"
      ) %>%
      mutate(
        scenario = sc_name,
        model = mdl
      )
    
    rm(pred_valid, pred_test, valid_eval, test_eval, fit_out)
    invisible(gc())
  }
}

validation_metrics_tbl <- bind_rows(validation_metric_rows)
test_metrics_tbl <- bind_rows(test_metric_rows)
test_by_class_tbl <- bind_rows(test_by_class_rows)
test_by_n_tbl <- bind_rows(test_by_n_rows)

write_parquet(validation_metrics_tbl, file.path(out_dir, "validation_metrics.parquet"))
write_parquet(test_metrics_tbl, file.path(out_dir, "test_metrics.parquet"))
write_parquet(test_by_class_tbl, file.path(out_dir, "test_by_class.parquet"))
write_parquet(test_by_n_tbl, file.path(out_dir, "test_by_n.parquet"))

# ---------------------------------------------------------
# 4. Report
# ---------------------------------------------------------
report_lines <- c(
  "======================================================",
  "ISOLATED FEATURE SET EVALUATION REPORT",
  "======================================================",
  paste0("Output directory: ", out_dir),
  "",
  paste0("Models evaluated: ", paste(run_models, collapse = ", ")),
  "",
  "Isolated feature sets:"
)

for (i in seq_len(nrow(feature_info))) {
  report_lines <- c(
    report_lines,
    paste0(
      "- ", feature_info$scenario[i],
      " | n_features = ", feature_info$n_features[i]
    )
  )
}

report_lines <- c(
  report_lines,
  "",
  paste0("Validation metric rows: ", nrow(validation_metrics_tbl)),
  paste0("Test metric rows: ", nrow(test_metrics_tbl)),
  paste0("Test by class rows: ", nrow(test_by_class_tbl)),
  paste0("Test by n rows: ", nrow(test_by_n_tbl))
)

write_lines(report_lines, file.path(out_dir, "run_report.txt"))

log_msg("041_isolated_feature_set_evaluation.R finished successfully.")