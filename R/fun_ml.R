# =========================================================
# fun_ml.R
# Dataset construction, model training, and evaluation
# utilities for FADS_AI
# =========================================================
#
# This file provides utilities for:
#   - reading and stacking feature/GOF tables;
#   - building the full ML dataset;
#   - defining feature scenarios;
#   - splitting data into train/validation/test sets;
#   - fitting candidate classifiers;
#   - evaluating validation and test performance.
#
# Important:
#   This file supports several model types. The active model
#   set used in a given experiment is defined in scripts/000_config.R.
# =========================================================

library(dplyr)
library(tibble)
library(purrr)
library(recipes)
library(parsnip)
library(workflows)
library(yardstick)
library(tidyselect)


# ---------------------------------------------------------
# Build full dataset by joining feature and GOF outputs
# ---------------------------------------------------------

build_full_ml_dataset <- function(feature_tbl, gof_tbl) {
  
  if (!"sample_id" %in% names(feature_tbl)) {
    stop("feature_tbl must contain column 'sample_id'.")
  }
  
  if (!"sample_id" %in% names(gof_tbl)) {
    stop("gof_tbl must contain column 'sample_id'.")
  }
  
  out <- feature_tbl %>%
    left_join(gof_tbl, by = "sample_id")
  
  if (!"true_family" %in% names(out)) {
    stop("Column 'true_family' not found in merged dataset.")
  }
  
  out %>%
    mutate(true_family = as.factor(true_family))
}



# ---------------------------------------------------------
# Create stratification label
# ---------------------------------------------------------

build_strata_label <- function(df) {
  
  if (!all(c("true_family", "n") %in% names(df))) {
    stop("df must contain 'true_family' and 'n' for stratification.")
  }
  
  paste(df$true_family, df$n, sep = "__")
}


# =========================================================
# TRAINING UTILITIES
# =========================================================


# ---------------------------------------------------------
# Validate model names from config
# ---------------------------------------------------------

validate_model_names <- function(model_names) {
  
  valid_models <- c("multinom", "rf", "xgb", "mlp", "cart", "svm_rbf")
  
  invalid <- setdiff(model_names, valid_models)
  
  if (length(invalid) > 0) {
    stop("Invalid models in config$ml$models: ", paste(invalid, collapse = ", "))
  }
  
  invisible(TRUE)
}


# ---------------------------------------------------------
# Select dataset columns for one scenario
# ---------------------------------------------------------

select_scenario_dataset <- function(df, scenario_cols) {
  
  id_cols <- c(
    "sample_id", "true_family", "param_id", "n",
    "replicate_id", "seed", "par1", "par2", "par3"
  )
  
  keep_cols <- unique(c(id_cols, scenario_cols))
  keep_cols <- keep_cols[keep_cols %in% names(df)]
  
  df[, keep_cols, drop = FALSE]
}


# ---------------------------------------------------------
# Build recipe for one scenario
# ---------------------------------------------------------

build_recipe_for_scenario <- function(train_tbl, scenario_cols, normalize = FALSE) {
  
  rec_tbl <- select_scenario_dataset(train_tbl, scenario_cols)
  
  id_vars <- intersect(
    c("sample_id", "param_id", "replicate_id", "seed"),
    names(rec_tbl)
  )
  
  metadata_vars <- intersect(
    c("par1", "par2", "par3", "n"),
    names(rec_tbl)
  )
  
  rec <- recipes::recipe(true_family ~ ., data = rec_tbl)
  
  if (length(id_vars) > 0) {
    rec <- rec %>%
      recipes::update_role(
        tidyselect::all_of(id_vars),
        new_role = "id variable"
      )
  }
  
  if (length(metadata_vars) > 0) {
    rec <- rec %>%
      recipes::update_role(
        tidyselect::all_of(metadata_vars),
        new_role = "metadata"
      )
  }
  
  rec <- rec %>%
    recipes::step_impute_median(recipes::all_numeric_predictors()) %>%
    recipes::step_impute_mode(recipes::all_nominal_predictors()) %>%
    recipes::step_zv(recipes::all_predictors())
  
  if (normalize) {
    rec <- rec %>%
      recipes::step_normalize(recipes::all_numeric_predictors())
  }
  
  rec
}


# ---------------------------------------------------------
# Whether model uses normalization in this pipeline
# ---------------------------------------------------------

model_requires_normalization <- function(model_name) {
  model_name %in% c("multinom", "xgb", "mlp", "svm_rbf")
}


# ---------------------------------------------------------
# Build model specification
# ---------------------------------------------------------

build_model_spec <- function(model_name, n_features = NULL) {
  
  if (model_name == "multinom") {
    return(
      parsnip::multinom_reg(
        penalty = 0
      ) %>%
        parsnip::set_engine("nnet", trace = FALSE, MaxNWts = 100000) %>%
        parsnip::set_mode("classification")
    )
  }
  
  if (model_name == "rf") {
    return(
      parsnip::rand_forest(
        trees = 100,
        mtry = NULL,
        min_n = 5
      ) %>%
        parsnip::set_engine(
          "ranger",
          importance = "none",
          num.threads = 1
        ) %>%
        parsnip::set_mode("classification")
    )
  }
  
  if (model_name == "xgb") {
    return(
      parsnip::boost_tree(
        trees = 500,
        tree_depth = 6,
        learn_rate = 0.05,
        min_n = 5,
        loss_reduction = 0,
        sample_size = 0.8,
        stop_iter = 25
      ) %>%
        parsnip::set_engine("xgboost", nthread = 1, verbosity = 0) %>%
        parsnip::set_mode("classification")
    )
  }
  
  if (model_name == "mlp") {
    return(
      parsnip::mlp(
        hidden_units = 10,
        penalty = 0.001,
        epochs = 100
      ) %>%
        parsnip::set_engine("nnet", trace = FALSE, MaxNWts = 100000) %>%
        parsnip::set_mode("classification")
    )
  }
  
  if (model_name == "cart") {
    return(
      parsnip::decision_tree(
        cost_complexity = 0.001,
        tree_depth = 30,
        min_n = 20
      ) %>%
        parsnip::set_engine("rpart") %>%
        parsnip::set_mode("classification")
    )
  }
  
  if (model_name == "svm_rbf") {
    return(
      parsnip::svm_rbf(
        cost = 0.5,
        rbf_sigma = 0.05
      ) %>%
        parsnip::set_engine("kernlab", prob.model = FALSE) %>%
        parsnip::set_mode("classification")
    )
  }
  
  stop("Unknown model: ", model_name)
}


# ---------------------------------------------------------
# Fit one workflow
# ---------------------------------------------------------

fit_one_model <- function(train_tbl, scenario_cols, model_name) {
  
  validate_model_names(model_name)
  
  train_sc <- select_scenario_dataset(train_tbl, scenario_cols)
  
  train_sc <- train_sc %>%
    mutate(true_family = as.factor(true_family))
  
  rec <- build_recipe_for_scenario(
    train_tbl = train_sc,
    scenario_cols = scenario_cols,
    normalize = model_requires_normalization(model_name)
  )
  
  spec <- build_model_spec(
    model_name = model_name,
    n_features = length(scenario_cols)
  )
  
  wf <- workflows::workflow() %>%
    workflows::add_recipe(rec) %>%
    workflows::add_model(spec)
  
  fit <- parsnip::fit(wf, data = train_sc)
  
  list(
    workflow = wf,
    fit = fit
  )
}


# ---------------------------------------------------------
# Predict on validation/test set
# Robust to probability prediction failures
# ---------------------------------------------------------

predict_multiclass <- function(fit_obj, new_data) {
  
  new_data <- new_data %>%
    mutate(true_family = as.factor(true_family))
  
  pred_class <- predict(fit_obj, new_data = new_data, type = "class")
  
  pred_prob <- tryCatch(
    predict(fit_obj, new_data = new_data, type = "prob"),
    error = function(e) NULL
  )
  
  base_tbl <- bind_cols(
    new_data %>% select(sample_id, true_family, n, param_id),
    pred_class
  )
  
  if (!is.null(pred_prob)) {
    return(bind_cols(base_tbl, pred_prob))
  }
  
  class_levels <- levels(new_data$true_family)
  
  prob_na_tbl <- tibble::as_tibble(
    setNames(
      replicate(length(class_levels), rep(NA_real_, nrow(new_data)), simplify = FALSE),
      paste0(".pred_", class_levels)
    )
  )
  
  bind_cols(base_tbl, prob_na_tbl)
}


# ---------------------------------------------------------
# Evaluate predictions
# ---------------------------------------------------------

evaluate_predictions <- function(pred_tbl) {
  
  metrics_tbl <- yardstick::metric_set(
    yardstick::accuracy,
    yardstick::kap
  )(
    data = pred_tbl,
    truth = true_family,
    estimate = .pred_class
  )
  
  conf_mat_tbl <- yardstick::conf_mat(
    data = pred_tbl,
    truth = true_family,
    estimate = .pred_class
  )
  
  list(
    metrics = metrics_tbl,
    conf_mat = conf_mat_tbl
  )
}

