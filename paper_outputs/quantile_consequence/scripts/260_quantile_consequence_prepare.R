# =========================================================
# 260_quantile_consequence_prepare.R
#
# Purpose:
# Build the case-level quantile-consequence layer for the
# final selected setting:
# - 10K
# - prediction_scope = best_by_scenario
# - model = xgb
# - scenario = classical
#
# This script prepares the data used later by:
# - 150_table4.R
# - 261_fig06_quantile_consequence_global.R
# - 262_fig07_quantile_consequence_heatmap.R
#
# Outputs:
# paper_output/quantile_consequence/
#   - quantile_consequence_case_level.parquet
#   - quantile_consequence_case_level.csv
#   - quantile_consequence_qc_summary.csv
#   - quantile_consequence_channel_summary.csv
#   - quantile_consequence_report.txt
#
# Core definitions:
# Q_true(T) = quantile of the true generating family using
#             true population parameters + stored delta
#
# Q_pred(T) = quantile of the predicted family using the
#             sample-level fitted parameters already stored
#             in the workflow outputs for that candidate family
#
# Error metrics:
# RE  = (Q_pred - Q_true) / Q_true
# ARE = abs(Q_pred - Q_true) / Q_true
# =========================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(tibble)
  library(readr)
  library(arrow)
  library(stringr)
  library(lmom)
})

# ---------------------------------------------------------
# 0. PATHS
# ---------------------------------------------------------
base_root <- "C:/Users/wilso/OneDrive/My papers/SEAF_AI/code"

master_dir <- file.path(base_root, "paper_output", "results_master")
run_root <- file.path(base_root, "code_output_10K")

preds_path <- file.path(master_dir, "results_master_predictions.parquet")
full_dataset_path <- file.path(run_root, "datasets", "full_dataset.parquet")
param_registry_path <- file.path(run_root, "simulated_samples", "valid_parameter_registry.parquet")

out_dir <- file.path(base_root, "paper_output", "quantile_consequence")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

required_files <- c(preds_path, full_dataset_path, param_registry_path)
missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop("Missing required files: ", paste(missing_files, collapse = " | "))
}

# ---------------------------------------------------------
# 1. HELPERS
# ---------------------------------------------------------
save_csv <- function(df, filename) {
  write_csv(df, file.path(out_dir, filename))
}

save_parquet_safe <- function(df, filename) {
  write_parquet(df, file.path(out_dir, filename))
}

family_order <- c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM")
return_periods <- c(50, 100, 500)

safe_family_quantile <- function(p, family, par1, par2, par3 = NA_real_) {
  tryCatch({
    if (family == "GEV") {
      return(lmom::quagev(f = p, para = c(par1, par2, par3)))
    }
    if (family == "GPA") {
      return(lmom::quagpa(f = p, para = c(par1, par2, par3)))
    }
    if (family == "PE3") {
      return(lmom::quape3(f = p, para = c(par1, par2, par3)))
    }
    if (family == "LN2") {
      return(stats::qlnorm(p, meanlog = par1, sdlog = par2))
    }
    if (family == "LN3") {
      return(lmom::qualn3(f = p, para = c(par1, par2, par3)))
    }
    if (family == "GUM") {
      return(lmom::quagum(f = p, para = c(par1, par2)))
    }
    NA_real_
  }, error = function(e) NA_real_)
}

# ---------------------------------------------------------
# 2. FINAL SELECTED PREDICTIONS
# ---------------------------------------------------------
preds_master <- read_parquet(preds_path)

req_pred_cols <- c(
  "sample_id", "true_family", "pred_family",
  "scenario", "model", "n", "param_id",
  "run_label", "prediction_scope"
)

missing_pred_cols <- setdiff(req_pred_cols, names(preds_master))
if (length(missing_pred_cols) > 0) {
  stop(
    "results_master_predictions.parquet is missing required columns: ",
    paste(missing_pred_cols, collapse = ", ")
  )
}

preds_final <- preds_master %>%
  mutate(
    scope = case_when(
      prediction_scope == "best_overall" ~ "best_overall",
      prediction_scope == "best_by_scenario" ~ "best_by_scenario",
      TRUE ~ as.character(prediction_scope)
    )
  ) %>%
  filter(run_label == "10K") %>%
  filter(scope == "best_by_scenario") %>%
  filter(model == "xgb") %>%
  filter(scenario == "classical") %>%
  transmute(
    sample_id = as.character(sample_id),
    true_family = as.character(true_family),
    pred_family = as.character(pred_family),
    scenario = as.character(scenario),
    model = as.character(model),
    n = as.integer(n),
    param_id = as.character(param_id),
    is_correct = true_family == pred_family
  )

if (nrow(preds_final) == 0) {
  stop("No rows found for the final selected setting.")
}

# ---------------------------------------------------------
# 3. TRUE POPULATION PARAMETERS
# ---------------------------------------------------------
param_registry <- read_parquet(param_registry_path) %>%
  transmute(
    param_id = as.character(param_id),
    registry_family = as.character(family),
    true_par1 = as.numeric(par1),
    true_par2 = as.numeric(par2),
    true_par3 = as.numeric(par3),
    par1_name = as.character(par1_name),
    par2_name = as.character(par2_name),
    par3_name = as.character(par3_name),
    delta = as.numeric(delta)
  )

# ---------------------------------------------------------
# 4. SAMPLE-LEVEL FITTED PARAMETERS FOR ALL CANDIDATE FAMILIES
# ---------------------------------------------------------
required_fit_cols <- c(
  "sample_id",
  paste0("fit_par1_", family_order),
  paste0("fit_par2_", family_order),
  paste0("fit_par3_", family_order)
)

full_cols <- names(read_parquet(full_dataset_path))
missing_fit_cols <- setdiff(required_fit_cols, full_cols)

if (length(missing_fit_cols) > 0) {
  stop(
    "full_dataset.parquet is missing required fitted-parameter columns: ",
    paste(missing_fit_cols, collapse = ", ")
  )
}

fit_tbl <- read_parquet(full_dataset_path) %>%
  select(all_of(required_fit_cols)) %>%
  transmute(
    sample_id = as.character(sample_id),
    across(-sample_id, as.numeric)
  ) %>%
  filter(sample_id %in% preds_final$sample_id)

# ---------------------------------------------------------
# 5. BUILD CASE BASE
# ---------------------------------------------------------
qc_tbl <- preds_final %>%
  left_join(param_registry, by = "param_id") %>%
  left_join(fit_tbl, by = "sample_id")

if (any(is.na(qc_tbl$true_par1)) || any(is.na(qc_tbl$delta))) {
  stop("Missing true population metadata after join with parameter registry.")
}

qc_tbl$pred_fit_par1 <- NA_real_
qc_tbl$pred_fit_par2 <- NA_real_
qc_tbl$pred_fit_par3 <- NA_real_

for (fam in family_order) {
  idx <- which(qc_tbl$pred_family == fam)
  if (length(idx) == 0) next
  
  qc_tbl$pred_fit_par1[idx] <- qc_tbl[[paste0("fit_par1_", fam)]][idx]
  qc_tbl$pred_fit_par2[idx] <- qc_tbl[[paste0("fit_par2_", fam)]][idx]
  qc_tbl$pred_fit_par3[idx] <- qc_tbl[[paste0("fit_par3_", fam)]][idx]
}

# ---------------------------------------------------------
# 6. CASE-LEVEL QUANTILE CONSEQUENCES
# ---------------------------------------------------------
case_parts <- vector("list", length(return_periods))

for (i in seq_along(return_periods)) {
  T_i <- return_periods[i]
  p_i <- 1 - 1 / T_i
  
  q_true_raw <- mapply(
    FUN = safe_family_quantile,
    family = qc_tbl$true_family,
    par1 = qc_tbl$true_par1,
    par2 = qc_tbl$true_par2,
    par3 = qc_tbl$true_par3,
    MoreArgs = list(p = p_i)
  )
  
  q_true <- q_true_raw + qc_tbl$delta
  
  q_pred <- mapply(
    FUN = safe_family_quantile,
    family = qc_tbl$pred_family,
    par1 = qc_tbl$pred_fit_par1,
    par2 = qc_tbl$pred_fit_par2,
    par3 = qc_tbl$pred_fit_par3,
    MoreArgs = list(p = p_i)
  )
  
  case_parts[[i]] <- qc_tbl %>%
    transmute(
      sample_id,
      true_family,
      pred_family,
      n,
      param_id,
      is_correct,
      T = as.integer(T_i),
      nonexceed_prob = p_i,
      q_true = as.numeric(q_true),
      q_pred = as.numeric(q_pred)
    ) %>%
    mutate(
      re = if_else(
        is.finite(q_true) & q_true != 0 & is.finite(q_pred),
        (q_pred - q_true) / q_true,
        NA_real_
      ),
      are = if_else(
        is.finite(q_true) & q_true != 0 & is.finite(q_pred),
        abs(q_pred - q_true) / q_true,
        NA_real_
      )
    )
}

quantile_case <- bind_rows(case_parts) %>%
  mutate(
    true_family = factor(true_family, levels = family_order),
    pred_family = factor(pred_family, levels = family_order),
    T = factor(T, levels = return_periods)
  )

if (nrow(quantile_case) == 0) {
  stop("quantile_case is empty.")
}

# ---------------------------------------------------------
# 7. QC SUMMARY
# ---------------------------------------------------------
qc_summary <- quantile_case %>%
  group_by(T) %>%
  summarise(
    n_rows = n(),
    n_nonmissing_true = sum(is.finite(q_true)),
    n_nonmissing_pred = sum(is.finite(q_pred)),
    n_nonmissing_are = sum(is.finite(are)),
    n_missing_pred_quantile = sum(!is.finite(q_pred)),
    .groups = "drop"
  )

# ---------------------------------------------------------
# 8. CHANNEL SUMMARY FOR FIGURE 7
# ---------------------------------------------------------
channel_grid <- expand.grid(
  T = factor(return_periods, levels = return_periods),
  true_family = factor(family_order, levels = family_order),
  pred_family = factor(family_order, levels = family_order),
  stringsAsFactors = FALSE
) %>%
  as_tibble()

channel_summary_obs <- quantile_case %>%
  filter(is.finite(are)) %>%
  group_by(T, true_family, pred_family) %>%
  summarise(
    n_cases = n(),
    median_are = median(are, na.rm = TRUE),
    mean_are = mean(are, na.rm = TRUE),
    p90_are = quantile(are, probs = 0.90, na.rm = TRUE),
    median_re = median(re, na.rm = TRUE),
    .groups = "drop"
  )

channel_summary <- channel_grid %>%
  left_join(
    channel_summary_obs,
    by = c("T", "true_family", "pred_family")
  ) %>%
  mutate(
    n_cases = dplyr::coalesce(n_cases, 0L),
    median_are = if_else(n_cases == 0L, NA_real_, median_are),
    mean_are = if_else(n_cases == 0L, NA_real_, mean_are),
    p90_are = if_else(n_cases == 0L, NA_real_, p90_are),
    median_re = if_else(n_cases == 0L, NA_real_, median_re)
  ) %>%
  arrange(T, true_family, pred_family)

# ---------------------------------------------------------
# 9. SAVE OUTPUTS
# ---------------------------------------------------------
save_parquet_safe(quantile_case, "quantile_consequence_case_level.parquet")

save_csv(
  quantile_case %>%
    mutate(
      true_family = as.character(true_family),
      pred_family = as.character(pred_family),
      T = as.character(T)
    ),
  "quantile_consequence_case_level.csv"
)

save_csv(qc_summary, "quantile_consequence_qc_summary.csv")

save_csv(
  channel_summary %>%
    mutate(
      true_family = as.character(true_family),
      pred_family = as.character(pred_family),
      T = as.character(T)
    ),
  "quantile_consequence_channel_summary.csv"
)

report_lines <- c(
  "======================================================",
  "QUANTILE CONSEQUENCE PREPARATION REPORT",
  "======================================================",
  paste0("Base root: ", base_root),
  "",
  "Source files:",
  paste0("- ", preds_path),
  paste0("- ", full_dataset_path),
  paste0("- ", param_registry_path),
  "",
  "Final selected setting:",
  "- run_label = 10K",
  "- prediction_scope = best_by_scenario",
  "- model = xgb",
  "- scenario = classical",
  "",
  "Definitions:",
  "- Q_true uses the true family and true population parameters plus stored delta.",
  "- Q_pred uses the predicted family and sample-level fitted parameters already stored by the workflow.",
  "- RE = (Q_pred - Q_true) / Q_true",
  "- ARE = abs(Q_pred - Q_true) / Q_true",
  "",
  paste0("n_final_cases = ", nrow(preds_final)),
  paste0("n_case_rows_after_expansion = ", nrow(quantile_case)),
  "",
  "QC summary:",
  capture.output(print(qc_summary))
)

write_lines(report_lines, file.path(out_dir, "quantile_consequence_report.txt"))

message("[260_quantile_consequence_prepare] Done.")