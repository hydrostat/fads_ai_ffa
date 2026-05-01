# =========================================================
# 130_table2.R
# Build Table 2:
# Scenario comparison table combining:
# - isolated feature-set results from 042
# - composed/nested scenario results from the 10K outputs
#
# Final row set:
# 1. Classical
# 2. LMRD
# 3. GOF
# 4. Bayes
# 5. GOF+Bayes
# 6. Classical+LMRD
# 7. Classical+LMRD+Bayes
# 8. Classical+LMRD+GOF
# 9. Classical+LMRD+GOF+Bayes
#
# Final columns:
# - Scenario
# - n_features
# - GEV
# - GPA
# - PE3
# - LN3
# - LN2
# - GUM
# - Overall_mean
#
# Notes:
# - Isolated rows come from: code_output_10K/ablation_isolated_scenarios
# - Composed rows come from: code_output_10K/evaluation
# - Composed rows are filtered to XGB only
# - Overall_mean = unweighted mean of family-level accuracies
# =========================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(tibble)
  library(readr)
  library(arrow)
})

# ---------------------------------------------------------
# 0. PATHS
# ---------------------------------------------------------
base_root <- "C:/Users/wilso/OneDrive/My papers/SEAF_AI/code"

run_root <- file.path(base_root, "code_output_10K")
dataset_dir <- file.path(run_root, "datasets")
eval_dir <- file.path(run_root, "evaluation")
ablation_dir <- file.path(run_root, "ablation_isolated_scenarios")

out_dir <- file.path(base_root, "paper_output", "final_tables")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

# ---------------------------------------------------------
# 1. INPUT FILES
# ---------------------------------------------------------
feature_info_isolated_path <- file.path(ablation_dir, "isolated_feature_set_info.csv")
isolated_by_family_path <- file.path(ablation_dir, "table_02_isolated_feature_sets_by_family.csv")

feature_info_composed_path <- file.path(dataset_dir, "feature_scenario_info.parquet")
composed_by_family_path <- file.path(eval_dir, "test_by_class_best_by_scenario.parquet")

required_files <- c(
  feature_info_isolated_path,
  isolated_by_family_path,
  feature_info_composed_path,
  composed_by_family_path
)

missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing required files: ",
    paste(missing_files, collapse = " | ")
  )
}

# ---------------------------------------------------------
# 2. HELPERS
# ---------------------------------------------------------
save_csv <- function(df, filename) {
  write_csv(df, file.path(out_dir, filename))
}

first_present <- function(df, candidates) {
  hit <- intersect(candidates, names(df))
  if (length(hit) == 0) return(NA_character_)
  hit[[1]]
}

family_order <- c("GEV", "GPA", "PE3", "LN3", "LN2", "GUM")

isolated_label_map <- c(
  classical_only = "Classical",
  lmrd_only = "LMRD",
  gof_ic_only = "GOF",
  bayes_only = "Bayes",
  gof_ic_bayes_only = "GOF+Bayes"
)

composed_label_map <- c(
  classical_lmrd = "Classical+LMRD",
  classical_lmrd_bayes = "Classical+LMRD+Bayes",
  classical_lmrd_gof_ic = "Classical+LMRD+GOF",
  classical_lmrd_gof_ic_bayes = "Classical+LMRD+GOF+Bayes"
)

final_row_order <- c(
  "Classical",
  "LMRD",
  "GOF",
  "Bayes",
  "GOF+Bayes",
  "Classical+LMRD",
  "Classical+LMRD+Bayes",
  "Classical+LMRD+GOF",
  "Classical+LMRD+GOF+Bayes"
)

# ---------------------------------------------------------
# 3. ISOLATED FEATURE SETS (from 042 outputs)
# ---------------------------------------------------------
isolated_feature_info <- read_csv(feature_info_isolated_path, show_col_types = FALSE)
isolated_by_family <- read_csv(isolated_by_family_path, show_col_types = FALSE)

# identify columns robustly
iso_scenario_col <- first_present(isolated_by_family, c("feature_set", "scenario"))
iso_model_col <- first_present(isolated_by_family, c("model"))
iso_family_col <- first_present(isolated_by_family, c("true_family", "family"))
iso_acc_col <- first_present(isolated_by_family, c("accuracy"))

if (any(is.na(c(iso_scenario_col, iso_family_col, iso_acc_col)))) {
  stop(
    "Could not identify required columns in isolated-family table. Columns found: ",
    paste(names(isolated_by_family), collapse = ", ")
  )
}

isolated_tbl <- isolated_by_family %>%
  mutate(
    scenario_raw = as.character(.data[[iso_scenario_col]]),
    model = if (!is.na(iso_model_col)) as.character(.data[[iso_model_col]]) else NA_character_,
    true_family = as.character(.data[[iso_family_col]]),
    accuracy = as.numeric(.data[[iso_acc_col]])
  ) %>%
  filter(scenario_raw %in% names(isolated_label_map))

# filter to XGB if model column exists and contains model info
if (!all(is.na(isolated_tbl$model))) {
  isolated_tbl <- isolated_tbl %>%
    filter(model == "xgb")
}

isolated_nfeat <- isolated_feature_info %>%
  mutate(
    scenario_raw = as.character(scenario),
    n_features = as.numeric(n_features),
    Scenario = unname(isolated_label_map[scenario_raw])
  ) %>%
  filter(!is.na(Scenario)) %>%
  select(Scenario, n_features)

isolated_wide <- isolated_tbl %>%
  mutate(
    Scenario = unname(isolated_label_map[scenario_raw]),
    true_family = factor(true_family, levels = family_order)
  ) %>%
  filter(!is.na(Scenario), true_family %in% family_order) %>%
  select(Scenario, true_family, accuracy) %>%
  pivot_wider(
    names_from = true_family,
    values_from = accuracy
  ) %>%
  left_join(isolated_nfeat, by = "Scenario")

# ---------------------------------------------------------
# 4. COMPOSED/NESTED SCENARIOS (from 10K evaluation outputs)
# ---------------------------------------------------------
composed_feature_info <- read_parquet(feature_info_composed_path)
composed_by_family <- read_parquet(composed_by_family_path)

comp_scenario_col <- first_present(composed_by_family, c("scenario"))
comp_model_col <- first_present(composed_by_family, c("model"))
comp_family_col <- first_present(composed_by_family, c("true_family", "family"))
comp_acc_col <- first_present(composed_by_family, c("accuracy", "accuracy_class"))

if (any(is.na(c(comp_scenario_col, comp_family_col, comp_acc_col)))) {
  stop(
    "Could not identify required columns in composed-family table. Columns found: ",
    paste(names(composed_by_family), collapse = ", ")
  )
}

composed_tbl <- composed_by_family %>%
  mutate(
    scenario_raw = as.character(.data[[comp_scenario_col]]),
    model = if (!is.na(comp_model_col)) as.character(.data[[comp_model_col]]) else NA_character_,
    true_family = as.character(.data[[comp_family_col]]),
    accuracy = as.numeric(.data[[comp_acc_col]])
  ) %>%
  filter(scenario_raw %in% names(composed_label_map))

# filter to XGB only
if (!all(is.na(composed_tbl$model))) {
  composed_tbl <- composed_tbl %>%
    filter(model == "xgb")
}

composed_nfeat <- composed_feature_info %>%
  mutate(
    scenario_raw = as.character(scenario),
    n_features = as.numeric(n_features),
    Scenario = unname(composed_label_map[scenario_raw])
  ) %>%
  filter(!is.na(Scenario)) %>%
  select(Scenario, n_features)

composed_wide <- composed_tbl %>%
  mutate(
    Scenario = unname(composed_label_map[scenario_raw]),
    true_family = factor(true_family, levels = family_order)
  ) %>%
  filter(!is.na(Scenario), true_family %in% family_order) %>%
  select(Scenario, true_family, accuracy) %>%
  pivot_wider(
    names_from = true_family,
    values_from = accuracy
  ) %>%
  left_join(composed_nfeat, by = "Scenario")

# ---------------------------------------------------------
# 5. COMBINE AND FINALIZE
# ---------------------------------------------------------
table_02 <- bind_rows(isolated_wide, composed_wide) %>%
  mutate(
    Overall_mean = rowMeans(
      dplyr::select(., any_of(family_order)),
      na.rm = TRUE
    )
  ) %>%
  select(
    Scenario,
    n_features,
    all_of(family_order),
    Overall_mean
  ) %>%
  mutate(
    Scenario = factor(Scenario, levels = final_row_order)
  ) %>%
  arrange(Scenario) %>%
  mutate(
    Scenario = as.character(Scenario)
  )

save_csv(table_02, "table_02_scenario_comparison.csv")

# ---------------------------------------------------------
# 6. SUPPORTING LONG TABLE
# ---------------------------------------------------------
table_02_long <- table_02 %>%
  pivot_longer(
    cols = all_of(c(family_order, "Overall_mean")),
    names_to = "metric",
    values_to = "value"
  )

save_csv(table_02_long, "table_02_scenario_comparison_long.csv")

# ---------------------------------------------------------
# 7. REPORT
# ---------------------------------------------------------
report_lines <- c(
  "======================================================",
  "TABLE 2 REPORT",
  "======================================================",
  paste0("Base root: ", base_root),
  paste0("10K root: ", run_root),
  "",
  "Files used:",
  paste0("- ", feature_info_isolated_path),
  paste0("- ", isolated_by_family_path),
  paste0("- ", feature_info_composed_path),
  paste0("- ", composed_by_family_path),
  "",
  "Construction rules:",
  "- Isolated rows are taken from the 042 outputs.",
  "- Composed rows are taken from code_output_10K/evaluation/test_by_class_best_by_scenario.parquet.",
  "- Composed rows are filtered to model == xgb.",
  "- Overall_mean is the unweighted mean of family-level accuracies.",
  "",
  "Family column order:",
  paste0("- ", paste(family_order, collapse = ", ")),
  "",
  "Final row order:",
  paste0("- ", paste(final_row_order, collapse = " | ")),
  "",
  paste0("Rows in final table: ", nrow(table_02))
)

# optional model diagnostics from source tables
if (!all(is.na(isolated_tbl$model))) {
  report_lines <- c(
    report_lines,
    "",
    "Models present in isolated-source table after reading:",
    paste0("- ", paste(sort(unique(na.omit(isolated_by_family[[iso_model_col]]))), collapse = ", "))
  )
}

if (!all(is.na(composed_tbl$model))) {
  report_lines <- c(
    report_lines,
    "Models present in composed-source table after reading:",
    paste0("- ", paste(sort(unique(na.omit(as.character(composed_by_family[[comp_model_col]])))), collapse = ", "))
  )
}

write_lines(report_lines, file.path(out_dir, "table_02_report.txt"))

message("[130_table2] Done.")