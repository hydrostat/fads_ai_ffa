# =========================================================
# 140_table3.R
# Build Table 3:
# Row-normalized confusion matrix for the FINAL selected setup:
# - 10K run
# - model = XGB
# - scenario = classical
#
# Final layout:
# - rows    = true family
# - columns = predicted family
# - cell(i,j) = P(predicted = j | true = i)
#
# Inputs:
# - code_output_10K/evaluation/test_predictions_best_by_scenario.parquet
#
# Outputs:
# - paper_output/final_tables/table_03_confusion_matrix_classical_xgb.csv
# - paper_output/final_tables/table_03_confusion_long_classical_xgb.csv
# - paper_output/final_tables/table_03_report.txt
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
eval_dir <- file.path(run_root, "evaluation")
out_dir <- file.path(base_root, "paper_output", "final_tables")

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

pred_path <- file.path(eval_dir, "test_predictions_best_by_scenario.parquet")
if (!file.exists(pred_path)) {
  stop("Missing file: ", pred_path)
}

# ---------------------------------------------------------
# 1. HELPERS
# ---------------------------------------------------------
save_csv <- function(df, filename) {
  write_csv(df, file.path(out_dir, filename))
}

first_present <- function(df, candidates) {
  hit <- intersect(candidates, names(df))
  if (length(hit) == 0) return(NA_character_)
  hit[[1]]
}

family_order <- c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM")

# ---------------------------------------------------------
# 2. READ PREDICTIONS
# ---------------------------------------------------------
pred_tbl <- read_parquet(pred_path)

true_col <- first_present(pred_tbl, c("true_family", "truth", "obs", "actual"))
pred_col <- first_present(pred_tbl, c(".pred_class", "pred_family", "prediction", "predicted"))
scenario_col <- first_present(pred_tbl, c("scenario"))
model_col <- first_present(pred_tbl, c("model"))

if (any(is.na(c(true_col, pred_col, scenario_col, model_col)))) {
  stop(
    "Could not identify required columns. Available columns: ",
    paste(names(pred_tbl), collapse = ", ")
  )
}

pred_use <- pred_tbl %>%
  transmute(
    true_family = as.character(.data[[true_col]]),
    pred_family = as.character(.data[[pred_col]]),
    scenario = as.character(.data[[scenario_col]]),
    model = as.character(.data[[model_col]])
  ) %>%
  filter(!is.na(true_family), !is.na(pred_family)) %>%
  filter(model == "xgb", scenario == "classical")

if (nrow(pred_use) == 0) {
  stop("No prediction rows found for model = 'xgb' and scenario = 'classical'.")
}

# ---------------------------------------------------------
# 3. FAMILY ORDER
# ---------------------------------------------------------
families_found <- sort(unique(c(pred_use$true_family, pred_use$pred_family)))
family_order_use <- c(
  family_order[family_order %in% families_found],
  setdiff(families_found, family_order)
)

# ---------------------------------------------------------
# 4. BUILD ROW-CONDITIONAL CONFUSION MATRIX
# ---------------------------------------------------------
confusion_long <- pred_use %>%
  mutate(
    true_family = factor(true_family, levels = family_order_use),
    pred_family = factor(pred_family, levels = family_order_use)
  ) %>%
  count(true_family, pred_family, name = "n_cases", .drop = FALSE) %>%
  group_by(true_family) %>%
  mutate(
    row_total = sum(n_cases, na.rm = TRUE),
    confusion = if_else(row_total > 0, n_cases / row_total, NA_real_)
  ) %>%
  ungroup() %>%
  mutate(
    is_diagonal = as.character(true_family) == as.character(pred_family)
  )

confusion_matrix <- confusion_long %>%
  select(true_family, pred_family, confusion) %>%
  pivot_wider(
    names_from = pred_family,
    values_from = confusion
  ) %>%
  rename(Family = true_family)

# ---------------------------------------------------------
# 5. SAVE OUTPUTS
# ---------------------------------------------------------
save_csv(confusion_matrix, "table_03_confusion_matrix_classical_xgb.csv")
save_csv(confusion_long, "table_03_confusion_long_classical_xgb.csv")

# ---------------------------------------------------------
# 6. REPORT
# ---------------------------------------------------------
diag_tbl <- confusion_long %>%
  filter(is_diagonal) %>%
  transmute(
    family = as.character(true_family),
    diagonal_accuracy = confusion
  )

top_offdiag_tbl <- confusion_long %>%
  filter(!is_diagonal) %>%
  arrange(desc(confusion), desc(n_cases)) %>%
  slice_head(n = 10) %>%
  transmute(
    pair = paste0(as.character(true_family), " -> ", as.character(pred_family)),
    confusion = confusion,
    n_cases = n_cases
  )

report_lines <- c(
  "======================================================",
  "TABLE 3 REPORT",
  "======================================================",
  paste0("Base root: ", base_root),
  paste0("Run root: ", run_root),
  "",
  "Source file:",
  paste0("- ", pred_path),
  "",
  "Final selection enforced:",
  "- model = xgb",
  "- scenario = classical",
  "",
  "Definition:",
  "- Table 3 is a row-conditional confusion matrix.",
  "- Rows = true family.",
  "- Columns = predicted family.",
  "- Cell(i,j) = P(predicted = j | true = i).",
  "- Therefore each row sums to 1.",
  "",
  paste0("n_prediction_rows = ", nrow(pred_use)),
  "",
  "Family order:",
  paste0("- ", paste(family_order_use, collapse = ", ")),
  "",
  "Diagonal cells (correct classification proportions):"
)

for (i in seq_len(nrow(diag_tbl))) {
  report_lines <- c(
    report_lines,
    paste0("- ", diag_tbl$family[i], " = ", round(diag_tbl$diagonal_accuracy[i], 4))
  )
}

report_lines <- c(
  report_lines,
  "",
  "Top off-diagonal confusions:"
)

for (i in seq_len(nrow(top_offdiag_tbl))) {
  report_lines <- c(
    report_lines,
    paste0(
      "- ", top_offdiag_tbl$pair[i],
      " | confusion = ", round(top_offdiag_tbl$confusion[i], 4),
      " | n_cases = ", top_offdiag_tbl$n_cases[i]
    )
  )
}

write_lines(report_lines, file.path(out_dir, "table_03_report.txt"))

message("[140_table3] Done.")