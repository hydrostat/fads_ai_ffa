# =========================================================
# 042_summarize_isolated_feature_sets.R
# Summarize outputs from 041_isolated_feature_set_evaluation.R
#
# Purpose:
# Build compact summary artifacts for the isolated feature-set study:
# 1) one summary table with validation/test performance
# 2) one comparison figure for test accuracy
# 3) one family-level comparison figure
#
# Inputs:
# - code_output/ablation_isolated_scenarios/isolated_feature_set_info.csv
# - code_output/ablation_isolated_scenarios/validation_metrics.parquet
# - code_output/ablation_isolated_scenarios/test_metrics.parquet
# - code_output/ablation_isolated_scenarios/test_by_class.parquet
# - code_output/ablation_isolated_scenarios/test_by_n.parquet
#
# Outputs:
# - code_output/ablation_isolated_scenarios/table_01_isolated_feature_sets_summary.csv
# - code_output/ablation_isolated_scenarios/table_02_isolated_feature_sets_by_family.csv
# - code_output/ablation_isolated_scenarios/fig_01_isolated_feature_sets_test_accuracy.png
# - code_output/ablation_isolated_scenarios/fig_02_isolated_feature_sets_by_family.png
# - code_output/ablation_isolated_scenarios/report_042.txt
# =========================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(tibble)
  library(readr)
  library(arrow)
  library(ggplot2)
  library(forcats)
})

source("script/fun/fun_io.R")

config <- readRDS("code_output/config/config_main.rds")
paths <- config$paths

base_dir <- file.path(paths$output, "ablation_isolated_scenarios")

feature_info_path <- file.path(base_dir, "isolated_feature_set_info.csv")
validation_metrics_path <- file.path(base_dir, "validation_metrics.parquet")
test_metrics_path <- file.path(base_dir, "test_metrics.parquet")
test_by_class_path <- file.path(base_dir, "test_by_class.parquet")
test_by_n_path <- file.path(base_dir, "test_by_n.parquet")

required_files <- c(
  feature_info_path,
  validation_metrics_path,
  test_metrics_path,
  test_by_class_path,
  test_by_n_path
)

missing_files <- required_files[!file.exists(required_files)]
if (length(missing_files) > 0) {
  stop(
    "Missing required files for 042: ",
    paste(missing_files, collapse = " | ")
  )
}

feature_info <- read_csv(feature_info_path, show_col_types = FALSE)
validation_metrics <- read_parquet(validation_metrics_path)
test_metrics <- read_parquet(test_metrics_path)
test_by_class <- read_parquet(test_by_class_path)
test_by_n <- read_parquet(test_by_n_path)

save_csv <- function(df, filename) {
  write_csv(df, file.path(base_dir, filename))
}

save_plot <- function(plot_obj, filename, width = 11, height = 7, dpi = 300) {
  ggsave(
    filename = file.path(base_dir, filename),
    plot = plot_obj,
    width = width,
    height = height,
    dpi = dpi,
    units = "in",
    limitsize = FALSE
  )
}

# ---------------------------------------------------------
# 1. SUMMARY TABLE
# ---------------------------------------------------------
validation_acc <- validation_metrics %>%
  filter(.metric == "accuracy") %>%
  select(scenario, model, validation_accuracy = .estimate, validation_status = status, validation_note = note)

validation_kap <- validation_metrics %>%
  filter(.metric == "kap") %>%
  select(scenario, model, validation_kappa = .estimate)

test_acc <- test_metrics %>%
  filter(.metric == "accuracy") %>%
  select(scenario, model, test_accuracy = .estimate, test_status = status, test_note = note)

test_kap <- test_metrics %>%
  filter(.metric == "kap") %>%
  select(scenario, model, test_kappa = .estimate)

table_01_isolated_feature_sets_summary <- feature_info %>%
  rename(feature_set = scenario) %>%
  left_join(
    validation_acc %>% rename(feature_set = scenario),
    by = "feature_set"
  ) %>%
  left_join(
    validation_kap %>% rename(feature_set = scenario),
    by = c("feature_set", "model")
  ) %>%
  left_join(
    test_acc %>% rename(feature_set = scenario),
    by = c("feature_set", "model")
  ) %>%
  left_join(
    test_kap %>% rename(feature_set = scenario),
    by = c("feature_set", "model")
  ) %>%
  arrange(desc(test_accuracy), desc(validation_accuracy), feature_set, model)

save_csv(
  table_01_isolated_feature_sets_summary,
  "table_01_isolated_feature_sets_summary.csv"
)

# ---------------------------------------------------------
# 2. FAMILY-LEVEL TABLE
# ---------------------------------------------------------
table_02_isolated_feature_sets_by_family <- test_by_class %>%
  rename(feature_set = scenario) %>%
  arrange(feature_set, model, true_family)

save_csv(
  table_02_isolated_feature_sets_by_family,
  "table_02_isolated_feature_sets_by_family.csv"
)

# ---------------------------------------------------------
# 3. FIGURE 1: OVERALL TEST ACCURACY
# ---------------------------------------------------------
feature_order <- table_01_isolated_feature_sets_summary %>%
  distinct(feature_set, test_accuracy) %>%
  arrange(desc(test_accuracy), feature_set) %>%
  pull(feature_set)

fig01_df <- table_01_isolated_feature_sets_summary %>%
  mutate(feature_set = factor(feature_set, levels = unique(feature_order)))

p1 <- ggplot(fig01_df, aes(x = feature_set, y = test_accuracy)) +
  geom_col() +
  facet_wrap(~model, scales = "free_x") +
  labs(
    title = "Held-out test accuracy for isolated feature sets",
    x = "Isolated feature set",
    y = "Test accuracy"
  ) +
  theme_bw() +
  theme(
    plot.title = element_text(face = "bold"),
    axis.text.x = element_text(angle = 30, hjust = 1)
  )

save_plot(
  p1,
  "fig_01_isolated_feature_sets_test_accuracy.png",
  width = 11,
  height = 7
)

# ---------------------------------------------------------
# 4. FIGURE 2: FAMILY-LEVEL TEST ACCURACY
# ---------------------------------------------------------
family_order <- c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM")

fig02_df <- table_02_isolated_feature_sets_by_family %>%
  mutate(
    feature_set = factor(feature_set, levels = unique(feature_order)),
    true_family = factor(true_family, levels = family_order)
  )

p2 <- ggplot(fig02_df, aes(x = true_family, y = accuracy)) +
  geom_col() +
  facet_grid(model ~ feature_set) +
  labs(
    title = "Held-out test accuracy by true generating distribution for isolated feature sets",
    x = "True family",
    y = "Accuracy"
  ) +
  theme_bw() +
  theme(
    plot.title = element_text(face = "bold"),
    axis.text.x = element_text(angle = 30, hjust = 1)
  )

save_plot(
  p2,
  "fig_02_isolated_feature_sets_by_family.png",
  width = 14,
  height = 8
)

# ---------------------------------------------------------
# 5. REPORT
# ---------------------------------------------------------
best_overall_line <- table_01_isolated_feature_sets_summary %>%
  filter(!is.na(test_accuracy)) %>%
  arrange(desc(test_accuracy), desc(validation_accuracy)) %>%
  slice(1)

report_lines <- c(
  "======================================================",
  "042 ISOLATED FEATURE SETS SUMMARY REPORT",
  "======================================================",
  paste0("Base directory: ", base_dir),
  "",
  "Files generated:",
  "- table_01_isolated_feature_sets_summary.csv",
  "- table_02_isolated_feature_sets_by_family.csv",
  "- fig_01_isolated_feature_sets_test_accuracy.png",
  "- fig_02_isolated_feature_sets_by_family.png",
  "",
  "Feature sets detected:"
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
  paste0("Rows in summary table: ", nrow(table_01_isolated_feature_sets_summary)),
  paste0("Rows in family-level table: ", nrow(table_02_isolated_feature_sets_by_family))
)

if (nrow(best_overall_line) == 1) {
  report_lines <- c(
    report_lines,
    "",
    "Best isolated feature-set result:",
    paste0(
      "- feature_set = ", best_overall_line$feature_set,
      " | model = ", best_overall_line$model,
      " | validation_accuracy = ", round(best_overall_line$validation_accuracy, 4),
      " | test_accuracy = ", round(best_overall_line$test_accuracy, 4)
    )
  )
}

write_lines(report_lines, file.path(base_dir, "report_042.txt"))

log_msg("042_summarize_isolated_feature_sets.R finished successfully.")