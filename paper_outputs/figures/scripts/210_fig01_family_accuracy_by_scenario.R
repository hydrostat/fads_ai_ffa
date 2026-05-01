# =========================================================
# 210_fig01_family_accuracy_by_scenario.R
# Figure 1:
# Held-out test accuracy by true generating distribution
# across scenario contexts (single-panel version)
#
# Final design:
# - one panel
# - x-axis = true family
# - y-axis = held-out test accuracy
# - color = scenario
# - model fixed to XGB
#
# Inputs:
# - code_output_10K/evaluation/test_by_class_best_by_scenario.parquet
#
# Outputs:
# - paper_output/final_figures/fig_01_family_accuracy_by_scenario.png
# - paper_output/final_figures/fig_01_family_accuracy_by_scenario.csv
# - paper_output/final_figures/fig_01_family_accuracy_by_scenario_report.txt
# =========================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tidyr)
  library(tibble)
  library(readr)
  library(arrow)
  library(ggplot2)
})

# ---------------------------------------------------------
# 0. PATHS
# ---------------------------------------------------------
base_root <- "C:/Users/wilso/OneDrive/My papers/SEAF_AI/code"
run_root <- file.path(base_root, "code_output_10K")
eval_dir <- file.path(run_root, "evaluation")
out_dir <- file.path(base_root, "paper_output", "final_figures")

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

input_path <- file.path(eval_dir, "test_by_class_best_by_scenario.parquet")
if (!file.exists(input_path)) {
  stop("Missing input file: ", input_path)
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

scenario_label_map <- c(
  classical = "Classical",
  classical_lmrd = "Classical+LMRD",
  classical_lmrd_bayes = "Classical+LMRD+Bayes",
  classical_lmrd_gof_ic = "Classical+LMRD+GOF",
  classical_lmrd_gof_ic_bayes = "Classical+LMRD+GOF+Bayes"
)

scenario_order <- c(
  "Classical",
  "Classical+LMRD",
  "Classical+LMRD+Bayes",
  "Classical+LMRD+GOF",
  "Classical+LMRD+GOF+Bayes"
)

family_order <- c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM")

# ---------------------------------------------------------
# 2. READ AND STANDARDIZE
# ---------------------------------------------------------
raw_tbl <- read_parquet(input_path)

scenario_col <- first_present(raw_tbl, c("scenario"))
model_col <- first_present(raw_tbl, c("model"))
family_col <- first_present(raw_tbl, c("true_family", "family"))
acc_col <- first_present(raw_tbl, c("accuracy", "accuracy_class"))

if (any(is.na(c(scenario_col, family_col, acc_col)))) {
  stop(
    "Could not identify required columns. Available columns: ",
    paste(names(raw_tbl), collapse = ", ")
  )
}

plot_tbl <- raw_tbl %>%
  transmute(
    scenario_raw = as.character(.data[[scenario_col]]),
    model = if (!is.na(model_col)) as.character(.data[[model_col]]) else NA_character_,
    true_family = as.character(.data[[family_col]]),
    accuracy = as.numeric(.data[[acc_col]])
  ) %>%
  filter(!is.na(scenario_raw), !is.na(true_family), !is.na(accuracy)) %>%
  filter(scenario_raw %in% names(scenario_label_map))

# restrict to XGB
if (!all(is.na(plot_tbl$model))) {
  plot_tbl <- plot_tbl %>%
    filter(model == "xgb")
}

plot_tbl <- plot_tbl %>%
  mutate(
    Scenario = unname(scenario_label_map[scenario_raw]),
    Scenario = factor(Scenario, levels = scenario_order),
    true_family = factor(true_family, levels = family_order)
  ) %>%
  arrange(true_family, Scenario)

if (nrow(plot_tbl) == 0) {
  stop("No rows available for plotting after filtering to XGB and known scenarios.")
}

# save plotting data
plot_data_out <- plot_tbl %>%
  select(Scenario, true_family, accuracy)

save_csv(
  plot_data_out,
  "fig_01_family_accuracy_by_scenario.csv"
)

# scale_fill_manual(name="Scenario", values=c(  "Classical"                = "#994455",
#                                               "Classical+LMRD"           = "#997700",
#                                               "Classical+LMRD+Bayes"     = "#6699CC",
#                                               "Classical+LMRD+GOF"       = "#EE99AA",
#                                               "Classical+LMRD+GOF+Bayes" = "#EECC66")) +
  #
  #
  #
  #
  #
# ---------------------------------------------------------
# 3. FIGURE
# ---------------------------------------------------------
p <- ggplot(
  plot_tbl,
  aes(x = true_family, y = accuracy, fill = Scenario)
) +
  geom_col(
    position = position_dodge(width = 0.8),
    width = 0.7
  ) +
  scale_fill_manual(name="Scenario", values=c(  "Classical"                = "#525252",
                                                "Classical+LMRD"           = "#737373",
                                                "Classical+LMRD+Bayes"     = "#969696",
                                                "Classical+LMRD+GOF"       = "#bdbdbd",
                                                "Classical+LMRD+GOF+Bayes" = "#d9d9d9")) +
  labs(
    # title = "Held-out test accuracy by true generating distribution",
    x = "True family",
    y = "Accuracy",
    fill = "Scenario"
  ) +
  coord_cartesian(ylim = c(0, 1)) +
  theme_classic() +
  theme(
    # axis.text.x = element_text(angle = 30, hjust = 1),
    legend.position = "bottom",
    axis.text = element_text(size = 12, color = "black"),
    axis.title = element_text(size = 14, color = "black", face = 'bold'),
    title = element_text(size = 10, color = "black", face = "bold"),
    legend.title = element_text(size = 12, color = "black", face = "bold")
  )

ggsave(
  filename = file.path(out_dir, "fig_01_family_accuracy_by_scenario.png"),
  plot = p,
  width = 11,
  height = 7,
  dpi = 300,
  units = "in"
)

# ---------------------------------------------------------
# 4. REPORT
# ---------------------------------------------------------
family_spread_tbl <- plot_tbl %>%
  group_by(true_family) %>%
  summarise(
    min_accuracy = min(accuracy, na.rm = TRUE),
    max_accuracy = max(accuracy, na.rm = TRUE),
    spread = max_accuracy - min_accuracy,
    .groups = "drop"
  ) %>%
  arrange(desc(spread))

scenario_mean_tbl <- plot_tbl %>%
  group_by(Scenario) %>%
  summarise(
    mean_family_accuracy = mean(accuracy, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(mean_family_accuracy))

report_lines <- c(
  "======================================================",
  "FIGURE 1 REPORT",
  "======================================================",
  paste0("Base root: ", base_root),
  paste0("10K run root: ", run_root),
  "",
  "Source file:",
  paste0("- ", input_path),
  "",
  "Figure definition:",
  "- Single-panel family-level held-out test accuracy comparison.",
  "- x-axis = true generating distribution.",
  "- y-axis = held-out test accuracy.",
  "- color = scenario.",
  "- model fixed to XGB.",
  "",
  paste0("Rows used in plotting table: ", nrow(plot_tbl)),
  "",
  "Scenario means (unweighted across families):"
)

for (i in seq_len(nrow(scenario_mean_tbl))) {
  report_lines <- c(
    report_lines,
    paste0(
      "- ", scenario_mean_tbl$Scenario[i],
      " = ", round(scenario_mean_tbl$mean_family_accuracy[i], 4)
    )
  )
}

report_lines <- c(
  report_lines,
  "",
  "Family-level spread across scenarios:"
)

for (i in seq_len(nrow(family_spread_tbl))) {
  report_lines <- c(
    report_lines,
    paste0(
      "- ", as.character(family_spread_tbl$true_family[i]),
      " | min = ", round(family_spread_tbl$min_accuracy[i], 4),
      " | max = ", round(family_spread_tbl$max_accuracy[i], 4),
      " | spread = ", round(family_spread_tbl$spread[i], 4)
    )
  )
}

write_lines(
  report_lines,
  file.path(out_dir, "fig_01_family_accuracy_by_scenario_report.txt")
)

message("[210_fig01_family_accuracy_by_scenario] Done.")