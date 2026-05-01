# =========================================================
# 240_fig04_parameter_range_classical.R
# Figure 4:
# Held-out test accuracy across the simulated parameter range
# under the final selected setting:
# - 10K
# - model = XGB
# - scenario = classical
#
# Design:
# - one row per true_family × param_id × n
# - accuracy = # correct / m for that parameter set and sample size
# - explicit manual family-to-parameter mapping
#
# Included families and x-axis parameter:
# - GEV -> k
# - GPA -> k
# - LN2 -> sdlog
# - LN3 -> k
# - PE3 -> gamma
#
# Excluded:
# - GUM
#
# Included sample sizes:
# - 10, 20, 30, 50, 75, 100, 150
#
# Input:
# - paper_output/results_master/results_master_predictions.parquet
#
# Outputs:
# - paper_output/final_figures/fig_04_parameter_range_classical.png
# - paper_output/final_figures/fig_04_parameter_range_classical.csv
# - paper_output/final_figures/fig_04_parameter_range_classical_report.txt
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
master_dir <- file.path(base_root, "paper_output", "results_master")
out_dir <- file.path(base_root, "paper_output", "final_figures")

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

preds_path <- file.path(master_dir, "results_master_predictions.parquet")
if (!file.exists(preds_path)) {
  stop("Missing predictions master: ", preds_path)
}

preds_master <- read_parquet(preds_path)

save_csv <- function(df, filename) {
  write_csv(df, file.path(out_dir, filename))
}

# ---------------------------------------------------------
# 1. EXPLICIT FAMILY -> PARAMETER MAPPING
# ---------------------------------------------------------
param_map <- tibble(
  true_family = c("GEV", "GPA", "LN2", "LN3", "PE3"),
  analysis_param_col = c("param_k", "param_k", "param_sdlog", "param_k", "param_gamma"),
  analysis_param_name = c("k", "k", "sdlog", "k", "gamma")
)

target_families <- param_map$true_family
target_n <- c(10, 20, 30, 50, 75, 100, 150)

# ---------------------------------------------------------
# 2. PREPARE DATA
# ---------------------------------------------------------
preds_use <- preds_master %>%
  filter(run_label == "10K") %>%
  mutate(
    scope = case_when(
      prediction_scope == "best_overall" ~ "best_overall",
      prediction_scope == "best_by_scenario" ~ "best_by_scenario",
      TRUE ~ as.character(prediction_scope)
    ),
    is_correct = true_family == pred_family
  ) %>%
  filter(scope == "best_by_scenario") %>%
  filter(model == "xgb") %>%
  filter(scenario == "classical") %>%
  filter(true_family %in% target_families) %>%
  filter(n %in% target_n) %>%
  left_join(param_map, by = "true_family")

# Check mapped columns
missing_param_cols <- setdiff(unique(param_map$analysis_param_col), names(preds_use))
if (length(missing_param_cols) > 0) {
  stop(
    "Mapped parameter columns not found in predictions master: ",
    paste(missing_param_cols, collapse = ", ")
  )
}

# Extract family-specific varying parameter
preds_use <- bind_rows(lapply(split(preds_use, preds_use$true_family), function(df_i) {
  chosen_col <- unique(df_i$analysis_param_col)
  
  if (length(chosen_col) != 1 || !(chosen_col %in% names(df_i))) {
    df_i$analysis_param_value <- NA_real_
  } else {
    df_i$analysis_param_value <- as.numeric(df_i[[chosen_col]])
  }
  
  df_i
}))

# ---------------------------------------------------------
# 3. PARAM_ID-LEVEL ACCURACY
# ---------------------------------------------------------
fig04_tbl <- preds_use %>%
  filter(is.finite(analysis_param_value)) %>%
  group_by(true_family, analysis_param_name, param_id, analysis_param_value, n) %>%
  summarise(
    n_cases = n(),
    n_correct = sum(is_correct, na.rm = TRUE),
    n_wrong = sum(!is_correct, na.rm = TRUE),
    accuracy = mean(is_correct, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(true_family, n, analysis_param_value, param_id)

if (nrow(fig04_tbl) == 0) {
  stop("No rows available for Figure 3 after filtering to 10K + best_by_scenario + classical + xgb.")
}

save_csv(fig04_tbl, "fig_04_parameter_range_classical.csv")

# ---------------------------------------------------------
# 4. BUILD FIGURE
# ---------------------------------------------------------
fig04_df <- fig04_tbl %>%
  mutate(
    true_family = factor(true_family, levels = target_families),
    n = factor(n, levels = target_n)
  )
# scale_color_manual(name="Sample size (n)", values=c('#4477AA', '#EE6677', '#228833', '#CCBB44', '#66CCEE', '#AA3377', '#BBBBBB')) +
  
p <- ggplot(fig04_df, aes(x = analysis_param_value, y = accuracy, group = n, color = n)) +
  geom_line(linewidth = 0.5) +
  geom_point(size = 1.1) +
  facet_wrap(~true_family, scales = "free_x") +
  labs(
    # title = "Held-out test accuracy across the simulated parameter range",
    x = "Population parameter",
    y = "Accuracy",
    color = "Sample size (n)"
  ) +
  scale_color_manual(name="Sample size (n)", values=c('#D5CE04', '#E7B503', '#F19903', '#F6790B', '#F94902', '#E40515', '#A80003')) +
  coord_cartesian(ylim = c(0, 1.02)) +
  theme_classic() +
  theme(
    panel.grid.major = element_line(color = "grey",
                                    linewidth = 0.1),
    panel.border = element_rect(fill = NA, linewidth = 0.8),
    axis.line = element_line(linewidth = 0.5),
    legend.position = "bottom",
    axis.text = element_text(size = 12, color = "black"),
    axis.title = element_text(size = 14, color = "black", face = 'bold'),
    # title = element_text(size = 10, color = "black", face = "bold"),
    legend.title = element_text(size = 12, color = "black", face = "bold"),
    strip.text = element_text(size=11, face="bold"),
    strip.background = element_rect(colour="black", fill="gray90", 
                                    linewidth=0.8, linetype="solid")
    
  )

ggsave(
  filename = file.path(out_dir, "fig_04_parameter_range_classical.png"),
  plot = p,
  width = 13,
  height = 9,
  dpi = 300,
  units = "in",
  limitsize = FALSE
)

# ---------------------------------------------------------
# 5. REPORT
# ---------------------------------------------------------
family_lines <- apply(param_map, 1, function(r) {
  paste0("- ", r[["true_family"]], " -> ", r[["analysis_param_col"]], " (", r[["analysis_param_name"]], ")")
})

coverage_lines <- fig04_tbl %>%
  group_by(true_family, n) %>%
  summarise(
    n_param_sets = n(),
    param_min = min(analysis_param_value, na.rm = TRUE),
    param_max = max(analysis_param_value, na.rm = TRUE),
    acc_min = min(accuracy, na.rm = TRUE),
    acc_max = max(accuracy, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(true_family, n) %>%
  group_split(true_family) %>%
  lapply(function(df_i) {
    fam <- unique(df_i$true_family)
    lines <- c(paste0("- ", fam))
    for (j in seq_len(nrow(df_i))) {
      lines <- c(
        lines,
        paste0(
          "    n=", df_i$n[j],
          " | n_param_sets=", df_i$n_param_sets[j],
          " | param_min=", signif(df_i$param_min[j], 6),
          " | param_max=", signif(df_i$param_max[j], 6),
          " | acc_min=", signif(df_i$acc_min[j], 4),
          " | acc_max=", signif(df_i$acc_max[j], 4)
        )
      )
    }
    lines
  }) %>%
  unlist()

report_lines <- c(
  "======================================================",
  "FIGURE 4 REPORT",
  "======================================================",
  paste0("Base root: ", base_root),
  "",
  "Source used:",
  "- results_master_predictions.parquet",
  "",
  "Files generated:",
  "- fig_04_parameter_range_classical.csv",
  "- fig_04_parameter_range_classical.png",
  "",
  "Design choice:",
  "- Scope shown: best_by_scenario only.",
  "- Scenario shown: classical only.",
  "- Model shown: xgb only.",
  "- Accuracy computed at the true_family × param_id × n level as #correct / m.",
  "- Included families: GEV, GPA, LN2, LN3, PE3.",
  "- Excluded family: GUM.",
  "- Included sample sizes: 10, 20, 30, 50, 75, 100, 150.",
  "- x-axis uses explicit family-specific varying parameters from the simulation registry.",
  "- one colored line per selected sample size n.",
  "",
  "Manual family-aware parameter mapping:",
  family_lines,
  "",
  paste0("Rows in fig_04 table: ", nrow(fig04_tbl)),
  "",
  "Coverage by family and n:",
  coverage_lines
)

write_lines(report_lines, file.path(out_dir, "fig_04_parameter_range_classical_report.txt"))

message("[240_fig04_parameter_range_classical] Done.")