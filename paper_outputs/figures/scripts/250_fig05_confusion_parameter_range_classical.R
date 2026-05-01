# =========================================================
# 250_fig05_confusion_parameter_range_classical.R
# Figure 5:
# Dominant confusion share across the simulated parameter range
# under the final selected setting:
# - 10K
# - model = XGB
# - scenario = classical
#
# Final design:
# - one panel per true family
# - x-axis = population parameter value used in simulation
# - y-axis = share assigned to the dominant rival family
# - color = sample size (n)
# - point shape = dominant rival family
#
# Definition:
# For each true_family × param_id × n:
# - compute prediction shares across rival families
# - exclude the diagonal (correct family)
# - keep the rival with the largest share
#
# Included families:
# - GEV
# - GPA
# - LN2
# - LN3
# - PE3
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
# - paper_output/final_figures/fig_05_confusion_parameter_range_classical.png
# - paper_output/final_figures/fig_05_confusion_parameter_range_classical.csv
# - paper_output/final_figures/fig_05_confusion_parameter_range_classical_report.txt
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

rival_shape_order <- c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM")

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
    )
  ) %>%
  filter(scope == "best_by_scenario") %>%
  filter(model == "xgb") %>%
  filter(scenario == "classical") %>%
  filter(true_family %in% target_families) %>%
  filter(n %in% target_n) %>%
  left_join(param_map, by = "true_family")

missing_param_cols <- setdiff(unique(param_map$analysis_param_col), names(preds_use))
if (length(missing_param_cols) > 0) {
  stop(
    "Mapped parameter columns not found in predictions master: ",
    paste(missing_param_cols, collapse = ", ")
  )
}

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
# 3. DOMINANT RIVAL CONFUSION
# ---------------------------------------------------------
pred_share_tbl <- preds_use %>%
  filter(is.finite(analysis_param_value)) %>%
  group_by(true_family, analysis_param_name, param_id, analysis_param_value, n, pred_family) %>%
  summarise(
    n_cases = n(),
    .groups = "drop_last"
  ) %>%
  mutate(
    total_cases = sum(n_cases, na.rm = TRUE),
    pred_share = if_else(total_cases > 0, n_cases / total_cases, NA_real_)
  ) %>%
  ungroup()

fig05_tbl <- pred_share_tbl %>%
  filter(pred_family != true_family) %>%
  group_by(true_family, analysis_param_name, param_id, analysis_param_value, n) %>%
  arrange(desc(pred_share), pred_family) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  transmute(
    true_family,
    analysis_param_name,
    param_id,
    analysis_param_value,
    n,
    dominant_rival = pred_family,
    dominant_confusion_share = pred_share,
    rival_cases = n_cases,
    total_cases
  ) %>%
  arrange(true_family, n, analysis_param_value, param_id)

if (nrow(fig05_tbl) == 0) {
  stop("No rows available for Figure 5 after filtering to 10K + best_by_scenario + classical + xgb.")
}

save_csv(fig05_tbl, "fig_05_confusion_parameter_range_classical.csv")

# ---------------------------------------------------------
# 4. BUILD FIGURE
# ---------------------------------------------------------
fig05_df <- fig05_tbl %>%
  mutate(
    true_family = factor(true_family, levels = target_families),
    n = factor(n, levels = target_n)
  )

p <- ggplot(
  fig05_df,
  aes(
    x = analysis_param_value,
    y = dominant_confusion_share,
    group = n,
    color = n
  )
) +
  geom_line(linewidth = 0.5) +
  geom_point(size = 1.1) +
  facet_wrap(~true_family, scales = "free_x") +
  labs(
    # title = "Dominant misclassification share across the simulated parameter range",
    x = "Population parameter value used in simulation",
    y = "Share assigned to dominant rival",
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
  filename = file.path(out_dir, "fig_05_confusion_parameter_range_classical.png"),
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
dominant_counts <- fig05_tbl %>%
  count(true_family, dominant_rival, sort = TRUE)

max_conf_tbl <- fig05_tbl %>%
  group_by(true_family) %>%
  slice_max(dominant_confusion_share, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  transmute(
    true_family,
    n,
    analysis_param_value,
    dominant_rival,
    dominant_confusion_share
  )

report_lines <- c(
  "======================================================",
  "FIGURE 5 REPORT",
  "======================================================",
  paste0("Base root: ", base_root),
  "",
  "Source used:",
  "- results_master_predictions.parquet",
  "",
  "Files generated:",
  "- fig_05_confusion_parameter_range_classical.csv",
  "- fig_05_confusion_parameter_range_classical.png",
  "",
  "Design choice:",
  "- Scope shown: best_by_scenario only.",
  "- Scenario shown: classical only.",
  "- Model shown: xgb only.",
  "- For each true_family × param_id × n, the figure shows the rival family with the largest prediction share.",
  "- y-axis is the dominant rival share among all cases, not among errors only.",
  "- Included families: GEV, GPA, LN2, LN3, PE3.",
  "- Excluded family: GUM.",
  "",
  paste0("Rows in fig_05 table: ", nrow(fig05_tbl)),
  "",
  "Most frequently dominant rivals by true family:"
)

for (fam in unique(dominant_counts$true_family)) {
  sub <- dominant_counts %>% filter(true_family == fam)
  txt <- paste0(sub$dominant_rival, " (", sub$n, ")", collapse = ", ")
  report_lines <- c(report_lines, paste0("- ", fam, ": ", txt))
}

report_lines <- c(
  report_lines,
  "",
  "Maximum dominant confusion share by family:"
)

for (i in seq_len(nrow(max_conf_tbl))) {
  report_lines <- c(
    report_lines,
    paste0(
      "- ", max_conf_tbl$true_family[i],
      " | n = ", max_conf_tbl$n[i],
      " | parameter = ", signif(max_conf_tbl$analysis_param_value[i], 6),
      " | rival = ", max_conf_tbl$dominant_rival[i],
      " | dominant confusion share = ", round(max_conf_tbl$dominant_confusion_share[i], 4)
    )
  )
}

write_lines(report_lines, file.path(out_dir, "fig_05_confusion_parameter_range_classical_report.txt"))

message("[250_fig05_confusion_parameter_range_classical] Done.")