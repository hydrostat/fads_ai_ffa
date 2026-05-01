# =========================================================
# 230_fig03_sample_size_classical.R
# Figure 3:
# Held-out test accuracy as a function of sample size
# under the final selected scenario:
# - model = XGB
# - scenario = classical
#
# Final design:
# - one panel
# - x-axis = sample size (n)
# - y-axis = held-out test accuracy
#
# Inputs:
# - code_output_10K/evaluation/test_by_n_best_by_scenario.parquet
#
# Outputs:
# - paper_output/final_figures/fig_02_sample_size_classical.png
# - paper_output/final_figures/fig_02_sample_size_classical.csv
# - paper_output/final_figures/fig_02_sample_size_classical_report.txt
# =========================================================

suppressPackageStartupMessages({
  library(dplyr)
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

input_path <- file.path(eval_dir, "test_by_n_best_by_scenario.parquet")
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

# ---------------------------------------------------------
# 2. READ AND STANDARDIZE
# ---------------------------------------------------------
raw_tbl <- read_parquet(input_path)

scenario_col <- first_present(raw_tbl, c("scenario"))
model_col <- first_present(raw_tbl, c("model"))
n_col <- first_present(raw_tbl, c("n"))
acc_col <- first_present(raw_tbl, c("accuracy"))

if (any(is.na(c(scenario_col, model_col, n_col, acc_col)))) {
  stop(
    "Could not identify required columns. Available columns: ",
    paste(names(raw_tbl), collapse = ", ")
  )
}

plot_tbl <- raw_tbl %>%
  transmute(
    scenario = as.character(.data[[scenario_col]]),
    model = as.character(.data[[model_col]]),
    n = as.numeric(.data[[n_col]]),
    accuracy = as.numeric(.data[[acc_col]])
  ) %>%
  filter(
    !is.na(scenario),
    !is.na(model),
    !is.na(n),
    !is.na(accuracy)
  ) %>%
  filter(
    scenario == "classical",
    model == "xgb"
  ) %>%
  arrange(n)

if (nrow(plot_tbl) == 0) {
  stop("No rows available after filtering to scenario = 'classical' and model = 'xgb'.")
}

save_csv(
  plot_tbl,
  "fig_03_sample_size_classical.csv"
)

# ---------------------------------------------------------
# 3. FIGURE
# ---------------------------------------------------------
p <- ggplot(plot_tbl, aes(x = n, y = accuracy)) +
  geom_line(linewidth = 0.6) +
  geom_point(size = 1.8) +
  labs(
    # title = "Held-out test performance as a function of sample size",
    x = "Sample size (n)",
    y = "Accuracy"
  ) +
  scale_x_continuous(breaks = seq(0,150, by=25)) +
  coord_cartesian(ylim = c(0.5, 1)) +
  theme_classic() +
  theme(
    panel.grid.major = element_line(color = "grey",
                                    linewidth = 0.1),
    axis.text = element_text(size = 12, color = "black"),
    axis.title = element_text(size = 14, color = "black", face = 'bold'),
    title = element_text(size = 10, color = "black", face = "bold"),
    legend.title = element_text(size = 12, color = "black", face = "bold")
  )

ggsave(
  filename = file.path(out_dir, "fig_03_sample_size_classical.png"),
  plot = p,
  width = 11,
  height = 7,
  dpi = 300,
  units = "in"
)

# ---------------------------------------------------------
# 4. REPORT
# ---------------------------------------------------------
gain_tbl <- plot_tbl %>%
  arrange(n) %>%
  mutate(
    delta_from_previous = accuracy - lag(accuracy)
  )

min_row <- plot_tbl %>%
  slice_min(accuracy, n = 1, with_ties = FALSE)

max_row <- plot_tbl %>%
  slice_max(accuracy, n = 1, with_ties = FALSE)

report_lines <- c(
  "======================================================",
  "FIGURE 3 REPORT",
  "======================================================",
  paste0("Base root: ", base_root),
  paste0("10K run root: ", run_root),
  "",
  "Source file:",
  paste0("- ", input_path),
  "",
  "Figure definition:",
  "- One-panel held-out test accuracy curve.",
  "- scenario fixed to classical.",
  "- model fixed to XGB.",
  "- x-axis = sample size (n).",
  "- y-axis = held-out test accuracy.",
  "",
  paste0("Rows used in plotting table: ", nrow(plot_tbl)),
  "",
  paste0(
    "Minimum accuracy: n = ", min_row$n,
    " | accuracy = ", round(min_row$accuracy, 4)
  ),
  paste0(
    "Maximum accuracy: n = ", max_row$n,
    " | accuracy = ", round(max_row$accuracy, 4)
  ),
  "",
  "Accuracy by sample size:"
)

for (i in seq_len(nrow(plot_tbl))) {
  report_lines <- c(
    report_lines,
    paste0(
      "- n = ", plot_tbl$n[i],
      " | accuracy = ", round(plot_tbl$accuracy[i], 4)
    )
  )
}

report_lines <- c(
  report_lines,
  "",
  "Incremental change relative to previous sample size:"
)

for (i in seq_len(nrow(gain_tbl))) {
  delta_i <- gain_tbl$delta_from_previous[i]
  delta_txt <- if (is.na(delta_i)) "NA" else sprintf("%.4f", delta_i)
  
  report_lines <- c(
    report_lines,
    paste0(
      "- n = ", gain_tbl$n[i],
      " | delta = ", delta_txt
    )
  )
}

write_lines(
  report_lines,
  file.path(out_dir, "fig_03_sample_size_classical_report.txt")
)

message("[220_fig03_sample_size_classical] Done.")