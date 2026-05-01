# =========================================================
# 220_fig02_fai_global_ranking.R
# Optional figure:
# Global ranking of isolated features by Feature Assertiveness Index (FAI)
#
# Final requested version:
# - single panel
# - bars ordered by feature type blocks
# - ranked by FAI within each feature-type block
# - prettier labels
# - FAI values printed at the end of each bar
#
# Input folder:
# C:/Users/wilso/OneDrive/My papers/SEAF_AI/code/code_output_10K/paper_output/feature_assertiveness
#
# Expected input:
# - feature_assertiveness_global.csv
#
# Outputs:
# - C:/Users/wilso/OneDrive/My papers/SEAF_AI/code/paper_output/final_figures/fig_fai_global_ranking.png
# - C:/Users/wilso/OneDrive/My papers/SEAF_AI/code/paper_output/final_figures/fig_fai_global_ranking.csv
# - C:/Users/wilso/OneDrive/My papers/SEAF_AI/code/paper_output/final_figures/fig_fai_global_ranking_report.txt
# =========================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(tibble)
  library(readr)
  library(ggplot2)
  library(stringr)
  library(forcats)
})

# ---------------------------------------------------------
# 0. PATHS
# ---------------------------------------------------------
base_root <- "C:/Users/wilso/OneDrive/My papers/SEAF_AI/code"
fai_dir <- file.path(base_root, "code_output_10K", "paper_output", "feature_assertiveness")
out_dir <- file.path(base_root, "paper_output", "final_figures")

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

input_path <- file.path(fai_dir, "feature_assertiveness_global.csv")
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

classify_feature_type <- function(x) {
  case_when(
    str_detect(x, "^(AIC|BIC)$") ~ "Information criteria",
    str_detect(x, "^LMRD") ~ "Structural",
    str_detect(x, "^(KS|AD|CvM)_p$") ~ "Classical GOF",
    str_detect(x, "^(KS|AD|CvM)_(bfmin|elogbf)$") ~ "Bayesian GOF",
    TRUE ~ "Other"
  )
}

pretty_feature_label <- function(x) {
  x %>%
    str_replace_all("^KS_p$", "KS p") %>%
    str_replace_all("^AD_p$", "AD p") %>%
    str_replace_all("^CvM_p$", "CvM p") %>%
    str_replace_all("^KS_bfmin$", "KS BFmin") %>%
    str_replace_all("^KS_elogbf$", "KS eLogBF") %>%
    str_replace_all("^AD_bfmin$", "AD BFmin") %>%
    str_replace_all("^AD_elogbf$", "AD eLogBF") %>%
    str_replace_all("^CvM_bfmin$", "CvM BFmin") %>%
    str_replace_all("^CvM_elogbf$", "CvM eLogBF") %>%
    str_replace_all("^LMRD_95$", "LMRD 95%")
}

# ---------------------------------------------------------
# 2. READ AND STANDARDIZE
# ---------------------------------------------------------
raw_tbl <- read_csv(input_path, show_col_types = FALSE)

feature_col <- first_present(raw_tbl, c("feature", "feature_name", "term"))
fai_col <- first_present(raw_tbl, c("fai_mean", "FAI mean", "fai", "FAI"))
ptrue_col <- first_present(raw_tbl, c("p_true_mean", "p_true mean", "p_true"))
pfalse_col <- first_present(raw_tbl, c("p_false_mean", "p_false mean", "p_false"))

if (any(is.na(c(feature_col, fai_col)))) {
  stop(
    "Could not identify required columns in feature_assertiveness_global.csv. Columns found: ",
    paste(names(raw_tbl), collapse = ", ")
  )
}

plot_tbl <- raw_tbl %>%
  transmute(
    feature_raw = as.character(.data[[feature_col]]),
    fai_mean = as.numeric(.data[[fai_col]]),
    p_true_mean = if (!is.na(ptrue_col)) as.numeric(.data[[ptrue_col]]) else NA_real_,
    p_false_mean = if (!is.na(pfalse_col)) as.numeric(.data[[pfalse_col]]) else NA_real_
  ) %>%
  filter(!is.na(feature_raw), !is.na(fai_mean)) %>%
  mutate(
    feature_type = classify_feature_type(feature_raw),
    feature = pretty_feature_label(feature_raw)
  )

feature_type_order <- c(
  "Information criteria",
  "Structural",
  "Classical GOF",
  "Bayesian GOF",
  "Other"
)

plot_tbl <- plot_tbl %>%
  mutate(
    feature_type = factor(feature_type, levels = feature_type_order)
  ) %>%
  arrange(feature_type, desc(fai_mean), feature) %>%
  group_by(feature_type) %>%
  mutate(rank_within_type = row_number()) %>%
  ungroup() %>%
  mutate(global_order = row_number())

if (nrow(plot_tbl) == 0) {
  stop("No rows available for plotting in feature_assertiveness_global.csv.")
}

# factor order for plotting:
# top-to-bottom follows feature type blocks, ranked within each block
plot_tbl <- plot_tbl %>%
  mutate(
    feature_plot = factor(feature, levels = rev(feature))
  )

plot_out <- plot_tbl %>%
  select(
    global_order,
    feature_type,
    rank_within_type,
    feature_raw,
    feature,
    fai_mean,
    p_true_mean,
    p_false_mean
  )

save_csv(plot_out, "fig_02_fai_global_ranking.csv")

# ---------------------------------------------------------
# 3. FIGURE
# ---------------------------------------------------------
x_max <- max(plot_tbl$fai_mean, na.rm = TRUE)

feature_type_order <- c(
  "Information criteria",
  "Structural",
  "Classical GOF",
  "Bayesian GOF",
  "Other"
)

p <- ggplot(plot_tbl, aes(x = fai_mean, y = feature_plot, fill = feature_type)) +
  geom_col(width = 0.72) +
  geom_text(
    aes(label = sprintf("%.3f", fai_mean)),
    hjust = -0.12,
    size = 3.8
  ) +
  scale_fill_manual(name="Scenario", values=c(  "Information criteria" = "#525252",
                                                "Structural"           = "#737373",
                                                "Classical GOF"        = "#969696",
                                                "Bayesian GOF"         = "#bdbdbd")) +
  labs(
    # title = "Global ranking of isolated features by FAI",
    x = "FAI mean",
    y = "Feature",
    fill = "Feature type"
  ) +
  coord_cartesian(xlim = c(0, x_max * 1.12)) +
  theme_classic() +
  theme(
    plot.title = element_text(face = "bold"),
    legend.position = "bottom",
    axis.text = element_text(size = 12, color = "black"),
    axis.title = element_text(size = 14, color = "black", face = 'bold'),
    title = element_text(size = 10, color = "black", face = "bold"),
    legend.title = element_text(size = 12, color = "black", face = "bold")
  )

ggsave(
  filename = file.path(out_dir, "fig_02_fai_global_ranking.png"),
  plot = p,
  width = 11,
  height = 7,
  dpi = 300,
  units = "in"
)

# ---------------------------------------------------------
# 4. REPORT
# ---------------------------------------------------------
top_tbl <- plot_out %>%
  arrange(global_order) %>%
  slice_head(n = 5)

type_summary_tbl <- plot_out %>%
  group_by(feature_type) %>%
  summarise(
    n_features = n(),
    mean_fai = mean(fai_mean, na.rm = TRUE),
    best_feature = feature[which.max(fai_mean)],
    best_fai = max(fai_mean, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(match(feature_type, feature_type_order))

report_lines <- c(
  "======================================================",
  "FAI GLOBAL RANKING FIGURE REPORT",
  "======================================================",
  paste0("Base root: ", base_root),
  paste0("FAI input directory: ", fai_dir),
  "",
  "Source file:",
  paste0("- ", input_path),
  "",
  "Figure definition:",
  "- Single-panel horizontal bar chart of global FAI.",
  "- Bars grouped by feature type through ordering.",
  "- Ranked by FAI within each feature-type block.",
  "- FAI values printed at the end of each bar.",
  "",
  paste0("Number of features plotted: ", nrow(plot_out)),
  "",
  "Top 5 features in displayed order:"
)

for (i in seq_len(nrow(top_tbl))) {
  report_lines <- c(
    report_lines,
    paste0(
      "- ", top_tbl$feature[i],
      " | FAI = ", round(top_tbl$fai_mean[i], 4),
      if (!is.na(top_tbl$p_true_mean[i])) paste0(" | p_true = ", round(top_tbl$p_true_mean[i], 4)) else "",
      if (!is.na(top_tbl$p_false_mean[i])) paste0(" | p_false = ", round(top_tbl$p_false_mean[i], 4)) else ""
    )
  )
}

report_lines <- c(
  report_lines,
  "",
  "Summary by feature type:"
)

for (i in seq_len(nrow(type_summary_tbl))) {
  report_lines <- c(
    report_lines,
    paste0(
      "- ", type_summary_tbl$feature_type[i],
      " | n_features = ", type_summary_tbl$n_features[i],
      " | mean_FAI = ", round(type_summary_tbl$mean_fai[i], 4),
      " | best_feature = ", type_summary_tbl$best_feature[i],
      " | best_FAI = ", round(type_summary_tbl$best_fai[i], 4)
    )
  )
}

write_lines(
  report_lines,
  file.path(out_dir, "fig_02_fai_global_ranking_report.txt")
)

message("[250_fig_fai_global_ranking] Done.")