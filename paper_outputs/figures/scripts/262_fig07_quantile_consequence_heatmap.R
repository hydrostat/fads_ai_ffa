# =========================================================
# 262_fig07_quantile_consequence_heatmap.R
#
# Purpose:
# Build Figure 7 from the outputs prepared by
# 260_quantile_consequence_prepare.R
#
# Output:
# paper_output/final_figures/fig_07_quantile_consequence_heatmap.png
# =========================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(ggplot2)
})

# ---------------------------------------------------------
# 0. PATHS
# ---------------------------------------------------------
base_root <- "C:/Users/wilso/OneDrive/My papers/SEAF_AI/code"
qc_dir <- file.path(base_root, "paper_output", "quantile_consequence")
fig_dir <- file.path(base_root, "paper_output", "final_figures")

dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

channel_path <- file.path(qc_dir, "quantile_consequence_channel_summary.csv")
if (!file.exists(channel_path)) {
  stop("Missing prepared file: ", channel_path)
}

# ---------------------------------------------------------
# 1. READ CHANNEL SUMMARY
# ---------------------------------------------------------
family_order <- c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM")

min_cases_heatmap <- 25L

channel_summary <- read_csv(channel_path, show_col_types = FALSE) %>%
  mutate(
    T = as.character(T),
    true_family = as.character(true_family),
    pred_family = as.character(pred_family)
  ) %>%
  filter(T %in% c("100", "500"))

# complete grid to guarantee full 6x6 display for each panel
plot_grid <- expand.grid(
  T = c("100", "500"),
  true_family = family_order,
  pred_family = family_order,
  stringsAsFactors = FALSE
) %>%
  as_tibble()

channel_summary <- plot_grid %>%
  left_join(
    channel_summary,
    by = c("T", "true_family", "pred_family")
  ) %>%
  mutate(
    T = factor(T, levels = c("100", "500")),
    true_family = factor(true_family, levels = family_order),
    pred_family = factor(pred_family, levels = family_order),
    n_cases = dplyr::coalesce(n_cases, 0),
    median_are_plot = if_else(n_cases >= min_cases_heatmap, median_are, NA_real_),
    label_txt = if_else(
      n_cases >= min_cases_heatmap & !is.na(median_are),
      sprintf("%.2f", median_are),
      ""
    )
  ) %>%
  mutate(median_are_plot = if_else(true_family != pred_family, median_are_plot, NA_real_)) %>%
  mutate(label_txt = if_else(is.na(median_are_plot),'' ,label_txt))



# ---------------------------------------------------------
# 2. FIGURE 7
# ---------------------------------------------------------
p7 <- ggplot(channel_summary, aes(x = pred_family, y = true_family, fill = median_are_plot)) +
  geom_tile(color = "black",
            linewidth = 0.2,
            linetype = 1) +
  geom_text(aes(label = label_txt), size = 4.0, color = 'black') +
  facet_wrap(~T) +
  scale_fill_distiller(
    type = "div",
    direction = 1,
    palette = "Blues",
    na.value = "transparent",
    ) +
  labs(
    # title = "Median quantile consequence by confusion channel",
    x = "Predicted family",
    y = "True family",
    fill = "Median ARE"
  ) +
  theme_classic() +
  theme(
    # panel.grid.major = element_line(color = "grey",
    #                                 linewidth = 0.1),
    panel.border = element_rect(fill = NA, linewidth = 0.8),
    axis.line = element_line(linewidth = 0.5),
    axis.text = element_text(size = 12, color = "black"),
    axis.title = element_text(size = 14, color = "black", face = 'bold'),
    strip.text = element_text(size=15, face="bold"),
    strip.background = element_rect(colour="black", fill="gray90", 
                                    linewidth=0.8, linetype="solid")
  )

ggsave(
  filename = file.path(fig_dir, "fig_07_quantile_consequence_heatmap.png"),
  plot = p7,
  width = 11,
  height = 7,
  dpi = 300,
  units = "in"
)

message("[262_fig07_quantile_consequence_heatmap] Done.")