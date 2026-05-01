# =========================================================
# 261_fig06_quantile_consequence_global.R
#
# Purpose:
# Build Figure 6 from the outputs prepared by
# 260_quantile_consequence_prepare.R
#
# Final design:
# - violin plot
# - embedded narrow boxplot
# - log-scale y-axis
#
# Output:
# paper_output/final_figures/fig_06_quantile_consequence_global.png
# =========================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(arrow)
  library(ggplot2)
})

# ---------------------------------------------------------
# 0. PATHS
# ---------------------------------------------------------
base_root <- "C:/Users/wilso/OneDrive/My papers/SEAF_AI/code"
qc_dir <- file.path(base_root, "paper_output", "quantile_consequence")
fig_dir <- file.path(base_root, "paper_output", "final_figures")

dir.create(fig_dir, recursive = TRUE, showWarnings = FALSE)

case_path <- file.path(qc_dir, "quantile_consequence_case_level.parquet")
if (!file.exists(case_path)) {
  stop("Missing prepared file: ", case_path)
}

# ---------------------------------------------------------
# 1. READ CASE-LEVEL DATA
# ---------------------------------------------------------
quantile_case <- read_parquet(case_path) %>%
  filter(is.finite(are)) %>%
  mutate(
    T = factor(as.character(T), levels = c("50", "100", "500")),
    # protect log scale against exact zeros
    are_plot = pmax(are, 1e-4)
  )

if (nrow(quantile_case) == 0) {
  stop("No finite ARE values available for Figure 6.")
}

# ---------------------------------------------------------
# 2. FIGURE 6
# ---------------------------------------------------------
p6 <- ggplot(quantile_case, aes(x = T, y = are_plot)) +
  #   geom_violin(
  #   trim = FALSE,
  #   bounds = c(-Inf, +Inf),
  #   scale = "width",
  #   fill = "grey95",
  #   color = "grey30",
  #   linewidth = 0.1
  # ) +
  geom_boxplot(
    width = 0.12,
    outliers = FALSE,
    outlier.shape = NA,
    fill = "grey80",
    color = "black",
    linewidth = 0.2,
    staple.linewidth=0.2,
    staplewidth = 1
  ) +
  scale_y_continuous( breaks = seq(0,0.6,0.1), minor_breaks = seq(0,0.6,0.025)) +
  # scale_y_continuous(limits = c(0.0001,100), 
  #                    breaks = c(0.001, 0.01, 0.1,1,10,100),
  #                    minor_breaks = c(0.0001, 0.0002, 0.0003, 0.0004, 0.0005, 0.0006, 0.0007, 0.0008, 0.0009, 0.001, 0.002, 0.003, 0.004, 0.005, 0.006, 0.007, 0.008, 0.009, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100),
  #                    transform = 'log10',
  #                    labels = scales::label_log()) +
  # scale_y_log10() +
  labs(
    # title = "Consequences for design quantiles across return periods",
    x = "Return period (years)",
    y = "Absolute relative error (ARE)"
  ) +
  theme_classic() +
  theme(
    panel.grid.major = element_line(color = "grey60",
                                    linewidth = 0.1),
    panel.grid.minor.y = element_line(color = "gray80",
                                    linewidth = 0.1),
    
    panel.border = element_rect(fill = NA, linewidth = 0.8),
    axis.line = element_line(linewidth = 0.2),
    axis.text = element_text(size = 12, color = "black"),
    axis.title = element_text(size = 14, color = "black", face = 'bold')
   
  )

ggsave(
  filename = file.path(fig_dir, "fig_06_quantile_consequence_global.png"),
  plot = p6,
  width = 11,
  height = 7,
  dpi = 300,
  units = "in"
)

message("[261_fig06_quantile_consequence_global] Done.")