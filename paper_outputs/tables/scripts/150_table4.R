# =========================================================
# 150_table4.R
#
# Purpose:
# Build Table 4 from the outputs prepared by
# 260_quantile_consequence_prepare.R
#
# Output:
# paper_output/final_tables/table_04_quantile_consequence_summary.csv
# =========================================================

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(arrow)
})

# ---------------------------------------------------------
# 0. PATHS
# ---------------------------------------------------------
base_root <- "C:/Users/wilso/OneDrive/My papers/SEAF_AI/code"
qc_dir <- file.path(base_root, "paper_output", "quantile_consequence")
tab_dir <- file.path(base_root, "paper_output", "final_tables")

dir.create(tab_dir, recursive = TRUE, showWarnings = FALSE)

case_path <- file.path(qc_dir, "quantile_consequence_case_level.parquet")
if (!file.exists(case_path)) {
  stop("Missing prepared file: ", case_path)
}

# ---------------------------------------------------------
# 1. READ CASE-LEVEL DATA
# ---------------------------------------------------------
quantile_case <- read_parquet(case_path)

# ---------------------------------------------------------
# 2. BUILD TABLE 4
# ---------------------------------------------------------
table_04 <- quantile_case %>%
  filter(is.finite(are)) %>%
  mutate(T = as.integer(as.character(T))) %>%
  group_by(T) %>%
  summarise(
    median_are_all = median(are, na.rm = TRUE),
    p90_are_all = quantile(are, probs = 0.90, na.rm = TRUE),
    median_are_correct = median(are[is_correct], na.rm = TRUE),
    median_are_misclassified = median(are[!is_correct], na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(T)

write_csv(
  table_04,
  file.path(tab_dir, "table_04_quantile_consequence_summary.csv")
)

message("[150_table4] Done.")