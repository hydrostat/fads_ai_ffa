# =========================================================
# 060_feature_assertiveness_index.R
# Compute feature-level assertiveness index (FAI)
# for:
# - GOF p-values
# - Bayesian calibrated GOF features
# - LMRD acceptance indicators
# - AIC / BIC
# =========================================================

library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(arrow)
library(readr)
library(tibble)

# ---------------------------------------------------------
# 0. SETTINGS
# ---------------------------------------------------------
root_dir <- "/home/thesys/santoswi/my_project"
setwd(root_dir)

dataset_path <- "code_output/datasets/full_dataset.parquet"
param_registry_path <- "code_output/config/parameter_registry.rds"
out_dir <- "code_output/paper_output/feature_assertiveness"

dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

alpha <- 0.05
delta_ic_threshold <- 2
tol <- 1e-12

candidate_families <- c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM")
gof_features <- c("KS_p", "AD_p", "CvM_p")
bayes_bf_features <- c("KS_bfmin", "AD_bfmin", "CvM_bfmin")
bayes_elog_features <- c("KS_elogbf", "AD_elogbf", "CvM_elogbf")
ic_features <- c("AIC", "BIC")

# ---------------------------------------------------------
# 1. LOAD DATA
# ---------------------------------------------------------
cat("\n[FAI] Loading full dataset...\n")
df <- read_parquet(dataset_path)

cat("[FAI] Loading parameter registry...\n")
param_tbl <- readRDS(param_registry_path)

cat("[FAI] Rows:", nrow(df), "\n")
cat("[FAI] Cols:", ncol(df), "\n")

# ---------------------------------------------------------
# 2. NORMALIZE CORE COLUMNS
# ---------------------------------------------------------
if ("true_family" %in% names(df)) {
  df <- df %>% mutate(true_family_std = as.character(true_family))
} else if ("family" %in% names(df)) {
  df <- df %>% mutate(true_family_std = as.character(family))
} else {
  stop("Could not find true_family or family in full_dataset.")
}

if ("param_id" %in% names(df)) {
  df <- df %>% mutate(param_id_std = as.character(param_id))
} else if (all(c("param_id.x", "param_id.y") %in% names(df))) {
  df <- df %>% mutate(param_id_std = coalesce(as.character(param_id.x), as.character(param_id.y)))
} else if ("param_id.x" %in% names(df)) {
  df <- df %>% mutate(param_id_std = as.character(param_id.x))
} else if ("param_id.y" %in% names(df)) {
  df <- df %>% mutate(param_id_std = as.character(param_id.y))
} else {
  stop("Could not find param_id / param_id.x / param_id.y in full_dataset.")
}

if ("n" %in% names(df)) {
  df <- df %>% mutate(n_std = as.integer(n))
} else if (all(c("n.x", "n.y") %in% names(df))) {
  df <- df %>% mutate(n_std = as.integer(coalesce(n.x, n.y)))
} else if ("n.x" %in% names(df)) {
  df <- df %>% mutate(n_std = as.integer(n.x))
} else if ("n.y" %in% names(df)) {
  df <- df %>% mutate(n_std = as.integer(n.y))
} else {
  stop("Could not find n / n.x / n.y in full_dataset.")
}

required_cols <- c("sample_id", "true_family_std", "param_id_std", "n_std")
missing_cols <- setdiff(required_cols, names(df))
if (length(missing_cols) > 0) {
  stop("Missing required columns: ", paste(missing_cols, collapse = ", "))
}

# ---------------------------------------------------------
# 3. DEFINE REAL FEATURE COLUMNS
# ---------------------------------------------------------
expected_gof_cols <- as.vector(outer(gof_features, candidate_families, paste, sep = "_"))
available_gof_cols <- intersect(expected_gof_cols, names(df))

expected_bf_cols <- as.vector(outer(bayes_bf_features, candidate_families, paste, sep = "_"))
available_bf_cols <- intersect(expected_bf_cols, names(df))

expected_elog_cols <- as.vector(outer(bayes_elog_features, candidate_families, paste, sep = "_"))
available_elog_cols <- intersect(expected_elog_cols, names(df))

expected_lmrd_cols <- paste0("lmrd_inside_", candidate_families)
available_lmrd_cols <- intersect(expected_lmrd_cols, names(df))

expected_ic_cols <- as.vector(outer(ic_features, candidate_families, paste, sep = "_"))
available_ic_cols <- intersect(expected_ic_cols, names(df))

feature_map_gof <- tibble(raw_col = available_gof_cols) %>%
  mutate(
    feature_name = str_extract(raw_col, "^(KS_p|AD_p|CvM_p)"),
    candidate_family = str_extract(raw_col, "(GEV|GPA|PE3|LN2|LN3|GUM)$"),
    feature_type = "gof_pvalue"
  )

feature_map_bf <- tibble(raw_col = available_bf_cols) %>%
  mutate(
    feature_name = str_extract(raw_col, "^(KS_bfmin|AD_bfmin|CvM_bfmin)"),
    candidate_family = str_extract(raw_col, "(GEV|GPA|PE3|LN2|LN3|GUM)$"),
    feature_type = "bayes_bfmin"
  )

feature_map_elog <- tibble(raw_col = available_elog_cols) %>%
  mutate(
    feature_name = str_extract(raw_col, "^(KS_elogbf|AD_elogbf|CvM_elogbf)"),
    candidate_family = str_extract(raw_col, "(GEV|GPA|PE3|LN2|LN3|GUM)$"),
    feature_type = "bayes_elogbf"
  )

feature_map_lmrd <- tibble(raw_col = available_lmrd_cols) %>%
  mutate(
    feature_name = "LMRD_95",
    candidate_family = str_extract(raw_col, "(GEV|GPA|PE3|LN2|LN3|GUM)$"),
    feature_type = "lmrd_binary"
  )

feature_map_ic <- tibble(raw_col = available_ic_cols) %>%
  mutate(
    feature_name = str_extract(raw_col, "^(AIC|BIC)"),
    candidate_family = str_extract(raw_col, "(GEV|GPA|PE3|LN2|LN3|GUM)$"),
    feature_type = "ic_value"
  )

feature_map_binary <- bind_rows(
  feature_map_gof,
  feature_map_bf,
  feature_map_elog,
  feature_map_lmrd
)

cat("\n[FAI] GOF columns found:\n")
print(feature_map_gof)

cat("\n[FAI] Bayesian bfmin columns found:\n")
print(feature_map_bf)

cat("\n[FAI] Bayesian elogbf columns found:\n")
print(feature_map_elog)

cat("\n[FAI] LMRD columns found:\n")
print(feature_map_lmrd)

cat("\n[FAI] IC columns found:\n")
print(feature_map_ic)

write_csv(
  bind_rows(feature_map_binary, feature_map_ic),
  file.path(out_dir, "feature_map_full.csv")
)

# ---------------------------------------------------------
# 4. BINARY-STYLE FEATURES: LONG TABLE
# ---------------------------------------------------------
cat("\n[FAI] Building long-format table for binary-style features...\n")

long_binary <- df %>%
  select(sample_id, true_family_std, param_id_std, n_std, all_of(feature_map_binary$raw_col)) %>%
  pivot_longer(
    cols = all_of(feature_map_binary$raw_col),
    names_to = "raw_col",
    values_to = "feature_value"
  ) %>%
  left_join(feature_map_binary, by = "raw_col") %>%
  mutate(
    approved = case_when(
      feature_type == "gof_pvalue" ~ if_else(!is.na(feature_value) & feature_value > alpha, 1L, 0L),
      feature_type == "bayes_bfmin" ~ if_else(!is.na(feature_value) & feature_value >= (1 - tol), 1L, 0L),
      feature_type == "bayes_elogbf" ~ if_else(!is.na(feature_value) & feature_value <= tol, 1L, 0L),
      feature_type == "lmrd_binary" ~ if_else(!is.na(feature_value) & as.numeric(feature_value) > 0, 1L, 0L),
      TRUE ~ NA_integer_
    )
  )

cat("[FAI] Binary-style long table rows:", nrow(long_binary), "\n")

# ---------------------------------------------------------
# 5. BINARY-STYLE FEATURES: APPROVAL RATES
# ---------------------------------------------------------
approval_rates_binary <- long_binary %>%
  group_by(feature_name, true_family_std, param_id_std, n_std, candidate_family) %>%
  summarise(
    p_accept = mean(approved, na.rm = TRUE),
    n_rep = sum(!is.na(approved)),
    .groups = "drop"
  )

# ---------------------------------------------------------
# 6. BINARY-STYLE FEATURES: CASE-LEVEL FAI
# ---------------------------------------------------------
fai_case_binary <- approval_rates_binary %>%
  group_by(feature_name, true_family_std, param_id_std, n_std) %>%
  summarise(
    p_true = p_accept[candidate_family == first(true_family_std)][1],
    p_false_mean = mean(p_accept[candidate_family != first(true_family_std)], na.rm = TRUE),
    p_false_max = max(p_accept[candidate_family != first(true_family_std)], na.rm = TRUE),
    n_rep_case = first(n_rep),
    fai = p_true * (1 - p_false_mean),
    fai_wc = p_true * (1 - p_false_max),
    .groups = "drop"
  ) %>%
  rename(
    true_family = true_family_std,
    param_id = param_id_std,
    n = n_std
  )

# ---------------------------------------------------------
# 7. IC FEATURES: LONG TABLE
# ---------------------------------------------------------
cat("\n[FAI] Building long-format table for AIC/BIC...\n")

long_ic <- df %>%
  select(sample_id, true_family_std, param_id_std, n_std, all_of(feature_map_ic$raw_col)) %>%
  pivot_longer(
    cols = all_of(feature_map_ic$raw_col),
    names_to = "raw_col",
    values_to = "ic_value"
  ) %>%
  left_join(feature_map_ic, by = "raw_col")

cat("[FAI] IC long table rows:", nrow(long_ic), "\n")

# ---------------------------------------------------------
# 8. IC FEATURES: WINNER + CLOSE FLAGS
# ---------------------------------------------------------
long_ic <- long_ic %>%
  group_by(sample_id, feature_name) %>%
  mutate(
    ic_min = suppressWarnings(min(ic_value, na.rm = TRUE)),
    ic_min = if_else(is.infinite(ic_min), NA_real_, ic_min),
    delta_ic = ic_value - ic_min,
    is_winner = if_else(!is.na(delta_ic) & delta_ic == 0, 1L, 0L),
    is_close = if_else(!is.na(delta_ic) & delta_ic <= delta_ic_threshold, 1L, 0L)
  ) %>%
  ungroup()

ic_case_components <- long_ic %>%
  group_by(feature_name, true_family_std, param_id_std, n_std, candidate_family) %>%
  summarise(
    p_win = mean(is_winner, na.rm = TRUE),
    p_close = mean(is_close, na.rm = TRUE),
    n_rep = sum(!is.na(ic_value)),
    .groups = "drop"
  )

fai_case_ic <- ic_case_components %>%
  group_by(feature_name, true_family_std, param_id_std, n_std) %>%
  summarise(
    p_true = p_win[candidate_family == first(true_family_std)][1],
    p_false_mean = mean(p_close[candidate_family != first(true_family_std)], na.rm = TRUE),
    p_false_max = max(p_close[candidate_family != first(true_family_std)], na.rm = TRUE),
    n_rep_case = first(n_rep),
    fai = p_true * (1 - p_false_mean),
    fai_wc = p_true * (1 - p_false_max),
    .groups = "drop"
  ) %>%
  rename(
    true_family = true_family_std,
    param_id = param_id_std,
    n = n_std
  )

# ---------------------------------------------------------
# 9. COMBINE ALL CASE-LEVEL RESULTS
# ---------------------------------------------------------
fai_case <- bind_rows(fai_case_binary, fai_case_ic)

# ---------------------------------------------------------
# 10. JOIN PARAMETER REGISTRY
# ---------------------------------------------------------
fai_case <- fai_case %>%
  left_join(
    param_tbl %>%
      transmute(
        param_id = as.character(param_id),
        param_family = as.character(family),
        par1, par2, par3,
        par1_name, par2_name, par3_name,
        delta
      ),
    by = "param_id"
  )

# ---------------------------------------------------------
# 11. SUMMARIES
# ---------------------------------------------------------
fai_global <- fai_case %>%
  group_by(feature_name) %>%
  summarise(
    n_cases = n(),
    fai_mean = mean(fai, na.rm = TRUE),
    fai_median = median(fai, na.rm = TRUE),
    fai_sd = sd(fai, na.rm = TRUE),
    fai_wc_mean = mean(fai_wc, na.rm = TRUE),
    fai_wc_median = median(fai_wc, na.rm = TRUE),
    p_true_mean = mean(p_true, na.rm = TRUE),
    p_false_mean_mean = mean(p_false_mean, na.rm = TRUE),
    p_false_max_mean = mean(p_false_max, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(fai_mean))

fai_by_family <- fai_case %>%
  group_by(feature_name, true_family) %>%
  summarise(
    n_cases = n(),
    fai_mean = mean(fai, na.rm = TRUE),
    fai_wc_mean = mean(fai_wc, na.rm = TRUE),
    p_true_mean = mean(p_true, na.rm = TRUE),
    p_false_mean_mean = mean(p_false_mean, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(true_family, desc(fai_mean))

fai_by_n <- fai_case %>%
  group_by(feature_name, n) %>%
  summarise(
    n_cases = n(),
    fai_mean = mean(fai, na.rm = TRUE),
    fai_wc_mean = mean(fai_wc, na.rm = TRUE),
    p_true_mean = mean(p_true, na.rm = TRUE),
    p_false_mean_mean = mean(p_false_mean, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(n, desc(fai_mean))

# ---------------------------------------------------------
# 12. FALSE BREAKDOWN
# ---------------------------------------------------------
false_breakdown_binary <- approval_rates_binary %>%
  filter(candidate_family != true_family_std) %>%
  group_by(feature_name, true_family_std, candidate_family) %>%
  summarise(
    p_accept_mean = mean(p_accept, na.rm = TRUE),
    p_accept_median = median(p_accept, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  rename(true_family = true_family_std) %>%
  mutate(metric_type = "binary_acceptance")

false_breakdown_ic <- ic_case_components %>%
  filter(candidate_family != true_family_std) %>%
  group_by(feature_name, true_family_std, candidate_family) %>%
  summarise(
    p_accept_mean = mean(p_close, na.rm = TRUE),
    p_accept_median = median(p_close, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  rename(true_family = true_family_std) %>%
  mutate(metric_type = "ic_close_competition")

false_acceptance_breakdown <- bind_rows(false_breakdown_binary, false_breakdown_ic) %>%
  arrange(feature_name, true_family, desc(p_accept_mean))

# ---------------------------------------------------------
# 13. SAVE OUTPUTS
# ---------------------------------------------------------
write_csv(fai_case, file.path(out_dir, "feature_assertiveness_case_level.csv"))
write_csv(fai_global, file.path(out_dir, "feature_assertiveness_global.csv"))
write_csv(fai_by_family, file.path(out_dir, "feature_assertiveness_by_family.csv"))
write_csv(fai_by_n, file.path(out_dir, "feature_assertiveness_by_n.csv"))
write_csv(false_acceptance_breakdown, file.path(out_dir, "feature_false_acceptance_breakdown.csv"))

# ---------------------------------------------------------
# 14. PRINT DIAGNOSTICS
# ---------------------------------------------------------
cat("\n[FAI] Global ranking of features:\n")
print(fai_global, n = 200)

cat("\n[FAI] Feature performance by true family:\n")
print(fai_by_family, n = 300)

cat("\n[FAI] Feature performance by sample size:\n")
print(fai_by_n, n = 300)

cat("\n[FAI] Outputs written to:\n")
cat(out_dir, "\n")