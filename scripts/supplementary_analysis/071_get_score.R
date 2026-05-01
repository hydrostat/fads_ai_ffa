
setwd('/home/thesys/santoswi/my_project')


library(dplyr)
library(arrow)
library(readr)

# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------
model_dir <- "code_output/ml_models"

registry_path <- file.path(model_dir, "training_registry_round1.parquet")
metrics_path  <- file.path(model_dir, "validation_metrics_round1.parquet")

# ---------------------------------------------------------
# Read outputs
# ---------------------------------------------------------
registry_tbl <- read_parquet(registry_path)
metrics_tbl  <- read_parquet(metrics_path)

# ---------------------------------------------------------
# Keep only successful runs
# ---------------------------------------------------------
registry_ok <- registry_tbl %>%
  filter(status == "ok")

metrics_acc <- metrics_tbl %>%
  filter(
    status == "ok",
    .metric == "accuracy"
  )

# ---------------------------------------------------------
# Helper: safe min-max normalization
# ---------------------------------------------------------
minmax_norm <- function(x) {
  rng <- range(x, na.rm = TRUE)
  if (!is.finite(rng[1]) || !is.finite(rng[2]) || diff(rng) == 0) {
    return(rep(0, length(x)))
  }
  (x - rng[1]) / (rng[2] - rng[1])
}

# ---------------------------------------------------------
# 1) Time summary per model
# ---------------------------------------------------------
time_summary <- registry_ok %>%
  group_by(model) %>%
  summarise(
    n_scenarios_ok   = n(),
    total_elapsed_sec = sum(elapsed_sec, na.rm = TRUE),
    mean_elapsed_sec  = mean(elapsed_sec, na.rm = TRUE),
    median_elapsed_sec = median(elapsed_sec, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    total_elapsed_min = total_elapsed_sec / 60,
    total_elapsed_hr  = total_elapsed_sec / 3600,
    mean_elapsed_min  = mean_elapsed_sec / 60
  )

# ---------------------------------------------------------
# 2) Accuracy + robustness summary per model
# ---------------------------------------------------------
acc_summary <- metrics_acc %>%
  group_by(model) %>%
  summarise(
    mean_accuracy   = mean(.estimate, na.rm = TRUE),
    median_accuracy = median(.estimate, na.rm = TRUE),
    sd_accuracy     = sd(.estimate, na.rm = TRUE),
    min_accuracy    = min(.estimate, na.rm = TRUE),
    max_accuracy    = max(.estimate, na.rm = TRUE),
    range_accuracy  = max_accuracy - min_accuracy,
    .groups = "drop"
  ) %>%
  mutate(
    sd_accuracy = if_else(is.na(sd_accuracy), 0, sd_accuracy)
  )

# ---------------------------------------------------------
# 3) Best scenario per model
# ---------------------------------------------------------
best_scenario_per_model <- metrics_acc %>%
  group_by(model) %>%
  slice_max(order_by = .estimate, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  transmute(
    model,
    best_scenario = scenario,
    best_accuracy = .estimate
  )

# ---------------------------------------------------------
# 4) Complexity score (lower = simpler)
# Adjust if you want a different theoretical ranking
# ---------------------------------------------------------
complexity_tbl <- tibble(
  model = c("multinom", "cart", "rf", "xgb", "svm_rbf", "mlp"),
  complexity_score = c(1, 2, 3, 4, 4, 5)
)

# ---------------------------------------------------------
# 5) Merge all summaries
# ---------------------------------------------------------
summary_tbl <- time_summary %>%
  left_join(acc_summary, by = "model") %>%
  left_join(best_scenario_per_model, by = "model") %>%
  left_join(complexity_tbl, by = "model")

# ---------------------------------------------------------
# 6) Build normalized dimensions
# Higher is better for acc_norm
# Higher is worse for the penalty terms
# ---------------------------------------------------------
summary_tbl <- summary_tbl %>%
  mutate(
    acc_norm            = minmax_norm(mean_accuracy),
    time_norm           = minmax_norm(total_elapsed_sec),
    complexity_norm     = minmax_norm(complexity_score),
    robustness_penalty  = minmax_norm(sd_accuracy),
    
    # Composite score:
    # reward performance, penalize time, complexity and instability
    score = acc_norm -
      0.30 * time_norm -
      0.20 * complexity_norm -
      0.20 * robustness_penalty
  ) %>%
  arrange(desc(score), desc(mean_accuracy))

# ---------------------------------------------------------
# 7) Pretty output table
# ---------------------------------------------------------
summary_tbl_pretty <- summary_tbl %>%
  transmute(
    model,
    n_scenarios_ok,
    total_elapsed_hr  = round(total_elapsed_hr, 2),
    total_elapsed_min = round(total_elapsed_min, 1),
    mean_elapsed_min  = round(mean_elapsed_min, 1),
    mean_accuracy     = round(mean_accuracy, 4),
    sd_accuracy       = round(sd_accuracy, 4),
    min_accuracy      = round(min_accuracy, 4),
    max_accuracy      = round(max_accuracy, 4),
    range_accuracy    = round(range_accuracy, 4),
    best_scenario,
    best_accuracy     = round(best_accuracy, 4),
    complexity_score,
    acc_norm          = round(acc_norm, 4),
    time_norm         = round(time_norm, 4),
    complexity_norm   = round(complexity_norm, 4),
    robustness_penalty = round(robustness_penalty, 4),
    score             = round(score, 4)
  )

print(summary_tbl_pretty, n = Inf)

# ---------------------------------------------------------
# 8) Optional: ranked table focused only on selection
# ---------------------------------------------------------
selection_tbl <- summary_tbl %>%
  transmute(
    model,
    score = round(score, 4),
    mean_accuracy = round(mean_accuracy, 4),
    sd_accuracy = round(sd_accuracy, 4),
    total_elapsed_hr = round(total_elapsed_hr, 2),
    complexity_score,
    best_scenario,
    best_accuracy = round(best_accuracy, 4)
  ) %>%
  arrange(desc(score), desc(mean_accuracy))

print(selection_tbl, n = Inf)

# ---------------------------------------------------------
# 9) Save outputs
# ---------------------------------------------------------
write_csv(
  summary_tbl_pretty,
  file.path(model_dir, "summary_models_multicriteria.csv")
)

write_csv(
  selection_tbl,
  file.path(model_dir, "selection_ranking_models_multicriteria.csv")
)

write_parquet(
  summary_tbl,
  file.path(model_dir, "summary_models_multicriteria.parquet")
)
