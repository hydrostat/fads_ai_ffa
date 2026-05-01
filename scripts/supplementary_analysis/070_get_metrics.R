

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
# 1) Total processing time per model
# ---------------------------------------------------------
time_summary <- registry_ok %>%
  group_by(model) %>%
  summarise(
    n_scenarios_ok = n(),
    total_elapsed_sec = sum(elapsed_sec, na.rm = TRUE),
    mean_elapsed_sec = mean(elapsed_sec, na.rm = TRUE),
    median_elapsed_sec = median(elapsed_sec, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    total_elapsed_min = total_elapsed_sec / 60,
    total_elapsed_hr  = total_elapsed_sec / 3600,
    mean_elapsed_min  = mean_elapsed_sec / 60
  )

# ---------------------------------------------------------
# 2) Accuracy summary per model
# ---------------------------------------------------------
acc_summary <- metrics_acc %>%
  group_by(model) %>%
  summarise(
    mean_accuracy = mean(.estimate, na.rm = TRUE),
    median_accuracy = median(.estimate, na.rm = TRUE),
    min_accuracy = min(.estimate, na.rm = TRUE),
    max_accuracy = max(.estimate, na.rm = TRUE),
    .groups = "drop"
  )

# ---------------------------------------------------------
# 3) Best scenario for each model
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
# 4) Final summary table
# ---------------------------------------------------------
summary_tbl <- time_summary %>%
  left_join(acc_summary, by = "model") %>%
  left_join(best_scenario_per_model, by = "model") %>%
  arrange(desc(best_accuracy), total_elapsed_sec)

# ---------------------------------------------------------
# 5) Optional: prettier formatting
# ---------------------------------------------------------
summary_tbl_pretty <- summary_tbl %>%
  transmute(
    model,
    n_scenarios_ok,
    total_elapsed_hr = round(total_elapsed_hr, 2),
    total_elapsed_min = round(total_elapsed_min, 1),
    mean_elapsed_min = round(mean_elapsed_min, 1),
    mean_accuracy = round(mean_accuracy, 4),
    median_accuracy = round(median_accuracy, 4),
    min_accuracy = round(min_accuracy, 4),
    max_accuracy = round(max_accuracy, 4),
    best_scenario,
    best_accuracy = round(best_accuracy, 4)
  )

print(summary_tbl_pretty, n = Inf)

# ---------------------------------------------------------
# 6) Save outputs
# ---------------------------------------------------------
write_csv(summary_tbl_pretty,
          file.path(model_dir, "summary_models_by_time_and_accuracy.csv"))

write_parquet(summary_tbl,
              file.path(model_dir, "summary_models_by_time_and_accuracy.parquet"))
