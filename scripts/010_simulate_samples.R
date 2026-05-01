# =========================================================
# 010_simulate_samples.R
# Build and validate simulation plan by distribution family
# =========================================================
#
# This script:
#   - builds the candidate-distribution parameter registry;
#   - adds population-specific positive-domain shifts;
#   - validates parameter sets;
#   - creates the simulation grid;
#   - saves simulation metadata.
#
# No raw Monte Carlo samples are stored at this stage.
# Samples are regenerated later from family, parameters,
# sample size, and seed.
#
# Required previous step:
#   scripts/000_config.R
# =========================================================

library(dplyr)
library(tidyr)
library(tibble)
library(arrow)
library(lmom)


# ---------------------------------------------------------
# Repository root check
# ---------------------------------------------------------

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

if (!file.exists(file.path(root_dir, "R")) ||
    !file.exists(file.path(root_dir, "scripts")) ||
    !file.exists(file.path(root_dir, "outputs", "config", "config_main.rds"))) {
  stop(
    "[010] This script must be run from the FADS_AI repository root ",
    "after scripts/000_config.R has been executed.\n",
    "Current working directory: ", root_dir
  )
}


# ---------------------------------------------------------
# Load helper functions and configuration
# ---------------------------------------------------------

source(file.path(root_dir, "R", "fun_io.R"))
source(file.path(root_dir, "R", "fun_distributions.R"))

config_path <- file.path(root_dir, "outputs", "config", "config_main.rds")

config <- read_rds(config_path)
paths  <- config$paths


# ---------------------------------------------------------
# Build parameter registry
# ---------------------------------------------------------

log_msg("[010] Building parameter registry...")

param_registry <- build_parameter_registry(config)


# ---------------------------------------------------------
# Add positive-domain deltas
# ---------------------------------------------------------

log_msg("[010] Adding population-specific positive-domain deltas...")

param_registry <- add_population_deltas(
  param_registry = param_registry,
  p_low = 1e-3,
  floor_value = 0,
  margin = 0.05
)


# ---------------------------------------------------------
# Delta summary
# ---------------------------------------------------------

log_msg("[010] Delta summary by family:")

print(
  param_registry %>%
    group_by(family) %>%
    summarise(
      n_pop = n(),
      min_delta = min(delta, na.rm = TRUE),
      mean_delta = mean(delta, na.rm = TRUE),
      max_delta = max(delta, na.rm = TRUE),
      .groups = "drop"
    )
)


# ---------------------------------------------------------
# Validate parameter registry
# ---------------------------------------------------------

log_msg("[010] Validating parameter registry...")

validation_out <- validate_parameter_registry(
  param_registry = param_registry,
  n_test = 5L,
  seed = config$experiment$seed_master
)

param_valid <- validation_out$valid_params
param_invalid <- validation_out$invalid_params


# ---------------------------------------------------------
# Build simulation grid
# ---------------------------------------------------------

log_msg("[010] Building simulation grid with valid parameters only...")

sim_grid <- build_simulation_grid(
  config = config,
  param_registry = param_valid
)


# ---------------------------------------------------------
# Save outputs
# ---------------------------------------------------------

log_msg("[010] Saving simulation metadata...")

save_rds(
  param_registry,
  file.path(paths$config, "parameter_registry.rds")
)

save_parquet(
  param_valid,
  file.path(paths$sim, "valid_parameter_registry.parquet")
)

save_parquet(
  param_invalid,
  file.path(paths$sim, "invalid_parameter_registry.parquet")
)

save_parquet(
  sim_grid,
  file.path(paths$sim, "simulation_grid.parquet")
)

save_session_info(
  file.path(paths$sim, "session_info_010.txt")
)


# ---------------------------------------------------------
# Summary
# ---------------------------------------------------------

log_msg("[010] Summary:")
log_msg("[010] Total parameter sets: ", nrow(param_registry))
log_msg("[010] Valid parameter sets: ", nrow(param_valid))
log_msg("[010] Invalid parameter sets: ", nrow(param_invalid))
log_msg("[010] Total planned simulations: ", nrow(sim_grid))

log_msg("[010] 010_simulate_samples.R finished successfully.")