# =========================================================
# fun_distributions.R
# Distribution registry, parameter validation, simulation grid,
# and positive-domain enforcement for FADS_AI
# =========================================================
#
# This file defines:
#   - candidate distribution parameter registry;
#   - theoretical quantile functions;
#   - population-level positive-domain shifts;
#   - simulation grid construction;
#   - parameter validation;
#   - sample generation by distribution family.
#
# Important:
#   Samples are generated from the specified theoretical
#   distributions and then shifted to enforce positive support.
#   A population-specific delta is computed first. An adaptive
#   fallback is retained in simulate_one_sample() to guarantee
#   positivity if a rare simulated value remains non-positive
#   after the population-level shift.
# =========================================================

library(dplyr)
library(tidyr)
library(tibble)
library(lmom)


# ---------------------------------------------------------
# Parameter registry
# Final parameter ranges used in the experiment
# ---------------------------------------------------------

build_parameter_registry <- function(config) {
  
  if (is.null(config$experiment$families)) {
    stop("config$experiment$families is missing.")
  }
  
  families <- as.character(config$experiment$families)
  
  supported_families <- c("GEV", "GPA", "PE3", "LN2", "LN3", "GUM")
  
  unknown_families <- setdiff(families, supported_families)
  
  if (length(unknown_families) > 0) {
    stop(
      "Unsupported families in config$experiment$families: ",
      paste(unknown_families, collapse = ", ")
    )
  }
  
  gev_tbl <- tibble(
    family = "GEV",
    param_id = paste0("GEV_", seq_along(c(-0.35, -0.20, -0.05, 0.10, 0.25, 0.40))),
    par1 = 0.0,
    par2 = 1.0,
    par3 = c(-0.35, -0.20, -0.05, 0.10, 0.25, 0.40),
    par1_name = "xi",
    par2_name = "alpha",
    par3_name = "k"
  )
  
  gpa_tbl <- tibble(
    family = "GPA",
    param_id = paste0("GPA_", seq_along(c(-0.20, -0.05, 0.05, 0.20, 0.35, 0.50))),
    par1 = 0.0,
    par2 = 1.0,
    par3 = c(-0.20, -0.05, 0.05, 0.20, 0.35, 0.50),
    par1_name = "xi",
    par2_name = "alpha",
    par3_name = "k"
  )
  
  pe3_tbl <- tibble(
    family = "PE3",
    param_id = paste0("PE3_", seq_along(c(-2.0, -1.2, -0.4, 0.4, 1.2, 2.0))),
    par1 = 0.0,
    par2 = 1.0,
    par3 = c(-2.0, -1.2, -0.4, 0.4, 1.2, 2.0),
    par1_name = "mu",
    par2_name = "sigma",
    par3_name = "gamma"
  )
  
  ln2_tbl <- tibble(
    family = "LN2",
    param_id = paste0("LN2_", seq_along(c(0.20, 0.35, 0.50, 0.70, 0.90, 1.15))),
    par1 = 0.0,
    par2 = c(0.20, 0.35, 0.50, 0.70, 0.90, 1.15),
    par3 = NA_real_,
    par1_name = "meanlog",
    par2_name = "sdlog",
    par3_name = NA_character_
  )
  
  ln3_tbl <- tibble(
    family = "LN3",
    param_id = paste0("LN3_", seq_along(c(0.05, 0.10, 0.18, 0.26, 0.35, 0.45))),
    par1 = 0.0,
    par2 = 1.0,
    par3 = c(0.05, 0.10, 0.18, 0.26, 0.35, 0.45),
    par1_name = "xi",
    par2_name = "alpha",
    par3_name = "k"
  )
  
  gum_tbl <- tibble(
    family = "GUM",
    param_id = "GUM_1",
    par1 = 0.0,
    par2 = 1.0,
    par3 = NA_real_,
    par1_name = "xi",
    par2_name = "alpha",
    par3_name = NA_character_
  )
  
  reg <- bind_rows(
    gev_tbl,
    gpa_tbl,
    pe3_tbl,
    ln2_tbl,
    ln3_tbl,
    gum_tbl
  ) %>%
    filter(family %in% families)
  
  reg$family <- factor(reg$family, levels = families)
  
  reg
}


# ---------------------------------------------------------
# Theoretical quantile by family
# ---------------------------------------------------------

get_family_quantile <- function(p, family, par1, par2, par3 = NA_real_) {
  
  if (!is.numeric(p) || any(p <= 0 | p >= 1, na.rm = TRUE)) {
    stop("p must contain probabilities strictly between 0 and 1.")
  }
  
  family <- as.character(family)
  
  if (family == "GEV") {
    return(lmom::quagev(f = p, para = c(par1, par2, par3)))
  }
  
  if (family == "GPA") {
    return(lmom::quagpa(f = p, para = c(par1, par2, par3)))
  }
  
  if (family == "PE3") {
    return(lmom::quape3(f = p, para = c(par1, par2, par3)))
  }
  
  if (family == "LN2") {
    return(stats::qlnorm(p, meanlog = par1, sdlog = par2))
  }
  
  if (family == "LN3") {
    return(lmom::qualn3(f = p, para = c(par1, par2, par3)))
  }
  
  if (family == "GUM") {
    return(lmom::quagum(f = p, para = c(par1, par2)))
  }
  
  stop("Unknown family: ", family)
}


# ---------------------------------------------------------
# Compute translation delta for one population
# ---------------------------------------------------------

compute_population_delta <- function(
    family,
    par1,
    par2,
    par3 = NA_real_,
    p_low = 1e-3,
    floor_value = 0,
    margin = 0.05
) {
  
  if (!is.numeric(p_low) || length(p_low) != 1 || p_low <= 0 || p_low >= 1) {
    stop("p_low must be a single number in (0, 1).")
  }
  
  if (!is.numeric(floor_value) || length(floor_value) != 1) {
    stop("floor_value must be a single numeric value.")
  }
  
  if (!is.numeric(margin) || length(margin) != 1 || margin < 0) {
    stop("margin must be a single non-negative numeric value.")
  }
  
  q_low <- get_family_quantile(
    p = p_low,
    family = family,
    par1 = par1,
    par2 = par2,
    par3 = par3
  )
  
  if (!is.finite(q_low)) {
    stop("Non-finite low quantile for family ", family)
  }
  
  max(0, floor_value - q_low + margin)
}


# ---------------------------------------------------------
# Add delta column to parameter registry
# ---------------------------------------------------------

add_population_deltas <- function(
    param_registry,
    p_low = 1e-3,
    floor_value = 0,
    margin = 0.05
) {
  
  required_cols <- c("family", "param_id", "par1", "par2", "par3")
  
  missing_cols <- setdiff(required_cols, names(param_registry))
  
  if (length(missing_cols) > 0) {
    stop(
      "param_registry is missing required columns: ",
      paste(missing_cols, collapse = ", ")
    )
  }
  
  delta_vec <- numeric(nrow(param_registry))
  
  for (i in seq_len(nrow(param_registry))) {
    row_i <- param_registry[i, ]
    
    delta_vec[i] <- compute_population_delta(
      family = as.character(row_i$family),
      par1 = as.numeric(row_i$par1),
      par2 = as.numeric(row_i$par2),
      par3 = as.numeric(row_i$par3),
      p_low = p_low,
      floor_value = floor_value,
      margin = margin
    )
  }
  
  param_registry$delta <- delta_vec
  
  param_registry
}


# ---------------------------------------------------------
# Build simulation grid using valid parameters only
# ---------------------------------------------------------

build_simulation_grid <- function(config, param_registry) {
  
  required_config <- c("sample_sizes", "n_rep_mc", "seed_master")
  
  missing_config <- setdiff(required_config, names(config$experiment))
  
  if (length(missing_config) > 0) {
    stop(
      "config$experiment is missing required fields: ",
      paste(missing_config, collapse = ", ")
    )
  }
  
  tidyr::crossing(
    param_registry %>% dplyr::select(family, param_id),
    n = config$experiment$sample_sizes,
    replicate_id = seq_len(config$experiment$n_rep_mc)
  ) %>%
    arrange(family, param_id, n, replicate_id) %>%
    mutate(
      sample_id = sprintf("S%07d", row_number()),
      seed = config$experiment$seed_master + row_number() - 1L
    ) %>%
    select(sample_id, family, param_id, n, replicate_id, seed)
}


# ---------------------------------------------------------
# Validate all parameter sets
# Validation is done on the raw family generator, before shift
# ---------------------------------------------------------

validate_parameter_registry <- function(param_registry, n_test = 5L, seed = 123L) {
  
  n_total <- nrow(param_registry)
  
  valid_list <- vector("list", n_total)
  invalid_list <- vector("list", n_total)
  
  empty_row <- tibble(
    family = character(),
    param_id = character(),
    par1 = numeric(),
    par2 = numeric(),
    par3 = numeric(),
    par1_name = character(),
    par2_name = character(),
    par3_name = character(),
    delta = numeric(),
    valid = logical(),
    validation_error = character()
  )
  
  for (i in seq_len(n_total)) {
    
    row_i <- param_registry[i, ]
    
    cat(sprintf(
      "[%s] Parameter validation: %d / %d | family = %s | param_id = %s\n",
      format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
      i,
      n_total,
      as.character(row_i$family),
      as.character(row_i$param_id)
    ))
    
    set.seed(seed + i - 1L)
    
    test_x <- tryCatch(
      r_sample_from_family(
        family = as.character(row_i$family),
        n = n_test,
        par1 = row_i$par1,
        par2 = row_i$par2,
        par3 = row_i$par3
      ),
      error = function(e) e
    )
    
    delta_i <- if ("delta" %in% names(row_i)) {
      as.numeric(row_i$delta)
    } else {
      NA_real_
    }
    
    if (inherits(test_x, "error")) {
      invalid_list[[i]] <- tibble(
        family = as.character(row_i$family),
        param_id = as.character(row_i$param_id),
        par1 = as.numeric(row_i$par1),
        par2 = as.numeric(row_i$par2),
        par3 = as.numeric(row_i$par3),
        par1_name = as.character(row_i$par1_name),
        par2_name = as.character(row_i$par2_name),
        par3_name = as.character(row_i$par3_name),
        delta = delta_i,
        valid = FALSE,
        validation_error = conditionMessage(test_x)
      )
    } else {
      valid_list[[i]] <- tibble(
        family = as.character(row_i$family),
        param_id = as.character(row_i$param_id),
        par1 = as.numeric(row_i$par1),
        par2 = as.numeric(row_i$par2),
        par3 = as.numeric(row_i$par3),
        par1_name = as.character(row_i$par1_name),
        par2_name = as.character(row_i$par2_name),
        par3_name = as.character(row_i$par3_name),
        delta = delta_i,
        valid = TRUE,
        validation_error = NA_character_
      )
    }
  }
  
  valid_tbl <- bind_rows(valid_list)
  invalid_tbl <- bind_rows(invalid_list)
  
  if (nrow(valid_tbl) == 0) valid_tbl <- empty_row
  if (nrow(invalid_tbl) == 0) invalid_tbl <- empty_row
  
  valid_tbl$family <- factor(valid_tbl$family, levels = levels(param_registry$family))
  invalid_tbl$family <- factor(invalid_tbl$family, levels = levels(param_registry$family))
  
  list(
    valid_params = valid_tbl,
    invalid_params = invalid_tbl
  )
}


# ---------------------------------------------------------
# Distribution dispatcher
# ---------------------------------------------------------

r_sample_from_family <- function(family, n, par1, par2, par3 = NA_real_) {
  
  family <- as.character(family)
  
  if (!is.numeric(n) || length(n) != 1 || n <= 0) {
    stop("n must be a positive integer.")
  }
  
  u <- stats::runif(n)
  
  if (family == "GEV") {
    return(lmom::quagev(f = u, para = c(par1, par2, par3)))
  }
  
  if (family == "GPA") {
    return(lmom::quagpa(f = u, para = c(par1, par2, par3)))
  }
  
  if (family == "PE3") {
    return(lmom::quape3(f = u, para = c(par1, par2, par3)))
  }
  
  if (family == "LN2") {
    return(stats::rlnorm(n = n, meanlog = par1, sdlog = par2))
  }
  
  if (family == "LN3") {
    return(lmom::qualn3(f = u, para = c(par1, par2, par3)))
  }
  
  if (family == "GUM") {
    return(lmom::quagum(f = u, para = c(par1, par2)))
  }
  
  stop("Unknown family: ", family)
}


# ---------------------------------------------------------
# Simulate one sample and apply positive-domain enforcement
# ---------------------------------------------------------

simulate_one_sample <- function(
    family,
    n,
    param_row,
    seed,
    adaptive_positive_fallback = TRUE
) {
  
  set.seed(seed)
  
  x_raw <- r_sample_from_family(
    family = family,
    n = n,
    par1 = param_row$par1,
    par2 = param_row$par2,
    par3 = param_row$par3
  )
  
  delta <- if ("delta" %in% names(param_row)) {
    as.numeric(param_row$delta[1])
  } else {
    0
  }
  
  x <- x_raw + delta
  
  # Adaptive positive-domain fallback.
  # This preserves strictly positive samples if a rare simulated
  # value remains non-positive after the population-level delta.
  if (adaptive_positive_fallback) {
    min_x <- min(x)
    
    if (min_x <= 0) {
      x <- x + abs(min_x) + 1e-6
    }
  } else {
    if (any(x <= 0)) {
      stop(
        "Non-positive simulated value after population-level delta. ",
        "Consider increasing the delta margin or enabling adaptive_positive_fallback."
      )
    }
  }
  
  x
}