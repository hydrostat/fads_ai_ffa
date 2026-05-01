# =========================================================
# fun_lmrd.R
# L-moment Ratio Diagram (LMRD) feature utilities for FADS_AI
# =========================================================
#
# This file computes LMRD-based features using a preprocessed
# polygon object and the point_in_polygon() function defined in:
#
#   R/fun_lmrd_preprocess.R
#
# Required fixed input:
#
#   data/input_data/poligonos_pre.rds
#
# Main outputs for one sample:
#   - lmrd_inside_<family>
#   - lmrd_n_families_accept
# =========================================================

library(tibble)


# ---------------------------------------------------------
# Read preprocessed polygon object
# ---------------------------------------------------------

read_lmrd_preprocessed <- function(path) {
  
  if (is.null(path) || length(path) == 0 || is.na(path) || path == "") {
    stop("Invalid path to preprocessed LMRD polygon file.")
  }
  
  if (!file.exists(path)) {
    stop("Preprocessed LMRD polygon file not found: ", path)
  }
  
  obj <- readRDS(path)
  
  if (!is.list(obj)) {
    stop("Preprocessed LMRD polygon object must be a list.")
  }
  
  if (!"polygons" %in% names(obj)) {
    stop("Preprocessed LMRD polygon object must contain a 'polygons' element.")
  }
  
  if (!"tol" %in% names(obj)) {
    stop("Preprocessed LMRD polygon object must contain a 'tol' element.")
  }
  
  obj
}


# ---------------------------------------------------------
# Build boundary id
#
# Examples:
#   GEV + 10  -> GEV_010
#   GUM + 50  -> GUM_050
#   LN3 + 150 -> LN3_150
# ---------------------------------------------------------

build_boundary_id <- function(family, n) {
  
  if (is.null(family) || length(family) != 1 || is.na(family) || family == "") {
    stop("family must be a single non-empty value.")
  }
  
  if (is.null(n) || length(n) != 1 || is.na(n) || !is.finite(n)) {
    stop("n must be a single finite numeric value.")
  }
  
  paste0(as.character(family), "_", sprintf("%03d", as.integer(n)))
}


# ---------------------------------------------------------
# Compute LMRD features for one sample point
#
# Arguments:
#   t3, t4        L-skewness and L-kurtosis
#   n             sample size
#   objeto_pre    preprocessed polygon object
#   family_levels candidate distribution families
#   strict        if TRUE, missing polygons raise an error;
#                 if FALSE, missing polygons return NA
# ---------------------------------------------------------

compute_lmrd_features_one <- function(
    t3,
    t4,
    n,
    objeto_pre,
    family_levels,
    strict = TRUE
) {
  
  if (is.null(family_levels) || length(family_levels) == 0) {
    stop("family_levels must contain at least one family.")
  }
  
  family_levels <- as.character(family_levels)
  
  out <- list()
  
  if (is.na(t3) || is.na(t4) || !is.finite(t3) || !is.finite(t4)) {
    for (fam in family_levels) {
      out[[paste0("lmrd_inside_", fam)]] <- NA_integer_
    }
    
    out$lmrd_n_families_accept <- NA_integer_
    
    return(tibble::as_tibble(out))
  }
  
  inside_count <- 0L
  
  for (fam in family_levels) {
    boundary_id <- build_boundary_id(fam, n)
    
    inside_val <- tryCatch(
      {
        point_in_polygon(
          px = as.numeric(t3),
          py = as.numeric(t4),
          boundary_id = boundary_id,
          objeto_pre = objeto_pre
        )
      },
      error = function(e) {
        if (isTRUE(strict)) {
          stop(
            "Could not compute LMRD membership for boundary_id ",
            boundary_id,
            ": ",
            conditionMessage(e)
          )
        } else {
          NA
        }
      }
    )
    
    inside_val <- as.integer(inside_val)
    
    out[[paste0("lmrd_inside_", fam)]] <- inside_val
    
    if (!is.na(inside_val)) {
      inside_count <- inside_count + inside_val
    }
  }
  
  out$lmrd_n_families_accept <- as.integer(inside_count)
  
  tibble::as_tibble(out)
}