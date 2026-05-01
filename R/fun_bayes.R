# =========================================================
# fun_bayes.R
# Bayesian calibration helpers for GOF p-values in FADS_AI
# =========================================================
#
# This file provides utilities to transform approximate
# goodness-of-fit p-values into minimum Bayes-factor-type
# descriptors.
#
# Important:
#   These descriptors are derived from p-values. They should
#   be interpreted as calibrated evidence summaries, not as a
#   complete Bayesian model comparison.
# =========================================================


# ---------------------------------------------------------
# Minimum Bayes-factor-type calibration from p-values
#
# For 0 < p < exp(-1), the calibration is:
#   -e * p * log(p)
#
# For p >= exp(-1), the minimum Bayes factor is set to 1.
# Invalid or missing p-values return NA.
# ---------------------------------------------------------

calibrate_pvalue_minbf <- function(p) {
  
  if (!is.numeric(p)) {
    stop("p must be numeric.")
  }
  
  out <- rep(NA_real_, length(p))
  
  valid <- is.finite(p) & p >= 0 & p <= 1
  
  out[valid] <- ifelse(
    p[valid] > 0 & p[valid] < exp(-1),
    -exp(1) * p[valid] * log(p[valid]),
    1
  )
  
  out
}


# ---------------------------------------------------------
# Negative log minimum Bayes-factor-type descriptor
# ---------------------------------------------------------

elog_minbf <- function(p) {
  
  bf <- calibrate_pvalue_minbf(p)
  
  out <- rep(NA_real_, length(bf))
  
  valid <- is.finite(bf) & bf > 0
  
  out[valid] <- -log(bf[valid])
  
  out
}


# ---------------------------------------------------------
# Add Bayesian-calibrated columns to a data frame
# ---------------------------------------------------------

add_bayes_columns <- function(df, pvalue_cols) {
  
  if (is.null(df) || !is.data.frame(df)) {
    stop("df must be a data.frame or tibble.")
  }
  
  if (is.null(pvalue_cols) || length(pvalue_cols) == 0) {
    return(df)
  }
  
  missing_cols <- setdiff(pvalue_cols, names(df))
  
  if (length(missing_cols) > 0) {
    stop(
      "pvalue_cols contains columns not found in df: ",
      paste(missing_cols, collapse = ", ")
    )
  }
  
  out <- df
  
  for (col in pvalue_cols) {
    
    if (!grepl("_p_", col, fixed = TRUE)) {
      stop(
        "p-value column name must contain '_p_' to generate calibrated names: ",
        col
      )
    }
    
    bf_col <- sub("_p_", "_bfmin_", col, fixed = TRUE)
    elog_col <- sub("_p_", "_elogbf_", col, fixed = TRUE)
    
    out[[bf_col]] <- calibrate_pvalue_minbf(out[[col]])
    out[[elog_col]] <- elog_minbf(out[[col]])
  }
  
  out
}