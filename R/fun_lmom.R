# =========================================================
# fun_lmom.R
# Sample L-moment utilities for FADS_AI
# =========================================================
#
# This file provides utilities to compute sample L-moments
# and L-moment ratios used as statistical descriptors in the
# FADS_AI pipeline.
# =========================================================

library(tibble)
library(lmom)


# ---------------------------------------------------------
# Compute sample L-moments and L-moment ratios
# ---------------------------------------------------------

compute_sample_lmom <- function(x, nmom = 4L) {
  
  if (!is.numeric(x)) {
    stop("x must be a numeric vector.")
  }
  
  if (length(x) < nmom) {
    stop("x must contain at least ", nmom, " observations.")
  }
  
  if (any(!is.finite(x))) {
    stop("x contains non-finite values.")
  }
  
  lm <- tryCatch(
    lmom::samlmu(x, nmom = nmom),
    error = function(e) {
      stop("Could not compute sample L-moments: ", conditionMessage(e))
    }
  )
  
  l1 <- as.numeric(lm[1])
  l2 <- as.numeric(lm[2])
  l3 <- as.numeric(lm[3])
  l4 <- as.numeric(lm[4])
  
  t3 <- if (isTRUE(l2 == 0)) NA_real_ else l3 / l2
  t4 <- if (isTRUE(l2 == 0)) NA_real_ else l4 / l2
  
  tibble(
    lmom_l1 = l1,
    lmom_l2 = l2,
    lmom_l3 = l3,
    lmom_l4 = l4,
    lmom_t3 = as.numeric(t3),
    lmom_t4 = as.numeric(t4)
  )
}