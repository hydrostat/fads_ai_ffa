# =========================================================
# fun_gof.R
# Goodness-of-fit and information-criterion utilities
# for FADS_AI
# =========================================================
#
# This file provides utilities to:
#   - fit candidate distributions by L-moments;
#   - compute EDF-based GOF statistics (KS, AD, CvM);
#   - compute approximate EDF p-values;
#   - compute analytic log-likelihood, AIC, and BIC.
#
# Important:
#   EDF p-values are approximate because distribution
#   parameters are estimated from the same sample.
#
# Dependencies:
#   - R/fun_pdf.R must be sourced before this file.
# =========================================================

library(tibble)
library(dplyr)
library(lmom)
library(goftest)


# ---------------------------------------------------------
# Clamp probabilities away from 0 and 1
# ---------------------------------------------------------

clamp_prob <- function(p, eps = 1e-12) {
  
  if (!is.numeric(p)) {
    stop("p must be numeric.")
  }
  
  if (!is.numeric(eps) || length(eps) != 1 || eps <= 0 || eps >= 0.5) {
    stop("eps must be a single number in (0, 0.5).")
  }
  
  pmin(pmax(p, eps), 1 - eps)
}


# ---------------------------------------------------------
# Build per-sample cache
# This avoids recomputing L-moments and log(x) repeatedly
# across all candidate fitted families.
# ---------------------------------------------------------

build_sample_cache <- function(x) {
  
  if (!is.numeric(x)) {
    stop("x must be a numeric vector.")
  }
  
  x <- as.numeric(x)
  
  if (length(x) < 4) {
    stop("Sample too short for L-moment fitting.")
  }
  
  if (any(!is.finite(x))) {
    stop("x contains non-finite values.")
  }
  
  is_positive <- all(x > 0)
  
  out <- list(
    x = x,
    n = length(x),
    lm_x = lmom::samlmu(x, nmom = 4),
    is_positive = is_positive
  )
  
  if (is_positive) {
    logx <- log(x)
    
    out$logx <- logx
    out$lm_logx <- lmom::samlmu(logx, nmom = 2)
  } else {
    out$logx <- NULL
    out$lm_logx <- NULL
  }
  
  out
}


# ---------------------------------------------------------
# Fit one family by L-moments using cache
# ---------------------------------------------------------

fit_lmom_family_from_cache <- function(cache, family) {
  
  if (is.null(cache$x) || is.null(cache$lm_x)) {
    stop("Invalid sample cache.")
  }
  
  family <- as.character(family)
  
  if (family == "GEV") {
    para <- lmom::pelgev(cache$lm_x)
    return(list(family = family, para = para, npar = 3L))
  }
  
  if (family == "GPA") {
    para <- lmom::pelgpa(cache$lm_x)
    return(list(family = family, para = para, npar = 3L))
  }
  
  if (family == "PE3") {
    para <- lmom::pelpe3(cache$lm_x)
    return(list(family = family, para = para, npar = 3L))
  }
  
  if (family == "LN3") {
    if (!cache$is_positive) {
      stop("LN3 fit invalid: sample contains non-positive values.")
    }
    
    para <- lmom::pelln3(cache$lm_x)
    return(list(family = family, para = para, npar = 3L))
  }
  
  if (family == "GUM") {
    para <- lmom::pelgum(cache$lm_x)
    return(list(family = family, para = para, npar = 2L))
  }
  
  if (family == "LN2") {
    if (!cache$is_positive) {
      stop("LN2 fit invalid: sample contains non-positive values.")
    }
    
    meanlog <- as.numeric(cache$lm_logx[1])
    sdlog <- as.numeric(cache$lm_logx[2]) * sqrt(pi)
    
    para <- c(meanlog = meanlog, sdlog = sdlog)
    
    return(list(family = family, para = para, npar = 2L))
  }
  
  stop("Unknown family: ", family)
}


# ---------------------------------------------------------
# CDF wrappers
# ---------------------------------------------------------

cdf_family <- function(x, family, para) {
  
  family <- as.character(family)
  
  if (family == "GEV") {
    return(lmom::cdfgev(x, para = para))
  }
  
  if (family == "GPA") {
    return(lmom::cdfgpa(x, para = para))
  }
  
  if (family == "PE3") {
    return(lmom::cdfpe3(x, para = para))
  }
  
  if (family == "LN3") {
    return(lmom::cdfln3(x, para = para))
  }
  
  if (family == "GUM") {
    return(lmom::cdfgum(x, para = para))
  }
  
  if (family == "LN2") {
    return(stats::plnorm(x, meanlog = para[1], sdlog = para[2]))
  }
  
  stop("Unknown family: ", family)
}


# ---------------------------------------------------------
# PDF wrappers
# Requires source("R/fun_pdf.R") before use.
# ---------------------------------------------------------

pdf_family <- function(x, family, para) {
  
  family <- as.character(family)
  
  if (family == "GEV") {
    return(pdfgev(x, para = para))
  }
  
  if (family == "GPA") {
    return(pdfgpa(x, para = para))
  }
  
  if (family == "PE3") {
    return(pdfpe3(x, para = para))
  }
  
  if (family == "LN3") {
    return(pdfln3(x, para = para))
  }
  
  if (family == "GUM") {
    return(pdfgum(x, para = para))
  }
  
  if (family == "LN2") {
    return(pdfln2(x, para = para))
  }
  
  stop("Unknown family: ", family)
}


# ---------------------------------------------------------
# Analytic log-likelihood from vectorized PDF
# ---------------------------------------------------------

compute_loglik_pdf <- function(x, family, para, small = 1e-300) {
  
  dens <- pdf_family(x = x, family = family, para = para)
  
  dens[!is.finite(dens) | dens <= 0] <- small
  
  sum(log(dens))
}


# ---------------------------------------------------------
# AIC / BIC
# ---------------------------------------------------------

compute_ic <- function(loglik, npar, n) {
  
  if (is.na(loglik) || !is.finite(loglik)) {
    return(list(AIC = NA_real_, BIC = NA_real_))
  }
  
  if (is.na(npar) || npar <= 0) {
    stop("npar must be a positive integer.")
  }
  
  if (is.na(n) || n <= 0) {
    stop("n must be a positive integer.")
  }
  
  aic <- -2 * loglik + 2 * npar
  bic <- -2 * loglik + log(n) * npar
  
  list(AIC = aic, BIC = bic)
}


# ---------------------------------------------------------
# EDF statistics from u = F(x)
#
# This is faster than calling ks.test/ad.test/cvm.test with
# null = cdf_fun. Statistics are computed on transformed
# values u.
# ---------------------------------------------------------

compute_edf_stats_from_u <- function(u) {
  
  if (!is.numeric(u)) {
    stop("u must be numeric.")
  }
  
  u <- sort(clamp_prob(as.numeric(u)))
  
  if (any(!is.finite(u))) {
    stop("u contains non-finite values.")
  }
  
  n <- length(u)
  
  if (n == 0) {
    stop("u must contain at least one value.")
  }
  
  i <- seq_len(n)
  
  d_plus <- max(i / n - u)
  d_minus <- max(u - (i - 1) / n)
  ks_stat <- max(d_plus, d_minus)
  
  cvm_stat <- 1 / (12 * n) +
    sum((u - (2 * i - 1) / (2 * n))^2)
  
  ad_sum <- sum((2 * i - 1) * (log(u) + log(1 - rev(u))))
  ad_stat <- -n - ad_sum / n
  
  list(
    KS_stat = as.numeric(ks_stat),
    CvM_stat = as.numeric(cvm_stat),
    AD_stat = as.numeric(ad_stat)
  )
}


# ---------------------------------------------------------
# Approximate p-values from EDF statistics
#
# These p-values are approximate because parameters are
# estimated from the same sample.
# ---------------------------------------------------------

compute_edf_pvalues <- function(u, stats_list) {
  
  if (is.null(stats_list)) {
    return(
      list(
        KS_p = NA_real_,
        AD_p = NA_real_,
        CvM_p = NA_real_
      )
    )
  }
  
  u <- sort(clamp_prob(as.numeric(u)))
  
  ks_p <- tryCatch(
    suppressWarnings(stats::ks.test(u, "punif", exact = FALSE)$p.value),
    error = function(e) NA_real_
  )
  
  ad_p <- tryCatch(
    if ("pAD" %in% getNamespaceExports("goftest")) {
      goftest::pAD(stats_list$AD_stat)
    } else {
      NA_real_
    },
    error = function(e) NA_real_
  )
  
  cvm_p <- tryCatch(
    if ("pCvM" %in% getNamespaceExports("goftest")) {
      goftest::pCvM(stats_list$CvM_stat)
    } else {
      NA_real_
    },
    error = function(e) NA_real_
  )
  
  list(
    KS_p = as.numeric(ks_p),
    AD_p = as.numeric(ad_p),
    CvM_p = as.numeric(cvm_p)
  )
}


# ---------------------------------------------------------
# Empty GOF result row
# ---------------------------------------------------------

empty_gof_result <- function(fit_ok = FALSE, fit_error = NA_character_) {
  
  tibble(
    fit_ok = fit_ok,
    fit_error = fit_error,
    fit_par1 = NA_real_,
    fit_par2 = NA_real_,
    fit_par3 = NA_real_,
    KS_stat = NA_real_,
    KS_p = NA_real_,
    AD_stat = NA_real_,
    AD_p = NA_real_,
    CvM_stat = NA_real_,
    CvM_p = NA_real_,
    logLik = NA_real_,
    AIC = NA_real_,
    BIC = NA_real_
  )
}


# ---------------------------------------------------------
# GOF for one fitted family
# ---------------------------------------------------------

compute_gof_one_fit <- function(cache, family, fit_obj) {
  
  x <- cache$x
  family <- as.character(family)
  
  u <- tryCatch(
    cdf_family(x, family = family, para = fit_obj$para),
    error = function(e) e
  )
  
  if (inherits(u, "error")) {
    return(
      empty_gof_result(
        fit_ok = FALSE,
        fit_error = paste("CDF error:", conditionMessage(u))
      )
    )
  }
  
  u <- clamp_prob(u)
  
  edf_stats <- tryCatch(
    compute_edf_stats_from_u(u),
    error = function(e) NULL
  )
  
  edf_p <- tryCatch(
    compute_edf_pvalues(u, edf_stats),
    error = function(e) {
      list(KS_p = NA_real_, AD_p = NA_real_, CvM_p = NA_real_)
    }
  )
  
  loglik <- tryCatch(
    compute_loglik_pdf(x, family = family, para = fit_obj$para),
    error = function(e) NA_real_
  )
  
  ic <- compute_ic(
    loglik = loglik,
    npar = fit_obj$npar,
    n = cache$n
  )
  
  tibble(
    fit_ok = TRUE,
    fit_error = NA_character_,
    fit_par1 = as.numeric(fit_obj$para[1]),
    fit_par2 = as.numeric(fit_obj$para[2]),
    fit_par3 = ifelse(
      length(fit_obj$para) >= 3,
      as.numeric(fit_obj$para[3]),
      NA_real_
    ),
    KS_stat = if (is.null(edf_stats)) NA_real_ else edf_stats$KS_stat,
    KS_p = edf_p$KS_p,
    AD_stat = if (is.null(edf_stats)) NA_real_ else edf_stats$AD_stat,
    AD_p = edf_p$AD_p,
    CvM_stat = if (is.null(edf_stats)) NA_real_ else edf_stats$CvM_stat,
    CvM_p = edf_p$CvM_p,
    logLik = as.numeric(loglik),
    AIC = as.numeric(ic$AIC),
    BIC = as.numeric(ic$BIC)
  )
}


# ---------------------------------------------------------
# Safe fit wrapper using cache
# ---------------------------------------------------------

safe_fit_and_gof_from_cache <- function(cache, family) {
  
  fit_obj <- tryCatch(
    fit_lmom_family_from_cache(cache, family),
    error = function(e) e
  )
  
  if (inherits(fit_obj, "error")) {
    return(
      empty_gof_result(
        fit_ok = FALSE,
        fit_error = conditionMessage(fit_obj)
      )
    )
  }
  
  compute_gof_one_fit(cache, family, fit_obj)
}


# ---------------------------------------------------------
# Compute GOF features for one sample against all candidate
# families. Output is a 1-row wide tibble.
# ---------------------------------------------------------

compute_all_gof_features_one_sample <- function(x, candidate_families) {
  
  if (is.null(candidate_families) || length(candidate_families) == 0) {
    stop("candidate_families must contain at least one family.")
  }
  
  cache <- build_sample_cache(x)
  
  out <- list()
  
  for (fam in as.character(candidate_families)) {
    res <- safe_fit_and_gof_from_cache(cache, fam)
    
    for (nm in names(res)) {
      out[[paste0(nm, "_", fam)]] <- res[[nm]][1]
    }
  }
  
  tibble::as_tibble(out)
}