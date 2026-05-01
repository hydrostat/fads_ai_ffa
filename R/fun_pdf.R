# =========================================================
# fun_pdf.R
# Analytical density functions for candidate distributions
# used in FADS_AI
# =========================================================
#
# This file defines probability density functions used mainly
# for log-likelihood and information-criterion calculations.
#
# Candidate distributions:
#   - GEV
#   - GPA
#   - GUM
#   - PE3
#   - LN2
#   - LN3
#
# Important:
#   These functions must remain consistent with the parameter
#   estimates produced in the GOF/IC stage.
# =========================================================


# ---------------------------------------------------------
# Generalized Extreme Value density
# Parameter vector: c(location, scale, shape)
# ---------------------------------------------------------

pdfgev <- function(x, para = c(0, 1, 0), small = 1e-15) {
  
  if (length(para) != 3) stop("parameter vector has wrong length")
  if (any(is.na(para))) stop("missing values in parameter vector")
  if (para[2] <= 0) stop("distribution parameters invalid")
  
  para <- unname(para)
  
  loc <- para[1]
  scale <- para[2]
  shape <- para[3]
  
  z <- (x - loc) / scale
  
  if (shape == 0) {
    return(exp(-z - exp(-z)) / scale)
  }
  
  arg <- 1 - shape * z
  
  dens <- ifelse(
    arg > small,
    (1 / scale) * arg^(1 / shape - 1) * exp(-arg^(1 / shape)),
    0
  )
  
  dens
}


# ---------------------------------------------------------
# Generalized Pareto density
# Parameter vector: c(location, scale, shape)
# ---------------------------------------------------------

pdfgpa <- function(x, para = c(0, 1, 0), small = 1e-15) {
  
  if (length(para) != 3) stop("parameter vector has wrong length")
  if (any(is.na(para))) stop("missing values in parameter vector")
  if (para[2] <= 0) stop("distribution parameters invalid")
  
  para <- unname(para)
  
  loc <- para[1]
  scale <- para[2]
  shape <- para[3]
  
  z <- (x - loc) / scale
  
  if (shape == 0) {
    return(ifelse(z >= 0, exp(-z) / scale, 0))
  }
  
  arg <- 1 - shape * z
  
  dens <- ifelse(
    (z >= 0) & (arg > small),
    (1 / scale) * arg^(1 / shape - 1),
    0
  )
  
  dens
}


# ---------------------------------------------------------
# Gumbel density
# Parameter vector: c(location, scale)
# ---------------------------------------------------------

pdfgum <- function(x, para = c(0, 1)) {
  
  if (length(para) != 2) stop("parameter vector has wrong length")
  if (any(is.na(para))) stop("missing values in parameter vector")
  if (para[2] <= 0) stop("distribution parameters invalid")
  
  para <- unname(para)
  
  loc <- para[1]
  scale <- para[2]
  
  z <- (x - loc) / scale
  
  exp(-z - exp(-z)) / scale
}


# ---------------------------------------------------------
# Pearson type III density
# Parameter vector follows the lmom PE3 convention used in
# the GOF/IC stage.
# ---------------------------------------------------------

pdfpe3 <- function(x, para = c(0, 1, 0)) {
  
  if (length(para) != 3) stop("parameter vector has wrong length")
  if (any(is.na(para))) stop("missing values in parameter vector")
  if (para[2] <= 0) stop("distribution parameters invalid")
  
  para <- unname(para)
  
  loc <- para[1]
  scale <- para[2]
  shape <- para[3]
  
  if (abs(shape) <= 1e-6) {
    return(stats::dnorm(x, mean = loc, sd = scale))
  }
  
  alpha <- 4 / shape^2
  z <- 2 * (x - loc) / (scale * shape) + alpha
  
  dens <- numeric(length(x))
  inside <- z > 0
  
  dens[inside] <- stats::dgamma(z[inside], shape = alpha, scale = 1) *
    abs(2 / (scale * shape))
  
  dens
}


# ---------------------------------------------------------
# Two-parameter lognormal density
# Parameter vector: c(meanlog, sdlog)
# ---------------------------------------------------------

pdfln2 <- function(x, para = c(0, 1)) {
  
  if (length(para) != 2) stop("parameter vector has wrong length")
  if (any(is.na(para))) stop("missing values in parameter vector")
  if (para[2] <= 0) stop("distribution parameters invalid")
  
  para <- unname(para)
  
  meanlog <- para[1]
  sdlog <- para[2]
  
  dens <- numeric(length(x))
  inside <- x > 0
  
  dens[inside] <- stats::dlnorm(
    x[inside],
    meanlog = meanlog,
    sdlog = sdlog
  )
  
  dens
}


# ---------------------------------------------------------
# Three-parameter lognormal density
#
# Parameter vector currently interpreted as:
#   c(threshold, meanlog, sdlog)
#
# This function must be checked against the LN3 parameter
# estimates generated in the GOF/IC stage.
# ---------------------------------------------------------

pdfln3 <- function(x, para = c(0, 0, 1)) {
  
  if (length(para) != 3) stop("parameter vector has wrong length")
  if (any(is.na(para))) stop("missing values in parameter vector")
  if (para[3] <= 0) stop("distribution parameters invalid")
  
  para <- unname(para)
  
  threshold <- para[1]
  meanlog <- para[2]
  sdlog <- para[3]
  
  z <- x - threshold
  
  dens <- numeric(length(x))
  inside <- z > 0
  
  dens[inside] <- stats::dlnorm(
    z[inside],
    meanlog = meanlog,
    sdlog = sdlog
  )
  
  dens
}