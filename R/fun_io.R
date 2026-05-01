# =========================================================
# fun_io.R
# Input/output helper functions for FADS_AI
# =========================================================
#
# This file provides lightweight utilities for directory
# creation, RDS/parquet input-output, log messages, and
# reproducibility metadata.
#
# These functions do not define project-specific paths.
# Paths should be provided by the calling script or by the
# main configuration file.
# =========================================================


# ---------------------------------------------------------
# Create directory if it does not exist
# ---------------------------------------------------------

ensure_dir <- function(path) {
  if (is.null(path) || length(path) == 0 || is.na(path) || path == "") {
    stop("Invalid directory path.")
  }
  
  if (!dir.exists(path)) {
    ok <- dir.create(path, recursive = TRUE, showWarnings = FALSE)
    
    if (!ok && !dir.exists(path)) {
      stop("Could not create directory: ", path)
    }
  }
  
  invisible(normalizePath(path, winslash = "/", mustWork = FALSE))
}


# ---------------------------------------------------------
# Save RDS with automatic directory creation
# ---------------------------------------------------------

save_rds <- function(object, path) {
  if (is.null(path) || length(path) == 0 || is.na(path) || path == "") {
    stop("Invalid RDS output path.")
  }
  
  ensure_dir(dirname(path))
  saveRDS(object, path)
  
  invisible(normalizePath(path, winslash = "/", mustWork = FALSE))
}


# ---------------------------------------------------------
# Read RDS safely
# ---------------------------------------------------------

read_rds <- function(path) {
  if (is.null(path) || length(path) == 0 || is.na(path) || path == "") {
    stop("Invalid RDS input path.")
  }
  
  if (!file.exists(path)) {
    stop("RDS file does not exist: ", path)
  }
  
  readRDS(path)
}


# ---------------------------------------------------------
# Save parquet with automatic directory creation
# ---------------------------------------------------------

save_parquet <- function(df, path) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required to write parquet files.")
  }
  
  if (is.null(path) || length(path) == 0 || is.na(path) || path == "") {
    stop("Invalid parquet output path.")
  }
  
  ensure_dir(dirname(path))
  arrow::write_parquet(df, path)
  
  invisible(normalizePath(path, winslash = "/", mustWork = FALSE))
}


# ---------------------------------------------------------
# Read parquet safely
# ---------------------------------------------------------

read_parquet <- function(path) {
  if (!requireNamespace("arrow", quietly = TRUE)) {
    stop("Package 'arrow' is required to read parquet files.")
  }
  
  if (is.null(path) || length(path) == 0 || is.na(path) || path == "") {
    stop("Invalid parquet input path.")
  }
  
  if (!file.exists(path)) {
    stop("Parquet file does not exist: ", path)
  }
  
  arrow::read_parquet(path)
}


# ---------------------------------------------------------
# Simple log message
# ---------------------------------------------------------

log_msg <- function(...) {
  msg <- paste(..., collapse = " ")
  timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  cat(sprintf("[%s] %s\n", timestamp, msg))
  invisible(NULL)
}


# ---------------------------------------------------------
# Session info saver
# ---------------------------------------------------------

save_session_info <- function(path) {
  if (is.null(path) || length(path) == 0 || is.na(path) || path == "") {
    stop("Invalid session-info output path.")
  }
  
  ensure_dir(dirname(path))
  writeLines(capture.output(sessionInfo()), path)
  
  invisible(normalizePath(path, winslash = "/", mustWork = FALSE))
}