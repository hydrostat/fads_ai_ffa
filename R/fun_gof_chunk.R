# =========================================================
# fun_gof_chunk.R
# Chunk-level GOF processing for FADS_AI
# =========================================================
#
# This file defines chunk-level processing for the GOF/IC
# stage. Each row of the input chunk identifies one simulated
# sample through its family, parameters, sample size, and seed.
#
# For each sample, the function:
#   - regenerates the sample;
#   - fits all candidate families by L-moments;
#   - computes GOF statistics, approximate p-values, logLik,
#     AIC and BIC;
#   - adds Bayesian-calibrated p-value columns.
#
# Dependencies:
#   - simulate_one_sample() from R/fun_distributions.R
#   - compute_all_gof_features_one_sample() from R/fun_gof.R
#   - add_bayes_columns() from R/fun_bayes.R
# =========================================================


# ---------------------------------------------------------
# Process one GOF chunk
# ---------------------------------------------------------

process_gof_chunk <- function(
    chunk_tbl,
    family_levels,
    progress_step = 250L,
    chunk_label = ""
) {
  
  if (is.null(chunk_tbl) || !is.data.frame(chunk_tbl)) {
    stop("chunk_tbl must be a data.frame or tibble.")
  }
  
  if (nrow(chunk_tbl) == 0) {
    return(tibble::tibble())
  }
  
  required_cols <- c(
    "sample_id",
    "true_family",
    "n",
    "seed",
    "par1",
    "par2",
    "par3"
  )
  
  missing_cols <- setdiff(required_cols, names(chunk_tbl))
  
  if (length(missing_cols) > 0) {
    stop(
      "chunk_tbl is missing required columns: ",
      paste(missing_cols, collapse = ", ")
    )
  }
  
  if (is.null(family_levels) || length(family_levels) == 0) {
    stop("family_levels must contain at least one candidate family.")
  }
  
  family_levels <- as.character(family_levels)
  
  if (
    is.null(progress_step) ||
    length(progress_step) != 1 ||
    is.na(progress_step) ||
    progress_step <= 0
  ) {
    stop("progress_step must be a positive integer.")
  }
  
  progress_step <- as.integer(progress_step)
  chunk_label <- as.character(chunk_label)
  
  chunk_results <- vector("list", length = nrow(chunk_tbl))
  n_total <- nrow(chunk_tbl)
  
  for (i in seq_len(n_total)) {
    
    if (i == 1L || i %% progress_step == 0L || i == n_total) {
      pct <- round(100 * i / n_total, 2)
      cat(sprintf(
        "[%s] %s | progress %d / %d (%.2f%%)\n",
        format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
        chunk_label,
        i,
        n_total,
        pct
      ))
    }
    
    row_i <- chunk_tbl[i, ]
    
    param_row <- tibble::tibble(
      par1 = as.numeric(row_i$par1),
      par2 = as.numeric(row_i$par2),
      par3 = as.numeric(row_i$par3),
      delta = if ("delta" %in% names(row_i)) {
        as.numeric(row_i$delta)
      } else {
        0
      }
    )
    
    x <- tryCatch(
      simulate_one_sample(
        family = as.character(row_i$true_family),
        n = as.integer(row_i$n),
        param_row = param_row,
        seed = as.integer(row_i$seed)
      ),
      error = function(e) e
    )
    
    if (inherits(x, "error")) {
      chunk_results[[i]] <- tibble::tibble(
        sample_id = row_i$sample_id,
        gof_ok = FALSE,
        gof_error = paste("Simulation error:", conditionMessage(x))
      )
      next
    }
    
    gof_row <- tryCatch(
      compute_all_gof_features_one_sample(
        x = x,
        candidate_families = family_levels
      ),
      error = function(e) e
    )
    
    if (inherits(gof_row, "error")) {
      chunk_results[[i]] <- tibble::tibble(
        sample_id = row_i$sample_id,
        gof_ok = FALSE,
        gof_error = paste("GOF error:", conditionMessage(gof_row))
      )
      next
    }
    
    pvalue_cols <- grep("_p_", names(gof_row), value = TRUE)
    
    if (length(pvalue_cols) > 0) {
      gof_row <- add_bayes_columns(
        df = gof_row,
        pvalue_cols = pvalue_cols
      )
    }
    
    chunk_results[[i]] <- dplyr::bind_cols(
      tibble::tibble(
        sample_id = row_i$sample_id,
        gof_ok = TRUE,
        gof_error = NA_character_
      ),
      gof_row
    )
  }
  
  dplyr::bind_rows(chunk_results)
}