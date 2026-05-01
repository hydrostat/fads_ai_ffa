# =========================================================
# 021_extract_gof_features.R
# Extract GOF, information-criterion, and calibrated evidence
# features from regenerated samples
# =========================================================
#
# This script:
#   - reads feature tables or feature shards from step 020;
#   - regenerates each sample from family, parameters, size,
#     and seed;
#   - fits all candidate families by L-moments;
#   - computes EDF-based GOF statistics and approximate p-values;
#   - computes logLik, AIC, and BIC;
#   - adds minimum Bayes-factor-type descriptors derived from
#     GOF p-values;
#   - writes GOF chunks by true family.
#
# Required previous steps:
#   scripts/000_config.R
#   scripts/010_simulate_samples.R
#   scripts/020_extract_features.R
#
# Note:
#   EDF p-values are approximate because parameters are
#   estimated from the same sample.
# =========================================================

library(dplyr)
library(tidyr)
library(tibble)
library(arrow)
library(lmom)
library(goftest)


# ---------------------------------------------------------
# Repository root check
# ---------------------------------------------------------

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

config_path <- file.path(root_dir, "outputs", "config", "config_main.rds")

if (!file.exists(file.path(root_dir, "R")) ||
    !file.exists(file.path(root_dir, "scripts")) ||
    !file.exists(config_path)) {
  stop(
    "[021] This script must be run from the FADS_AI repository root ",
    "after scripts/000_config.R has been executed.\n",
    "Current working directory: ", root_dir
  )
}


# ---------------------------------------------------------
# Load helper functions and configuration
# ---------------------------------------------------------

source(file.path(root_dir, "R", "fun_io.R"))
source(file.path(root_dir, "R", "fun_distributions.R"))
source(file.path(root_dir, "R", "fun_bayes.R"))
source(file.path(root_dir, "R", "fun_pdf.R"))
source(file.path(root_dir, "R", "fun_gof.R"))
source(file.path(root_dir, "R", "fun_gof_chunk.R"))

config <- read_rds(config_path)
paths  <- config$paths


# ---------------------------------------------------------
# Basic paths and settings
# ---------------------------------------------------------

family_levels <- config$experiment$families

input_dir  <- paths$features
output_dir <- paths$gof

ensure_dir(output_dir)

chunk_size <- if (!is.null(config$gof$chunk_size)) {
  as.integer(config$gof$chunk_size)
} else {
  1000L
}

progress_step <- if (!is.null(config$gof$progress_step)) {
  as.integer(config$gof$progress_step)
} else {
  250L
}

use_parallel <- if (!is.null(config$gof$parallel)) {
  isTRUE(config$gof$parallel)
} else {
  FALSE
}

n_workers <- if (!is.null(config$gof$workers)) {
  as.integer(config$gof$workers)
} else {
  4L
}

cleanup_feature_shards_after_gof <- if (!is.null(config$gof$cleanup_feature_shards_after_gof)) {
  isTRUE(config$gof$cleanup_feature_shards_after_gof)
} else {
  FALSE
}

if (chunk_size <= 0) {
  stop("[021] chunk_size must be positive.")
}

if (progress_step <= 0) {
  stop("[021] progress_step must be positive.")
}

if (n_workers <= 0) {
  stop("[021] n_workers must be positive.")
}


# ---------------------------------------------------------
# Parallelization policy
# ---------------------------------------------------------

can_fork <- identical(.Platform$OS.type, "unix")

if (use_parallel && can_fork) {
  log_msg("[021] Parallel GOF extraction enabled with mclapply | workers = ", n_workers)
} else if (use_parallel && !can_fork) {
  log_msg("[021] Parallel GOF requested, but non-Unix OS detected. Falling back to sequential execution.")
  use_parallel <- FALSE
} else {
  log_msg("[021] Parallel GOF extraction disabled.")
}


# ---------------------------------------------------------
# Helper: output chunk path
# ---------------------------------------------------------

chunk_output_path <- function(output_dir, fam, cc) {
  file.path(
    output_dir,
    paste0("gof_table_", fam, "_chunk_", sprintf("%05d", cc), ".parquet")
  )
}


# ---------------------------------------------------------
# Helper: validate existing chunk file
# ---------------------------------------------------------

is_valid_chunk_file <- function(path) {
  
  if (!file.exists(path)) {
    return(FALSE)
  }
  
  out <- tryCatch(
    {
      tbl <- arrow::read_parquet(path)
      is.data.frame(tbl) &&
        nrow(tbl) > 0 &&
        "sample_id" %in% names(tbl)
    },
    error = function(e) FALSE
  )
  
  isTRUE(out)
}


# ---------------------------------------------------------
# Helper: robust parquet row count
# ---------------------------------------------------------

safe_nrow_parquet <- function(path) {
  
  if (!file.exists(path)) {
    return(NA_integer_)
  }
  
  out <- tryCatch(
    {
      tbl <- arrow::read_parquet(path, col_select = "sample_id")
      nrow(tbl)
    },
    error = function(e) NA_integer_
  )
  
  as.integer(out)
}


# ---------------------------------------------------------
# Helper: atomic parquet write
# ---------------------------------------------------------

save_parquet_atomic <- function(df, path) {
  
  tmp <- paste0(path, ".tmp_", Sys.getpid(), "_", sample.int(1e6, 1))
  
  if (file.exists(tmp)) {
    unlink(tmp, force = TRUE)
  }
  
  save_parquet(df, tmp)
  
  if (file.exists(path)) {
    unlink(path, force = TRUE)
  }
  
  ok <- file.rename(tmp, path)
  
  if (!ok) {
    ok2 <- file.copy(tmp, path, overwrite = TRUE)
    unlink(tmp, force = TRUE)
    
    if (!ok2) {
      stop("[021] Failed to move parquet into place: ", path)
    }
  }
  
  invisible(path)
}


# ---------------------------------------------------------
# Helper: process and write one chunk
# ---------------------------------------------------------

run_one_chunk <- function(
    cc,
    chunk_tbl,
    fam,
    family_levels,
    progress_step,
    output_dir,
    n_total_chunks_est
) {
  
  chunk_path <- chunk_output_path(output_dir, fam, cc)
  
  if (is_valid_chunk_file(chunk_path)) {
    log_msg(
      "[021] Skipping existing valid GOF chunk for family ", fam,
      " | chunk ", cc,
      " | path = ", chunk_path
    )
    return(invisible(NULL))
  }
  
  if (file.exists(chunk_path)) {
    file.remove(chunk_path)
    log_msg(
      "[021] Removed stale/corrupted GOF chunk for family ", fam,
      " | chunk ", cc,
      " | path = ", chunk_path
    )
  }
  
  log_msg(
    "[021] Processing family ", fam,
    " | GOF chunk ", cc, "/", n_total_chunks_est,
    " | rows = ", nrow(chunk_tbl)
  )
  
  chunk_label <- paste0(
    "Family ", fam,
    " | GOF chunk ", cc, "/", n_total_chunks_est
  )
  
  chunk_out <- process_gof_chunk(
    chunk_tbl = chunk_tbl,
    family_levels = family_levels,
    progress_step = progress_step,
    chunk_label = chunk_label
  )
  
  if (nrow(chunk_out) == 0) {
    stop("[021] Empty GOF chunk produced for family ", fam, ", chunk ", cc)
  }
  
  save_parquet_atomic(chunk_out, chunk_path)
  
  rm(chunk_out)
  invisible(gc())
  
  invisible(NULL)
}


# ---------------------------------------------------------
# Check required upstream feature directory
# ---------------------------------------------------------

if (!dir.exists(input_dir)) {
  stop(
    "[021] Feature directory not found: ",
    input_dir,
    "\nRun scripts/020_extract_features.R first."
  )
}


# ---------------------------------------------------------
# Process each true family
# ---------------------------------------------------------

for (fam in family_levels) {
  
  shard_dir <- file.path(input_dir, paste0("feature_table_", fam, "_shards"))
  
  shard_files <- if (dir.exists(shard_dir)) {
    sort(list.files(shard_dir, pattern = "\\.parquet$", full.names = TRUE))
  } else {
    character(0)
  }
  
  if (length(shard_files) > 0) {
    log_msg("[021] Using feature shards for family ", fam, " | n_shards = ", length(shard_files))
    source_mode <- "shards"
  } else {
    in_path <- file.path(input_dir, paste0("feature_table_", fam, ".parquet"))
    
    if (!file.exists(in_path)) {
      log_msg("[021] Skipping family ", fam, ": no feature shards and no family parquet found.")
      next
    }
    
    log_msg("[021] Using family parquet for family ", fam)
    source_mode <- "family_parquet"
  }
  
  global_chunk_counter <- 0L
  
  if (source_mode == "shards") {
    source_files <- shard_files
  } else {
    source_files <- file.path(input_dir, paste0("feature_table_", fam, ".parquet"))
  }
  
  # Estimate total chunk count for logs only.
  n_total_chunks_est <- 0L
  
  for (sf in source_files) {
    n_sf <- safe_nrow_parquet(sf)
    
    if (is.na(n_sf) || n_sf == 0L) {
      next
    }
    
    n_total_chunks_est <- n_total_chunks_est + ceiling(n_sf / chunk_size)
  }
  
  if (n_total_chunks_est == 0L) {
    log_msg("[021] No valid feature rows found for family ", fam, ". Skipping.")
    next
  }
  
  for (sf in source_files) {
    
    base_tbl <- read_parquet(sf) %>%
      dplyr::select(
        sample_id,
        true_family,
        param_id,
        n,
        replicate_id,
        seed,
        par1,
        par2,
        par3,
        dplyr::any_of("delta"),
        feature_ok
      ) %>%
      filter(feature_ok) %>%
      arrange(param_id, n, replicate_id)
    
    if (nrow(base_tbl) == 0) {
      rm(base_tbl)
      invisible(gc())
      next
    }
    
    idx <- seq_len(nrow(base_tbl))
    local_chunk_id <- ceiling(idx / chunk_size)
    chunk_index_list <- split(idx, local_chunk_id)
    
    if (!use_parallel) {
      
      for (kk in seq_along(chunk_index_list)) {
        
        global_chunk_counter <- global_chunk_counter + 1L
        idx_chunk <- chunk_index_list[[kk]]
        
        chunk_tbl <- base_tbl[idx_chunk, , drop = FALSE]
        
        run_one_chunk(
          cc = global_chunk_counter,
          chunk_tbl = chunk_tbl,
          fam = fam,
          family_levels = family_levels,
          progress_step = progress_step,
          output_dir = output_dir,
          n_total_chunks_est = n_total_chunks_est
        )
        
        rm(chunk_tbl)
        invisible(gc())
      }
      
    } else {
      
      local_chunks <- lapply(
        chunk_index_list,
        function(ix) base_tbl[ix, , drop = FALSE]
      )
      
      global_ids <- seq.int(
        global_chunk_counter + 1L,
        global_chunk_counter + length(local_chunks)
      )
      
      global_chunk_counter <- global_chunk_counter + length(local_chunks)
      
      parallel::mclapply(
        X = seq_along(local_chunks),
        FUN = function(jj) {
          run_one_chunk(
            cc = global_ids[[jj]],
            chunk_tbl = local_chunks[[jj]],
            fam = fam,
            family_levels = family_levels,
            progress_step = progress_step,
            output_dir = output_dir,
            n_total_chunks_est = n_total_chunks_est
          )
          NULL
        },
        mc.cores = min(n_workers, length(local_chunks)),
        mc.preschedule = FALSE
      )
      
      rm(local_chunks, global_ids)
      invisible(gc())
    }
    
    rm(base_tbl)
    invisible(gc())
  }
  
  all_chunk_files <- sort(
    list.files(
      output_dir,
      pattern = paste0("^gof_table_", fam, "_chunk_[0-9]{5}\\.parquet$"),
      full.names = TRUE
    )
  )
  
  valid_after <- vapply(
    all_chunk_files,
    is_valid_chunk_file,
    logical(1)
  )
  
  log_msg(
    "[021] Finished family ", fam,
    " | valid_chunks_after = ", sum(valid_after),
    "/", length(all_chunk_files)
  )
  
  if (!all(valid_after)) {
    bad_files <- basename(all_chunk_files[!valid_after])
    
    stop(
      "[021] Family ", fam,
      " finished with invalid GOF chunks: ",
      paste(bad_files, collapse = ", ")
    )
  }
  
  if (cleanup_feature_shards_after_gof && dir.exists(shard_dir)) {
    unlink(shard_dir, recursive = TRUE, force = TRUE)
    log_msg("[021] Removed feature shard directory after GOF completion for family ", fam)
  }
}


# ---------------------------------------------------------
# Session info
# ---------------------------------------------------------

save_session_info(
  file.path(paths$gof, "session_info_021.txt")
)

log_msg("[021] 021_extract_gof_features.R finished successfully.")