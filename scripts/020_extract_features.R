# =========================================================
# 020_extract_features.R
# Extract L-moment and LMRD features from regenerated samples
# =========================================================
#
# This script:
#   - reads the simulation grid and valid parameter registry;
#   - regenerates samples on the fly;
#   - computes sample L-moments;
#   - computes LMRD-based features;
#   - writes feature tables by true family.
#
# Raw Monte Carlo samples are not stored.
#
# Required previous steps:
#   scripts/000_config.R
#   scripts/010_simulate_samples.R
# =========================================================

library(dplyr)
library(tidyr)
library(tibble)
library(arrow)
library(lmom)
library(readr)


# ---------------------------------------------------------
# Repository root check
# ---------------------------------------------------------

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

config_path <- file.path(root_dir, "outputs", "config", "config_main.rds")

if (!file.exists(file.path(root_dir, "R")) ||
    !file.exists(file.path(root_dir, "scripts")) ||
    !file.exists(config_path)) {
  stop(
    "[020] This script must be run from the FADS_AI repository root ",
    "after scripts/000_config.R has been executed.\n",
    "Current working directory: ", root_dir
  )
}


# ---------------------------------------------------------
# Load helper functions and configuration
# ---------------------------------------------------------

source(file.path(root_dir, "R", "fun_io.R"))
source(file.path(root_dir, "R", "fun_distributions.R"))
source(file.path(root_dir, "R", "fun_lmom.R"))
source(file.path(root_dir, "R", "fun_lmrd_preprocess.R"))
source(file.path(root_dir, "R", "fun_lmrd.R"))

config <- read_rds(config_path)
paths  <- config$paths


# ---------------------------------------------------------
# Check required upstream outputs
# ---------------------------------------------------------

sim_grid_path <- file.path(paths$sim, "simulation_grid.parquet")
param_valid_path <- file.path(paths$sim, "valid_parameter_registry.parquet")

if (!file.exists(sim_grid_path)) {
  stop(
    "[020] Simulation grid not found: ",
    sim_grid_path,
    "\nRun scripts/010_simulate_samples.R first."
  )
}

if (!file.exists(param_valid_path)) {
  stop(
    "[020] Valid parameter registry not found: ",
    param_valid_path,
    "\nRun scripts/010_simulate_samples.R first."
  )
}

if (!file.exists(paths$polygons_rds)) {
  stop("[020] Required LMRD polygon input not found: ", paths$polygons_rds)
}


# ---------------------------------------------------------
# Read fixed inputs and metadata
# ---------------------------------------------------------

param_valid <- read_parquet(param_valid_path)

log_msg("[020] Reading preprocessed LMRD polygons...")
objeto_pre <- read_lmrd_preprocessed(paths$polygons_rds)

family_levels <- config$experiment$families
param_lookup  <- split(param_valid, param_valid$param_id)

output_dir <- paths$features
ensure_dir(output_dir)

feature_chunk_size <- if (!is.null(config$features$chunk_size)) {
  as.integer(config$features$chunk_size)
} else {
  5000L
}

progress_step_020 <- if (!is.null(config$features$progress_step)) {
  as.integer(config$features$progress_step)
} else {
  1000L
}

keep_feature_shards <- if (!is.null(config$features$keep_feature_shards)) {
  isTRUE(config$features$keep_feature_shards)
} else {
  TRUE
}

if (feature_chunk_size <= 0) {
  stop("[020] feature_chunk_size must be positive.")
}

if (progress_step_020 <= 0) {
  stop("[020] progress_step_020 must be positive.")
}


# ---------------------------------------------------------
# Error row helper
# ---------------------------------------------------------

make_feature_error_row <- function(row_i, par_row = NULL, msg = "Unknown error") {
  
  tibble(
    sample_id = row_i$sample_id,
    true_family = as.character(row_i$family),
    param_id = as.character(row_i$param_id),
    n = as.integer(row_i$n),
    replicate_id = as.integer(row_i$replicate_id),
    seed = as.integer(row_i$seed),
    par1 = if (is.null(par_row) || nrow(par_row) == 0) {
      NA_real_
    } else {
      as.numeric(par_row$par1[[1]])
    },
    par2 = if (is.null(par_row) || nrow(par_row) == 0) {
      NA_real_
    } else {
      as.numeric(par_row$par2[[1]])
    },
    par3 = if (is.null(par_row) || nrow(par_row) == 0) {
      NA_real_
    } else {
      as.numeric(par_row$par3[[1]])
    },
    delta = if (
      is.null(par_row) ||
      nrow(par_row) == 0 ||
      !"delta" %in% names(par_row)
    ) {
      NA_real_
    } else {
      as.numeric(par_row$delta[[1]])
    },
    feature_ok = FALSE,
    feature_error = msg
  )
}


# ---------------------------------------------------------
# Atomic parquet writer
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
      stop("[020] Failed to move parquet into place: ", path)
    }
  }
  
  invisible(path)
}


# ---------------------------------------------------------
# Combine feature shards
# ---------------------------------------------------------

combine_feature_shards <- function(shard_dir, out_path, batch_size = 25L) {
  
  files_i <- list.files(shard_dir, pattern = "\\.parquet$", full.names = TRUE)
  files_i <- sort(files_i)
  
  if (length(files_i) == 0L) {
    stop("[020] No feature shards found in: ", shard_dir)
  }
  
  if (batch_size <= 0) {
    stop("[020] batch_size must be positive.")
  }
  
  batch_dir <- file.path(shard_dir, "_combine_batches")
  ensure_dir(batch_dir)
  
  batch_ids <- ceiling(seq_along(files_i) / batch_size)
  
  for (bb in sort(unique(batch_ids))) {
    files_bb <- files_i[batch_ids == bb]
    
    tbl_bb <- bind_rows(lapply(files_bb, read_parquet))
    
    batch_path <- file.path(batch_dir, sprintf("batch_%03d.parquet", bb))
    
    save_parquet_atomic(tbl_bb, batch_path)
    
    rm(tbl_bb)
    invisible(gc())
  }
  
  batch_files <- list.files(batch_dir, pattern = "\\.parquet$", full.names = TRUE)
  batch_files <- sort(batch_files)
  
  full_tbl <- bind_rows(lapply(batch_files, read_parquet))
  
  save_parquet_atomic(full_tbl, out_path)
  
  unlink(batch_dir, recursive = TRUE, force = TRUE)
  
  invisible(out_path)
}


# ---------------------------------------------------------
# Open simulation grid lazily
# ---------------------------------------------------------

sim_ds <- arrow::open_dataset(sim_grid_path, format = "parquet")


# ---------------------------------------------------------
# Process each true family
# ---------------------------------------------------------

for (fam in family_levels) {
  
  log_msg("[020] Reading simulation plan for family: ", fam)
  
  fam_grid <- sim_ds %>%
    select(sample_id, family, param_id, n, replicate_id, seed) %>%
    filter(family == !!fam) %>%
    collect() %>%
    arrange(param_id, n, replicate_id)
  
  log_msg("[020] Starting family: ", fam, " | rows = ", nrow(fam_grid))
  
  if (nrow(fam_grid) == 0) {
    log_msg("[020] Skipping family ", fam, ": no rows in simulation grid.")
    next
  }
  
  shard_dir <- file.path(output_dir, paste0("feature_table_", fam, "_shards"))
  ensure_dir(shard_dir)
  
  # Clean stale shard files only.
  stale_shards <- list.files(shard_dir, pattern = "\\.parquet$", full.names = TRUE)
  
  if (length(stale_shards) > 0) {
    unlink(stale_shards, force = TRUE)
  }
  
  n_total <- nrow(fam_grid)
  chunk_starts <- seq.int(1L, n_total, by = feature_chunk_size)
  n_chunks <- length(chunk_starts)
  
  for (cc in seq_along(chunk_starts)) {
    
    i_start <- chunk_starts[[cc]]
    i_end   <- min(i_start + feature_chunk_size - 1L, n_total)
    
    chunk_grid <- fam_grid[i_start:i_end, , drop = FALSE]
    chunk_results <- vector("list", length = nrow(chunk_grid))
    
    log_msg(
      "[020] Family ", fam,
      " | feature chunk ", cc, "/", n_chunks,
      " | rows = ", nrow(chunk_grid)
    )
    
    for (i in seq_len(nrow(chunk_grid))) {
      
      if (i == 1L || i %% progress_step_020 == 0L || i == nrow(chunk_grid)) {
        pct <- round(100 * i / nrow(chunk_grid), 2)
        
        cat(sprintf(
          "[%s] Family %s | feature chunk %d/%d | progress %d/%d (%.2f%%)\n",
          format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
          fam,
          cc,
          n_chunks,
          i,
          nrow(chunk_grid),
          pct
        ))
      }
      
      row_i <- chunk_grid[i, ]
      par_row <- param_lookup[[as.character(row_i$param_id)]]
      
      if (is.null(par_row) || nrow(par_row) == 0) {
        chunk_results[[i]] <- make_feature_error_row(
          row_i = row_i,
          par_row = NULL,
          msg = "Parameter row not found in param_lookup"
        )
        next
      }
      
      par_row <- par_row[1, , drop = FALSE]
      
      x <- tryCatch(
        simulate_one_sample(
          family = as.character(row_i$family),
          n = as.integer(row_i$n),
          param_row = par_row,
          seed = as.integer(row_i$seed)
        ),
        error = function(e) e
      )
      
      if (inherits(x, "error")) {
        chunk_results[[i]] <- make_feature_error_row(
          row_i = row_i,
          par_row = par_row,
          msg = paste("Simulation error:", conditionMessage(x))
        )
        next
      }
      
      lmom_row <- tryCatch(
        compute_sample_lmom(x),
        error = function(e) e
      )
      
      if (inherits(lmom_row, "error")) {
        chunk_results[[i]] <- make_feature_error_row(
          row_i = row_i,
          par_row = par_row,
          msg = paste("L-moment error:", conditionMessage(lmom_row))
        )
        next
      }
      
      lmrd_row <- tryCatch(
        compute_lmrd_features_one(
          t3 = lmom_row$lmom_t3,
          t4 = lmom_row$lmom_t4,
          n = as.integer(row_i$n),
          objeto_pre = objeto_pre,
          family_levels = family_levels,
          strict = TRUE
        ),
        error = function(e) e
      )
      
      if (inherits(lmrd_row, "error")) {
        chunk_results[[i]] <- make_feature_error_row(
          row_i = row_i,
          par_row = par_row,
          msg = paste("LMRD error:", conditionMessage(lmrd_row))
        )
        next
      }
      
      chunk_results[[i]] <- bind_cols(
        tibble(
          sample_id = row_i$sample_id,
          true_family = as.character(row_i$family),
          param_id = as.character(row_i$param_id),
          n = as.integer(row_i$n),
          replicate_id = as.integer(row_i$replicate_id),
          seed = as.integer(row_i$seed),
          par1 = as.numeric(par_row$par1[[1]]),
          par2 = as.numeric(par_row$par2[[1]]),
          par3 = as.numeric(par_row$par3[[1]]),
          delta = if ("delta" %in% names(par_row)) {
            as.numeric(par_row$delta[[1]])
          } else {
            NA_real_
          },
          feature_ok = TRUE,
          feature_error = NA_character_
        ),
        lmom_row,
        lmrd_row
      )
    }
    
    chunk_tbl <- bind_rows(chunk_results)
    
    if (nrow(chunk_tbl) == 0) {
      stop("[020] Family ", fam, " | feature chunk ", cc, " produced empty output.")
    }
    
    shard_path <- file.path(
      shard_dir,
      paste0("feature_table_", fam, "_chunk_", sprintf("%04d", cc), ".parquet")
    )
    
    save_parquet_atomic(chunk_tbl, shard_path)
    
    rm(chunk_grid, chunk_results, chunk_tbl)
    invisible(gc())
  }
  
  out_path <- file.path(output_dir, paste0("feature_table_", fam, ".parquet"))
  
  log_msg("[020] Combining feature shards for family ", fam, " ...")
  
  combine_feature_shards(
    shard_dir = shard_dir,
    out_path = out_path,
    batch_size = 25L
  )
  
  if (!keep_feature_shards) {
    unlink(shard_dir, recursive = TRUE, force = TRUE)
    log_msg("[020] Removed feature shard directory for family ", fam)
  }
  
  log_msg("[020] Saved feature table for family ", fam, " | path = ", out_path)
  
  rm(fam_grid)
  invisible(gc())
}


# ---------------------------------------------------------
# Session info
# ---------------------------------------------------------

save_session_info(
  file.path(paths$features, "session_info_020.txt")
)

log_msg("[020] 020_extract_features.R finished successfully.")