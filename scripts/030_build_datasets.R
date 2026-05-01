# =========================================================
# 030_build_datasets.R
# Build ML datasets from feature and GOF/IC tables
# =========================================================
#
# This script:
#   - processes the experiment family by family;
#   - joins feature tables and GOF/IC tables by sample_id;
#   - defines feature scenarios;
#   - creates train/validation/test splits within each true
#     distribution family;
#   - writes final train, validation, and test datasets.
#
# Precomputed datasets are not distributed with the repository.
#
# Required previous steps:
#   scripts/000_config.R
#   scripts/010_simulate_samples.R
#   scripts/020_extract_features.R
#   scripts/021_extract_gof_features.R
#   scripts/022_combine_gof_chunks.R
# =========================================================

library(dplyr)
library(tibble)
library(arrow)


# ---------------------------------------------------------
# Repository root check
# ---------------------------------------------------------

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

config_path <- file.path(root_dir, "outputs", "config", "config_main.rds")

if (!file.exists(file.path(root_dir, "R")) ||
    !file.exists(file.path(root_dir, "scripts")) ||
    !file.exists(config_path)) {
  stop(
    "[030] This script must be run from the FADS_AI repository root ",
    "after scripts/000_config.R has been executed.\n",
    "Current working directory: ", root_dir
  )
}


# ---------------------------------------------------------
# Load helper functions and configuration
# ---------------------------------------------------------

source(file.path(root_dir, "R", "fun_io.R"))
source(file.path(root_dir, "R", "fun_ml.R"))

config <- read_rds(config_path)
paths  <- config$paths


# ---------------------------------------------------------
# Paths
# ---------------------------------------------------------

feature_dir <- paths$features
gof_dir     <- paths$gof
dataset_dir <- paths$datasets

ensure_dir(dataset_dir)

train_shard_dir <- file.path(dataset_dir, "train_shards")
valid_shard_dir <- file.path(dataset_dir, "valid_shards")
test_shard_dir  <- file.path(dataset_dir, "test_shards")
full_shard_dir  <- file.path(dataset_dir, "full_shards")

ensure_dir(train_shard_dir)
ensure_dir(valid_shard_dir)
ensure_dir(test_shard_dir)
ensure_dir(full_shard_dir)

save_full_dataset <- if (!is.null(config$datasets$save_full_dataset)) {
  isTRUE(config$datasets$save_full_dataset)
} else {
  TRUE
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
      stop("[030] Failed to move parquet into place: ", path)
    }
  }
  
  invisible(path)
}


# ---------------------------------------------------------
# Robust parquet row count
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
# Combine shards into one parquet file
# ---------------------------------------------------------

combine_shards_to_parquet <- function(shard_dir, out_path, batch_size = 10L) {
  
  files_i <- sort(
    list.files(shard_dir, pattern = "\\.parquet$", full.names = TRUE)
  )
  
  if (length(files_i) == 0L) {
    stop("[030] No shard files found in: ", shard_dir)
  }
  
  if (batch_size <= 0) {
    stop("[030] batch_size must be positive.")
  }
  
  batch_dir <- paste0(shard_dir, "_combine_batches")
  ensure_dir(batch_dir)
  
  stale_batches <- list.files(batch_dir, pattern = "\\.parquet$", full.names = TRUE)
  
  if (length(stale_batches) > 0) {
    unlink(stale_batches, force = TRUE)
  }
  
  batch_ids <- ceiling(seq_along(files_i) / batch_size)
  
  for (bb in sort(unique(batch_ids))) {
    
    files_bb <- files_i[batch_ids == bb]
    
    log_msg(
      "[030] Combining batch ", bb,
      " | files = ", length(files_bb)
    )
    
    tbl_bb <- bind_rows(lapply(files_bb, read_parquet))
    
    batch_path <- file.path(batch_dir, sprintf("batch_%03d.parquet", bb))
    
    save_parquet_atomic(tbl_bb, batch_path)
    
    rm(tbl_bb)
    invisible(gc())
  }
  
  batch_files <- sort(
    list.files(batch_dir, pattern = "\\.parquet$", full.names = TRUE)
  )
  
  if (length(batch_files) == 0L) {
    stop("[030] No batch files created while combining shards: ", shard_dir)
  }
  
  out_tbl <- bind_rows(lapply(batch_files, read_parquet))
  
  if (!"sample_id" %in% names(out_tbl)) {
    stop("[030] Combined dataset lacks required column 'sample_id': ", out_path)
  }
  
  save_parquet_atomic(out_tbl, out_path)
  
  rm(out_tbl)
  invisible(gc())
  
  unlink(batch_dir, recursive = TRUE, force = TRUE)
  
  invisible(out_path)
}


# ---------------------------------------------------------
# Create train/validation/test labels within one family
#
# This preserves the original pipeline logic: splitting is
# performed within each true distribution family, using a
# deterministic family-specific seed.
# ---------------------------------------------------------

make_split_labels <- function(df, split, seed, fam_index) {
  
  if (!all(c("train", "valid", "test") %in% names(split))) {
    stop("[030] split must contain train, valid, and test proportions.")
  }
  
  if (abs(split$train + split$valid + split$test - 1) > 1e-8) {
    stop("[030] Split proportions must sum to 1.")
  }
  
  n_total <- nrow(df)
  
  if (n_total == 0) {
    return(character(0))
  }
  
  set.seed(seed + fam_index)
  
  ord <- sample.int(n_total, size = n_total, replace = FALSE)
  
  n_train <- floor(split$train * n_total)
  n_valid <- floor(split$valid * n_total)
  n_test  <- n_total - n_train - n_valid
  
  lbl <- rep(NA_character_, n_total)
  
  if (n_train > 0) {
    lbl[ord[seq_len(n_train)]] <- "train"
  }
  
  if (n_valid > 0) {
    idx_valid <- ord[(n_train + 1L):(n_train + n_valid)]
    lbl[idx_valid] <- "valid"
  }
  
  if (n_test > 0) {
    idx_test <- ord[(n_train + n_valid + 1L):n_total]
    lbl[idx_test] <- "test"
  }
  
  if (any(is.na(lbl))) {
    stop("[030] Some rows were not assigned to train/valid/test.")
  }
  
  lbl
}


# ---------------------------------------------------------
# Build feature scenarios from available column names
# ---------------------------------------------------------

build_feature_scenarios_from_names <- function(all_names) {
  
  classical <- c(
    "lmom_l1", "lmom_l2", "lmom_l3", "lmom_l4",
    "lmom_t3", "lmom_t4"
  )
  
  lmrd <- grep("^lmrd_", all_names, value = TRUE)
  
  gof_stats <- grep("^(KS_stat|AD_stat|CvM_stat)_", all_names, value = TRUE)
  
  ic_cols <- grep("^(AIC|BIC)_", all_names, value = TRUE)
  
  bayes_cols <- grep(
    "^(KS_bfmin|KS_elogbf|AD_bfmin|AD_elogbf|CvM_bfmin|CvM_elogbf)_",
    all_names,
    value = TRUE
  )
  
  feature_scenarios <- list(
    classical = unique(intersect(classical, all_names)),
    
    classical_lmrd = unique(c(
      intersect(classical, all_names),
      lmrd
    )),
    
    classical_lmrd_bayes = unique(c(
      intersect(classical, all_names),
      lmrd,
      bayes_cols
    )),
    
    classical_lmrd_gof_ic = unique(c(
      intersect(classical, all_names),
      lmrd,
      gof_stats,
      ic_cols
    )),
    
    classical_lmrd_gof_ic_bayes = unique(c(
      intersect(classical, all_names),
      lmrd,
      gof_stats,
      ic_cols,
      bayes_cols
    ))
  )
  
  lapply(feature_scenarios, function(cols) cols[cols %in% all_names])
}


# ---------------------------------------------------------
# Main processing
# ---------------------------------------------------------

log_msg("[030] Building datasets family by family...")

feature_scenarios <- NULL

for (ff in seq_along(config$experiment$families)) {
  
  fam <- config$experiment$families[[ff]]
  
  feature_path <- file.path(feature_dir, paste0("feature_table_", fam, ".parquet"))
  gof_path     <- file.path(gof_dir, paste0("gof_table_", fam, ".parquet"))
  
  if (!file.exists(feature_path)) {
    stop(
      "[030] Missing feature table for family ",
      fam,
      ": ",
      feature_path,
      "\nRun scripts/020_extract_features.R first."
    )
  }
  
  if (!file.exists(gof_path)) {
    stop(
      "[030] Missing GOF table for family ",
      fam,
      ": ",
      gof_path,
      "\nRun scripts/021_extract_gof_features.R and scripts/022_combine_gof_chunks.R first."
    )
  }
  
  log_msg("[030] Reading family tables for ", fam)
  
  feature_tbl_fam <- read_parquet(feature_path)
  gof_tbl_fam     <- read_parquet(gof_path)
  
  if (!"sample_id" %in% names(feature_tbl_fam)) {
    stop("[030] Feature table lacks sample_id for family: ", fam)
  }
  
  if (!"sample_id" %in% names(gof_tbl_fam)) {
    stop("[030] GOF table lacks sample_id for family: ", fam)
  }
  
  log_msg("[030] Building family ML dataset for ", fam)
  
  full_tbl_fam <- build_full_ml_dataset(
    feature_tbl = feature_tbl_fam,
    gof_tbl = gof_tbl_fam
  )
  
  if (is.null(feature_scenarios)) {
    feature_scenarios <- build_feature_scenarios_from_names(names(full_tbl_fam))
  }
  
  split_label <- make_split_labels(
    df = full_tbl_fam,
    split = config$split,
    seed = config$experiment$seed_master,
    fam_index = ff
  )
  
  full_tbl_fam$split_set <- split_label
  
  train_tbl_fam <- full_tbl_fam %>%
    filter(split_set == "train") %>%
    select(-split_set)
  
  valid_tbl_fam <- full_tbl_fam %>%
    filter(split_set == "valid") %>%
    select(-split_set)
  
  test_tbl_fam <- full_tbl_fam %>%
    filter(split_set == "test") %>%
    select(-split_set)
  
  train_path_i <- file.path(train_shard_dir, paste0("train_", fam, ".parquet"))
  valid_path_i <- file.path(valid_shard_dir, paste0("valid_", fam, ".parquet"))
  test_path_i  <- file.path(test_shard_dir,  paste0("test_", fam, ".parquet"))
  full_path_i  <- file.path(full_shard_dir,  paste0("full_", fam, ".parquet"))
  
  save_parquet_atomic(train_tbl_fam, train_path_i)
  save_parquet_atomic(valid_tbl_fam, valid_path_i)
  save_parquet_atomic(test_tbl_fam,  test_path_i)
  
  if (save_full_dataset) {
    save_parquet_atomic(select(full_tbl_fam, -split_set), full_path_i)
  }
  
  log_msg(
    "[030] Saved split shards for family ", fam,
    " | train = ", nrow(train_tbl_fam),
    " | valid = ", nrow(valid_tbl_fam),
    " | test = ", nrow(test_tbl_fam)
  )
  
  rm(
    feature_tbl_fam,
    gof_tbl_fam,
    full_tbl_fam,
    train_tbl_fam,
    valid_tbl_fam,
    test_tbl_fam,
    split_label
  )
  
  invisible(gc())
}


# ---------------------------------------------------------
# Save feature scenario information
# ---------------------------------------------------------

scenario_info <- tibble(
  scenario = names(feature_scenarios),
  n_features = vapply(feature_scenarios, length, integer(1))
)

log_msg("[030] Feature scenarios summary:")
print(scenario_info)

save_rds(
  feature_scenarios,
  file.path(dataset_dir, "feature_scenarios.rds")
)

save_parquet_atomic(
  scenario_info,
  file.path(dataset_dir, "feature_scenario_info.parquet")
)


# ---------------------------------------------------------
# Combine split shards
# ---------------------------------------------------------

log_msg("[030] Combining split shards into final datasets...")

combine_shards_to_parquet(
  train_shard_dir,
  file.path(dataset_dir, "train.parquet"),
  batch_size = 6L
)

combine_shards_to_parquet(
  valid_shard_dir,
  file.path(dataset_dir, "valid.parquet"),
  batch_size = 6L
)

combine_shards_to_parquet(
  test_shard_dir,
  file.path(dataset_dir, "test.parquet"),
  batch_size = 6L
)

if (save_full_dataset) {
  combine_shards_to_parquet(
    full_shard_dir,
    file.path(dataset_dir, "full_dataset.parquet"),
    batch_size = 6L
  )
}


# ---------------------------------------------------------
# Diagnostics from final saved files
# ---------------------------------------------------------

train_n <- safe_nrow_parquet(file.path(dataset_dir, "train.parquet"))
valid_n <- safe_nrow_parquet(file.path(dataset_dir, "valid.parquet"))
test_n  <- safe_nrow_parquet(file.path(dataset_dir, "test.parquet"))

full_n <- if (save_full_dataset) {
  safe_nrow_parquet(file.path(dataset_dir, "full_dataset.parquet"))
} else {
  train_n + valid_n + test_n
}

log_msg("[030] Final dataset row counts:")
log_msg("[030] full_dataset rows: ", full_n)
log_msg("[030] train rows: ", train_n)
log_msg("[030] valid rows: ", valid_n)
log_msg("[030] test rows: ", test_n)


# ---------------------------------------------------------
# Cleanup temporary split shards
# ---------------------------------------------------------

unlink(train_shard_dir, recursive = TRUE, force = TRUE)
unlink(valid_shard_dir, recursive = TRUE, force = TRUE)
unlink(test_shard_dir, recursive = TRUE, force = TRUE)

if (save_full_dataset) {
  unlink(full_shard_dir, recursive = TRUE, force = TRUE)
}


# ---------------------------------------------------------
# Session info
# ---------------------------------------------------------

save_session_info(
  file.path(dataset_dir, "session_info_030.txt")
)

log_msg("[030] 030_build_datasets.R finished successfully.")