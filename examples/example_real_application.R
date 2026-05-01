# =========================================================
# example_real_application.R
# Minimal FADS_AI application example for one annual-maximum sample
# =========================================================
#
# This script demonstrates how to apply the application-ready
# FADS_AI model to a new annual-maximum sample.
#
# The released application model uses:
#   scenario  = classical
#   algorithm = xgb
#   features  = sample L-moment descriptors
#
# Input file format:
#
#   Optional metadata lines may be included at the beginning
#   of the file. Metadata lines must start with '#', using:
#
#     # key: value
#
#   The data table must start after the metadata block and
#   must contain a numeric column named 'value'.
#
# Example:
#
#   # station_code: 00000000
#   # station_name: Example station
#   # variable: annual maximum discharge
#   # unit: m3/s
#   year,value
#   1981,421.3
#   1982,358.7
#
# Run from the repository root:
#
#   source("examples/example_real_application.R")
# =========================================================


# ---------------------------------------------------------
# Repository root check
# ---------------------------------------------------------

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

required_root_items <- c("R", "data", "examples", "models")

missing_root_items <- required_root_items[
  !file.exists(file.path(root_dir, required_root_items))
]

if (length(missing_root_items) > 0) {
  stop(
    "This script must be run from the FADS_AI repository root.\n",
    "Current working directory: ", root_dir, "\n",
    "Missing expected items: ", paste(missing_root_items, collapse = ", ")
  )
}


# ---------------------------------------------------------
# Required packages
# ---------------------------------------------------------

required_packages <- c(
  "dplyr",
  "tibble",
  "readr",
  "lmom",
  "recipes",
  "parsnip",
  "workflows",
  "xgboost",
  "tidyr"
)

missing_packages <- required_packages[
  !vapply(required_packages, requireNamespace, logical(1), quietly = TRUE)
]

if (length(missing_packages) > 0) {
  stop(
    "Missing required packages: ",
    paste(missing_packages, collapse = ", ")
  )
}

library(dplyr)
library(tibble)
library(readr)


# ---------------------------------------------------------
# Load helper functions
# ---------------------------------------------------------

source(file.path(root_dir, "R", "fun_io.R"))
source(file.path(root_dir, "R", "fun_lmom.R"))


# ---------------------------------------------------------
# Helper: parse metadata lines
# ---------------------------------------------------------

parse_metadata_lines <- function(lines) {
  
  metadata_lines <- lines[grepl("^\\s*#", lines)]
  
  if (length(metadata_lines) == 0) {
    return(tibble::tibble(
      field = character(),
      value = character()
    ))
  }
  
  metadata_lines <- sub("^\\s*#\\s*", "", metadata_lines)
  
  has_key_value <- grepl(":", metadata_lines, fixed = TRUE)
  
  metadata_lines <- metadata_lines[has_key_value]
  
  if (length(metadata_lines) == 0) {
    return(tibble::tibble(
      field = character(),
      value = character()
    ))
  }
  
  field <- trimws(sub(":.*$", "", metadata_lines))
  value <- trimws(sub("^[^:]*:", "", metadata_lines))
  
  tibble::tibble(
    field = field,
    value = value
  )
}


# ---------------------------------------------------------
# Helper: read annual-maximum input file
# ---------------------------------------------------------

read_application_input <- function(path) {
  
  if (!file.exists(path)) {
    stop("Input file not found: ", path)
  }
  
  lines <- readLines(path, warn = FALSE, encoding = "UTF-8")
  
  if (length(lines) == 0) {
    stop("Input file is empty: ", path)
  }
  
  metadata_tbl <- parse_metadata_lines(lines)
  
  data_lines <- lines[!grepl("^\\s*#", lines)]
  data_lines <- data_lines[nzchar(trimws(data_lines))]
  
  if (length(data_lines) == 0) {
    stop("Input file does not contain a data table after metadata lines.")
  }
  
  input_tbl <- readr::read_csv(
    paste(data_lines, collapse = "\n"),
    show_col_types = FALSE
  )
  
  if (!"value" %in% names(input_tbl)) {
    stop("Input data table must contain a numeric column named 'value'.")
  }
  
  x <- input_tbl$value
  
  if (!is.numeric(x)) {
    stop("Column 'value' must be numeric.")
  }
  
  if (length(x) < 4) {
    stop("At least 4 observations are required to compute sample L-moments.")
  }
  
  if (any(!is.finite(x))) {
    stop("Column 'value' contains non-finite values.")
  }
  
  if (any(x <= 0)) {
    stop("Column 'value' must contain strictly positive values for this application example.")
  }
  
  list(
    metadata = metadata_tbl,
    data = input_tbl,
    values = x
  )
}


# ---------------------------------------------------------
# Required model files
# ---------------------------------------------------------

model_path <- file.path(root_dir, "models", "fads_ai_final_model.rds")
metadata_path <- file.path(root_dir, "models", "fads_ai_model_metadata.json")
feature_dictionary_path <- file.path(root_dir, "models", "fads_ai_feature_dictionary.csv")
candidate_path <- file.path(root_dir, "models", "fads_ai_candidate_distributions.csv")

required_model_files <- c(
  model_path,
  metadata_path,
  feature_dictionary_path,
  candidate_path
)

missing_model_files <- required_model_files[
  !file.exists(required_model_files)
]

if (length(missing_model_files) > 0) {
  stop(
    "Missing application model files:\n",
    paste(missing_model_files, collapse = "\n")
  )
}


# ---------------------------------------------------------
# Load application-ready model
# ---------------------------------------------------------

cat("\nLoading FADS_AI application-ready model...\n")

fads_model <- read_rds(model_path)

if (!is.list(fads_model)) {
  stop("The model file must contain a list object.")
}

required_model_fields <- c(
  "scenario",
  "algorithm",
  "candidate_distributions",
  "feature_columns",
  "fit"
)

missing_model_fields <- setdiff(required_model_fields, names(fads_model))

if (length(missing_model_fields) > 0) {
  stop(
    "The model object is missing required fields: ",
    paste(missing_model_fields, collapse = ", ")
  )
}

if (!identical(fads_model$scenario, "classical")) {
  stop("This example expects the released model scenario to be 'classical'.")
}

if (!identical(fads_model$algorithm, "xgb")) {
  stop("This example expects the released model algorithm to be 'xgb'.")
}

feature_columns <- fads_model$feature_columns
fit_ready <- fads_model$fit

cat("Model scenario :", fads_model$scenario, "\n")
cat("Model algorithm:", fads_model$algorithm, "\n")
cat("Model features :", paste(feature_columns, collapse = ", "), "\n")


# ---------------------------------------------------------
# Read example annual-maximum sample
# ---------------------------------------------------------

input_path <- file.path(root_dir, "examples", "example_input_annual_maxima.csv")

input_obj <- read_application_input(input_path)

input_metadata <- input_obj$metadata
input_tbl <- input_obj$data
x <- input_obj$values

cat("\nInput sample loaded successfully.\n")
cat("Number of observations:", length(x), "\n")

if (nrow(input_metadata) > 0) {
  cat("\nInput metadata:\n")
  print(input_metadata)
} else {
  cat("\nNo input metadata found.\n")
}


# ---------------------------------------------------------
# Compute classical L-moment features
# ---------------------------------------------------------

cat("\nComputing classical L-moment descriptors...\n")

lmom_row <- compute_sample_lmom(x)

application_row <- tibble(
  sample_id = "example_annual_maxima",
  param_id = "observed_sample",
  n = as.integer(length(x)),
  replicate_id = NA_integer_,
  seed = NA_integer_,
  par1 = NA_real_,
  par2 = NA_real_,
  par3 = NA_real_
) %>%
  bind_cols(lmom_row)


# ---------------------------------------------------------
# Check required model features
# ---------------------------------------------------------

missing_features <- setdiff(feature_columns, names(application_row))

if (length(missing_features) > 0) {
  stop(
    "The application row is missing features required by the selected model:\n",
    paste(missing_features, collapse = ", ")
  )
}


# ---------------------------------------------------------
# Create prediction row
#
# The fitted workflow was trained with a recipe in which ID
# and metadata variables are assigned non-predictor roles.
# They are included here so the new data has the same structure
# expected by the workflow.
# ---------------------------------------------------------

application_sc <- application_row


# ---------------------------------------------------------
# Predict class and probabilities
# ---------------------------------------------------------

cat("\nPredicting candidate-distribution support...\n")

pred_class <- predict(
  fit_ready,
  new_data = application_sc,
  type = "class"
)

pred_prob <- tryCatch(
  predict(
    fit_ready,
    new_data = application_sc,
    type = "prob"
  ),
  error = function(e) NULL
)

cat("\nPredicted distribution class:\n")
print(pred_class)

if (!is.null(pred_prob)) {
  
  prob_long <- pred_prob %>%
    tidyr::pivot_longer(
      cols = everything(),
      names_to = "family",
      values_to = "probability"
    ) %>%
    mutate(
      family = sub("^\\.pred_", "", family)
    ) %>%
    arrange(desc(probability))
  
  cat("\nPredicted class probabilities:\n")
  print(prob_long)
  
} else {
  
  prob_long <- tibble(
    family = character(),
    probability = numeric()
  )
  
  cat("\nProbability predictions were not available for this model.\n")
}


# ---------------------------------------------------------
# Save example output locally
# ---------------------------------------------------------

example_output_dir <- file.path(root_dir, "outputs", "example_application")
ensure_dir(example_output_dir)

readr::write_csv(
  input_metadata,
  file.path(example_output_dir, "example_application_metadata.csv")
)

readr::write_csv(
  input_tbl,
  file.path(example_output_dir, "example_application_input_data.csv")
)

readr::write_csv(
  application_row,
  file.path(example_output_dir, "example_application_features.csv")
)

readr::write_csv(
  tibble(
    model_scenario = fads_model$scenario,
    model_algorithm = fads_model$algorithm,
    predicted_class = as.character(pred_class$.pred_class[[1]])
  ),
  file.path(example_output_dir, "example_application_prediction.csv")
)

if (nrow(prob_long) > 0) {
  readr::write_csv(
    prob_long,
    file.path(example_output_dir, "example_application_probabilities.csv")
  )
}

cat("\nExample application outputs written to:\n")
cat(example_output_dir, "\n")

cat("\nInterpretation note:\n")
cat(
  "In a real application, the true generating distribution is unknown. ",
  "The predicted class and probabilities should be interpreted as model-based ",
  "support for candidate families, not as proof of the true distribution.\n",
  sep = ""
)

cat("\nexample_real_application.R finished successfully.\n")