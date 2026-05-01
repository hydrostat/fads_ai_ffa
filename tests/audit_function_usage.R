# =========================================================
# audit_function_usage.R
# Static function-usage audit for FADS_AI
# =========================================================
#
# Run from the repository root:
#
#   source("tests/audit_function_usage.R")
#
# This script scans functions defined in R/ and searches for
# calls to those functions in:
#
#   R/
#   scripts/
#   paper_outputs/
#   examples/
#   tests/
#
# Static analysis is approximate, but useful for identifying
# functions that may be unused or only used internally.
# =========================================================

setwd("C:/Users/wilso/OneDrive/My papers/SEAF_AI/github_zenodo/fads_ai_ffa")

# ---------------------------------------------------------
# Repository root check
# ---------------------------------------------------------

root_dir <- normalizePath(getwd(), winslash = "/", mustWork = TRUE)

required_items <- c("R", "scripts", "tests")

missing_items <- required_items[
  !file.exists(file.path(root_dir, required_items))
]

if (length(missing_items) > 0) {
  stop(
    "Run this script from the FADS_AI repository root. Missing: ",
    paste(missing_items, collapse = ", ")
  )
}


# ---------------------------------------------------------
# Utility functions
# ---------------------------------------------------------

read_text_lines <- function(path) {
  readLines(path, warn = FALSE, encoding = "UTF-8")
}

to_rel_path <- function(path) {
  path <- normalizePath(path, winslash = "/", mustWork = FALSE)
  sub(paste0("^", gsub("\\\\", "/", root_dir), "/?"), "", path)
}

regex_escape <- function(x) {
  gsub("([][{}()+*^$|\\\\?.])", "\\\\\\1", x)
}

strip_line_comments <- function(lines) {
  sub("#.*$", "", lines)
}


# ---------------------------------------------------------
# Files to scan
# ---------------------------------------------------------

definition_files <- list.files(
  file.path(root_dir, "R"),
  pattern = "\\.R$",
  recursive = FALSE,
  full.names = TRUE
)

scan_dirs <- c(
  file.path(root_dir, "R"),
  file.path(root_dir, "scripts"),
  file.path(root_dir, "paper_outputs"),
  file.path(root_dir, "examples"),
  file.path(root_dir, "tests")
)

scan_dirs <- scan_dirs[dir.exists(scan_dirs)]

scan_files <- sort(unique(unlist(
  lapply(
    scan_dirs,
    function(d) list.files(d, pattern = "\\.R$", recursive = TRUE, full.names = TRUE)
  ),
  use.names = FALSE
)))

if (length(definition_files) == 0) {
  stop("No R function files found in R/.")
}

if (length(scan_files) == 0) {
  stop("No R files found to scan.")
}


# ---------------------------------------------------------
# Extract function definitions from R/
# ---------------------------------------------------------

extract_defs_from_file <- function(path) {
  
  lines <- read_text_lines(path)
  
  # Do not remove comments before searching definitions because
  # function definitions should not be inside comments in public code.
  txt <- paste(lines, collapse = "\n")
  
  pattern <- "([A-Za-z][A-Za-z0-9_\\.]*)\\s*(<-|=)\\s*function\\s*\\("
  
  m <- gregexpr(pattern, txt, perl = TRUE)
  hits <- regmatches(txt, m)[[1]]
  
  if (length(hits) == 0 || identical(hits, character(0))) {
    return(data.frame())
  }
  
  fn_names <- sub("\\s*(<-|=)\\s*function\\s*\\($", "", hits, perl = TRUE)
  
  data.frame(
    function_name = fn_names,
    defined_in = to_rel_path(path),
    stringsAsFactors = FALSE
  )
}

defs <- do.call(
  rbind,
  lapply(definition_files, extract_defs_from_file)
)

if (is.null(defs) || nrow(defs) == 0) {
  stop("No function definitions found in R/.")
}

defs <- unique(defs)
defs <- defs[order(defs$function_name, defs$defined_in), ]

duplicate_names <- unique(defs$function_name[duplicated(defs$function_name)])

if (length(duplicate_names) > 0) {
  warning(
    "Duplicate function names detected in R/: ",
    paste(duplicate_names, collapse = ", ")
  )
}


# ---------------------------------------------------------
# Count calls to one function in one file
# ---------------------------------------------------------

count_calls_in_file <- function(function_name, path) {
  
  lines <- read_text_lines(path)
  lines <- strip_line_comments(lines)
  
  fn <- regex_escape(function_name)
  
  call_pattern <- paste0("\\b", fn, "\\s*\\(")
  def_pattern  <- paste0("\\b", fn, "\\s*(<-|=)\\s*function\\s*\\(")
  
  n_calls <- 0L
  
  for (ln in lines) {
    
    # Remove the function definition itself.
    if (grepl(def_pattern, ln, perl = TRUE)) {
      ln <- gsub(def_pattern, "", ln, perl = TRUE)
    }
    
    m <- gregexpr(call_pattern, ln, perl = TRUE)
    hits <- regmatches(ln, m)[[1]]
    
    if (!(length(hits) == 0 || identical(hits, character(0)) || hits[1] == "-1")) {
      n_calls <- n_calls + length(hits)
    }
  }
  
  n_calls
}


# ---------------------------------------------------------
# Build long usage table
# ---------------------------------------------------------

usage_rows <- list()
counter <- 0L

for (i in seq_len(nrow(defs))) {
  
  fn <- defs$function_name[[i]]
  def_file <- defs$defined_in[[i]]
  
  for (path in scan_files) {
    
    n_calls <- count_calls_in_file(fn, path)
    
    if (n_calls > 0) {
      counter <- counter + 1L
      
      usage_rows[[counter]] <- data.frame(
        function_name = fn,
        defined_in = def_file,
        used_in = to_rel_path(path),
        n_calls = n_calls,
        stringsAsFactors = FALSE
      )
    }
  }
}

usage_long <- if (length(usage_rows) == 0) {
  data.frame(
    function_name = character(),
    defined_in = character(),
    used_in = character(),
    n_calls = integer(),
    stringsAsFactors = FALSE
  )
} else {
  do.call(rbind, usage_rows)
}


# ---------------------------------------------------------
# Summarise function usage
# ---------------------------------------------------------

if (nrow(usage_long) == 0) {
  
  usage_counts <- data.frame(
    function_name = defs$function_name,
    n_calls = 0L,
    stringsAsFactors = FALSE
  )
  
} else {
  
  usage_counts <- aggregate(
    n_calls ~ function_name,
    data = usage_long,
    FUN = sum
  )
}

usage_summary <- merge(
  defs,
  usage_counts,
  by = "function_name",
  all.x = TRUE,
  sort = FALSE
)

usage_summary$n_calls[is.na(usage_summary$n_calls)] <- 0L


# ---------------------------------------------------------
# File-use summaries
# ---------------------------------------------------------

get_used_files <- function(fn) {
  if (nrow(usage_long) == 0) {
    return(character(0))
  }
  
  unique(usage_long$used_in[usage_long$function_name == fn])
}

is_R_file <- function(path) {
  grepl("^R/", path)
}

is_script_or_output_file <- function(path) {
  grepl("^(scripts|paper_outputs|examples|tests)/", path)
}

usage_summary$used_in_files <- vapply(
  usage_summary$function_name,
  function(fn) {
    files <- get_used_files(fn)
    
    if (length(files) == 0) {
      return("")
    }
    
    paste(files, collapse = " | ")
  },
  character(1)
)

usage_summary$used_in_R_only <- vapply(
  usage_summary$function_name,
  function(fn) {
    files <- get_used_files(fn)
    
    if (length(files) == 0) {
      return(FALSE)
    }
    
    all(is_R_file(files))
  },
  logical(1)
)

usage_summary$used_in_scripts_or_tests <- vapply(
  usage_summary$function_name,
  function(fn) {
    files <- get_used_files(fn)
    
    if (length(files) == 0) {
      return(FALSE)
    }
    
    any(is_script_or_output_file(files))
  },
  logical(1)
)

usage_summary$status <- ifelse(
  usage_summary$n_calls == 0,
  "apparently_unused",
  ifelse(
    usage_summary$used_in_scripts_or_tests,
    "used_by_script_or_test",
    "used_only_by_other_R_functions"
  )
)

usage_summary <- usage_summary[
  order(usage_summary$status, usage_summary$function_name),
]


# ---------------------------------------------------------
# Write outputs
# ---------------------------------------------------------

out_dir <- file.path(root_dir, "tests", "function_usage_audit")
dir.create(out_dir, recursive = TRUE, showWarnings = FALSE)

write.csv(
  defs,
  file.path(out_dir, "function_definitions.csv"),
  row.names = FALSE
)

write.csv(
  usage_long,
  file.path(out_dir, "function_usage_long.csv"),
  row.names = FALSE
)

write.csv(
  usage_summary,
  file.path(out_dir, "function_usage_summary.csv"),
  row.names = FALSE
)

unused <- usage_summary[
  usage_summary$status == "apparently_unused",
]

write.csv(
  unused,
  file.path(out_dir, "apparently_unused_functions.csv"),
  row.names = FALSE
)


# ---------------------------------------------------------
# Console summary
# ---------------------------------------------------------

cat("\nFunction-usage audit completed.\n")
cat("Repository root:", root_dir, "\n")
cat("R files scanned:", length(scan_files), "\n")
cat("Functions defined in R/:", nrow(defs), "\n\n")

cat("Status counts:\n")
print(table(usage_summary$status))

cat("\nApparently unused functions:\n")

if (nrow(unused) == 0) {
  cat("None detected.\n")
} else {
  print(unused[, c("function_name", "defined_in", "n_calls", "status")])
}

cat("\nAudit files written to:\n")
cat(out_dir, "\n")