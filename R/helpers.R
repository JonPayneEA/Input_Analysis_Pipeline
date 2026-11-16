# ============================================================
# Tool:         pipeline helpers
# Description:  Shared helpers for the Databricks jobs: find the repository, read the
#               configuration, and move CSV logs between local disk and a volume
# Flode Module: analytics_pipelines_r
# Author:       analytics_pipelines_r contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       config/config.yml; CSV files on local disk and on a Unity Catalog volume
# Outputs:      A configuration list; CSV files rewritten whole on the volume
# Dependencies: yaml, data.table
# ============================================================

# Unity Catalog volumes may refuse appends to an existing file, and reach.io appends to its
# register and logs. The jobs therefore write those files to the driver's local disk and copy
# them to the volume with merge_csv_to_volume(), which rewrites the volume copy whole.

# The repository root: PIPELINES_ROOT if set, else the first folder at or above the working
# directory that holds R/helpers.R.
pipeline_root <- function(start = getwd()) {
  env <- Sys.getenv("PIPELINES_ROOT")
  if (nzchar(env)) return(normalizePath(env, mustWork = TRUE))
  dir <- normalizePath(start, mustWork = TRUE)
  repeat {
    if (file.exists(file.path(dir, "R", "helpers.R"))) return(dir)
    parent <- dirname(dir)
    if (identical(parent, dir)) {
      stop("Cannot find the repository root; set PIPELINES_ROOT", call. = FALSE)
    }
    dir <- parent
  }
}

# The configuration, with the store's derived paths added: the file PIPELINES_CONFIG names (for
# example a config.yml kept on the volume), else config/config.yml. Neither is committed; copy
# config/config.example.yml and fill it in.
load_config <- function(root = pipeline_root(), file = Sys.getenv("PIPELINES_CONFIG")) {
  if (!nzchar(file)) file <- file.path(root, "config", "config.yml")
  if (!file.exists(file)) {
    stop("No ", file, ": copy config/config.example.yml to it and fill it in", call. = FALSE)
  }
  cfg <- yaml::read_yaml(file)
  need <- c("store_root", "local_dir", "backfill", "trial", "incremental")
  missing <- setdiff(need, names(cfg))
  if (length(missing)) stop("config.yml lacks: ", paste(missing, collapse = ", "), call. = FALSE)
  cfg$register_dir <- file.path(cfg$store_root, "register")
  cfg$registry_path <- file.path(cfg$register_dir, "gauge_registry.parquet")
  cfg$volume_register <- file.path(cfg$register_dir, "hydrometric_data_register.csv")
  cfg$volume_logs <- file.path(cfg$store_root, "logs")
  dir.create(cfg$local_dir, recursive = TRUE, showWarnings = FALSE)
  cfg
}

# Adds the rows of a local CSV to its copy on the volume, rewriting the volume file whole.
# Rows already there are not repeated. Returns the number of rows added.
merge_csv_to_volume <- function(local, volume) {
  if (!file.exists(local)) return(invisible(0L))
  new <- data.table::fread(local, colClasses = "character")
  if (!nrow(new)) return(invisible(0L))
  old <- if (file.exists(volume)) data.table::fread(volume, colClasses = "character") else NULL
  all <- unique(data.table::rbindlist(list(old, new), use.names = TRUE, fill = TRUE))
  added <- nrow(all) - if (is.null(old)) 0L else nrow(unique(old))
  dir.create(dirname(volume), recursive = TRUE, showWarnings = FALSE)
  tmp <- tempfile(fileext = ".csv")
  data.table::fwrite(all, tmp)
  if (!file.copy(tmp, volume, overwrite = TRUE)) stop("Could not write ", volume, call. = FALSE)
  unlink(tmp)
  invisible(as.integer(added))
}

# Splits a vector into consecutive chunks of at most `size`.
chunks <- function(x, size) {
  if (!length(x)) return(list())
  split(x, ceiling(seq_along(x) / size))
}
