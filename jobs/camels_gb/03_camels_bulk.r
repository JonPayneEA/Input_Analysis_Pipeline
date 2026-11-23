# Databricks notebook source
# ============================================================
# Tool:         03 CAMELS-GB v2 bulk calibration
# Description:  Every screened catchment through the PDM protocol settled in the trials, as a
#               training set for the descriptor model. Runs camels_bulk.R beside it;
#               resumable, with results reaching the volume as each chunk of catchments ends
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-23
# Tier:         1
# Inputs:       store_root from the config; gold/camels_gb_v2/ as 01_camels_prepare writes it,
#               with the treatment and pack columns in screening.csv
# Outputs:      gold/camels_gb_v2/bulk/: results/<gauge>.csv, projects/<gauge>/, results.csv
# Dependencies: R; pdmS7 (>= 0.37.0), arrow and yaml installed on the cluster
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## CAMELS-GB v2: the bulk calibration
# MAGIC **An R notebook, for a personal cluster**, like `02_camels_trial`. It runs
# MAGIC `camels_bulk.R`, a copy of the script in pdmS7 (`inst/scripts/camels_bulk.R`).
# MAGIC
# MAGIC Each catchment the screening keeps goes through the protocol of the trials: the structure
# MAGIC chosen on the record, then stages by DDS with a Box-Cox polish and the largest events,
# MAGIC the rainfall factor fixed at 1. The screening's `treatment` and `pack` columns add:
# MAGIC
# MAGIC - **pack**: PACK ahead of the PDM, where 5% or more of precipitation falls as snow;
# MAGIC - **free_fc**: the rainfall factor searched from 0.9 to 1.5, where the rain is likely too
# MAGIC   low (not groundwater-fed, runoff ratio 0.95 or more);
# MAGIC - **abstraction** or **import**: the groundwater extension, losing or gaining water, kept
# MAGIC   only if it beats the protocol on validation by more than 0.02 NSE.
# MAGIC
# MAGIC Each result records the structures that were equally good at each step of the choice,
# MAGIC so the descriptor model can tell a firm choice from a tie.
# MAGIC
# MAGIC **It takes about 30 hours** on 12 cores (39 minutes a catchment in trial 4). Set the
# MAGIC cluster's auto-termination longer than that. **It resumes**: catchments run in chunks of
# MAGIC 12, each chunk's results are copied to the volume when it ends, and a rerun skips what is
# MAGIC there. A stop loses only the chunk in hand. Start with `LIMIT <- 12` for a first look.

# COMMAND ----------

# Settings. CORES: catchments calibrated at once, one chunk.
CORES <- 12
STARTS <- 3       # random starts per search
BUDGET <- 600     # model runs per start, DDS
LIMIT <- 0        # at most this many catchments this run; 0 for all
ONLY <- ""        # e.g. "7002,23006" to run only these gauges
RECORDED_AS <- "" # the name the calibration logs record; empty: the cluster user

# COMMAND ----------

# The packages. pdmS7 must be installed, not loaded from source: the calibrations run in
# parallel worker processes, and each loads the installed package.
need <- c(pdmS7 = "0.37.0", arrow = "0", yaml = "0")
missing <- names(need)[!vapply(names(need), requireNamespace, logical(1), quietly = TRUE)]
if (length(missing)) {
  stop("Install on the cluster (Compute, the cluster, Libraries): ",
       paste(missing, collapse = ", "), call. = FALSE)
}
if (utils::packageVersion("pdmS7") < need[["pdmS7"]]) {
  stop("pdmS7 ", as.character(utils::packageVersion("pdmS7")), " is installed; the bulk run ",
       "needs 0.37.0 or later, from master", call. = FALSE)
}
cat("pdmS7", as.character(utils::packageVersion("pdmS7")), "\n")

# COMMAND ----------

# The config, found as the Python jobs find it: PIPELINES_CONFIG if set, else config/config.yml
# in this Git folder.
find_root <- function() {
  root <- Sys.getenv("PIPELINES_ROOT")
  if (nzchar(root)) return(root)
  dir <- normalizePath(getwd())
  repeat {
    if (file.exists(file.path(dir, "pipelines", "__init__.py"))) return(dir)
    up <- dirname(dir)
    if (up == dir) stop("Cannot find the repository; set PIPELINES_ROOT", call. = FALSE)
    dir <- up
  }
}
root <- find_root()
script <- file.path(root, "jobs", "camels_gb", "camels_bulk.R")
if (!file.exists(script)) stop("No ", script, ": pull main in the Git folder", call. = FALSE)
config_file <- Sys.getenv("PIPELINES_CONFIG")
if (!nzchar(config_file)) config_file <- file.path(root, "config", "config.yml")
if (!file.exists(config_file)) {
  stop("No ", config_file, ". Set PIPELINES_CONFIG to your config file, with ",
       "Sys.setenv(PIPELINES_CONFIG = '...') at the top of this notebook or on the cluster.",
       call. = FALSE)
}
cfg <- yaml::read_yaml(config_file)
if (is.null(cfg$store_root)) stop("No store_root in ", config_file, call. = FALSE)
camels <- file.path(cfg$store_root, "gold", "camels_gb_v2")
if (!file.exists(file.path(camels, "screening.csv"))) {
  stop("No CAMELS-GB tables under ", camels, ": run 01_camels_prepare first", call. = FALSE)
}
cat("Config:", config_file, "\nStore:", cfg$store_root, "\nBulk script:", script, "\n")

# COMMAND ----------

# The bulk run. Progress prints after each chunk, with the hours to go.
Sys.setenv(CAMELS_STORE = cfg$store_root, CAMELS_CORES = as.character(CORES),
           CAMELS_STARTS = as.character(STARTS), CAMELS_BUDGET = as.character(BUDGET),
           CAMELS_LIMIT = as.character(LIMIT), CAMELS_ONLY = ONLY)
if (nzchar(RECORDED_AS)) Sys.setenv(PDMS7_USER = RECORDED_AS)
if (!nzchar(Sys.getenv("PDMS7_USER"))) {
  Sys.setenv(PDMS7_USER = Sys.getenv("USER", "CAMELS-GB bulk"))
}
source(script)

# COMMAND ----------

# The results. An empty table is described, not displayed: display() fails on one.
show <- function(table, empty) {
  if (is.null(table) || !nrow(table)) cat(empty, "\n") else display(table)  # nolint
}
cat("Every catchment calibrated so far\n")
show(results, "No results yet.")

# COMMAND ----------

failed <- results[results$state != "done", c("gauge", "name", "treatment", "message")]
show(failed, "Every calibration finished.")
