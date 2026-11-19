# Databricks notebook source
# ============================================================
# Tool:         02 CAMELS-GB v2 calibration trial
# Description:  The 12-catchment trial before the bulk PDM calibration: whether two seeds give
#               the same calibration, how much the evaporation product matters, and how long
#               the bulk run will take. Runs the trial script that ships with pdmS7
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-19
# Tier:         1
# Inputs:       store_root from the config; gold/camels_gb_v2/ as 01_camels_prepare writes it
# Outputs:      gold/camels_gb_v2/trial/: runs.csv, seed_test.csv, pet_test.csv, runtime.csv
#               and the calibration projects
# Dependencies: R; pdmS7 (>= 0.35.0), arrow and yaml installed on the cluster
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## CAMELS-GB v2: the calibration trial
# MAGIC **An R notebook, for a personal cluster.** Every other job in this repository is Python,
# MAGIC because the shared clusters allow only Python and SQL. This one runs pdmS7, which is R,
# MAGIC so it needs a cluster you own with R enabled. The calibration itself lives in pdmS7
# MAGIC (`inst/scripts/camels_trial.R`), with its tests; this notebook finds the store, checks
# MAGIC the packages, runs the trial and shows its tables.
# MAGIC
# MAGIC Twelve screened catchments, spread through the baseflow index, are calibrated three
# MAGIC times with the staged DDS search: Hydro-PE with two seeds, and CHESS-PE with one.
# MAGIC
# MAGIC - **Seed test**: do two seeds give the same parameters (within 5% of each range) and
# MAGIC   score (within 0.01 NSE)? If not, the bulk run needs more starts or fewer free
# MAGIC   parameters.
# MAGIC - **Evaporation test**: how far the validation score and the parameters move with the
# MAGIC   evaporation product.
# MAGIC - **Runtime**: minutes per catchment, and the hours the bulk run will take.
# MAGIC
# MAGIC Run `01_camels_prepare` first: the trial reads each evaporation product from the hourly
# MAGIC tables. The test windows are not scored; they are kept for the final protocol.

# COMMAND ----------

# Settings. CORES: catchments calibrated at once; leave a few of the cluster's cores free.
CORES <- 12
STARTS <- 4       # random starts per stage
BUDGET <- 300     # model runs per start and stage
RECORDED_AS <- "" # the name the calibration logs record; empty: the cluster user

# COMMAND ----------

# The packages. pdmS7 must be installed, not loaded from source: the trial calibrates in
# parallel worker processes, and each loads the installed package.
need <- c(pdmS7 = "0.35.0", arrow = "0", yaml = "0")
missing <- names(need)[!vapply(names(need), requireNamespace, logical(1), quietly = TRUE)]
if (length(missing)) {
  stop("Install on the cluster (Compute, the cluster, Libraries): ",
       paste(missing, collapse = ", "), call. = FALSE)
}
if (utils::packageVersion("pdmS7") < need[["pdmS7"]]) {
  stop("pdmS7 ", as.character(utils::packageVersion("pdmS7")), " is installed; the trial ",
       "needs 0.35.0 or later, from master", call. = FALSE)
}
script <- system.file("scripts", "camels_trial.R", package = "pdmS7")
cat("pdmS7", as.character(utils::packageVersion("pdmS7")), "| trial script:", script, "\n")

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
config_file <- Sys.getenv("PIPELINES_CONFIG")
if (!nzchar(config_file)) config_file <- file.path(find_root(), "config", "config.yml")
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
cat("Config:", config_file, "\nStore:", cfg$store_root, "\n")

# COMMAND ----------

# The trial: three batches, each about the time of the slowest catchment's calibration.
Sys.setenv(CAMELS_STORE = cfg$store_root, CAMELS_CORES = as.character(CORES),
           CAMELS_STARTS = as.character(STARTS), CAMELS_BUDGET = as.character(BUDGET))
if (nzchar(RECORDED_AS)) Sys.setenv(PDMS7_USER = RECORDED_AS)
if (!nzchar(Sys.getenv("PDMS7_USER"))) {
  Sys.setenv(PDMS7_USER = Sys.getenv("USER", "CAMELS-GB trial"))
}
source(script)

# COMMAND ----------

# The results. An empty table is described, not displayed: display() fails on one.
show <- function(table, empty) {
  if (is.null(table) || !nrow(table)) cat(empty, "\n") else display(table)  # nolint
}
cat("Seed test: the same answer from both seeds?\n")
show(seed_test, "No catchment finished both seeds.")

# COMMAND ----------

cat("Evaporation test: CHESS-PE against Hydro-PE\n")
show(pet_test, "No catchment finished both evaporation products.")

# COMMAND ----------

cat("Runtime, and the hours the bulk run would take\n")
show(runtime, "No catchment finished.")
failed <- runs[runs$state != "done", c("run", "gauge", "state", "message")]
show(failed, "Every calibration finished.")
