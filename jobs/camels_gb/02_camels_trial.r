# Databricks notebook source
# ============================================================
# Tool:         02 CAMELS-GB v2 calibration trial
# Description:  The 12-catchment trial before the bulk PDM calibration: the structure each
#               catchment chooses, whether two seeds give the same calibration, and how long
#               the bulk run will take. Runs camels_trial.R beside it
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-19
# Tier:         1
# Inputs:       store_root from the config; gold/camels_gb_v2/ as 01_camels_prepare writes it
# Outputs:      gold/camels_gb_v2/trial/: runs.csv, seed_test.csv, structures.csv,
#               groundwater.csv, snow.csv, runtime.csv and the calibration projects
# Dependencies: R; pdmS7 (>= 0.37.0), arrow and yaml installed on the cluster
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## CAMELS-GB v2: the calibration trial
# MAGIC **An R notebook, for a personal cluster.** Every other job in this repository is Python,
# MAGIC because the shared clusters allow only Python and SQL. This one runs pdmS7, which is R,
# MAGIC so it needs a cluster you own with R enabled. The trial itself is `camels_trial.R`
# MAGIC beside this notebook, a copy of the script in pdmS7 (`inst/scripts/camels_trial.R`); this
# MAGIC notebook finds the store, checks the packages, runs the trial and shows its tables.
# MAGIC
# MAGIC Twelve screened catchments, spread through the baseflow index, each run twice through
# MAGIC the protocol the bulk run will use, with seeds 1 and 2: the structure chosen on the
# MAGIC record (recharge, soil store, groundwater store), then stages by DDS: volume and
# MAGIC baseflow on 6-hour steps, fast response, a Box-Cox polish, and the largest events. The
# MAGIC rainfall factor is fixed at 1 and the evaporation is Hydro-PE; rain bias shows in each
# MAGIC catchment's water balance (`loss_over_pet`).
# MAGIC
# MAGIC - **Structures**: what each catchment chose, with its validation NSE and runoff ratio. A
# MAGIC   runoff ratio far from the others, or above 1, points to the data, not the model.
# MAGIC - **Seed test**: does the protocol choose the same structure twice
# MAGIC   (`same_structure`), and give the same parameters (within 5% of each range) and score
# MAGIC   (within 0.01 NSE)? The two surface stores are compared as a sorted pair, since swapping
# MAGIC   them gives the same flow. `apart` lists the parameters left apart.
# MAGIC - **Snow arm**: catchments with at least 5% of precipitation as snow, run again with PACK
# MAGIC   turning precipitation and the day's temperature into rain and melt.
# MAGIC - **Groundwater arm**: up to six aquifer-dominated catchments (those screened out for
# MAGIC   aquifers, groundwater losses or water from outside the catchment): the chosen
# MAGIC   structure; the nearest one that takes the extension, where the chosen one does not;
# MAGIC   that with underflow; with constant abstraction; and, where the runoff ratio is above
# MAGIC   0.9, with springs bringing water in. Two seeds each. The extension earns its place
# MAGIC   where it scores clearly higher and its seeds agree.
# MAGIC - **Runtime**: minutes per catchment, and the hours the bulk run will take.
# MAGIC
# MAGIC Run `01_camels_prepare` first. The test windows are not scored; they are kept for the
# MAGIC final protocol.

# COMMAND ----------

# Settings. CORES: catchments calibrated at once; leave a few of the cluster's cores free.
CORES <- 12
STARTS <- 3       # random starts per search
BUDGET <- 600     # model runs per start, DDS
GW_N <- 6         # catchments in the groundwater arm; 0 to skip it
RECORDED_AS <- "" # the name the calibration logs record; empty: the cluster user

# COMMAND ----------

# The packages. pdmS7 must be installed, not loaded from source: the trial calibrates in
# parallel worker processes, and each loads the installed package.
need <- c(pdmS7 = "0.37.0", arrow = "0", yaml = "0")
missing <- names(need)[!vapply(names(need), requireNamespace, logical(1), quietly = TRUE)]
if (length(missing)) {
  stop("Install on the cluster (Compute, the cluster, Libraries): ",
       paste(missing, collapse = ", "), call. = FALSE)
}
if (utils::packageVersion("pdmS7") < need[["pdmS7"]]) {
  stop("pdmS7 ", as.character(utils::packageVersion("pdmS7")), " is installed; the trial ",
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
script <- file.path(root, "jobs", "camels_gb", "camels_trial.R")
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
cat("Config:", config_file, "\nStore:", cfg$store_root, "\nTrial script:", script, "\n")

# COMMAND ----------

# The trial, in two rounds: first every structure choice (main trial and groundwater arm),
# then the second seeds and the groundwater variants. Each round takes about as long as its
# slowest calibration; the structure choice makes the first the longer.
Sys.setenv(CAMELS_STORE = cfg$store_root, CAMELS_CORES = as.character(CORES),
           CAMELS_STARTS = as.character(STARTS), CAMELS_BUDGET = as.character(BUDGET),
           CAMELS_GW_N = as.character(GW_N))
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
cat("Structures chosen, with each catchment's validation NSE and runoff ratio\n")
show(structures, "No catchment finished.")

# COMMAND ----------

cat("Seed test: the same answer from both seeds?\n")
show(seed_test, "No catchment finished both seeds.")

# COMMAND ----------

cat("Groundwater arm: chosen, compatible, underflow, abstraction and import\n")
show(if (exists("groundwater")) groundwater, "No groundwater arm was run.")

# COMMAND ----------

cat("Snow arm: the main protocol against the same with PACK\n")
show(if (exists("snow")) snow, "No catchment reached the snow share for the snow arm.")

# COMMAND ----------

cat("Runtime, and the hours the bulk run would take\n")
show(runtime, "No catchment finished.")
failed <- runs[runs$state != "done", c("arm", "seed", "gauge", "state", "message")]
show(failed, "Every calibration finished.")
