# Databricks notebook source
# ============================================================
# Tool:         02 build the registry
# Description:  Lists every 15-minute rain gauge on the Hydrology API and writes the reach.io
#               gauge registry
# Flode Module: analytics_pipelines_r
# Author:       analytics_pipelines_r contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The Hydrology API; optional lookup CSV
# Outputs:      register/rain_gauges.csv and register/gauge_registry.parquet on the volume
# Dependencies: reach.io, data.table, arrow, yaml
# ============================================================

# COMMAND ----------

root <- local({
  d <- getwd()
  while (!file.exists(file.path(d, "R", "helpers.R")) && dirname(d) != d) d <- dirname(d)
  if (nzchar(Sys.getenv("PIPELINES_ROOT"))) Sys.getenv("PIPELINES_ROOT") else d
})
source(file.path(root, "R", "helpers.R"))
source(file.path(root, "R", "rain_gauges.R"))
library(reach.io)
cfg <- load_config(root)

# COMMAND ----------

measures <- get_measures("rainfall", period_name = "15min")
message(nrow(measures), " 15-minute rainfall measures")

lookup <- NULL
if (!is.null(cfg$lookup_csv) && nzchar(cfg$lookup_csv)) {
  lookup <- data.table::fread(cfg$lookup_csv, colClasses = "character")
}
gauges <- rain_gauge_list(measures, lookup, cfg$lookup_id_col, cfg$lookup_catchment_col)
message(nrow(gauges), " gauges; ", sum(!is.na(gauges$catchment)), " with a catchment")

# Build on local disk, then copy: the registry is a single file rewritten whole
csv <- file.path(cfg$local_dir, "rain_gauges.csv")
data.table::fwrite(gauges, csv)
build_gauge_registry(csv, cfg$local_dir)
file.copy(csv, file.path(cfg$register_dir, "rain_gauges.csv"), overwrite = TRUE)
file.copy(file.path(cfg$local_dir, "gauge_registry.parquet"), cfg$registry_path,
          overwrite = TRUE)
message("Registry written to ", cfg$registry_path)
