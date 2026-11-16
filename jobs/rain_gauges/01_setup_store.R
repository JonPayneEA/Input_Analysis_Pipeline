# Databricks notebook source
# ============================================================
# Tool:         01 set up the store
# Description:  Creates the reach.io folder tree inside the volume; safe to rerun
# Flode Module: analytics_pipelines_r
# Author:       analytics_pipelines_r contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       config.yml
# Outputs:      The store's folders under store_root
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

setup_hydro_store(cfg$store_root)
dir.create(cfg$volume_logs, showWarnings = FALSE)
message("Store ready at ", cfg$store_root)
