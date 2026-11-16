# Databricks notebook source
# ============================================================
# Tool:         05 incremental
# Description:  Fetches each gauge's new data since its latest stored timestamp; run daily
# Flode Module: analytics_pipelines_r
# Author:       analytics_pipelines_r contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The registry and Bronze store on the volume
# Outputs:      New Bronze Parquet; the register and the incremental log on the volume
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

local_register <- file.path(cfg$local_dir, "register.csv")
local_log <- file.path(cfg$local_dir, "incremental_log.csv")
unlink(c(local_register, local_log))
log <- run_incremental(cfg$registry_path, cfg$store_root, register_path = local_register,
                       log_path = local_log, n_workers = cfg$incremental$n_workers)
merge_csv_to_volume(local_register, cfg$volume_register)
merge_csv_to_volume(local_log, file.path(cfg$volume_logs, "incremental_log.csv"))
print(table(log$status))
