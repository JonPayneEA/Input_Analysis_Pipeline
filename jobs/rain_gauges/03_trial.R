# Databricks notebook source
# ============================================================
# Tool:         03 trial
# Description:  Backfills a few gauges for one year, to prove the volume accepts the writes and to
#               time a gauge-year
# Flode Module: analytics_pipelines_r
# Author:       analytics_pipelines_r contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The registry on the volume
# Outputs:      Bronze Parquet for the trial gauges; the trial log on the volume
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

registry <- data.table::as.data.table(arrow::read_parquet(cfg$registry_path))
trial <- head(registry[active == TRUE], cfg$trial$n_gauges)
trial_registry <- file.path(cfg$local_dir, "trial_registry.parquet")
arrow::write_parquet(trial, trial_registry)   # the trial never marks the main registry done

local_register <- file.path(cfg$local_dir, "trial_register.csv")
local_log <- file.path(cfg$local_dir, "trial_log.csv")
unlink(c(local_register, local_log))
started <- proc.time()[["elapsed"]]
log <- run_backfill(trial_registry, cfg$store_root, cfg$trial$start_date, cfg$trial$end_date,
                    register_path = local_register, log_path = local_log, n_workers = 1)
elapsed <- proc.time()[["elapsed"]] - started
merge_csv_to_volume(local_register, cfg$volume_register)
merge_csv_to_volume(local_log, file.path(cfg$volume_logs, "trial_log.csv"))
print(log)

# COMMAND ----------

# Read one back, and estimate the full backfill from the time per gauge-year
obj <- read_bronze(cfg$store_root, trial$gauge_id[1], "P",
                   start = cfg$trial$start_date, end = cfg$trial$end_date)
print(obj@readings)
years <- as.numeric(difftime(as.Date(cfg$backfill$end_date), as.Date(cfg$backfill$start_date),
                             units = "days")) / 365.25
years_trial <- as.numeric(difftime(as.Date(cfg$trial$end_date), as.Date(cfg$trial$start_date),
                                   units = "days")) / 365.25
per_gauge_year <- elapsed / nrow(trial) / years_trial
n_active <- nrow(registry[active == TRUE])
message(sprintf("%.1f s per gauge-year: %d gauges over %.0f years needs about %.0f hours",
                per_gauge_year, n_active, years, per_gauge_year * n_active * years / 3600))
