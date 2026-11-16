# Databricks notebook source
# ============================================================
# Tool:         04 backfill
# Description:  Backfills every gauge not yet done, in chunks, copying the register, log and
#               registry to the volume after each chunk so a stopped job loses at most one chunk
# Flode Module: analytics_pipelines_r
# Author:       analytics_pipelines_r contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The registry on the volume
# Outputs:      Bronze Parquet; the register, the backfill log and an updated registry on the volume
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
todo <- gauges_to_do(registry)
message(nrow(todo), " of ", nrow(registry), " gauges still to do")

for (ids in chunks(todo$gauge_id, cfg$backfill$chunk_size)) {
  chunk_registry <- file.path(cfg$local_dir, "chunk_registry.parquet")
  arrow::write_parquet(todo[gauge_id %in% ids], chunk_registry)
  local_register <- file.path(cfg$local_dir, "register.csv")
  local_log <- file.path(cfg$local_dir, "backfill_log.csv")
  unlink(c(local_register, local_log))
  log <- run_backfill(chunk_registry, cfg$store_root, cfg$backfill$start_date,
                      cfg$backfill$end_date, register_path = local_register,
                      log_path = local_log, n_workers = cfg$backfill$n_workers)
  # Register first: once a gauge is marked done, its files must already be registered
  merge_csv_to_volume(local_register, cfg$volume_register)
  merge_csv_to_volume(local_log, file.path(cfg$volume_logs, "backfill_log.csv"))
  registry <- mark_done(registry, log)
  tmp <- file.path(cfg$local_dir, "gauge_registry.parquet")
  arrow::write_parquet(registry, tmp)
  file.copy(tmp, cfg$registry_path, overwrite = TRUE)
  message(sprintf("Chunk done: %d succeeded, %d empty, %d failed; %d gauges left",
                  sum(log$status == "SUCCESS"), sum(log$status == "EMPTY"),
                  sum(log$status == "FAILED"), nrow(gauges_to_do(registry))))
}
