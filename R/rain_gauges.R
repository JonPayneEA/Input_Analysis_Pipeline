# ============================================================
# Tool:         rain gauge registry
# Description:  Builds the gauge list for reach.io from the Hydrology API's 15-minute rainfall
#               measures, with an optional lookup for catchment names, and picks the gauges a
#               backfill still has to do
# Flode Module: analytics_pipelines_r
# Author:       analytics_pipelines_r contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The measures table from reach.io::get_measures(); an optional lookup table
# Outputs:      A gauge list in the columns reach.io::build_gauge_registry() needs
# Dependencies: data.table
# ============================================================

# One row per WISKI ID with a 15-minute rainfall measure, in reach.io's gauge list columns.
# `lookup`, if given, is a data frame with the WISKI ID in column `id_col` and the catchment in
# `catchment_col`; gauges it does not name keep an empty catchment.
rain_gauge_list <- function(measures, lookup = NULL, id_col = NULL, catchment_col = NULL) {
  ids <- measures[["station.wiskiID"]]
  if (is.null(ids)) stop("measures has no station.wiskiID column", call. = FALSE)
  ids <- trimws(as.character(ids))
  ids <- sort(unique(ids[!is.na(ids) & nzchar(ids)]))
  out <- data.table::data.table(gauge_id = ids, source_system = "HDE", data_type = "rainfall",
                                category = "hydrometric", catchment = NA_character_,
                                ea_site_ref = ids)
  if (!is.null(lookup)) {
    if (!all(c(id_col, catchment_col) %in% names(lookup))) {
      stop("lookup lacks ", id_col, " or ", catchment_col, call. = FALSE)
    }
    key <- trimws(as.character(lookup[[id_col]]))
    hit <- match(out$gauge_id, key)
    out$catchment <- as.character(lookup[[catchment_col]])[hit]
  }
  out
}

# The registry rows a backfill still has to do: active and not yet marked done. Rows are
# returned whole, so the result can be written as a registry of its own.
gauges_to_do <- function(registry) {
  registry <- data.table::as.data.table(registry)
  done <- if ("backfill_done" %in% names(registry)) registry$backfill_done %in% TRUE else FALSE
  registry[registry$active %in% TRUE & !done]
}

# Marks gauges done in the registry from a backfill log: a gauge is done when its status is
# SUCCESS or EMPTY (the source has nothing for it). FAILED gauges stay to do.
mark_done <- function(registry, log) {
  registry <- data.table::copy(data.table::as.data.table(registry))
  if (!"backfill_done" %in% names(registry)) registry$backfill_done <- FALSE
  ok <- log$gauge_id[log$status %in% c("SUCCESS", "EMPTY")]
  registry$backfill_done[registry$gauge_id %in% ok] <- TRUE
  registry
}
