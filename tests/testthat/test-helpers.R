# Expected values are counted by hand from the small tables built in each test.

source(file.path("..", "..", "R", "helpers.R"))
source(file.path("..", "..", "R", "rain_gauges.R"))

test_that("merge_csv_to_volume adds new rows once and rewrites the file whole", {
  dir <- withr::local_tempdir()
  local <- file.path(dir, "local.csv")
  volume <- file.path(dir, "vol", "register.csv")
  data.table::fwrite(data.frame(id = c("a", "b"), n = 1:2), local)
  expect_equal(merge_csv_to_volume(local, volume), 2L)           # a, b
  data.table::fwrite(data.frame(id = c("b", "c"), n = 2:3), local)
  expect_equal(merge_csv_to_volume(local, volume), 1L)           # b repeats; only c is new
  out <- data.table::fread(volume, colClasses = "character")
  expect_equal(out$id, c("a", "b", "c"))
  expect_equal(merge_csv_to_volume(file.path(dir, "none.csv"), volume), 0L)
})

test_that("merge_csv_to_volume keeps columns that only one side has", {
  dir <- withr::local_tempdir()
  local <- file.path(dir, "local.csv")
  volume <- file.path(dir, "register.csv")
  data.table::fwrite(data.frame(id = "a"), volume)
  data.table::fwrite(data.frame(id = "b", extra = "x"), local)
  merge_csv_to_volume(local, volume)
  out <- data.table::fread(volume, colClasses = "character")
  expect_equal(names(out), c("id", "extra"))
  expect_equal(out$extra, c("", "x"))                             # fwrite writes NA as empty
})

test_that("chunks splits in order with a short last chunk", {
  expect_equal(unname(chunks(1:5, 2)), list(1:2, 3:4, 5L))
  expect_equal(chunks(character(0), 2), list())
})

test_that("rain_gauge_list keeps one row per WISKI ID and drops blanks", {
  measures <- data.frame(station.wiskiID = c("B1", "A1", "B1", NA, " "))
  out <- rain_gauge_list(measures)
  expect_equal(out$gauge_id, c("A1", "B1"))
  expect_equal(out$source_system, c("HDE", "HDE"))
  expect_equal(out$data_type, c("rainfall", "rainfall"))
  expect_equal(out$ea_site_ref, out$gauge_id)
  expect_true(all(is.na(out$catchment)))
})

test_that("a lookup fills the catchments it names", {
  measures <- data.frame(station.wiskiID = c("A1", "B1"))
  lookup <- data.frame(WISKI = c("B1 ", "Z9"), Catchment = c("Avon", "Exe"))
  out <- rain_gauge_list(measures, lookup, "WISKI", "Catchment")
  expect_equal(out$catchment, c(NA, "Avon"))
  expect_error(rain_gauge_list(measures, lookup, "id", "Catchment"), "lookup lacks")
})

test_that("gauges_to_do and mark_done track the backfill", {
  registry <- data.frame(gauge_id = c("A", "B", "C", "D"), active = c(TRUE, TRUE, TRUE, FALSE),
                         backfill_done = c(FALSE, TRUE, FALSE, FALSE))
  expect_equal(gauges_to_do(registry)$gauge_id, c("A", "C"))     # B done, D inactive
  log <- data.frame(gauge_id = c("A", "C"), status = c("SUCCESS", "FAILED"))
  registry <- mark_done(registry, log)
  expect_equal(registry$backfill_done, c(TRUE, TRUE, FALSE, FALSE))
  expect_equal(gauges_to_do(registry)$gauge_id, "C")             # the failure stays to do
  fresh <- data.frame(gauge_id = "E", active = TRUE)
  expect_equal(gauges_to_do(fresh)$gauge_id, "E")                # no backfill_done column yet
})

test_that("load_config refuses a missing file and adds the derived paths", {
  dir <- withr::local_tempdir()
  withr::local_envvar(PIPELINES_CONFIG = "")
  expect_error(load_config(dir), "copy config/config.example.yml")
  dir.create(file.path(dir, "config"))
  yaml::write_yaml(list(store_root = "/v/store", local_dir = file.path(dir, "local"),
                        backfill = list(), trial = list(), incremental = list()),
                   file.path(dir, "config", "config.yml"))
  cfg <- load_config(dir)
  expect_equal(cfg$registry_path, "/v/store/register/gauge_registry.parquet")
  expect_equal(cfg$volume_logs, "/v/store/logs")
  expect_true(dir.exists(file.path(dir, "local")))
  elsewhere <- file.path(dir, "on_volume.yml")
  file.rename(file.path(dir, "config", "config.yml"), elsewhere)
  withr::local_envvar(PIPELINES_CONFIG = elsewhere)
  expect_equal(load_config(dir)$store_root, "/v/store")
})
