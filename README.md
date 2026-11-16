# analytics_pipelines_r

Databricks jobs, written in R, that run the reach.* packages on a schedule. The first pipeline
downloads the EA's 15-minute rain gauge records from the Hydrology API into a reach.io store
on a Unity Catalog volume.

The packages do the work; this repository only says when, where and in what order. Fetching is
`reach.io`, quality control `reach.validate`, model forcing `reach.forcing`.

## Layout

| Path | What it holds |
|---|---|
| `config/config.example.yml` | The settings: store path, dates, chunk size, workers. Copy it; never commit the copy |
| `R/helpers.R` | Finding the repository, reading the config, copying CSVs to the volume |
| `R/rain_gauges.R` | The gauge list from the API's measures, and which gauges are still to do |
| `jobs/rain_gauges/01_setup_store.R` to `05_incremental.R` | The jobs, in the order they run |
| `tests/testthat/` | Tests of the helpers |

Each job file starts with `# Databricks notebook source`, so a Databricks Git folder opens it
as an R notebook.

## Why the register goes via local disk

Unity Catalog volumes may refuse appends to an existing file, and reach.io appends to its
register and logs. Every job therefore writes those to the driver's local disk and then
rewrites the volume copy whole (`merge_csv_to_volume()`). Bronze Parquet files go straight to
the volume, since reach.io writes each one whole.

## Setting up in Databricks

1. **Clone this repository** as a Git folder in the workspace.
2. **Cluster libraries.** Install reach.io and what it needs: `remotes::install_github(c(
   "JonPayneEA/reach.utils", "JonPayneEA/reach.io"))`, with `arrow`, `data.table`, `S7`,
   `httr`, `future`, `future.apply` and `yaml` from CRAN.
3. **Network.** The cluster must reach `environment.data.gov.uk`.
4. **Config.** Copy `config/config.example.yml` to the volume, for example
   `<volume>/hydrometric_config/config.yml`, fill it in, and set the cluster or job
   environment variable `PIPELINES_CONFIG` to its path. A `config/config.yml` in the Git folder
   works too; it is ignored by git.
5. **Root.** The jobs find the repository by looking upwards from the working directory. If a
   job cannot, set `PIPELINES_ROOT` to the Git folder's path.

Keep restricted data off unrestricted volumes. Open gauge data from the Hydrology API is fine
on an unrestricted one; H19, FEH SAAR and MOSES need a restricted volume of their own.

## Running the rain gauge pipeline

| Order | Job | Run | What it does |
|---|---|---|---|
| 1 | `01_setup_store` | Once | Creates reach.io's folders under `store_root`; safe to rerun |
| 2 | `02_build_registry` | Once, and when gauges change | Lists every 15-minute rain gauge on the API and writes the registry |
| 3 | `03_trial` | Once, before the backfill | Backfills a few gauges for one year, reads one back and estimates the full run's hours |
| 4 | `04_backfill` | As a job until nothing is left | Backfills every gauge not yet done, chunk by chunk |
| 5 | `05_incremental` | Daily, as a scheduled job | Fetches each gauge's new data since its latest timestamp |

**Read the trial before the backfill.** It shows whether the volume accepts the writes and how
long a gauge-year takes. The EA asks for one request at a time, so the backfill is slow: about
22,000 annual requests for 900 gauges over 25 years.

**Stopping and resuming.** After each chunk of `chunk_size` gauges, `04_backfill` copies the
register and log to the volume and marks the chunk's gauges done in the registry. A stopped
job loses at most one chunk; rerun it and it carries on. Failed gauges are not marked done, so
the next run tries them again.

**From files to tables.** The Bronze store is Parquet in folders. To query it:

```sql
CREATE TABLE <catalog>.<schema>.bronze_rain AS
SELECT * FROM parquet.`<store_root>/bronze/hydrometric/EA/P/*/*.parquet`;
```

Bronze is append-only, so a timestamp can appear in more than one file; when building Silver,
keep the most recently received row per gauge and timestamp, as `reach.io::read_bronze()` does.

## Testing

```r
testthat::test_dir("tests/testthat")
```

The tests cover the helpers. The jobs were dry-run end to end against stubbed reach.io calls,
which checks their order and their copies to the volume but not the API or a real volume; the
trial job is the first real check.
