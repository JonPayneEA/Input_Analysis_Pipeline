# Input_Analysis_Pipeline

Databricks jobs for the Flood Forecast Modelling team. The first pipeline downloads the EA's
15-minute rain gauge records from the Hydrology API into a Bronze store on a Unity Catalog
volume.

The jobs are written in **Python**, because the workspace's shared clusters allow only
Python and SQL, and to match the team's other notebooks. They write exactly the layout reach.io
writes, so the store reads back in R with `reach.io::read_bronze()`.

## Layout

| Path | What it holds |
|---|---|
| `jobs/rain_gauges/01_setup_store.py` to `05_incremental.py` | The jobs, as Databricks notebooks, in the order they run |
| `pipelines/hydrology_api.py` | The Hydrology API: measures, annual chunks of readings, retries, the one-second pause |
| `pipelines/store.py` | reach.io's Bronze layout: dataset IDs, files by year, the register and the registry |
| `pipelines/rain_gauges.py` | The registry, the chunked backfill and the incremental update |
| `pipelines/config.py` | Reading the configuration |
| `config/config.example.yml` | The settings. Copy it; never commit the copy |
| `tests/python/` | Tests, with a fake API so none touches the network |

## Setting up in Databricks

1. **Clone this repository** as a Git folder in the workspace.
2. **Nothing to install.** pandas, pyarrow, requests and pyyaml come with the Databricks
   runtime.
3. **Network.** The cluster must reach `environment.data.gov.uk`.
4. **Config.** Copy `config/config.example.yml` to the volume, for example
   `<volume>/hydrometric_config/config.yml`, and fill it in. Set the environment variable
   `PIPELINES_CONFIG` to its path, on the cluster or the job, or at the top of a notebook
   with `os.environ["PIPELINES_CONFIG"] = "..."`. A `config/config.yml` in the Git folder works
   too; git ignores it.
5. **Root.** The notebooks find the repository by looking upwards from their folder. If one
   cannot, set `PIPELINES_ROOT` to the Git folder's path.

Keep restricted data off unrestricted volumes. Open gauge data from the Hydrology API is fine
on an unrestricted one; H19, FEH SAAR and MOSES need a restricted volume of their own.

## Running the rain gauge pipeline

| Order | Job | Run | What it does |
|---|---|---|---|
| 1 | `01_setup_store` | Once | Creates the store's folders under `store_root`; safe to rerun |
| 2 | `02_build_registry` | Once, and when gauges change | Lists every 15-minute rain gauge on the API and writes the registry; a rerun adds new gauges and keeps the rest |
| 3 | `03_trial` | Once, before the backfill | Backfills a few gauges for one year, reads one back and estimates the full run's hours |
| 4 | `04_backfill` | As a job until nothing is left | Backfills every gauge not yet done, chunk by chunk |
| 5 | `05_incremental` | Monthly, as a scheduled job, a few days after the EA's monthly release | Fetches each gauge's new readings, and any the EA has revised in the look-back period |

**Read the trial before the backfill.** It shows whether the volume accepts the writes and how
long a gauge-year takes. The EA asks for one request at a time, so the jobs make one request a
second.

**Each gauge's own record.** The backfill first asks the API for a gauge's first and last
readings, then fetches only the years between, clipped to the config's `start_date` and
`end_date`. A `start_date` of 1900 therefore costs nothing for a gauge that opened in 2010. In
a sample of 40 gauges in November 2026, records began between 1966 and 2020 (60% before 2000)
and averaged about 30 years: some 30,000 annual requests for 995 gauges, about 24 hours.

**Stopping and resuming.** After each chunk of `chunk_size` gauges, the backfill saves the
register, the log and the registry's done flags. A stopped job loses at most one chunk; rerun
it and it carries on. A gauge is done when it succeeded or the API holds nothing for it; a
failed gauge stays to do, so the next run tries it again.

**Monthly updates and revisions.** The EA publishes its checked 15-minute rain monthly, so
`05_incremental` runs monthly. Each run re-fetches from `lookback_days` (default 90) before a
gauge's latest stored reading, compares with what is stored, and writes only readings that are
new or that the EA has changed since: a value or quality flag revised after publication. The
log's `revised` column counts those. Unchanged readings are not written again. About 2 s a
gauge, some 35 minutes a month for 995 gauges.

**What is stored.** One Parquet file per gauge per observation year, at
`bronze/hydrometric/EA/P/<year>/EA_<gauge>_P_<received date>.parquet`, with the columns
`timestamp` (UTC), `value` (mm in the 15 minutes), `supplier_flag` (the EA's quality),
`dataset_id`, `site_id`, `data_type` and `period`. Every file written gets a row in
`register/hydrometric_data_register.csv`. Bronze is append-only: a later download of the same
gauge adds files with a later received date, and readers keep the most recently received row
per timestamp.

**Volumes and appends.** Volumes may refuse appends and in-place rewrites, so every file is
written to local disk first and copied into place whole.

**From files to tables.**

```sql
CREATE TABLE <catalog>.<schema>.bronze_rain AS
SELECT * FROM parquet.`<store_root>/bronze/hydrometric/EA/P/*/*.parquet`;
```

When building Silver, keep the most recently received row per gauge and timestamp, as the
readers do.

## Testing

```bash
python -m pytest
ruff check .
```

The tests use a fake API and a temporary folder for the volume. The five notebooks were also
dry-run in order against the fake API, including a failed gauge retried on a second run. Not
yet tested: the real API, a real volume, and reading the Python-written files back with
reach.io in R. The trial job is the first real check.
