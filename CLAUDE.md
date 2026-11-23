# Input_Analysis_Pipeline: Claude Code guide

Databricks jobs for the Flood Forecast Modelling team. The jobs are Python notebooks, since the
workspace's shared clusters allow only Python and SQL. They write reach.io's Bronze layout
exactly, so R reads the store with reach.io; any change to the layout must match reach.io's
`schema.R` and `read_bronze.R`.

## Conventions

- Every source file starts with the Flode header block (Tool, Description, Flode Module,
  Author, Created, Tier, Inputs, Outputs, Dependencies). Notebooks put
  `# Databricks notebook source` above it and separate cells with `# COMMAND ----------`.
- Notebooks import only from `pipelines/`; logic goes there, with tests.
- One exception, agreed: `jobs/camels_gb/02_camels_trial.r` and `03_camels_bulk.r` are R
  notebooks for a personal cluster, and `camels_trial.R` and `camels_bulk.R` beside them are
  copies of pdmS7's `inst/scripts/` scripts. The calibration code and tests live in pdmS7;
  keep each script's two copies in step, and the notebooks thin wrappers.
- Times in UTC. Depths in mm per 15 minutes, as the EA supplies them.
- The EA asks for one request at a time: no parallel requests to the Hydrology API.
- No workspace names, catalog paths or credentials in committed files: they go in the config,
  which is not committed. `config/config.example.yml` shows the keys with placeholders.
- Files on a volume are written whole, via a local temporary file; never appended.
- Restricted data (H19, FEH SAAR, MOSES) never on an unrestricted volume, and never committed.
- Lines up to 100 characters; `ruff check .` clean. UK English, plain prose: no em dashes, no
  exclamation marks.

## Testing

`python -m pytest` and `ruff check .`. Tests never
touch the network: use the fake API in `tests/python/conftest.py`. Expected values come from
hand counts stated in the test. Every bug fix gets a regression test that fails on the old code.
