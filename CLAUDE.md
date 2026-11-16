# analytics_pipelines_r: Claude Code guide

Databricks jobs in R that run the reach.* packages. The packages hold the logic; this
repository holds configuration, job scripts and small helpers only. If a job needs new
behaviour, add it to the package that owns it (fetching `reach.io`, QC `reach.validate`, forcing
`reach.forcing`) and call it from here.

## Conventions

- Every R file starts with the Flode header block (Tool, Description, Flode Module, Author,
  Created, Tier, Inputs, Outputs, Dependencies). Job files put `# Databricks notebook source`
  above it and separate cells with `# COMMAND ----------`.
- No workspace names, catalog paths or credentials in committed files: they go in the config,
  which is not committed. `config/config.example.yml` shows the keys with placeholders.
- Volumes may refuse appends: write anything appended to `local_dir` and copy it with
  `merge_csv_to_volume()`.
- Restricted data (H19, FEH SAAR, MOSES) never on an unrestricted volume, and never committed.
- Lines up to 100 characters. UK English, plain prose: no em dashes, no exclamation marks.

## Testing

`testthat::test_dir("tests/testthat")`, edition 3. Expected values come from hand counts stated
in the test. Every bug fix gets a regression test that fails on the old code.
