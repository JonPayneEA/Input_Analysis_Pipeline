# Databricks notebook source
# ============================================================
# Tool:         03 trial
# Description:  Backfills a few gauges for one year, reads one back and estimates the full run
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The registry
# Outputs:      Bronze Parquet for the trial gauges; logs/trial_log.csv
# Dependencies: pandas, pyarrow, requests, pyyaml (all in the Databricks runtime)
# ============================================================

# COMMAND ----------

import os
import sys
from pathlib import Path

_here = Path(os.getcwd()).resolve()
_root = os.environ.get("PIPELINES_ROOT") or next(
    (str(p) for p in (_here, *_here.parents) if (p / "pipelines" / "__init__.py").exists()),
    None)
if _root is None:
    raise RuntimeError("Cannot find the repository; set PIPELINES_ROOT")
if _root not in sys.path:
    sys.path.insert(0, _root)

from pipelines import rain_gauges, store
from pipelines.config import load_config

cfg = load_config(_root)

# COMMAND ----------

import time
from datetime import date

reg = store.read_registry(cfg["registry_path"])
active = rain_gauges.to_do(reg)
trial = active.head(cfg["trial"]["n_gauges"])
t0 = time.monotonic()
log = rain_gauges.run_backfill(cfg, cfg["trial"]["start_date"], cfg["trial"]["end_date"],
                               registry=trial, log_name="trial_log.csv", mark=False)
elapsed = time.monotonic() - t0
for r in log:
    print(r)

# COMMAND ----------

first = trial["gauge_id"].iloc[0]
print(store.read_series(cfg["store_root"], first).head(10))


def _years(a, b):
    return (date.fromisoformat(b) - date.fromisoformat(a)).days / 365.25


per_gauge_year = elapsed / len(trial) / _years(cfg["trial"]["start_date"],
                                               cfg["trial"]["end_date"])
years = _years(cfg["backfill"]["start_date"], cfg["backfill"]["end_date"])
print(f"{per_gauge_year:.1f} s per gauge-year: {len(active)} gauges over {years:.0f} years "
      f"needs about {per_gauge_year * len(active) * years / 3600:.0f} hours")
