# Databricks notebook source
# ============================================================
# Tool:         04 backfill
# Description:  Backfills every gauge not yet done, in chunks; rerun until nothing is left
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The registry
# Outputs:      Bronze Parquet; the register, logs/backfill_log.csv and the registry's done flags
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
# Forget any copy of the pipelines code loaded before a git pull, so the new code is used
for _name in [m for m in sys.modules if m == "pipelines" or m.startswith("pipelines.")]:
    del sys.modules[_name]

from pipelines import rain_gauges
from pipelines.config import load_config

cfg = load_config(_root)

# COMMAND ----------

log = rain_gauges.run_backfill(cfg, cfg["backfill"]["start_date"], cfg["backfill"]["end_date"],
                               received_by=cfg.get("received_by"))
print(len(log), "gauges attempted this run")
