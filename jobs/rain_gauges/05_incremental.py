# Databricks notebook source
# ============================================================
# Tool:         05 incremental
# Description:  Fetches each gauge's new readings since its latest stored timestamp; run daily
# Flode Module: analytics_pipelines_r
# Author:       analytics_pipelines_r contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The registry and the Bronze store
# Outputs:      New Bronze Parquet; the register and logs/incremental_log.csv
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

from pipelines import rain_gauges
from pipelines.config import load_config

cfg = load_config(_root)

# COMMAND ----------

import pandas as pd

log = rain_gauges.run_incremental(cfg, received_by=cfg.get("received_by"))
print(pd.Series([r["status"] for r in log]).value_counts())
