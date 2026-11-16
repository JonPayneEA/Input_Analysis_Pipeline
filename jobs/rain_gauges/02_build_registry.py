# Databricks notebook source
# ============================================================
# Tool:         02 build the registry
# Description:  Lists every 15-minute rain gauge on the Hydrology API and writes the
#               registry, keeping the progress of gauges already registered
# Flode Module: analytics_pipelines_r
# Author:       analytics_pipelines_r contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The Hydrology API; an optional lookup CSV for catchments
# Outputs:      register/gauge_registry.parquet and register/rain_gauges.csv
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

from pipelines import hydrology_api as api
from pipelines import rain_gauges, store
from pipelines.config import load_config

cfg = load_config(_root)

# COMMAND ----------

import pandas as pd

measures = api.rain_measures()
print(len(measures), "15-minute rainfall measures")

lookup = None
if cfg.get("lookup_csv"):
    lookup = pd.read_csv(cfg["lookup_csv"], dtype=str)
reg = rain_gauges.gauge_registry(measures, lookup, cfg.get("lookup_id_col"),
                                 cfg.get("lookup_catchment_col"))
if not len(reg):
    raise RuntimeError("No WISKI IDs found in the measures; the API's station fields may have "
                       f"changed. First measure: {measures[:1]}")
print(len(reg), "gauges;", int(reg["catchment"].notna().sum()), "with a catchment")

# Rerunning keeps what is known about gauges already registered (done flags, notes)
if Path(cfg["registry_path"]).exists():
    old = store.read_registry(cfg["registry_path"])
    reg = pd.concat([old, reg[~reg["gauge_id"].isin(old["gauge_id"])]], ignore_index=True)
store.write_registry(reg, cfg["registry_path"])
store.write_csv(reg, f"{cfg['register_dir']}/rain_gauges.csv")
print("Registry written to", cfg["registry_path"])
