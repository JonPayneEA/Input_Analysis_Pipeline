# Databricks notebook source
# ============================================================
# Tool:         01 CAMELS-GB v2 preparation
# Description:  Turns CAMELS-GB v2 into PDM input: one hourly table per catchment, the
#               descriptors, the flow signatures kept apart, the hydrometry and a screening
#               table of which catchments suit the PDM
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-18
# Tier:         1
# Inputs:       The CAMELS-GB v2 folder named by camels.root in the config
# Outputs:      gold/camels_gb_v2/hourly/<gauge>.parquet; descriptors.parquet,
#               signatures.parquet, hydrometry.parquet and screening.csv beside them
# Dependencies: pandas, pyarrow (in the Databricks runtime)
# ============================================================

# COMMAND ----------

# MAGIC %md
# MAGIC ## CAMELS-GB v2 for the bulk calibration
# MAGIC CAMELS-GB v2 (Coxon et al., 2026; Open Government Licence v3: cite it in anything
# MAGIC built on it) gives hourly rain and flow for 671 catchments from October 1990 to October
# MAGIC 2022. Evaporation and temperature are daily only, so each day's evaporation is spread
# MAGIC over its daylight hours (a half sine from sunrise to sunset at the gauge's latitude,
# MAGIC the day's total kept exactly), and each hour takes its day's temperature.
# MAGIC
# MAGIC The attributes are split three ways. **Descriptors** can be known for a catchment with
# MAGIC no gauge, so the model of parameters may use them. **Signatures** (mean flow, baseflow
# MAGIC index, runoff ratio) come from the gauged flow; a model trained on them would cheat,
# MAGIC so they are kept apart. **Hydrometry** says how far to trust each record.
# MAGIC
# MAGIC Set `camels.root` in the config to the CAMELS-GB v2 folder (the one holding
# MAGIC `Catchment_Attributes` and `Catchment_Timeseries`). Every setting under `camels` is
# MAGIC optional; see `config/config.example.yml`.

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

from pipelines import camels  # noqa: E402
from pipelines.config import load_config  # noqa: E402

cfg = load_config(_root)
st = camels.settings(cfg)
print("Evaporation:", st["pet"], "| temperature:", st["temperature"])

# COMMAND ----------

# One catchment first: its files, its first day, and the evaporation check. The day's
# evaporation in the hourly table must equal the daily file's.
if "cfg" not in globals():
    raise RuntimeError("Python has restarted since the set-up cells ran: press Run all")
files = camels.find_files(cfg["camels"]["root"])
print(len(files), "catchments with time series")
first = sorted(files)[0]
desc = camels.attributes(cfg["camels"]["root"])["descriptors"].set_index("gauge_id")
t = camels.hourly_table(files[first]["hourly"], files[first]["daily"],
                        float(desc.at[first, "gauge_lat"]), st)
print(first, desc.at[first, "gauge_name"], len(t), "hours from", t["time"].iloc[0])
display(t.head(30))  # noqa: F821 - Databricks notebooks provide display()

# COMMAND ----------

# Every catchment. The first run reads all 671 pairs of files (allow some minutes); later
# runs redo only catchments whose source files have changed.
screen = camels.run(cfg)
print(screen["use"].sum(), "of", len(screen), "catchments suit a PDM calibration")
print(screen.loc[~screen["use"], "reasons"].str.split("; ").explode().value_counts())

# COMMAND ----------

# The screening table: every catchment, whether it is used, and why not
display(screen)  # noqa: F821
