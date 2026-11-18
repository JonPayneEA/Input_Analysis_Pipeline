# Databricks notebook source
# ============================================================
# Tool:         08 rain gauge QC, stage 1
# Description:  Single-gauge quality control over the network (Lewis et al. 2018, 2021): flags
#               readings and days, and reports each gauge's years; run after each update
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-18
# Tier:         1
# Inputs:       The Bronze rain store; the qc section of the config (optional)
# Outputs:      gold/qc/{readings,days,years}/<gauge>.parquet; gold/qc/summary.csv
# Dependencies: pandas, pyarrow (in the Databricks runtime)
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

from pipelines import rain_qc  # noqa: E402
from pipelines.config import load_config  # noqa: E402

cfg = load_config(_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## What is checked
# MAGIC Each gauge's own 15-minute readings, nothing else. Flags: 4 rejected, 3 suspect; any
# MAGIC reading or day without a flag is good. Nothing is removed or changed in the store.
# MAGIC
# MAGIC | Rule | Flag | Raised when |
# MAGIC |---|---|---|
# MAGIC | `off_grid` | 4 | A reading not on the quarter hour |
# MAGIC | `negative` | 4 | A reading below zero |
# MAGIC | `extreme_15min` | 3 or 4 | Above 40 mm in 15 minutes (3), above 100 mm (4) |
# MAGIC | `extreme_day` | 3 or 4 | A 09:00 rain day above 200 mm (3), above 400 mm (4) |
# MAGIC | `repeated` | 3 | One amount over and over: 1 mm or more for 2 hours, any for 12 |
# MAGIC | `accumulation` | 3 | 5 mm or more straight after 24 hours with no readings |
# MAGIC | `ea_suspect` | 3 | The EA flagged the reading Suspect |
# MAGIC
# MAGIC Each year is also reported: the tip resolution (a change means a new instrument or
# MAGIC processing), the largest share of the year's rain in one hour of the day (a clock or
# MAGIC reporting fault concentrates it), and the weekend share (manual readings), judged only in
# MAGIC years with at least 200 wet readings. The thresholds
# MAGIC are settings under `qc` in the config, and provisional.

# COMMAND ----------

# The run: every gauge whose readings changed since it was last checked. The first run checks
# all of them, about 2 s a gauge.
summary = rain_qc.run_qc(cfg)
print(len(summary), "gauges checked")
rules = [c for c in summary.columns if c.startswith("n_")]
print("Readings flagged, by rule, across the network:")
print(summary[rules].sum().sort_values(ascending=False))
print("Gauges with at least one rejected day:", int((summary["days_rejected"] > 0).sum()))

# COMMAND ----------

# The gauges with the most rejected and suspect days
display(summary.head(30))  # noqa: F821 - Databricks notebooks provide display()

# COMMAND ----------

# Gauges whose instrument or processing seems to have changed (the tip resolution changed),
# and those with an hour-of-day or weekend pattern. A resolution change splits the record into
# two that may not measure rain the same way: worth asking the hydrometry team about.
for col in ("resolution_change_years", "hour_bias_years", "weekend_bias_years"):
    hit = summary[summary[col].fillna("").str.len() > 0]
    print(f"{col.replace('_years', '').replace('_', ' ')}: {len(hit)} gauges")
    display(hit[["gauge_id", col]])  # noqa: F821

# COMMAND ----------

# One gauge in detail: its flagged days and its years
GAUGE = summary["gauge_id"].iloc[0]
print("Gauge", GAUGE)
display(rain_qc.day_flags(cfg, GAUGE))  # noqa: F821
import pyarrow.parquet as pq  # noqa: E402

display(pq.read_table(rain_qc._path(cfg, "years", GAUGE)).to_pandas())  # noqa: F821
