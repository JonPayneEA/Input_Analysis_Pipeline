# Databricks notebook source
# ============================================================
# Tool:         07 rainfall accumulation
# Description:  Monthly rainfall accumulation charts in the Met Office style, for gauges, catchments
#               and areas, and a table of where every gauge's month stands
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-17
# Tier:         1
# Inputs:       The Bronze rain store; the HadUK-Grid extract (06); outlines in the config
# Outputs:      PNG charts under gold/charts/accumulation/<YYYY-MM>/; month_table.csv there
# Dependencies: pandas, pyarrow, matplotlib, xarray, netCDF4
# ============================================================

# COMMAND ----------

# MAGIC %md xarray and netCDF4 read the HadUK-Grid files; this installs them for the notebook.

# COMMAND ----------

# MAGIC %pip install -q xarray netCDF4

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

from pipelines import rain_charts  # noqa: E402
from pipelines.config import load_config  # noqa: E402

cfg = load_config(_root)

# COMMAND ----------

# Choose the month and what to draw. GAUGES and AREAS may be empty; AREAS uses the ids in the
# config's areas list. ALL_GAUGES draws a chart for every gauge (about 1,000 PNGs).
YEAR, MONTH = 2026, 10
GAUGES = ["000008"]
AREAS = []
ALL_GAUGES = False

# COMMAND ----------

import matplotlib.pyplot as plt  # noqa: E402

for g in GAUGES:
    fig, summary = rain_charts.gauge_chart(cfg, g, YEAR, MONTH)
    print(summary)
    display(fig)  # noqa: F821 - Databricks notebooks provide display()
    plt.close(fig)

for a in AREAS:
    fig, summary = rain_charts.area_chart(cfg, a, YEAR, MONTH)
    print(summary)
    display(fig)  # noqa: F821
    plt.close(fig)

# COMMAND ----------

# Every gauge's month against its own record and its five nearest neighbours. A gauge whose
# rank sits far from its neighbours' (odd_one_out) has more likely failed than seen unusual
# weather: check it before using it. Reads every gauge's record: allow some minutes.
table = rain_charts.month_table(cfg, YEAR, MONTH)
out = f"{cfg['store_root']}/gold/charts/accumulation/{YEAR}-{MONTH:02d}/month_table.csv"
table.to_csv(out, index=False)
print(table["standing"].value_counts())
print("Odd ones out:")
print(table[table["odd_one_out"]])

# COMMAND ----------

if ALL_GAUGES:
    for g in table["gauge_id"]:
        fig, _ = rain_charts.gauge_chart(cfg, g, YEAR, MONTH)
        plt.close(fig)
    print("Charts saved under", f"{cfg['store_root']}/gold/charts/accumulation/{YEAR}-{MONTH:02d}/")
