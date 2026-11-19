# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# ============================================================
# Tool:         07 rainfall accumulation
# Description:  Rainfall accumulation charts in the Met Office style for a gauge or an area,
#               chosen from drop-downs, over a month, season, water year or calendar year
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
# Forget any copy of the pipelines code loaded before a git pull, so the new code is used
for _name in [m for m in sys.modules if m == "pipelines" or m.startswith("pipelines.")]:
    del sys.modules[_name]

from pipelines import rain_charts  # noqa: E402
from pipelines.config import load_config  # noqa: E402
from pipelines.notebook import show  # noqa: E402

cfg = load_config(_root)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Choose a chart
# MAGIC The drop-downs at the top of the notebook choose what to draw. Type in the gauge box to
# MAGIC search by id or name. To redraw as soon as a choice changes, open the widget settings
# MAGIC (the cog at the right of the widget bar) and set "On widget change" to "Run notebook";
# MAGIC otherwise choose, then press Run all.
# MAGIC
# MAGIC Periods: a month; three, six or twelve months from the start month (three from December
# MAGIC is the winter); the water year, October to September, named by the year it starts; or
# MAGIC the calendar year. A period still in progress is drawn to its latest day.

# COMMAND ----------

import calendar  # noqa: E402

import matplotlib.pyplot as plt  # noqa: E402
import pandas as pd  # noqa: E402

now = pd.Timestamp.now(tz="UTC")
gauges = rain_charts.gauge_choices(cfg)
areas = [f"{a['id']} - {a.get('name', a['id'])}" for a in cfg.get("areas", []) or []]
if not gauges:
    raise RuntimeError("No gauges with records: run the backfill (04) and 06_haduk_extract")
default_gauge = gauges[0]
w = dbutils.widgets  # noqa: F821 - Databricks notebooks provide dbutils
w.dropdown("chart", "Gauge", ["Gauge", "Area", "Both"], "1. Chart")
w.combobox("gauge", default_gauge, gauges, "2. Gauge")
w.dropdown("area", areas[0] if areas else "(none)", areas or ["(none)"], "3. Area")
w.dropdown("period", "Month", list(rain_charts.PERIODS), "4. Period")
w.dropdown("year", str(now.year), [str(y) for y in range(now.year, 1960, -1)], "5. Start year")
w.dropdown("month", calendar.month_name[now.month], list(calendar.month_name)[1:],
           "6. Start month")
w.dropdown("table", "No", ["No", "Yes"], "7. Gauge table (slow)")

# COMMAND ----------

# Each cell below reads the drop-downs itself, so it can be run on its own. If Python has
# restarted since the set-up cells ran (serverless compute does after a pause), press Run all.
if "cfg" not in globals():
    raise RuntimeError("Python has restarted since the set-up cells ran: press Run all")
ch = rain_charts.read_choices(dbutils.widgets.get)  # noqa: F821 - Databricks provides dbutils
year, month, n_months = ch["year"], ch["month"], ch["n_months"]
print("Drawing", rain_charts.period_label(year, month, n_months))

if ch["chart"] in ("Gauge", "Both"):
    fig, summary = rain_charts.gauge_chart(cfg, ch["gauge"], year, month, n_months=n_months)
    print(summary)
    display(fig)  # noqa: F821 - Databricks notebooks provide display()
    plt.close(fig)

if ch["chart"] in ("Area", "Both") and ch["area"] != "(none)":
    fig, summary = rain_charts.area_chart(cfg, ch["area"], year, month, n_months=n_months)
    print(summary)
    display(fig)  # noqa: F821
    plt.close(fig)

# COMMAND ----------

# Every gauge's period against its own record and its five nearest neighbours, over the same
# days. A gauge whose rank sits far from its neighbours' (odd_one_out) has more likely failed
# than seen unusual weather: check it before using it. A gauge `behind` stopped reporting
# before the rest. Reads every gauge's record: allow some minutes.
if "cfg" not in globals():
    raise RuntimeError("Python has restarted since the set-up cells ran: press Run all")
ch = rain_charts.read_choices(dbutils.widgets.get)  # noqa: F821
year, month, n_months = ch["year"], ch["month"], ch["n_months"]
if ch["table"]:
    table = rain_charts.month_table(cfg, year, month, n_months=n_months)
    folder = f"{cfg['store_root']}/gold/charts/accumulation/{year}-{month:02d}"
    folder += f"_{n_months}m" if n_months > 1 else ""
    Path(folder).mkdir(parents=True, exist_ok=True)
    table.to_csv(f"{folder}/gauge_table.csv", index=False)
    print(table["standing"].value_counts())
    behind = table[table["behind"]]
    print(f"{len(behind)} of {len(table)} gauges stop before day {table['days'].max()}:")
    print(behind["days"].value_counts().sort_index(ascending=False).to_string())
    print("Odd ones out:")
    show(table[table["odd_one_out"]], display,  # noqa: F821
         "None: every gauge is in line with its neighbours.")
    display(table[table["odd_one_out"]])  # noqa: F821
