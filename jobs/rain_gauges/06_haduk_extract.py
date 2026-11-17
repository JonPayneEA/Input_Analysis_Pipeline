# Databricks notebook source
# ============================================================
# Tool:         06 HadUK-Grid extract
# Description:  Gauge positions, then HadUK-Grid daily rainfall for every gauge's 1 km cell and
#               every configured area, one Parquet file a year; rerun after each HadUK release
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-17
# Tier:         1
# Inputs:       The Hydrology API (positions); HadUK-Grid NetCDF under haduk.root
# Outputs:      register/rain_gauge_locations.csv; gold/haduk/rainfall_daily/<year>.parquet
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

from pipelines import haduk, rain_charts  # noqa: E402
from pipelines.config import load_config  # noqa: E402

cfg = load_config(_root)

# COMMAND ----------

# Gauge positions, eastings and northings on British National Grid, from the Hydrology API
loc = rain_charts.update_locations(cfg)
print(len(loc), "gauge positions")

# COMMAND ----------

# Which files are there, and how a rain day is labelled. HadUK days run 09:00 to 09:00 UTC;
# the first file's time bounds should start at 09:00, and each day is labelled by that date.
files = haduk.find_files(cfg["haduk"]["root"])
print(len(files), "daily rainfall files,", files[0][0].date(), "to", files[-1][0].date())
ds = haduk._open(files[-1][1])
print(ds)
print("First day:", haduk.day_labels(ds)[0].date())
ds.close()

# COMMAND ----------

# The extraction: one pass over the files from haduk.start_year. The first run reads every
# year and takes a while; later runs read only the latest year and any new ones.
written = rain_charts.run_haduk_extract(cfg)
print("Years written:", written)
