# Databricks notebook source
# ============================================================
# Tool:         08 rain gauge QC
# Description:  Quality control over the network (Lewis et al. 2018, 2021). Stage 1: each
#               gauge's own record. Stage 2: against HadUK-Grid and neighbouring gauges.
#               Stage 3: whole records, their level, trends and steps. Gauge swaps: the
#               step in level when a tipping bucket gives way to a Rain[e]
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-18
# Tier:         1
# Inputs:       The Bronze rain store; the qc section of the config (optional)
# Outputs:      gold/qc/{readings,days,years}/<gauge>.parquet; gold/qc/summary.csv;
#               gold/qc/stage2/{days,months,years}.parquet and summary.csv;
#               gold/qc/stage3/checks.parquet and summary.csv;
#               gold/qc/swap/dates.csv and steps.csv
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 2: against HadUK-Grid and neighbouring gauges
# MAGIC Each gauge's daily totals against two references: HadUK-Grid at its 1 km cell (to the
# MAGIC end of the HadUK files) and the median of its five nearest gauges within 30 km, each
# MAGIC first brought to the gauge's own long-term level so a hill gauge is not flagged for
# MAGIC being wetter than the valley. Days rejected at stage 1 are left out of the medians. All
# MAGIC flags are suspect (3): a summer storm can hit one gauge and miss its neighbours.
# MAGIC
# MAGIC | Rule | Raised when |
# MAGIC |---|---|
# MAGIC | `nb_dry`, `haduk_dry` | The gauge has 0.2 mm or less; the reference 5 mm or more |
# MAGIC | `nb_wet`, `haduk_wet` | The gauge has 20 mm or more while the reference has under 1 mm |
# MAGIC | `nb_factor`, `haduk_factor` | Both have 5 mm or more and differ by more than 3 times |
# MAGIC | months | A month's total under half or over twice the reference's (reference 20 mm+) |
# MAGIC | shifted years | The record lines up better a day or two early or late |
# MAGIC
# MAGIC Months near zero against wet references are the strongest sign of a blocked or failed
# MAGIC gauge; wet summer months are often real storms.

# COMMAND ----------

# Reads every gauge's daily totals and its HadUK-Grid cell at once: allow some minutes
summary2 = rain_qc.run_stage2(cfg)
display(summary2.head(30))  # noqa: F821

# COMMAND ----------

import pandas as pd  # noqa: E402

months = pd.read_parquet(f"{cfg['store_root']}/gold/qc/stage2/months.parquet")
off = months[months["flag"] == 3].sort_values("ratio")
print(len(off), "months off, of", len(months), "judged")
print("The driest against their references (blocked or failed gauges):")
display(off.head(30))  # noqa: F821
years = pd.read_parquet(f"{cfg['store_root']}/gold/qc/stage2/years.parquet")
print("Years shifted by a day or two against the neighbours:")
display(years[years["shifted"]])  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Stage 3: whole records
# MAGIC A gauge reading 10% low all its life, or stepping when it was moved, passes every daily
# MAGIC check. These look at the whole record, month by month, against HadUK-Grid and the
# MAGIC neighbours (the plan's source comparisons; the same as reach.forcing's ratio_checks):
# MAGIC
# MAGIC | Check | Flagged when |
# MAGIC |---|---|
# MAGIC | `typical_ratio` (HadUK) | Its whole 90% interval lies outside 0.9 to 1.1 |
# MAGIC | `winter_summer` (HadUK) | Winter reads low against summer: under-catch of snow or wind |
# MAGIC | `trend` | The annual ratio drifts more than 3% a decade (Sen's slope) |
# MAGIC | `change_point` | A significant step (Pettitt) larger than 5%, dated by its first year |
# MAGIC | `b` | The long-term mean against SAAR moved to its years, outside 0.9 to 1.1 |
# MAGIC
# MAGIC Against the neighbours (scaled to the gauge's level) only the trend and steps count. `b`
# MAGIC is against SAAR where `qc.saar_csv` names a file of gauge_id and saar_mm (SAAR is
# MAGIC restricted: keep the file on a restricted volume), else against HadUK-Grid.
# MAGIC
# MAGIC At Cannon Hall (085322) the annual ratio to its neighbours averages 1.005 to 2011 and is
# MAGIC above 1 every year from 2012, averaging 1.088: a step of 9% when something changed.

# COMMAND ----------

summary3 = rain_qc.run_stage3(cfg)
print((summary3["flags"] > 0).sum(), "of", len(summary3), "gauges with a whole-record flag")
display(summary3.head(40))  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gauge swaps: tipping bucket to Rain[e]
# MAGIC A bucket reports whole tips of 0.2 mm; the Rain[e] much finer amounts. The swap date is
# MAGIC where a gauge stops reporting whole tips, or the install register if `qc.swap_csv`
# MAGIC names one (gauge_id, swap_date).
# MAGIC
# MAGIC The step: the gauge's ratio to each neighbour still on a bucket, over up to two whole
# MAGIC years after the swap against the years before, less 1. The neighbour's own level cancels.
# MAGIC `nb_noise_pct` is how far the same comparison moves at earlier dates when nothing
# MAGIC changed; `nb_flag` is `up` or `down` when the step is at least twice that. `haduk_*` is
# MAGIC the same against HadUK-Grid at the gauge's cell, weaker since the gauge feeds it.
# MAGIC
# MAGIC Gauges that swapped less than a year ago have no step yet (`years` 0).

# COMMAND ----------

swaps = rain_qc.run_swaps(cfg)
measured = swaps.dropna(subset=["nb_step_pct"]) if len(swaps) else swaps
print(f"{len(swaps)} gauges left whole tips; {len(measured)} with a step against neighbours")
if len(swaps):
    print("Swaps by year:", swaps["swap_date"].map(lambda d: d.year).value_counts()
          .sort_index().to_dict())
if len(measured):
    q = measured["nb_step_pct"].quantile([0.25, 0.5, 0.75]).round(1).tolist()
    print(f"Step against unchanged neighbours, per cent: median {q[1]}, middle half {q[0]} "
          f"to {q[2]}")
    print("Flagged:", measured["nb_flag"].value_counts().to_dict())
display(swaps.head(40))  # noqa: F821
