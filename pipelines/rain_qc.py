# ============================================================
# Tool:         rain gauge QC runner
# Description:  Runs the single-gauge checks over the network, saving each gauge's flagged
#               readings, flagged days and years, and a summary table across all gauges;
#               a gauge is checked again only when its Bronze files change
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-18
# Tier:         1
# Inputs:       The Bronze rain store; the qc section of the config
# Outputs:      gold/qc/{readings,days,years}/<gauge>.parquet; gold/qc/summary.csv
# Dependencies: pandas, pyarrow
# ============================================================
"""Quality control across the network, kept up to date with the Bronze store."""

from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

from pipelines import qc, store

KINDS = ("readings", "days", "years")


def _path(cfg, kind, gauge_id):
    return Path(f"{cfg['store_root']}/gold/qc/{kind}/{gauge_id}.parquet")


def check_gauge(cfg, gauge_id, files=None, refresh=False):
    """Runs the checks on one gauge if its Bronze files are newer than its saved results (or
    with `refresh`), and returns its summary row. A gauge with no files returns None."""
    root = cfg["store_root"]
    files = store.series_files(root, gauge_id) if files is None else files
    if not files:
        return None
    newest = max(f.stat().st_mtime for f in files)
    paths = {k: _path(cfg, k, gauge_id) for k in KINDS}
    current = not refresh and all(p.exists() and p.stat().st_mtime >= newest
                                  for p in paths.values())
    if current:
        out = {k: pq.read_table(p).to_pandas() for k, p in paths.items()}
    else:
        series = store.read_series(root, gauge_id, files=files)
        flagged = qc.check_readings(series, cfg)
        out = {"readings": flagged, "days": qc.check_days(series, flagged, cfg),
               "years": qc.check_years(series, cfg)}
        for k, df in out.items():
            store.write_parquet(df, str(paths[k]))
    return qc.gauge_summary(gauge_id, out["readings"], out["days"], out["years"])


def run_qc(cfg, gauges=None, refresh=False):
    """Checks every gauge in the registry (or `gauges`), skipping those whose results are up to
    date, and writes gold/qc/summary.csv. Returns the summary, worst gauges first."""
    ids = gauges if gauges is not None else [
        str(g) for g in store.read_registry(cfg["registry_path"])["gauge_id"]]
    index = store.file_index(cfg["store_root"])
    rows = []
    for k, g in enumerate(ids, start=1):
        row = check_gauge(cfg, g, files=index.get(g, []), refresh=refresh)
        if row is not None:
            rows.append(row)
        if k % 100 == 0:
            print(f"{k}/{len(ids)} gauges checked", flush=True)
    summary = pd.DataFrame(rows)
    if len(summary):
        summary = summary.sort_values(["days_rejected", "days_suspect"], ascending=False)
        store.write_csv(summary, f"{cfg['store_root']}/gold/qc/summary.csv")
    return summary.reset_index(drop=True)


def day_flags(cfg, gauge_id):
    """A gauge's flagged days (date, flag, rules); days not listed are good."""
    p = _path(cfg, "days", gauge_id)
    if not p.exists():
        return pd.DataFrame(columns=qc.DAY_COLUMNS)
    d = pq.read_table(p).to_pandas()
    d["date"] = pd.to_datetime(d["date"])
    return d
