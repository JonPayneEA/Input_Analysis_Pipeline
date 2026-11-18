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
# Outputs:      gold/qc/{readings,days,years}/<gauge>.parquet; gold/qc/summary.csv; stage 2 in
#               gold/qc/stage2/{days,months,years}.parquet and summary.csv
# Dependencies: pandas, pyarrow
# ============================================================
"""Quality control across the network, kept up to date with the Bronze store."""

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from pipelines import qc, qc2, store

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


def _stage1_days(cfg, gauge_id):
    p = _path(cfg, "days", gauge_id)
    if not p.exists():
        return pd.DataFrame(columns=qc.DAY_COLUMNS)
    d = pq.read_table(p).to_pandas()
    d["date"] = pd.to_datetime(d["date"])
    return d


def _stage2_path(cfg, name):
    return Path(f"{cfg['store_root']}/gold/qc/stage2/{name}.parquet")


def day_flags(cfg, gauge_id):
    """A gauge's flagged days from both stages (date, flag, rules): each day takes its worst
    flag and all its rules. Days not listed are good."""
    parts = [_stage1_days(cfg, gauge_id)]
    p2 = _stage2_path(cfg, "days")
    if p2.exists():
        d2 = pq.read_table(p2, filters=[("gauge_id", "=", str(gauge_id))],
                           columns=["date", "flag", "rules"]).to_pandas()
        d2["date"] = pd.to_datetime(d2["date"])
        parts.append(d2)
    parts = [p for p in parts if len(p)]
    if not parts:
        return pd.DataFrame(columns=qc.DAY_COLUMNS)
    d = pd.concat(parts, ignore_index=True)
    out = d.groupby("date").agg(
        flag=("flag", "max"),
        rules=("rules", lambda x: "; ".join(sorted({r for s in x for r in s.split("; ")}))))
    return out.reset_index()[qc.DAY_COLUMNS]


# ---- Stage 2: against HadUK-Grid and neighbouring gauges --------------------------------------


def _daily_matrix(cfg, ids, index):
    """Daily totals of every gauge as one table (days by gauges), stage-1 rejected days blank."""
    from pipelines import rain_charts
    cols = {}
    for g in ids:
        d = rain_charts.gauge_daily(cfg, g, files=index.get(g, []))
        if not len(d):
            continue
        s = d.set_index("date")["rain_mm"].astype("float32")
        bad = _stage1_days(cfg, g)
        bad = bad.loc[bad["flag"] >= qc.REJECTED, "date"]
        s[s.index.isin(bad)] = np.nan
        cols[g] = s
    return pd.DataFrame(cols).sort_index()


def _haduk_matrix(cfg, ids):
    """HadUK-Grid daily rainfall at each gauge's cell (days by gauges), from the extract."""
    folder = Path(f"{cfg['store_root']}/gold/haduk/rainfall_daily")
    parts = []
    for f in sorted(folder.glob("*.parquet")) if folder.exists() else []:
        t = pq.read_table(f, filters=[("kind", "=", "gauge")],
                          columns=["date", "id", "rain_mm"]).to_pandas()
        t = t[t["id"].isin(ids)]
        parts.append(t.pivot_table(index="date", columns="id", values="rain_mm",
                                   aggfunc="first").astype("float32"))
    if not parts:
        return pd.DataFrame()
    out = pd.concat(parts).sort_index()
    out.index = pd.to_datetime(out.index)
    return out


def run_stage2(cfg, gauges=None):
    """Stage 2 over the network: each gauge's days, months and years against HadUK-Grid at
    its cell and against the median of its nearest neighbours. Writes gold/qc/stage2/ and
    returns a summary, one row per gauge, most flagged first."""
    from pipelines import rain_charts
    st = qc2.settings2(cfg)
    loc = rain_charts.load_locations(cfg)
    reg = [str(g) for g in store.read_registry(cfg["registry_path"])["gauge_id"]]
    ids = [g for g in (gauges or reg) if g in loc.index]
    index = store.file_index(cfg["store_root"])
    print("Reading daily totals...", flush=True)
    m = _daily_matrix(cfg, ids, index)
    ids = list(m.columns)
    print(f"{len(ids)} gauges, {len(m)} days. Reading HadUK-Grid...", flush=True)
    h = _haduk_matrix(cfg, ids)
    nb = qc2.nearest_neighbours(loc.loc[ids], st["neighbours"], st["neighbour_max_km"])
    med = qc2.neighbour_median(m, nb, st["min_neighbours"])
    days, months, years, rows = [], [], [], []
    for k, g in enumerate(ids, start=1):
        gs = m[g].dropna()
        hs = h[g].reindex(gs.index) if g in h else None
        ns = med[g].reindex(gs.index)
        found = [qc2.day_rules(gs, ns, "nb", cfg)]
        if hs is not None:
            found.append(qc2.day_rules(gs, hs, "haduk", cfg))
        found = [f for f in found if len(f)]
        if found:
            f = pd.concat(found).groupby("date")["rule"].apply(
                lambda x: "; ".join(sorted(set(x)))).reset_index(name="rules")
            f["gauge_id"], f["flag"] = g, qc2.SUSPECT
            days.append(f)
        mt = qc2.month_table(gs, hs, ns, cfg)
        mt.insert(0, "gauge_id", g)
        months.append(mt)
        yt = qc2.day_shift(gs, ns, cfg)
        yt.insert(0, "gauge_id", g)
        years.append(yt)
        rules = pd.concat(found)["rule"].value_counts() if found else pd.Series(dtype=int)
        rows.append({"gauge_id": g, "neighbours": len(nb.get(g, [])),
                     **{f"n_{r}": int(rules.get(r, 0)) for r in
                        ("nb_dry", "nb_wet", "nb_factor", "haduk_dry", "haduk_wet",
                         "haduk_factor")},
                     "months_off": int((mt["flag"] == qc2.SUSPECT).sum()),
                     "shifted_years": " ".join(str(y) for y in yt.loc[yt["shifted"], "year"])})
        if k % 100 == 0:
            print(f"{k}/{len(ids)} gauges", flush=True)
    out = {"days": pd.concat(days, ignore_index=True) if days else
           pd.DataFrame(columns=["date", "rules", "gauge_id", "flag"]),
           "months": pd.concat(months, ignore_index=True),
           "years": pd.concat(years, ignore_index=True)}
    for name, df in out.items():
        store.write_parquet(df, str(_stage2_path(cfg, name)))
    summary = pd.DataFrame(rows)
    flagged = [c for c in summary.columns if c.startswith("n_")]
    summary["days_flagged"] = summary[flagged].sum(axis=1)
    summary = summary.sort_values(["months_off", "days_flagged"], ascending=False)
    store.write_csv(summary, f"{cfg['store_root']}/gold/qc/stage2/summary.csv")
    return summary.reset_index(drop=True)
