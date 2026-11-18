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
#               gold/qc/stage2/{days,months,years}.parquet and summary.csv; stage 3 in
#               gold/qc/stage3/checks.parquet and summary.csv
# Dependencies: pandas, pyarrow
# ============================================================
"""Quality control across the network, kept up to date with the Bronze store."""

from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from pipelines import qc, qc2, qc3, qc_swap, store

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


# ---- Stage 3: whole records against HadUK-Grid, the neighbours and SAAR --------------------


def _complete_months(months, share):
    """Stage 2's months with at least `share` of their days shared, with water years."""
    m = months.copy()
    dim = pd.to_datetime(dict(year=m["year"], month=m["month"], day=1)).dt.days_in_month
    m = m[m["days"] >= share * dim]
    m["water_year"] = m["year"] + (m["month"] >= 10)
    return m


def _haduk_baselines(cfg, ids, first=1961, last=1990, min_years=20):
    """HadUK-Grid's mean annual rain at each gauge's cell over complete calendar years from
    `first` to `last`, needing `min_years` of them; NaN otherwise."""
    h = _haduk_matrix(cfg, ids)
    if not len(h):
        return pd.Series(np.nan, index=ids, dtype=float)
    h = h[(h.index.year >= first) & (h.index.year <= last)]
    sums = h.groupby(h.index.year).sum(min_count=1)
    counts = h.groupby(h.index.year).count()
    days = pd.Series([366 if pd.Timestamp(year=y, month=12, day=31).dayofyear == 366 else 365
                      for y in counts.index], index=counts.index)
    full = sums.where(counts.eq(days, axis=0))
    base = full.mean()
    base[full.count() < min_years] = np.nan
    return base.reindex(ids)


def _saar(cfg):
    """SAAR by gauge from the optional CSV (gauge_id, saar_mm) the config names under
    qc.saar_csv. SAAR is restricted: keep the file on a restricted volume; never commit it."""
    path = ((cfg.get("qc", {}) or {}).get("saar_csv") or "")
    if not path:
        return {}
    s = pd.read_csv(path, dtype={"gauge_id": str})
    return dict(zip(s["gauge_id"], s["saar_mm"].astype(float), strict=True))


def run_stage3(cfg):
    """Stage 3 over the network, from stage 2's monthly table (run stage 2 first): each
    gauge's whole record against HadUK-Grid and its neighbours (ratio_checks), and its
    long-term mean against SAAR where qc.saar_csv gives it. Writes gold/qc/stage3/ and returns
    a summary, the gauges with most flags first."""
    st = qc3.settings3(cfg)
    p = _stage2_path(cfg, "months")
    if not p.exists():
        raise FileNotFoundError("No stage 2 months: run stage 2 (rain_qc.run_stage2) first")
    months = _complete_months(pd.read_parquet(p), st["month_complete"])
    ids = sorted(months["gauge_id"].unique())
    baselines = _haduk_baselines(cfg, ids)
    saar = _saar(cfg)
    checks, rows = [], []
    for g in ids:
        row = {"gauge_id": g}
        for ref in ("haduk", "neighbours"):
            m = months[(months["gauge_id"] == g) & (months["reference"] == ref)]
            if not len(m):
                continue
            c = qc3.ratio_checks(m["gauge_mm"], m["ref_mm"], m["water_year"], m["month"], cfg)
            c.insert(0, "reference", ref)
            c.insert(0, "gauge_id", g)
            checks.append(c)
            by = c.set_index("check")
            tag = "haduk" if ref == "haduk" else "nb"
            # Against the neighbours, scaled to the gauge's level, the typical ratio is 1 by
            # construction: only its trend and steps say anything
            if ref == "haduk":
                row["typical_ratio"] = round(by.at["typical_ratio", "value"], 3)
                row["typical_flag"] = by.at["typical_ratio", "flag"]
                row["winter_summer"] = round(by.at["winter_summer", "value"], 3)
                row["winter_summer_flag"] = by.at["winter_summer", "flag"]
            row[f"trend_{tag}_pct_decade"] = round(by.at["trend", "value"], 1)
            row[f"trend_{tag}_flag"] = by.at["trend", "flag"]
            row[f"step_{tag}_pct"] = round(by.at["change_point", "value"], 1)
            row[f"step_{tag}_year"] = by.at["change_point", "at"]
            row[f"step_{tag}_flag"] = by.at["change_point", "flag"]
        # Annual totals over water years complete against HadUK-Grid, for the long-term check
        mh = months[(months["gauge_id"] == g) & (months["reference"] == "haduk")]
        full = mh.groupby("water_year").filter(lambda x: len(x) == 12)
        annual = full.groupby("water_year")[["gauge_mm", "ref_mm"]].sum()
        lt = qc3.longterm_ratios(annual["gauge_mm"], annual["ref_mm"], baselines.get(g, np.nan),
                                 saar.get(g), cfg)
        row.update({k: lt[k] for k in ("years", "c", "m", "b", "b_against", "b_flag",
                                       "m_flag")})
        rows.append(row)
    out = pd.concat(checks, ignore_index=True) if checks else pd.DataFrame()
    store.write_parquet(out, str(Path(f"{cfg['store_root']}/gold/qc/stage3/checks.parquet")))
    summary = pd.DataFrame(rows)
    flag_cols = [c for c in summary.columns if c.endswith("_flag")]
    summary["flags"] = summary[flag_cols].isin(["low", "high", "flagged"]).sum(axis=1)
    summary = summary.sort_values("flags", ascending=False).reset_index(drop=True)
    store.write_csv(summary, f"{cfg['store_root']}/gold/qc/stage3/summary.csv")
    return summary



# ---- Gauge swaps: the step in level when a tipping bucket gives way to a finer gauge -------


def _swap_register(cfg):
    """Swap dates from the optional CSV (gauge_id, swap_date) the config names under
    qc.swap_csv: the hydrometry team's install register, which beats dates read from the
    readings."""
    path = ((cfg.get("qc", {}) or {}).get("swap_csv") or "")
    if not path:
        return {}
    s = pd.read_csv(path, dtype={"gauge_id": str})
    return dict(zip(s["gauge_id"], pd.to_datetime(s["swap_date"]), strict=True))


def swap_dates(cfg, ids, index):
    """Each gauge's swap date and its source (register or readings). Read from the readings
    only for gauges whose stage-1 years show a change of resolution (run stage 1 first), and
    kept in gold/qc/swap/dates.csv until their Bronze files change."""
    path = f"{cfg['store_root']}/gold/qc/swap/dates.csv"
    cached = {}
    if Path(path).exists():
        c = pd.read_csv(path, dtype={"gauge_id": str})
        cached = {r.gauge_id: r for r in c.itertuples()}
    register = _swap_register(cfg)
    rows = []
    for g in ids:
        if g in register:
            rows.append({"gauge_id": g, "swap_date": register[g], "source": "register",
                         "bronze_mtime": np.nan})
            continue
        p = _path(cfg, "years", g)
        if not p.exists():
            continue
        years = pq.read_table(p, columns=["resolution_change"]).to_pandas()
        if not years["resolution_change"].any():
            continue
        files = index.get(g, [])
        if not files:
            continue
        newest = round(max(f.stat().st_mtime for f in files), 3)
        old = cached.get(g)
        if old is not None and old.source == "readings" and old.bronze_mtime == newest:
            date = pd.to_datetime(old.swap_date)
        else:
            date = qc_swap.swap_date(store.read_series(cfg["store_root"], g, files=files), cfg)
        rows.append({"gauge_id": g, "swap_date": date, "source": "readings",
                     "bronze_mtime": newest})
    out = pd.DataFrame(rows, columns=["gauge_id", "swap_date", "source", "bronze_mtime"])
    store.write_csv(out, path)
    return out


def run_swaps(cfg, gauges=None):
    """The step at each swapped gauge against its nearest neighbours that did not change in the
    years either side (at most qc.neighbours within qc.neighbour_max_km) and against HadUK-Grid
    at its cell. Writes gold/qc/swap/dates.csv and steps.csv and returns the steps, largest
    first. Steps are in per cent; noise is the same comparison at earlier dates."""
    from pipelines import rain_charts
    st2 = qc2.settings2(cfg)
    loc = rain_charts.load_locations(cfg)
    reg = [str(g) for g in store.read_registry(cfg["registry_path"])["gauge_id"]]
    ids = [g for g in (gauges or reg) if g in loc.index]
    index = store.file_index(cfg["store_root"])
    print("Dating swaps...", flush=True)
    dates = swap_dates(cfg, ids, index)
    swapped = dates.dropna(subset=["swap_date"])
    swap_of = dict(zip(dates["gauge_id"], pd.to_datetime(dates["swap_date"]), strict=True))
    print(f"{len(swapped)} gauges left whole tips. Reading daily totals...", flush=True)
    m = _daily_matrix(cfg, ids, index)
    have = [g for g in ids if g in m.columns]
    nb = qc2.nearest_neighbours(loc.loc[have], 50, st2["neighbour_max_km"])
    h = _haduk_matrix(cfg, list(swapped["gauge_id"]))
    rows = []
    for r in swapped.itertuples():
        g, date = r.gauge_id, pd.Timestamp(r.swap_date)
        if g not in m.columns:
            continue
        n = qc_swap.usable_years(m[g], date, cfg)
        span = pd.DateOffset(years=max(n, 1))
        # Neighbours unchanged over the windows: no swap, or one outside them
        keep = [x for x in nb.get(g, [])
                if pd.isna(swap_of.get(x, pd.NaT))
                or not (date - span <= swap_of[x] < date + span)][:st2["neighbours"]]
        res_nb = qc_swap.swap_step(m[g], m[keep], date, cfg, years=n)
        res_h = (qc_swap.swap_step(m[g], h[[g]], date, cfg, years=n) if g in h
                 else qc_swap.swap_step(m[g], pd.DataFrame(index=m.index), date, cfg, years=n))
        row = {"gauge_id": g, "swap_date": date.date(), "source": r.source, "years": n}
        for tag, res in (("nb", res_nb), ("haduk", res_h)):
            pct = {k: round(100 * res[k], 1) if np.isfinite(res[k]) else np.nan
                   for k in ("step", "step_min", "step_max", "noise")}
            row.update({f"{tag}_refs": res["refs"], f"{tag}_step_pct": pct["step"],
                        f"{tag}_noise_pct": pct["noise"], f"{tag}_placebos": res["placebos"],
                        f"{tag}_z": round(res["z"], 1) if np.isfinite(res["z"]) else np.nan,
                        f"{tag}_flag": res["flag"]})
            if tag == "nb":
                row.update(nb_step_min_pct=pct["step_min"], nb_step_max_pct=pct["step_max"])
        rows.append(row)
    out = pd.DataFrame(rows)
    if len(out):
        out = out.sort_values("nb_step_pct", key=lambda x: x.abs(), ascending=False,
                              na_position="last").reset_index(drop=True)
    store.write_csv(out, f"{cfg['store_root']}/gold/qc/swap/steps.csv")
    return out
