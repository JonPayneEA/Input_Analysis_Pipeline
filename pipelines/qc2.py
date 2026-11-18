# ============================================================
# Tool:         rain gauge quality control, stage 2
# Description:  Checks of each gauge's daily totals against references: HadUK-Grid at its
#               1 km cell and the median of its nearest neighbouring gauges. Days dry when the
#               references are wet, wet when they are dry, or out by a large factor; months far
#               from the references; years shifted by a day against the neighbours
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-18
# Tier:         1
# Inputs:       Daily totals (09:00 UTC rain days) for the gauge and its references
# Outputs:      Flagged days, a table of months and a table of years per gauge
# Dependencies: numpy, pandas
# ============================================================
"""Daily, monthly and yearly checks against HadUK-Grid and neighbouring gauges.

Neighbours, after Lewis et al. (2018, 2021): a gauge's daily totals should agree, in whether
it rained and roughly how much, with the gauges around it and with the gridded estimate for its
cell. Disagreement on a day is suspect, not proof: a summer storm can hit one gauge and miss
its neighbours. Disagreement month after month, or a series that lines up better a day early or
late, is stronger evidence of a fault. All flags here are suspect (3); stage 2 rejects nothing.

HadUK-Grid is interpolated from rain gauges, possibly including this one, so the gauge pulls
its own cell towards itself: the HadUK checks are weaker against small errors than against
gross ones. The neighbour checks leave the gauge itself out, and bring each neighbour to the
gauge's own long-term level first, so a hill gauge is not flagged for being wetter than the
valley; a gauge reading persistently high or low is left to the long-term checks against
HadUK-Grid (stage 3), which are not scaled.
"""

import warnings

import numpy as np
import pandas as pd

SUSPECT = 3

DEFAULTS2 = {
    "neighbours": 5,               # nearest gauges used as the neighbour reference
    "neighbour_max_km": 30.0,      # no further than this
    "min_neighbours": 3,           # a day needs this many neighbours reporting
    "dry_mm": 0.2,                 # a day at or below this is dry
    "ref_wet_mm": 5.0,             # the reference counts as wet at or above this
    "ref_dry_mm": 1.0,             # the reference counts as dry below this
    "spurious_mm": 20.0,           # a gauge this wet while the reference is dry is suspect
    "factor": 3.0,                 # gauge over reference beyond this, or under its inverse,
    "factor_min_mm": 5.0,          # on days when both reach this, is suspect
    "month_ratio_range": [0.5, 2.0],  # a month's total over the reference's outside this,
    "month_min_mm": 20.0,             # in months when the reference reaches this
    "shift_days": 2,               # lags tried, in days either way
    "shift_gain": 0.1,             # a lag must raise the correlation by this much
    "shift_min_corr": 0.5,         # and reach this, in a year with at least
    "shift_min_days": 200,         # this many days of both
}


def settings2(cfg=None):
    out = dict(DEFAULTS2)
    out.update((cfg or {}).get("qc", {}) or {})
    return out


def nearest_neighbours(locations, k=5, max_km=30.0):
    """Each gauge's nearest gauges within `max_km`, nearest first: {gauge: [gauges]}.
    `locations` is indexed by gauge id with easting and northing in metres."""
    ids = list(locations.index)
    xy = locations[["easting", "northing"]].to_numpy(dtype=float)
    out = {}
    for i, g in enumerate(ids):
        d = np.hypot(*(xy - xy[i]).T)
        d[i] = np.inf
        order = np.argsort(d)[:k]
        out[g] = [ids[j] for j in order if d[j] <= max_km * 1000]
    return out


def level_factors(matrix, gauge, neighbours, min_days=365, limits=(0.25, 4.0)):
    """Each neighbour's factor to the gauge's level: the gauge's mean over its neighbour's
    mean, over the days both report. A hill gauge normally wetter than the valley gauges
    round it (Langsett, on the Pennine edge, takes about 1.5 times its neighbours' rain) is
    then compared with what its neighbours imply for it, not with their raw totals. Fewer
    than `min_days` shared days give a factor of 1; factors are kept within `limits`."""
    g = matrix[gauge].to_numpy(dtype=float)
    out = {}
    for n in neighbours:
        v = matrix[n].to_numpy(dtype=float)
        ok = np.isfinite(g) & np.isfinite(v)
        mean_n = v[ok].mean() if ok.sum() >= min_days else 0.0
        out[n] = float(np.clip(g[ok].mean() / mean_n, *limits)) if mean_n > 0 else 1.0
    return out


def neighbour_median(matrix, neighbours, min_n=3, scale=True):
    """For each gauge (column of `matrix`, days by gauges), the median of its neighbours'
    totals on each day, where at least `min_n` of them report; NaN otherwise. With `scale`,
    each neighbour is first brought to the gauge's level (level_factors)."""
    out = {}
    for g in matrix.columns:
        nb = [n for n in neighbours.get(g, []) if n in matrix.columns]
        if len(nb) < min_n:
            out[g] = pd.Series(np.nan, index=matrix.index)
            continue
        vals = matrix[nb].to_numpy(dtype=float)
        if scale:
            f = level_factors(matrix, g, nb)
            vals = vals * np.array([f[n] for n in nb])
        n = np.isfinite(vals).sum(axis=1)
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", RuntimeWarning)   # days with too few: NaN
            med = np.nanmedian(np.where(n[:, None] >= min_n, vals, np.nan), axis=1)
        out[g] = pd.Series(med, index=matrix.index)
    return pd.DataFrame(out)


def day_rules(gauge, ref, prefix, cfg=None):
    """Days on which `gauge` disagrees with `ref` (two daily series on one date index): the
    gauge dry while the reference is wet, wet while it is dry, or out by more than `factor` on
    days both are wet. Returns date and rule, the rule named `<prefix>_dry`, `<prefix>_wet` or
    `<prefix>_factor`."""
    st = settings2(cfg)
    both = pd.DataFrame({"g": gauge, "r": ref}).dropna()
    g, r = both["g"], both["r"]
    hits = []
    hits.append(both.index[(g <= st["dry_mm"]) & (r >= st["ref_wet_mm"])].to_series()
                .rename(f"{prefix}_dry"))
    hits.append(both.index[(g >= st["spurious_mm"]) & (r < st["ref_dry_mm"])].to_series()
                .rename(f"{prefix}_wet"))
    big = (g >= st["factor_min_mm"]) & (r >= st["factor_min_mm"])
    off = big & ((g > st["factor"] * r) | (g * st["factor"] < r))
    hits.append(both.index[off].to_series().rename(f"{prefix}_factor"))
    rows = [pd.DataFrame({"date": h.index, "rule": h.name}) for h in hits if len(h)]
    if not rows:
        return pd.DataFrame(columns=["date", "rule"])
    return pd.concat(rows, ignore_index=True)


def month_table(gauge, haduk=None, neighbours=None, cfg=None):
    """Monthly totals of the gauge and its references over the days all present ones share,
    with the ratios and a flag where a ratio falls outside `month_ratio_range` in a month the
    reference reaches `month_min_mm`."""
    st = settings2(cfg)
    lo, hi = st["month_ratio_range"]
    frame = {"gauge": gauge}
    if haduk is not None:
        frame["haduk"] = haduk
    if neighbours is not None:
        frame["neighbours"] = neighbours
    d = pd.DataFrame(frame)
    rows = []
    for ref in [c for c in ("haduk", "neighbours") if c in d]:
        both = d[["gauge", ref]].dropna()
        if not len(both):
            continue
        m = both.groupby([both.index.year, both.index.month]).agg(
            gauge_mm=("gauge", "sum"), ref_mm=(ref, "sum"), days=("gauge", "size"))
        m["reference"] = ref
        rows.append(m)
    if not rows:
        return pd.DataFrame(columns=["year", "month", "reference", "days", "gauge_mm", "ref_mm",
                                     "ratio", "flag"])
    out = pd.concat(rows).rename_axis(["year", "month"]).reset_index()
    out["ratio"] = (out["gauge_mm"].astype(float)
                    / out["ref_mm"].astype(float).where(out["ref_mm"] > 0)).round(3)
    judged = out["ref_mm"] >= st["month_min_mm"]
    out["flag"] = np.where(judged & ((out["ratio"] < lo) | (out["ratio"] > hi)), SUSPECT, 1)
    out[["gauge_mm", "ref_mm"]] = out[["gauge_mm", "ref_mm"]].astype(float).round(1)
    return out[["year", "month", "reference", "days", "gauge_mm", "ref_mm", "ratio", "flag"]]


def _corr(a, b):
    ok = np.isfinite(a) & np.isfinite(b)
    if ok.sum() < 3 or np.std(a[ok]) == 0 or np.std(b[ok]) == 0:
        return np.nan, int(ok.sum())
    return float(np.corrcoef(a[ok], b[ok])[0, 1]), int(ok.sum())


def day_shift(gauge, ref, cfg=None):
    """For each year, the correlation of the gauge's daily totals with the reference at lags of
    up to `shift_days` days, and the best lag. A positive lag means the gauge's days are late:
    its rain is booked a day after the reference's. `shifted` marks years where another lag
    beats lag 0 by `shift_gain` and reaches `shift_min_corr`."""
    st = settings2(cfg)
    d = pd.DataFrame({"g": gauge, "r": ref}).sort_index()
    d = d.reindex(pd.date_range(d.index.min(), d.index.max())) if len(d) else d
    lags = range(-st["shift_days"], st["shift_days"] + 1)
    # The reference on day t - lag, against the gauge on day t: shifted once for every year
    shifted = {lag: d["r"].shift(lag).to_numpy(dtype=float) for lag in lags}
    g_all = d["g"].to_numpy(dtype=float)
    years_of = d.index.year
    rows = []
    for year in np.unique(years_of):
        sel = years_of == year
        corr = {lag: _corr(g_all[sel], shifted[lag][sel]) for lag in lags}
        c0, n0 = corr[0]
        finite = {k: v for k, v in corr.items() if np.isfinite(v[0])}
        best = max(finite, key=lambda k: finite[k][0]) if finite else 0
        cb = corr[best][0]
        moved = (n0 >= st["shift_min_days"] and best != 0 and np.isfinite(cb)
                 and cb >= st["shift_min_corr"] and np.isfinite(c0)
                 and cb - c0 >= st["shift_gain"])
        rows.append({"year": int(year), "days": n0,
                     "corr_lag0": round(c0, 3) if np.isfinite(c0) else np.nan,
                     "best_lag": int(best), "corr_best": round(cb, 3) if np.isfinite(cb)
                     else np.nan, "shifted": bool(moved)})
    return pd.DataFrame(rows, columns=["year", "days", "corr_lag0", "best_lag", "corr_best",
                                       "shifted"])
