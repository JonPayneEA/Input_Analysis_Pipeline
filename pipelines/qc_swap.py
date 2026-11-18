# ============================================================
# Tool:         rain gauge swap check
# Description:  Dates the swap from a tipping bucket to a finer gauge (the Rain[e]) from the
#               readings themselves, and measures the step in the gauge's level at the swap
#               against neighbours that did not change and against HadUK-Grid, with the step
#               the same comparison gives at earlier dates when nothing changed
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-18
# Tier:         1
# Inputs:       A gauge's 15-minute readings; daily totals (09:00 UTC rain days) of the gauge
#               and its references
# Outputs:      A swap date per gauge; a step per gauge with its noise and a flag
# Dependencies: numpy, pandas
# ============================================================
"""Did the new gauge change the record?

The EA is replacing tipping-bucket gauges (TBR) with the Rain[e], one Area at a time. A
bucket reports whole tips of 0.2 mm; the Rain[e] reports much finer amounts. So the swap shows
in the readings: the month from which most readings stop being whole tips, and stay that way.

The Rain[e] is said to be more accurate, so a record may step at the swap. Because the
rollout is staggered, a swapped gauge has, for some years, neighbours still on buckets. Its
ratio to each unchanged neighbour over the years after the swap, against the same ratio over
the years before, measures the step: a neighbour's own level cancels in the ratio of ratios,
and whole years on each side keep the seasons balanced. The same comparison at the same date in
earlier years, when nothing changed, shows how much the ratio wanders anyway (`noise`); a step
is flagged only when it stands well clear of that. HadUK-Grid at the gauge's cell is a second
reference, weaker because the gauge itself feeds HadUK-Grid.
"""

import numpy as np
import pandas as pd

DEFAULTS_SWAP = {
    "tip_mm": 0.2,          # the bucket's tip
    "min_wet": 20,          # a month is judged with this many wet readings
    "fine_share": 0.5,      # a month is fine when this share of its wet readings are not tips
    "hold": 0.8,            # and the swap month is the first fine one with this share of the
                            # judged months after it fine too
    "years": 2,             # years each side of the swap, fewer if the record is shorter
    "min_share": 0.8,       # each side needs this share of its days in both series
    "min_ref_mm": 200.0,    # and this much rain in the reference a year
    "placebos": 20,         # earlier dates tried, at most
    "min_placebos": 4,      # the noise needs this many
    "z": 2.0,               # a step is flagged at this many times the noise
}


def settings_swap(cfg=None):
    out = dict(DEFAULTS_SWAP)
    out.update(((cfg or {}).get("qc", {}) or {}).get("swap", {}) or {})
    return out


def _rain_day(ts):
    return (ts - pd.Timedelta(hours=9)).dt.tz_convert(None).dt.normalize()


def swap_date(series, cfg=None):
    """The rain day the gauge left whole tips, or NaT. Months with `min_wet` wet on-grid
    readings are judged; a month is fine when `fine_share` of them are not whole tips. The swap
    month is the first fine month with `hold` of the judged months after it fine. A swap late
    in a month leaves that month mostly tips, so the date is sought from the month before:
    the first rain day with a reading that is not a whole tip and from which, to the end of
    the swap month, `hold` of the wet readings are not. A record fine from its first month
    never left tips: NaT."""
    st = settings_swap(cfg)
    s = series.dropna(subset=["timestamp"]).copy()
    s["timestamp"] = pd.to_datetime(s["timestamp"], utc=True)
    s = s[(s["timestamp"].dt.minute % 15 == 0) & (s["timestamp"].dt.second == 0)
          & (s["value"] > 0)]
    if not len(s):
        return pd.NaT
    q = s["value"] / st["tip_mm"]
    s["fine"] = np.abs(q - np.round(q)) > 1e-6
    s["day"] = _rain_day(s["timestamp"])
    s["month"] = s["day"].dt.to_period("M")
    m = s.groupby("month").agg(wet=("fine", "size"), share=("fine", "mean"))
    m = m[m["wet"] >= st["min_wet"]]
    fine = (m["share"] >= st["fine_share"]).to_numpy()
    for k in range(len(m)):
        if fine[k] and fine[k:].mean() >= st["hold"]:
            if k == 0:
                return pd.NaT
            w = s[(s["month"] >= m.index[k] - 1) & (s["month"] <= m.index[k])]
            # Share of fine readings from each reading to the end of the swap month
            tail = w["fine"][::-1].cumsum()[::-1] / np.arange(len(w), 0, -1)
            start = w.loc[w["fine"] & (tail >= st["hold"]), "day"]
            return pd.Timestamp(start.min())
    return pd.NaT


def _window_ratio(g, r, start, end, st):
    """The gauge's total over the reference's on the days in [start, end) both report, or NaN
    when too few days or too little reference rain."""
    sel = (g.index >= start) & (g.index < end)
    a, b = g[sel], r.reindex(g.index)[sel]
    ok = a.notna() & b.notna()
    days = (end - start).days
    years = days / 365.25
    if ok.sum() < st["min_share"] * days or b[ok].sum() < st["min_ref_mm"] * years:
        return np.nan
    return float(a[ok].sum() / b[ok].sum())


def step_at(g, r, date, years, cfg=None):
    """The step in the gauge against reference `r` at `date`: its ratio over the `years`
    whole years from `date`, over its ratio in the `years` before, less 1. NaN when either
    side is short."""
    st = settings_swap(cfg)
    date = pd.Timestamp(date)
    span = pd.DateOffset(years=years)
    before = _window_ratio(g, r, date - span, date, st)
    after = _window_ratio(g, r, date, date + span, st)
    if not (np.isfinite(before) and np.isfinite(after)) or before == 0:
        return np.nan
    return after / before - 1


def usable_years(g, date, cfg=None):
    """Whole years each side of `date` the gauge's record allows, up to `years`."""
    st = settings_swap(cfg)
    d = g.dropna().index
    if not len(d):
        return 0
    date = pd.Timestamp(date)
    n = st["years"]
    while n > 0 and (d.min() > date - pd.DateOffset(years=n)
                     or d.max() < date + pd.DateOffset(years=n) - pd.Timedelta(days=1)):
        n -= 1
    return n


def _robust_sd(x):
    x = np.asarray(x, dtype=float)
    return float(1.4826 * np.median(np.abs(x - np.median(x)))) if len(x) else np.nan


def swap_step(g, refs, date, cfg=None, years=None):
    """The step at the swap against each reference (columns of `refs`, a daily frame), their
    median, and the noise: the robust spread of the median step at the same day of the year
    1, 2, 3... years earlier, each placebo's window wholly before the swap. Placebo windows
    overlap, so the noise is a rough guide, not an exact standard error. Returns a dict."""
    st = settings_swap(cfg)
    date = pd.Timestamp(date)
    n = usable_years(g, date, cfg) if years is None else years
    out = {"years": n, "refs": 0, "step": np.nan, "step_min": np.nan, "step_max": np.nan,
           "noise": np.nan, "placebos": 0, "z": np.nan, "flag": ""}
    if n < 1 or refs.shape[1] == 0:
        return out
    steps = [step_at(g, refs[c], date, n, cfg) for c in refs.columns]
    steps = np.array([s for s in steps if np.isfinite(s)])
    if not len(steps):
        return out
    out.update(refs=len(steps), step=float(np.median(steps)), step_min=float(steps.min()),
               step_max=float(steps.max()))
    placebo = []
    for k in range(n, n + st["placebos"]):
        p = date - pd.DateOffset(years=k)
        s = [step_at(g, refs[c], p, n, cfg) for c in refs.columns]
        s = [v for v in s if np.isfinite(v)]
        if s:
            placebo.append(float(np.median(s)))
    out["placebos"] = len(placebo)
    if len(placebo) >= st["min_placebos"]:
        out["noise"] = _robust_sd(placebo)
        if out["noise"] > 0:
            out["z"] = (out["step"] - float(np.median(placebo))) / out["noise"]
            if abs(out["z"]) >= st["z"]:
                out["flag"] = "up" if out["z"] > 0 else "down"
    return out
