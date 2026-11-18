# ============================================================
# Tool:         rain gauge quality control, stage 3
# Description:  Whole-record checks of a gauge against a reference: its typical ratio with a
#               bootstrap interval over water years, spread, winter against summer, trend and
#               change point in the annual ratio; and its long-term mean against SAAR moved to
#               its own years with HadUK-Grid. A port of reach.forcing's ratio_checks() and
#               longterm_ratios(), giving the same answers on the same inputs
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-18
# Tier:         1
# Inputs:       Monthly totals (mm) of the gauge and a reference, by water year and month;
#               annual totals and, optionally, SAAR
# Outputs:      Data frames of checks: check, value, lower, upper, n, flag, at
# Dependencies: numpy, pandas
# ============================================================
"""Whole-record checks: is the gauge's level right, and did it change?

A gauge that reads 10% low for its whole life, or steps down when it is moved, passes every
day-by-day check: each day looks plausible. These checks look at the record as a whole,
against HadUK-Grid (and the neighbours) month by month:

- typical_ratio: the median monthly ratio, with an interval from resampling whole water
  years, flagged low or high only when the whole interval lies outside `limits`;
- spread: how much the monthly ratio varies (median absolute deviation of its log);
- winter_summer: the winter ratio over the summer one; low means winter under-catch (snow,
  wind) the summer months do not show;
- trend: the drift in the annual ratio, per cent a decade (Sen's slope), flagged beyond
  `trend_limit`;
- change_point: the largest single step in the annual ratio (Pettitt's test), in per cent and
  dated by its first year, flagged when significant and larger than `step_limit`.

And with SAAR, after the rainfall plan: the climate factor c (HadUK over the gauge's years
against HadUK 1961-1990), the method ratio m (HadUK 1961-1990 against SAAR) and the bias index
b (the gauge against SAAR moved to its years by c).
"""

import numpy as np
import pandas as pd

DEFAULTS3 = {
    "limits": [0.9, 1.1],        # typical ratio range, and the bias index's
    "trend_limit": 3.0,          # per cent a decade
    "step_limit": 5.0,           # per cent
    "alpha": 0.05,               # Pettitt significance
    "min_months": 24,
    "min_years": 10,
    "boot": 1000,
    "level": 0.9,
    "seed": 1,
    "month_complete": 0.9,       # a month counts with this share of its days in both series
    "m_limits": [0.95, 1.05],    # method ratio range
}


def settings3(cfg=None):
    out = dict(DEFAULTS3)
    out.update(((cfg or {}).get("qc", {}) or {}).get("stage3", {}) or {})
    return out


def _row(check, value, lower, upper, n, flag, at=np.nan):
    return {"check": check, "value": value, "lower": lower, "upper": upper, "n": int(n),
            "flag": flag, "at": at}


def _mad(x):
    x = np.asarray(x, dtype=float)
    return float(1.4826 * np.median(np.abs(x - np.median(x)))) if len(x) else np.nan


def sen_slope(x, y):
    """Sen's slope: the median of the slopes between every pair of points."""
    x, y = np.asarray(x, dtype=float), np.asarray(y, dtype=float)
    i, j = np.triu_indices(len(x), k=1)
    dx = x[j] - x[i]
    keep = dx != 0
    return float(np.median((y[j][keep] - y[i][keep]) / dx[keep])) if keep.any() else np.nan


def pettitt(x):
    """Pettitt's test for one change in level: (index, p, k), index the number of values
    before the change, p the approximate p-value 2 exp(-6 K^2 / (n^3 + n^2))."""
    x = np.asarray(x, dtype=float)
    n = len(x)
    s = np.sign(x[:, None] - x[None, :])      # row j, column i: the sign of x_j - x_i
    u = np.array([s[t:, :t].sum() for t in range(1, n)])
    k = float(np.abs(u).max())
    index = int(np.abs(u).argmax()) + 1
    return index, float(min(1.0, 2 * np.exp(-6 * k ** 2 / (n ** 3 + n ** 2)))), k


def _boot_years(r, yr, boot, level, rng):
    groups = [r[yr == y] for y in np.unique(yr)]
    stats = np.empty(boot)
    for b in range(boot):
        pick = rng.integers(0, len(groups), len(groups))
        stats[b] = np.exp(np.median(np.concatenate([groups[p] for p in pick])))
    a = (1 - level) / 2
    return np.quantile(stats, [a, 1 - a])


def ratio_checks(gauge, reference, year, month=None, cfg=None):
    """The whole-record checks of a gauge's monthly totals against a reference's, by water
    year (and month, for winter against summer). Returns one row per check."""
    st = settings3(cfg)
    g, r_ = np.asarray(gauge, dtype=float), np.asarray(reference, dtype=float)
    year = np.asarray(year)
    ok = np.isfinite(g) & np.isfinite(r_) & (g > 0) & (r_ > 0)
    r = np.log(g[ok] / r_[ok])
    yr = year[ok]
    mo = None if month is None else np.asarray(month)[ok]
    lo, hi = st["limits"]
    rows = []
    typical = float(np.exp(np.median(r))) if len(r) else np.nan
    if len(r):
        ci = _boot_years(r, yr, int(st["boot"]), st["level"], np.random.default_rng(st["seed"]))
    else:
        ci = [np.nan, np.nan]
    if len(r) < st["min_months"] or not np.all(np.isfinite(ci)):
        flag = "too_few"
    else:
        flag = "low" if ci[1] < lo else "high" if ci[0] > hi else "ok"
    rows.append(_row("typical_ratio", typical, ci[0], ci[1], len(r), flag))
    rows.append(_row("spread", _mad(r), np.nan, np.nan, len(r),
                     "too_few" if len(r) < st["min_months"] else "ok"))
    if mo is not None:
        winter, summer = r[np.isin(mo, [12, 1, 2])], r[np.isin(mo, [6, 7, 8])]
        ws = float(np.exp(np.median(winter) - np.median(summer))) \
            if len(winter) and len(summer) else np.nan
        enough = len(winter) >= st["min_months"] / 4 and len(summer) >= st["min_months"] / 4
        rows.append(_row("winter_summer", ws, np.nan, np.nan, len(winter) + len(summer),
                         "too_few" if not enough else "low" if ws < lo else "ok"))
    years = np.unique(yr)
    annual = np.array([np.log(g[ok][yr == y].sum() / r_[ok][yr == y].sum()) for y in years])
    enough_years = len(years) >= st["min_years"]
    slope = sen_slope(years, annual) if len(years) >= 2 else np.nan
    trend = (np.exp(10 * slope) - 1) * 100
    rows.append(_row("trend", trend, np.nan, np.nan, len(years),
                     "too_few" if not enough_years else
                     "flagged" if abs(trend) > st["trend_limit"] else "ok"))
    step, at, flag = np.nan, np.nan, "too_few"
    if len(years) >= 3:
        index, p, _ = pettitt(annual)
        step = (np.exp(annual[index:].mean() - annual[:index].mean()) - 1) * 100
        at = int(years[index])
        if enough_years:
            flag = "flagged" if p < st["alpha"] and abs(step) > st["step_limit"] else "ok"
    rows.append(_row("change_point", step, np.nan, np.nan, len(years), flag, at))
    return pd.DataFrame(rows)


def longterm_ratios(gauge, haduk, haduk_baseline, saar=None, cfg=None):
    """The gauge's long-term mean against SAAR moved to its years. `gauge` and `haduk` are
    annual totals (mm) indexed by year; over the years both have: c = mean(HadUK) / HadUK
    1961-1990 mean, m = HadUK 1961-1990 / SAAR, b = mean(gauge) / (SAAR c). Without SAAR, b
    is the gauge over HadUK, mean(gauge) / mean(HadUK). Returns one row."""
    st = settings3(cfg)
    gauge, haduk = pd.Series(gauge, dtype=float), pd.Series(haduk, dtype=float)
    years = gauge.dropna().index.intersection(haduk.dropna().index)
    m = haduk_baseline / saar if saar else np.nan
    lo, hi = st["limits"]

    def flag(v, lim):
        return "low" if v < lim[0] else "high" if v > lim[1] else "ok"
    if not len(years) or not np.isfinite(haduk_baseline) or haduk_baseline <= 0:
        return {"years": len(years), "c": np.nan, "m": m, "b": np.nan,
                "b_against": "SAAR" if saar else "HadUK-Grid", "b_flag": "too_few_years",
                "m_flag": flag(m, st["m_limits"]) if np.isfinite(m) else "no_saar"}
    c = haduk[years].mean() / haduk_baseline
    b = gauge[years].mean() / (saar * c) if saar else gauge[years].mean() / haduk[years].mean()
    return {"years": len(years), "c": round(float(c), 3),
            "m": round(float(m), 3) if np.isfinite(m) else np.nan,
            "b": round(float(b), 3), "b_against": "SAAR" if saar else "HadUK-Grid",
            "b_flag": "too_few_years" if len(years) < st["min_years"] else flag(b, (lo, hi)),
            "m_flag": flag(m, st["m_limits"]) if np.isfinite(m) else "no_saar"}
