# ============================================================
# Tool:         rain gauge quality control, stage 1
# Description:  Checks on a single gauge's own 15-minute readings, after Lewis et al. (2018,
#               2021): readings off the grid, negative, extreme or stuck, totals dumped after an
#               outage, and the EA's own Suspect flag; then each year's tip resolution and its
#               hour-of-day and day-of-week pattern
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-18
# Tier:         1
# Inputs:       A gauge's Bronze readings (timestamp UTC, value mm, supplier_flag)
# Outputs:      Flagged readings, flagged days and a table of years per gauge, on reach.io's
#               scale: 1 good, 3 suspect, 4 rejected
# Dependencies: numpy, pandas
# ============================================================
"""Single-gauge quality control for 15-minute rainfall.

Nothing is removed or changed. Each rule marks readings (or days) with a flag and the rule's
name, and users of the data decide what to leave out: by default, rejected (4) is left out
and suspect (3) is kept but reported. A reading or day with no flag is good (1).

The thresholds are settings (`qc` in the config) and provisional: they follow UK records where
these are known, and should be checked against Lewis et al. before they are relied on.

References: Lewis, E. et al. (2018). A rule based quality control method for hourly rainfall
data and a 1 km resolution gridded hourly rainfall dataset for Great Britain: CEH-GEAR1hr.
Journal of Hydrology 564, 930-943. Lewis, E. et al. (2021). Quality control of a global hourly
rainfall dataset. Environmental Modelling and Software 144, 105169.
"""

import numpy as np
import pandas as pd

GOOD, SUSPECT, REJECTED = 1, 3, 4
RAIN_DAY_START_H = 9

DEFAULTS = {
    # A 15-minute reading above these, in mm. The UK's most intense short falls are of the order
    # of 50 to 100 mm in an hour; 40 mm in 15 minutes is exceptional and worth checking, 100 mm
    # implausible.
    "suspect_15min_mm": 40.0,
    "reject_15min_mm": 100.0,
    # A rain day (09:00 to 09:00 UTC) above these, in mm. The UK 24-hour record is about 341 mm
    # (Honister Pass, December 2015).
    "suspect_day_mm": 200.0,
    "reject_day_mm": 400.0,
    # Runs of the same non-zero reading: at least `steps` readings in a row of at least `mm`.
    # 1 mm or more every 15 minutes for 2 hours, or any non-zero amount for 12 hours, is more
    # likely a stuck gauge or filled data than rain.
    "repeat_runs": [[1.0, 8], [0.2, 48]],
    # A reading of at least this many mm straight after a gap of at least this many hours with
    # no readings: a gauge dumping the rain of its outage into one reading.
    "accumulation_mm": 5.0,
    "accumulation_gap_h": 24.0,
    # Treat the EA's Suspect flag as suspect here too.
    "use_ea_suspect": True,
    # A year whose rain falls more than this share in one hour of the day (UTC) is flagged:
    # 1/24 is even; real afternoon storms rarely exceed about 0.1.
    "hour_share_max": 0.2,
    # A year whose weekend share of rain is below or above these, as a ratio to 2/7, suggests
    # manual readings: fewer readings at weekends, or weekend rain booked to Monday.
    "weekend_ratio_range": [0.5, 1.5],
    # The hour and weekend patterns are judged only in years with at least this many wet
    # readings: a part-year with a handful of showers says nothing about a clock or a reader.
    "min_wet_readings": 200,
    # The whole-record weekly test: random rotations of each week's days give the spread of the
    # weekend and Monday ratios with no weekly pattern; a ratio is flagged when chance gives one
    # as far from 1 less often than `weekly_alpha`, and it is at least `weekly_min_effect` from 1.
    "weekly_permutations": 999,
    "weekly_alpha": 0.01,
    "weekly_min_effect": 0.1,
}

READING_COLUMNS = ["timestamp", "value", "rule", "flag"]
DAY_COLUMNS = ["date", "flag", "rules"]


def settings(cfg=None):
    """The QC settings: the defaults, with any given in the config's `qc` section."""
    out = dict(DEFAULTS)
    out.update((cfg or {}).get("qc", {}) or {})
    return out


def _rain_day(ts):
    day = pd.to_datetime(ts, utc=True) - pd.Timedelta(hours=RAIN_DAY_START_H)
    return day.dt.tz_localize(None).dt.normalize()


def _flagged(s, mask, rule, flag):
    out = s.loc[mask, ["timestamp", "value"]].copy()
    out["rule"] = rule
    out["flag"] = flag
    return out


def _runs(values, min_mm, min_steps):
    """A mask of the readings in runs of at least `min_steps` identical values >= `min_mm`."""
    v = values.to_numpy(dtype=float)
    keep = np.zeros(len(v), dtype=bool)
    if not len(v):
        return keep
    same = np.r_[False, (v[1:] == v[:-1]) & np.isfinite(v[1:])]
    run_id = np.cumsum(~same)
    sizes = np.bincount(run_id)
    big = (sizes[run_id] >= min_steps) & (v >= min_mm) & np.isfinite(v)
    keep[big] = True
    return keep


def check_readings(series, cfg=None):
    """Flags a gauge's readings. `series` has timestamp (UTC), value (mm) and, if known,
    supplier_flag. Returns one row per flag raised (a reading may raise several): timestamp,
    value, rule, flag."""
    st = settings(cfg)
    s = series.dropna(subset=["timestamp"]).reset_index(drop=True).copy()
    s["timestamp"] = pd.to_datetime(s["timestamp"], utc=True)
    s = s.sort_values("timestamp").reset_index(drop=True)
    on_grid = (s["timestamp"].dt.minute % 15 == 0) & (s["timestamp"].dt.second == 0)
    parts = [_flagged(s, ~on_grid, "off_grid", REJECTED),
             _flagged(s, s["value"] < 0, "negative", REJECTED)]
    g = s[on_grid].reset_index(drop=True)
    v = g["value"]
    parts.append(_flagged(g, v > st["reject_15min_mm"], "extreme_15min", REJECTED))
    parts.append(_flagged(g, (v > st["suspect_15min_mm"]) & (v <= st["reject_15min_mm"]),
                          "extreme_15min", SUSPECT))
    stuck = np.zeros(len(g), dtype=bool)
    for min_mm, min_steps in st["repeat_runs"]:
        stuck |= _runs(v, float(min_mm), int(min_steps))
    parts.append(_flagged(g, stuck, "repeated", SUSPECT))
    # Accumulation: the time since the previous reading with a value
    present = g[v.notna()]
    gap_h = present["timestamp"].diff().dt.total_seconds() / 3600
    dumped = (present["value"] >= st["accumulation_mm"]) & (gap_h >= st["accumulation_gap_h"])
    parts.append(_flagged(present, dumped, "accumulation", SUSPECT))
    if st["use_ea_suspect"] and "supplier_flag" in g:
        ea = g["supplier_flag"].astype("object").fillna("").astype(str).str.lower() == "suspect"
        parts.append(_flagged(g, ea, "ea_suspect", SUSPECT))
    parts = [p for p in parts if len(p)]
    if not parts:
        return pd.DataFrame(columns=READING_COLUMNS)
    return pd.concat(parts, ignore_index=True)[READING_COLUMNS]


def check_days(series, flagged, cfg=None):
    """Flags rain days: each day takes the worst flag of its readings, and a day whose total of
    on-grid readings is extreme is flagged itself. Returns flagged days only: date, flag, and
    the rules raised, separated by "; "."""
    st = settings(cfg)
    s = series.dropna(subset=["timestamp"]).copy()
    s["timestamp"] = pd.to_datetime(s["timestamp"], utc=True)
    on_grid = (s["timestamp"].dt.minute % 15 == 0) & (s["timestamp"].dt.second == 0)
    g = s[on_grid]
    totals = g["value"].fillna(0).groupby(_rain_day(g["timestamp"])).sum()
    rows = []
    if len(flagged):
        f = flagged.assign(date=_rain_day(flagged["timestamp"]))
        rows.append(f[["date", "rule", "flag"]])
    for limit, flag in ((st["reject_day_mm"], REJECTED), (st["suspect_day_mm"], SUSPECT)):
        hit = totals[totals > limit]
        if flag == SUSPECT:
            hit = hit[hit <= st["reject_day_mm"]]
        rows.append(pd.DataFrame({"date": hit.index, "rule": "extreme_day", "flag": flag}))
    rows = [r for r in rows if len(r)]
    if not rows:
        return pd.DataFrame(columns=DAY_COLUMNS)
    r = pd.concat(rows, ignore_index=True)
    out = r.groupby("date").agg(flag=("flag", "max"),
                                rules=("rule", lambda x: "; ".join(sorted(set(x)))))
    return out.reset_index()[DAY_COLUMNS]


def check_years(series, cfg=None):
    """One row per calendar year of the gauge's on-grid readings: readings, the tip resolution
    (the smallest amount making up at least 1% of wet readings) and the share of them that are whole
    multiples of it, the largest share of the year's rain in one hour of the day (UTC), the
    weekend share of rain as a ratio to 2/7, and flags: resolution_change (the resolution
    differs from the year before), hour_bias and weekend_bias."""
    st = settings(cfg)
    s = series.dropna(subset=["timestamp"]).copy()
    s["timestamp"] = pd.to_datetime(s["timestamp"], utc=True)
    s = s[(s["timestamp"].dt.minute % 15 == 0) & (s["timestamp"].dt.second == 0)]
    rows = []
    for year, y in s.groupby(s["timestamp"].dt.year):
        wet = y[y["value"] > 0]
        # The resolution: the smallest amount that makes up at least 1% of the wet readings,
        # so one odd small reading does not count as a change of instrument
        shares = wet["value"].round(3).value_counts(normalize=True).sort_index()
        common = shares[shares >= 0.01]
        res = float(common.index[0]) if len(common) else np.nan
        multiple = (np.abs(wet["value"] / res - np.round(wet["value"] / res)) < 1e-6).mean() \
            if len(wet) and res > 0 else np.nan
        total = wet["value"].sum()
        by_hour = wet["value"].groupby(wet["timestamp"].dt.hour).sum()
        hour_share = by_hour.max() / total if total > 0 else np.nan
        weekend = wet.loc[wet["timestamp"].dt.dayofweek >= 5, "value"].sum()
        weekend_ratio = (weekend / total) / (2 / 7) if total > 0 else np.nan
        rows.append({"year": int(year), "readings": len(y), "wet_readings": len(wet),
                     "total_mm": round(total, 1), "resolution_mm": res,
                     "share_multiples": round(float(multiple), 3) if pd.notna(multiple)
                     else np.nan,
                     "max_hour_share": round(float(hour_share), 3) if pd.notna(hour_share)
                     else np.nan,
                     "weekend_ratio": round(float(weekend_ratio), 2) if pd.notna(weekend_ratio)
                     else np.nan})
    out = pd.DataFrame(rows, columns=["year", "readings", "wet_readings", "total_mm",
                                      "resolution_mm", "share_multiples", "max_hour_share",
                                      "weekend_ratio"])
    lo, hi = st["weekend_ratio_range"]
    out["resolution_change"] = out["resolution_mm"].ne(out["resolution_mm"].shift()) & \
        out["resolution_mm"].shift().notna()
    enough = out["wet_readings"] >= st["min_wet_readings"]
    out["hour_bias"] = enough & (out["max_hour_share"] > st["hour_share_max"])
    out["weekend_bias"] = enough & ((out["weekend_ratio"] < lo) | (out["weekend_ratio"] > hi))
    return out


WEEKDAYS = ("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday")
WEEKLY_TEST = ("weekend_ratio_all", "weekend_p", "monday_ratio", "monday_p",
               "weekday_wet_ratio", "weekly_pattern")


def _daily_by_calendar_day(series):
    """On-grid readings summed by calendar day (UTC): rain and the number of wet readings."""
    s = series.dropna(subset=["timestamp"]).copy()
    s["timestamp"] = pd.to_datetime(s["timestamp"], utc=True)
    s = s[(s["timestamp"].dt.minute % 15 == 0) & (s["timestamp"].dt.second == 0)]
    s["day"] = s["timestamp"].dt.tz_convert(None).dt.normalize()
    s["wet"] = s["value"] > 0
    return s.groupby("day").agg(rain=("value", "sum"), wet=("wet", "sum"))


def _week_ratios(rain, weekday):
    """The weekend ratio (weekend share of rain over weekend share of days) and the Monday ratio
    (likewise for Mondays)."""
    total = rain.sum()
    weekend = weekday >= 5
    monday = weekday == 0
    if total <= 0 or not weekend.any() or not monday.any():
        return np.nan, np.nan
    return ((rain[weekend].sum() / total) / weekend.mean(),
            (rain[monday].sum() / total) / monday.mean())


def check_weekly(series, cfg=None, seed=1):
    """The gauge's whole record by day of the week. Real rain has no weekly rhythm, so one in
    the record points to how the gauge was read or kept: weekend rain booked to Monday (a manual
    or storage gauge read on weekdays), weekends filled in afterwards, or spurious small tips on
    weekdays from maintenance visits.

    Returns the profile, one row a weekday: days, rain, rain_ratio (its share of the rain over
    its share of the days; 1 is even), wet readings, wet_ratio (likewise) and the mean wet
    reading; with the whole-record test in the columns of WEEKLY_TEST, the same on every row:
    weekend_ratio_all and monday_ratio, each with its p-value against chance, and
    weekday_wet_ratio (wet readings a weekday over wet readings a weekend day).

    Chance is judged by rotating each complete week's seven days by a random amount, a
    `weekly_permutations` times: storms that last several days stay together, as they do in the
    weather, while any tie to the day of the week is broken. Shuffling single days instead would
    make chance look too tidy and flag too many gauges. A p-value is the share of rotations whose
    ratio lies as far from 1 as the gauge's, or further. `weekly_pattern` is true when either
    ratio's p-value is below `weekly_alpha` and the ratio is at least `weekly_min_effect` from 1."""
    st = settings(cfg)
    d = _daily_by_calendar_day(series)
    columns = ["weekday", "day", "days", "rain_mm", "rain_ratio", "wet_readings", "wet_ratio",
               "mean_wet_mm", *WEEKLY_TEST]
    if not len(d) or d["rain"].sum() <= 0:
        return pd.DataFrame(columns=columns)
    weekday = d.index.dayofweek.to_numpy()
    days = np.bincount(weekday, minlength=7)
    rain = np.bincount(weekday, weights=d["rain"].to_numpy(), minlength=7)
    wet = np.bincount(weekday, weights=d["wet"].to_numpy(), minlength=7)
    share_days = np.where(days > 0, days / days.sum(), np.nan)
    profile = pd.DataFrame({
        "weekday": np.arange(7), "day": WEEKDAYS, "days": days, "rain_mm": rain.round(1),
        "rain_ratio": np.round((rain / rain.sum()) / share_days, 3),
        "wet_readings": wet.astype(int),
        "wet_ratio": np.round((wet / max(wet.sum(), 1)) / share_days, 3),
        "mean_wet_mm": np.round(np.where(wet > 0, rain / np.maximum(wet, 1), np.nan), 3),
    })
    weekend_obs, monday_obs = _week_ratios(d["rain"].to_numpy(), weekday)
    if not (np.isfinite(weekend_obs) and np.isfinite(monday_obs)):
        weekend_obs = monday_obs = np.nan
    # Complete weeks, Monday to Sunday, for the rotations
    full = pd.Series(d["rain"].to_numpy(), index=d.index).asfreq("D")
    start = full.index[0] + pd.Timedelta(days=(7 - full.index[0].dayofweek) % 7)
    weeks = full[start:]
    n_weeks = len(weeks) // 7
    block = weeks.to_numpy()[:n_weeks * 7].reshape(n_weeks, 7)
    block = block[np.isfinite(block).all(axis=1)]
    p_weekend = p_monday = np.nan
    if len(block) >= 10 and np.isfinite(weekend_obs):
        rng = np.random.default_rng(seed)
        wk = np.tile(np.arange(7), len(block))
        far_w = far_m = 0
        n = st["weekly_permutations"]
        for _ in range(n):
            shift = rng.integers(0, 7, len(block))
            idx = (np.arange(7)[None, :] + shift[:, None]) % 7
            rotated = np.take_along_axis(block, idx, axis=1).ravel()
            w, m = _week_ratios(rotated, wk)
            far_w += abs(w - 1) >= abs(weekend_obs - 1) - 1e-12
            far_m += abs(m - 1) >= abs(monday_obs - 1) - 1e-12
        p_weekend = (1 + far_w) / (n + 1)
        p_monday = (1 + far_m) / (n + 1)
    weekend = weekday >= 5
    wet_day = d["wet"].to_numpy()
    weekday_wet = (wet_day[~weekend].mean() / wet_day[weekend].mean()
                   if weekend.any() and wet_day[weekend].mean() > 0 else np.nan)
    pattern = any(np.isfinite(p) and p < st["weekly_alpha"]
                  and abs(r - 1) >= st["weekly_min_effect"]
                  for p, r in ((p_weekend, weekend_obs), (p_monday, monday_obs)))
    test = {"weekend_ratio_all": round(float(weekend_obs), 3) if np.isfinite(weekend_obs)
            else np.nan,
            "weekend_p": p_weekend,
            "monday_ratio": round(float(monday_obs), 3) if np.isfinite(monday_obs) else np.nan,
            "monday_p": p_monday,
            "weekday_wet_ratio": round(float(weekday_wet), 3) if np.isfinite(weekday_wet)
            else np.nan,
            "weekly_pattern": bool(pattern)}
    for k, v in test.items():
        profile[k] = v
    return profile[columns]


def gauge_summary(gauge_id, flagged, days, years, weekly=None):
    """One row for the gauge: readings flagged by each rule, days suspect and rejected, the
    years with a resolution change, an hour-of-day or a weekend bias, and the whole-record
    weekly test (check_weekly) when given."""
    counts = flagged["rule"].value_counts() if len(flagged) else pd.Series(dtype=int)
    row = {"gauge_id": gauge_id,
           "first_year": int(years["year"].min()) if len(years) else None,
           "last_year": int(years["year"].max()) if len(years) else None,
           "days_suspect": int((days["flag"] == SUSPECT).sum()) if len(days) else 0,
           "days_rejected": int((days["flag"] == REJECTED).sum()) if len(days) else 0}
    for rule in ("off_grid", "negative", "extreme_15min", "repeated", "accumulation",
                 "ea_suspect"):
        row[f"n_{rule}"] = int(counts.get(rule, 0))
    for col in ("resolution_change", "hour_bias", "weekend_bias"):
        yrs = years.loc[years[col], "year"].tolist() if len(years) else []
        row[f"{col}_years"] = " ".join(str(y) for y in yrs)
    for col in WEEKLY_TEST:
        row[col] = weekly[col].iloc[0] if weekly is not None and len(weekly) else np.nan
    return row
