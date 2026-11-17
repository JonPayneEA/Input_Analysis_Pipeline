# ============================================================
# Tool:         rainfall accumulation
# Description:  Daily totals on the 09:00 UTC rain day, each month's running total day by
#               day, the reference lines from earlier years (the normal-period mean, the 5,
#               10, 90 and 95% lines, the lowest and the highest), and the accumulation chart
#               in the style the Met Office publishes
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-17
# Tier:         1
# Inputs:       15-minute gauge readings, or daily series (HadUK-Grid)
# Outputs:      Data frames of running totals and reference lines; PNG charts
# Dependencies: numpy, pandas, matplotlib
# ============================================================
"""Monthly rainfall accumulation against the record.

The rain day runs from 09:00 UTC to 09:00 UTC the next day and is labelled by the date it
starts, as in HadUK-Grid and Met Office daily rainfall. A day counts only if at least
`min_complete` of its readings are present; a month counts towards the reference lines only
if every one of its days counts, so a gap is never mistaken for a dry spell.
"""

import calendar

import numpy as np
import pandas as pd

STEPS_PER_DAY = 96          # 15-minute readings
RAIN_DAY_START_H = 9        # 09:00 UTC


def daily_0900(series, min_complete=0.95):
    """Daily totals from 15-minute readings (`timestamp` UTC, `value` mm): `date` (the day the
    09:00 UTC rain day starts), `rain_mm`, and `complete`, the share of the 96 readings
    present. Days below `min_complete` get no total."""
    s = series.dropna(subset=["timestamp"])
    day = s["timestamp"] - pd.Timedelta(hours=RAIN_DAY_START_H)
    if day.dt.tz is not None:
        day = day.dt.tz_convert("UTC").dt.tz_localize(None)
    day = day.dt.normalize()
    present = s["value"].notna()
    out = pd.DataFrame({
        "rain_mm": s["value"].where(present, 0.0).groupby(day).sum(),
        "complete": present.groupby(day).sum() / STEPS_PER_DAY,
    })
    out.loc[out["complete"] < min_complete, "rain_mm"] = np.nan
    out.index.name = "date"
    return out.reset_index()


def month_matrix(daily, month):
    """Running totals within `month` for every year: one row per year, one column per day of
    the month (1 to 31; NaN beyond the month's length). A year keeps only the days up to its
    first missing day, so a gap ends its line rather than flattening it."""
    d = daily.copy()
    d["date"] = pd.to_datetime(d["date"])
    d = d[d["date"].dt.month == month]
    rows = {}
    for year, g in d.groupby(d["date"].dt.year):
        n = calendar.monthrange(year, month)[1]
        vals = g.set_index(g["date"].dt.day)["rain_mm"].reindex(range(1, n + 1))
        cum = vals.cumsum()
        first_gap = np.flatnonzero(vals.isna().to_numpy())
        if len(first_gap):
            cum.iloc[first_gap[0]:] = np.nan
        rows[year] = cum.reindex(range(1, 32))
    out = pd.DataFrame(rows, index=range(1, 32)).T.sort_index()
    out.attrs["month"] = month
    return out


def reference_lines(matrix, normal=(1991, 2020), exclude=None):
    """The reference lines by day of month, from the years whose month is complete (except
    `exclude`, the year being plotted): `mean` over the normal period, the 5, 10, 90 and 95%
    lines and the lowest and highest over all complete years. Also `years` (all complete
    years) and `normal_years`."""
    m = matrix.drop(index=[exclude], errors="ignore")
    month = matrix.attrs.get("month")
    # A year is complete when its last day (28, 29, 30 or 31) has a running total
    lengths = [calendar.monthrange(y, month)[1] if month else 31 for y in m.index]
    full = m[[pd.notna(m.at[y, n]) for y, n in zip(m.index, lengths, strict=True)]]
    in_normal = full[(full.index >= normal[0]) & (full.index <= normal[1])]
    # A record that misses the normal period entirely takes its mean over all its years
    mean_from = in_normal if len(in_normal) else full
    days = full.columns
    with np.errstate(all="ignore"):
        out = pd.DataFrame({
            "mean": mean_from.mean(),
            "p5": full.quantile(0.05), "p10": full.quantile(0.10),
            "p90": full.quantile(0.90), "p95": full.quantile(0.95),
            "lowest": full.min(), "highest": full.max(),
        }, index=days)
    out.attrs["years"] = len(full)
    out.attrs["normal_years"] = len(in_normal)
    out.attrs["mean_label"] = (f"{normal[0]}-{normal[1]} mean" if len(in_normal)
                               else f"Mean of {len(full)} years")
    out.attrs["first_year"] = int(full.index.min()) if len(full) else None
    return out


def accumulation_chart(current, ref, title, source, normal=(1991, 2020), path=None,
                       reference=None, reference_label=None):
    """The chart: grey bands for 5 to 95% and 10 to 90%, the normal mean, the lowest and
    highest, and the month so far, shaded blue where above the mean and orange where below.
    `current` is a running total by day of month; `reference`, an optional second running
    total drawn dashed (HadUK-Grid at the gauge's cell, say). Saves to `path` if given and
    returns the figure."""
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    n = int(ref["mean"].last_valid_index() or len(current))
    days = np.arange(1, n + 1)
    r = ref.loc[days]
    fig, ax = plt.subplots(figsize=(11, 6.5), dpi=150)
    ax.fill_between(days, r["p5"], r["p95"], color="0.85", lw=0, label="5% to 95%")
    ax.fill_between(days, r["p10"], r["p90"], color="0.7", lw=0, label="10% to 90%")
    ax.plot(days, r["lowest"], color="#6fb3b8", lw=1, label="Lowest")
    ax.plot(days, r["highest"], color="#d9a07a", lw=1, label="Highest")
    ax.plot(days, r["mean"], color="0.25", lw=1.6,
            label=ref.attrs.get("mean_label", f"{normal[0]}-{normal[1]} mean"))
    cur = current.reindex(days)
    upto = cur.notna().to_numpy()
    if upto.any():
        dd, cc, mm = days[upto], cur[upto].to_numpy(), r["mean"].to_numpy()[upto]
        ax.fill_between(dd, cc, mm, where=cc >= mm, interpolate=True, color="#2aa6b8",
                        alpha=0.85, lw=0)
        ax.fill_between(dd, cc, mm, where=cc < mm, interpolate=True, color="#e07b39",
                        alpha=0.85, lw=0)
        ax.plot(dd, cc, color="#1b2a6b", lw=2.2, label="This month")
    if reference is not None:
        ref_cur = reference.reindex(days)
        ax.plot(days, ref_cur, color="0.1", lw=1.2, ls="--", label=reference_label)
    ax.set_xlim(0.8, n + 0.2)
    ax.set_ylim(0, None)
    ax.set_xticks(days)
    ax.tick_params(axis="x", labelrotation=90, labelsize=8)
    ax.set_xlabel("Day of month (rain day starting 09:00 UTC)")
    ax.set_ylabel("Rainfall amount (mm)")
    ax.set_title(title, fontweight="bold")
    ax.grid(color="0.9", lw=0.6)
    ax.set_axisbelow(True)
    from matplotlib.patches import Patch
    handles, labels = ax.get_legend_handles_labels()
    if upto.any():
        handles += [Patch(color="#2aa6b8"), Patch(color="#e07b39")]
        labels += ["Above the mean", "Below the mean"]
    ax.legend(handles, labels, loc="upper center", bbox_to_anchor=(0.5, -0.16), ncol=5,
              frameon=False, fontsize=8)
    fig.tight_layout(rect=(0, 0.05, 1, 1))
    fig.text(0.01, 0.01, source, fontsize=7.5, color="0.35", wrap=True)
    if path is not None:
        fig.savefig(path)
    return fig


def standing(current_total, ref):
    """Where a month's total stands against the reference lines at the same day: one of
    "lowest on record", "below 5%", "below 10%", "normal range", "above 90%", "above 95%",
    "highest on record"."""
    if pd.isna(current_total):
        return "incomplete"
    if pd.notna(ref["lowest"]) and current_total < ref["lowest"]:
        return "lowest on record"
    if pd.notna(ref["highest"]) and current_total > ref["highest"]:
        return "highest on record"
    for key, label in (("p5", "below 5%"), ("p10", "below 10%")):
        if pd.notna(ref[key]) and current_total < ref[key]:
            return label
    for key, label in (("p95", "above 95%"), ("p90", "above 90%")):
        if pd.notna(ref[key]) and current_total > ref[key]:
            return label
    return "normal range"
