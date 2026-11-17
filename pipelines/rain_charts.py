# ============================================================
# Tool:         rain charts
# Description:  Gauge locations, HadUK-Grid extraction to the store, and the monthly
#               accumulation charts for a gauge, a catchment or an area, with a table of
#               where every gauge's month stands against its record and its neighbours
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-17
# Tier:         1
# Inputs:       The Bronze rain store; HadUK-Grid daily rainfall; outlines in GeoJSON
# Outputs:      register/rain_gauge_locations.csv; gold/haduk/rainfall_daily/<year>.parquet;
#               PNG charts under gold/charts/accumulation/<YYYY-MM>/; the monthly table
# Dependencies: numpy, pandas, pyarrow, matplotlib, xarray, netCDF4
# ============================================================
"""Monthly rainfall accumulation charts, and the extraction they draw their history from."""

import calendar
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from pipelines import accumulation as acc
from pipelines import areas as ar
from pipelines import haduk, store
from pipelines import hydrology_api as api

# ---- Paths -----------------------------------------------------------------------------------


def _paths(cfg):
    root = cfg["store_root"]
    return {"locations": f"{root}/register/rain_gauge_locations.csv",
            "haduk": f"{root}/gold/haduk/rainfall_daily",
            "charts": f"{root}/gold/charts/accumulation"}


def _settings(cfg):
    a = cfg.get("accumulation", {}) or {}
    return {"normal": (int(a.get("normal_start", 1991)), int(a.get("normal_end", 2020))),
            "min_years": int(a.get("min_years", 20)),
            "min_complete": float(a.get("min_complete", 0.95))}


# ---- Gauge locations ---------------------------------------------------------------------------


def update_locations(cfg, session=None):
    """Every rain station's position from the Hydrology API, written to the register."""
    loc = pd.DataFrame(api.rain_stations(session))
    store.write_csv(loc, _paths(cfg)["locations"])
    return loc


def load_locations(cfg):
    path = _paths(cfg)["locations"]
    if not Path(path).exists():
        raise FileNotFoundError(f"No {path}: run 06_haduk_extract (or update_locations) first")
    loc = pd.read_csv(path, dtype={"wiski_id": str})
    return loc.drop_duplicates("wiski_id").set_index("wiski_id")


def gauge_choices(cfg):
    """The gauges with records in the store, as "id - name" sorted by name, for a selector."""
    reg = store.read_registry(cfg["registry_path"]) if "registry_path" in cfg else None
    ids = (reg.loc[reg["backfill_done"].fillna(False).astype(bool), "gauge_id"].astype(str)
           if reg is not None else pd.Series(dtype=str))
    try:
        names = load_locations(cfg)["label"]
    except FileNotFoundError:
        names = pd.Series(dtype=str)
    rows = [(str(names.get(g, "") or g).strip(), g) for g in ids]
    return [f"{g} - {n}" for n, g in sorted(rows)]


def read_choices(get):
    """The chart choices from the notebook's widgets (`get` is dbutils.widgets.get): chart,
    gauge and area ids, year, first month, number of months and whether to build the table."""
    first_month, n_months = PERIODS[get("period")]
    return {"chart": get("chart"), "gauge": choice_id(get("gauge")),
            "area": choice_id(get("area")), "year": int(get("year")),
            "month": first_month or list(calendar.month_name).index(get("month")),
            "n_months": n_months, "table": get("table") == "Yes"}


def choice_id(choice):
    """The gauge id from a selector choice such as "000008 - Harley"."""
    return str(choice).split(" - ", 1)[0].strip()


# ---- Areas -------------------------------------------------------------------------------------


def load_areas(cfg):
    """The areas in the config: {id: {"name", "cells"}}, cells the 1 km cell centres inside."""
    out = {}
    for a in cfg.get("areas", []) or []:
        polygons = ar.read_outline(a["geojson"])
        out[str(a["id"])] = {"name": a.get("name", a["id"]), "cells": ar.cells_in(polygons)}
    return out


# ---- HadUK-Grid extraction ---------------------------------------------------------------------


def run_haduk_extract(cfg, refresh=False):
    """Extracts HadUK-Grid daily rainfall, a year at a time from `haduk.start_year`, for every
    gauge's cell and every configured area, into one Parquet file a year. Years already
    extracted are skipped, except the latest year in the files (it may still be growing) and
    all of them with `refresh`. Returns the years written."""
    h = cfg.get("haduk", {}) or {}
    files = haduk.find_files(h["root"])
    if not files:
        raise FileNotFoundError(f"No HadUK-Grid daily rainfall files under {h['root']}")
    start = int(h.get("start_year", 1961))
    years = sorted({d.year for d, _ in files if d.year >= start})
    loc = load_locations(cfg)
    points = {g: (r.easting, r.northing) for g, r in loc.iterrows()}
    areas = {k: v["cells"] for k, v in load_areas(cfg).items()}
    out_dir = _paths(cfg)["haduk"]
    written = []
    for year in years:
        path = f"{out_dir}/{year}.parquet"
        if Path(path).exists() and not refresh and year != years[-1]:
            continue
        df = haduk.extract_year(files, year, points, areas)
        store.write_parquet(df, path)
        written.append(year)
        print(f"HadUK-Grid {year}: {df['date'].nunique()} days, {df['id'].nunique()} series",
              flush=True)
    return written


def read_haduk(cfg, kind, key):
    """The extracted HadUK-Grid daily series for one gauge or area: date, rain_mm."""
    folder = Path(_paths(cfg)["haduk"])
    files = sorted(folder.glob("*.parquet")) if folder.exists() else []
    if not files:
        return pd.DataFrame(columns=["date", "rain_mm"])
    parts = [pq.read_table(f, filters=[("kind", "=", kind), ("id", "=", str(key))],
                           columns=["date", "rain_mm"]).to_pandas() for f in files]
    out = pd.concat(parts, ignore_index=True)
    out["date"] = pd.to_datetime(out["date"])
    return out.sort_values("date").reset_index(drop=True)


# ---- Gauge rain ----------------------------------------------------------------------------------


def _daily_path(cfg, gauge_id):
    return f"{cfg['store_root']}/gold/rain_daily/{gauge_id}.parquet"


def gauge_daily(cfg, gauge_id, since=None, files=None):
    """A gauge's daily totals on the 09:00 UTC rain day: date, rain_mm, complete (the share of
    the day's 96 readings present) and off_grid (readings left out). Days with less than
    `accumulation.min_complete` of their readings get no total.

    Read from a saved daily table, gold/rain_daily/<gauge>.parquet, about 11,000 rows for 30
    years, rather than from the million or so 15-minute readings behind it. The table is
    rebuilt from the Bronze store whenever any of the gauge's Bronze files is newer than it,
    so it never lags the readings. `files`, the gauge's list from store.file_index(), saves
    listing the store again when visiting many gauges."""
    root = cfg["store_root"]
    files = store.series_files(root, gauge_id) if files is None else files
    if not files:
        return acc.daily_0900(pd.DataFrame(columns=["timestamp", "value"]))
    path = Path(_daily_path(cfg, gauge_id))
    newest = max(f.stat().st_mtime for f in files)
    if path.exists() and path.stat().st_mtime >= newest:
        daily = pq.read_table(path).to_pandas()
    else:
        # Every day with its completeness; the threshold is applied on reading, so changing
        # it needs no rebuild
        daily = acc.daily_0900(store.read_series(root, gauge_id, files=files),
                               min_complete=0.0)
        store.write_parquet(daily, str(path))
    daily["date"] = pd.to_datetime(daily["date"])
    daily.loc[daily["complete"] < _settings(cfg)["min_complete"], "rain_mm"] = np.nan
    if since is not None:
        cut = pd.Timestamp(since).tz_localize(None) if pd.Timestamp(since).tzinfo else since
        daily = daily[daily["date"] >= pd.Timestamp(cut).normalize() - pd.Timedelta(days=1)]
    daily = daily.reset_index(drop=True)
    daily.attrs["off_grid"] = int(daily["off_grid"].sum()) if "off_grid" in daily else 0
    return daily


def refresh_daily(cfg, gauges=None):
    """Brings every gauge's saved daily table up to date with its Bronze files; run after
    each update (05_incremental does). Returns how many tables were rebuilt."""
    ids = gauges if gauges is not None else [
        str(g) for g in store.read_registry(cfg["registry_path"])["gauge_id"]]
    index = store.file_index(cfg["store_root"])
    rebuilt = 0
    for k, g in enumerate(ids, start=1):
        path = Path(_daily_path(cfg, g))
        before = path.stat().st_mtime if path.exists() else None
        gauge_daily(cfg, g, files=index.get(g, []))
        after = path.stat().st_mtime if path.exists() else None
        rebuilt += after is not None and after != before
        if k % 100 == 0:
            print(f"{k}/{len(ids)} gauges checked, {rebuilt} daily tables rebuilt", flush=True)
    return rebuilt


# The periods the charts offer, as (first month, number of months); None for the first month
# means the month chosen.
PERIODS = {"Month": (None, 1), "3 months": (None, 3), "6 months": (None, 6),
           "12 months": (None, 12), "Water year (Oct-Sep)": (10, 12),
           "Calendar year": (1, 12)}


def period_label(year, month, n_months=1):
    """A period's name: "October 2026", "Water year 2025-26 (Oct-Sep)", "2026", or
    "Dec 2025 to Feb 2026"."""
    start = pd.Timestamp(year=year, month=month, day=1)
    end = start + pd.DateOffset(months=n_months) - pd.Timedelta(days=1)
    if n_months == 1:
        return f"{calendar.month_name[month]} {year}"
    if n_months == 12 and month == 10:
        return f"Water year {year}-{str(year + 1)[2:]} (Oct-Sep)"
    if n_months == 12 and month == 1:
        return str(year)
    return f"{start:%b %Y} to {end:%b %Y}"


def _month_bounds(year, month, n_months=1):
    start = pd.Timestamp(year=year, month=month, day=1)
    return start, start + pd.DateOffset(months=n_months)


def _this_month(daily, year, month, n_months=1):
    m = acc.period_matrix(daily, month, n_months)
    return m.loc[year] if year in m.index else pd.Series(dtype=float)


# ---- Charts ----------------------------------------------------------------------------------


def _save_path(cfg, year, month, name, n_months=1):
    folder = Path(_paths(cfg)["charts"]) / (f"{year}-{month:02d}"
                                            + (f"_{n_months}m" if n_months > 1 else ""))
    folder.mkdir(parents=True, exist_ok=True)
    return str(folder / f"{name}.png")


def _summary(kind, key, name, current, ref, history):
    last = current.last_valid_index()
    total = current[last] if last is not None else np.nan
    mean = ref["mean"].get(last, np.nan) if last is not None else np.nan
    return {"kind": kind, "id": key, "name": name, "days": last, "total_mm": total,
            "mean_mm": mean, "share_of_mean": total / mean if mean else np.nan,
            "standing": acc.standing(total, ref.loc[last]) if last is not None else "no data",
            "history": history, "years": ref.attrs["years"]}


def gauge_chart(cfg, gauge_id, year, month, save=True, n_months=1):
    """The accumulation chart for one gauge. The reference lines come from the gauge's own
    record if it has at least `min_years` complete months, else from HadUK-Grid at the gauge's
    cell; the HadUK-Grid cell's own month is drawn dashed where it exists, as a check on the
    gauge. Returns (figure, summary)."""
    s = _settings(cfg)
    daily = gauge_daily(cfg, gauge_id)
    matrix = acc.period_matrix(daily, month, n_months)
    ref = acc.reference_lines(matrix, s["normal"], exclude=year)
    history = "gauge record"
    cell = read_haduk(cfg, "gauge", gauge_id)
    if ref.attrs["years"] < s["min_years"] and len(cell):
        ref = acc.reference_lines(acc.period_matrix(cell, month, n_months), s["normal"],
                                  exclude=year)
        history = "HadUK-Grid at the gauge's 1 km cell"
    current = matrix.loc[year] if year in matrix.index else pd.Series(dtype=float)
    cell_now = _this_month(cell, year, month, n_months) if len(cell) else None
    try:
        name = load_locations(cfg).loc[str(gauge_id), "label"]
    except (FileNotFoundError, KeyError):
        name = str(gauge_id)
    first = ref.attrs["first_year"]
    dropped = daily.attrs.get("off_grid", 0)
    source = (f"Gauge {gauge_id}, EA Hydrology API, 15-minute totals"
              + (f" ({dropped} readings off the 15-minute grid left out)" if dropped else "")
              + ". Reference lines: "
              f"{history}, {ref.attrs['years']} complete years from {first}; mean "
              f"{s['normal'][0]}-{s['normal'][1]} ({ref.attrs['normal_years']} years)")
    title = f"Rainfall amount - {period_label(year, month, n_months)} - {name}"
    fig = acc.accumulation_chart(
        current, ref, title, source, s["normal"],
        _save_path(cfg, year, month, f"gauge_{gauge_id}", n_months) if save else None,
        reference=cell_now if cell_now is not None and len(cell_now) else None,
        reference_label="HadUK-Grid, same period",
        period_start=_month_bounds(year, month)[0])
    return fig, _summary("gauge", gauge_id, name, current, ref, history)


def area_daily_from_gauges(cfg, cells, year, month, locations=None, n_months=1):
    """An area's daily rain for a period from the gauges, by Thiessen weights over its 1 km
    cells, the weights of gauges with no reading on a day shared among those that have one
    (the operational rule). Returns date, rain_mm and the weights used."""
    loc = locations if locations is not None else load_locations(cfg)
    weights = ar.thiessen_weights(cells, {g: (r.easting, r.northing) for g, r in loc.iterrows()})
    start, end = _month_bounds(year, month, n_months)
    since = (start - pd.Timedelta(days=1)).tz_localize("UTC")
    cols = {}
    for g in weights:
        d = gauge_daily(cfg, g, since=since)
        d = d[(d["date"] >= start) & (d["date"] < end)]
        cols[g] = d.set_index("date")["rain_mm"]
    days = pd.date_range(start, end - pd.Timedelta(days=1))
    table = pd.DataFrame(cols).reindex(days)
    w = pd.Series(weights).reindex(table.columns)
    present = table.notna()
    weight_present = present.mul(w, axis=1).sum(axis=1)
    rain = table.fillna(0).mul(w, axis=1).sum(axis=1) / weight_present.replace(0, np.nan)
    out = pd.DataFrame({"date": days, "rain_mm": rain.to_numpy()})
    return out, weights


def area_chart(cfg, area_id, year, month, save=True, n_months=1):
    """The accumulation chart for a configured catchment or area. Reference lines from
    HadUK-Grid over the area; the month itself from HadUK-Grid where the files cover it, else
    from the gauges by Thiessen weights over the area's cells. Returns (figure, summary)."""
    s = _settings(cfg)
    area = load_areas(cfg)[str(area_id)]
    hist = read_haduk(cfg, "area", area_id)
    if not len(hist):
        raise ValueError(f"No HadUK-Grid series for area {area_id}: run 06_haduk_extract")
    matrix = acc.period_matrix(hist, month, n_months)
    ref = acc.reference_lines(matrix, s["normal"], exclude=year)
    if year in matrix.index and matrix.loc[year].notna().any():
        current, now_from = matrix.loc[year], "HadUK-Grid"
    else:
        daily, weights = area_daily_from_gauges(cfg, area["cells"], year, month,
                                                n_months=n_months)
        current = _this_month(daily, year, month, n_months)
        now_from = (f"{len(weights)} EA gauge{'s' if len(weights) != 1 else ''} by Thiessen "
                    "weights")
    source = (f"This period: {now_from}. Reference lines: HadUK-Grid over {len(area['cells'])} "
              f"cells, {ref.attrs['years']} complete years from {ref.attrs['first_year']}; "
              f"mean {s['normal'][0]}-{s['normal'][1]}")
    title = f"Rainfall amount - {period_label(year, month, n_months)} - {area['name']}"
    fig = acc.accumulation_chart(current, ref, title, source, s["normal"],
                                 _save_path(cfg, year, month, f"area_{area_id}", n_months)
                                 if save else None,
                                 period_start=_month_bounds(year, month)[0])
    return fig, _summary("area", area_id, area["name"], current, ref, now_from)


# ---- Every gauge in one table --------------------------------------------------------------


def month_table(cfg, year, month, neighbours=5, n_months=1):
    """Where every gauge's month stands: its total, the share of its mean, its standing, and
    its rank among its own years (0 driest to 1 wettest) beside the median rank of its
    `neighbours` nearest gauges. A gauge far from its neighbours (`odd_one_out`, ranks more
    than 0.6 apart) has more likely failed than met unusual weather."""
    s = _settings(cfg)
    loc = load_locations(cfg)
    index = store.file_index(cfg["store_root"])
    rows = []
    for g in loc.index:
        if g not in index:
            continue
        try:
            m = acc.period_matrix(gauge_daily(cfg, g, files=index[g]), month, n_months)
        except Exception:  # noqa: BLE001 - a gauge with no store is simply left out
            continue
        if year not in m.index:
            continue
        ref = acc.reference_lines(m, s["normal"], exclude=year)
        cur = m.loc[year]
        last = cur.last_valid_index()
        if last is None or ref.attrs["years"] < min(5, s["min_years"]):
            continue
        others = m.drop(index=year)[last].dropna()
        total = cur[last]
        rows.append({"gauge_id": g, "name": loc.at[g, "label"], "days": last,
                     "total_mm": round(total, 1),
                     "share_of_mean": round(total / ref.at[last, "mean"], 2)
                     if ref.at[last, "mean"] else np.nan,
                     "standing": acc.standing(total, ref.loc[last]),
                     "rank": round(float((others < total).mean()), 2),
                     "years": ref.attrs["years"]})
    table = pd.DataFrame(rows)
    if not len(table):
        return table
    xy = loc.loc[table["gauge_id"], ["easting", "northing"]].to_numpy(dtype=float)
    d2 = ((xy[:, None, :] - xy[None, :, :]) ** 2).sum(axis=2)
    np.fill_diagonal(d2, np.inf)
    near = np.argsort(d2, axis=1)[:, :neighbours]
    table["neighbour_rank"] = np.round(np.median(table["rank"].to_numpy()[near], axis=1), 2)
    table["odd_one_out"] = (table["rank"] - table["neighbour_rank"]).abs() > 0.6
    return table.sort_values("rank").reset_index(drop=True)
