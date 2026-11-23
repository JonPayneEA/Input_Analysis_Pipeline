# ============================================================
# Tool:         CAMELS-GB v2 preparation
# Description:  Turns CAMELS-GB v2 into model input: one hourly table per catchment (rain,
#               potential evaporation spread over daylight hours, temperature, flow and its
#               flags), the catchment descriptors, the flow signatures kept apart from them,
#               the hydrometry, and a screening table saying which catchments suit the PDM
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-18
# Tier:         1
# Inputs:       The CAMELS-GB v2 folder (camels.root in the config): hourly and daily
#               hydro-meteorological CSVs and the catchment attribute CSVs
# Outputs:      gold/camels_gb_v2/hourly/<gauge>.parquet, with every evaporation product;
#               descriptors.parquet, signatures.parquet, hydrometry.parquet and screening.csv
#               beside them
# Dependencies: numpy, pandas, pyarrow
# ============================================================
"""CAMELS-GB v2 (Coxon et al., 2026; Open Government Licence v3) as PDM input.

The hourly files give rain and flow; potential evaporation and temperature are daily only, in
the daily files. A day's evaporation is spread over its daylight hours as a half sine, from
sunrise to sunset at the gauge's latitude: none at night, most at midday, the day's total kept
exactly. The temperature of the day is given to each of its hours.

The attribute tables split three ways. Descriptors (topography, climate, soils, hydrogeology,
land cover, human influence) can be known for an ungauged catchment, so a model of parameters
may use them. Signatures (mean flow, baseflow index, runoff ratio, flow percentiles) come from
the gauged flow itself: a model trained on them would learn from information an ungauged
catchment does not have, so they are kept apart, for checking only. Hydrometry (record
completeness, the highest gauged flow, flow uncertainty) says how far to trust each record.

Times are taken as UTC, the hour labelled by its start, as in the files (the first hour is
1990-10-01 09:00). The daily evaporation of a date is spread over that calendar day.
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

from pipelines import store

DEFAULTS = {
    "pet": "pet_hydrope",              # daily evaporation column: pet_chess, peti_chess,
                                       # pet_hydrope or peti_hydrope (i: with interception)
    "temperature": "temperature_chess",  # or temperature_haduk
    "min_hourly_complete": 80.0,       # hourly flow record at least this complete, per cent
    "max_frac_snow": 0.25,             # share of precipitation falling as snow; above it, out
    "pack_frac_snow": 0.05,            # from this share, PACK runs ahead of the PDM
    "max_reservoir_capacity": 0.2,     # reservoir capacity over mean annual runoff volume
    "max_urban": 30.0,                 # urban area, per cent; above it, out
    "max_runoff_ratio": 2.0,           # more flow than rain beyond what springs can bring in
    "max_high_perm": 50.0,             # area on highly productive aquifers, per cent: above
    "groundwater_bfi": 0.65,           # it, or at this baseflow index, groundwater-fed
    "losing_runoff_ratio": 0.35,       # groundwater-fed under this runoff ratio: abstraction
    "rain_low_runoff_ratio": 0.95,     # not groundwater-fed from this runoff ratio: free fc
}

DESCRIPTOR_TABLES = ("topographic", "climatic", "soil", "hydrogeology", "landcover",
                     "humaninfluence")
SIGNATURE_TABLES = ("hydrologic",)
HYDROMETRY_TABLES = ("hydrometry",)

PET_PRODUCTS = ("pet_chess", "peti_chess", "pet_hydrope", "peti_hydrope")

HOURLY = re.compile(r"hydromet_hourly_timeseries_(\w+?)_\d{8}-\d{8}\.csv$")
DAILY = re.compile(r"hydromet_daily_timeseries_(\w+?)_\d{8}-\d{8}\.csv$")


def settings(cfg):
    out = dict(DEFAULTS)
    out.update((cfg or {}).get("camels", {}) or {})
    return out


def out_dir(cfg):
    return Path(f"{cfg['store_root']}/gold/camels_gb_v2")


def find_files(root):
    """Each gauge's hourly and daily files under the CAMELS-GB v2 folder: {gauge: {kind: path}}."""
    found = {}
    for path in Path(root).rglob("*.csv"):
        for kind, pattern in (("hourly", HOURLY), ("daily", DAILY)):
            m = pattern.search(path.name)
            if m:
                found.setdefault(m.group(1), {})[kind] = path
    return found


def daylight_weights(dates, latitude):
    """For each date, the share of the day's evaporation falling in each of its 24 hours (UTC):
    a half sine from sunrise to sunset, each row summing to 1. Day length from the solar
    declination, delta = 23.44 deg x sin(2 pi (284 + day of year) / 365), and the sunset hour
    angle, w0 = arccos(-tan(lat) tan(delta)); sunrise and sunset sit w0 / 15 hours either side
    of noon. Clock time is taken as solar time, within half an hour across Great Britain."""
    doy = pd.DatetimeIndex(dates).dayofyear.to_numpy()
    decl = np.radians(23.44) * np.sin(2 * np.pi * (284 + doy) / 365)
    cos_w0 = np.clip(-np.tan(np.radians(latitude)) * np.tan(decl), -1.0, 1.0)
    half = np.degrees(np.arccos(cos_w0)) / 15.0            # hours from noon to sunset
    rise, length = 12.0 - half, 2.0 * half
    edges = np.arange(25, dtype=float)
    # The integral of sin(pi (t - rise) / length) from rise to t, clipped to the daylight hours
    t = np.clip(edges[None, :], rise[:, None], (rise + length)[:, None])
    cum = (length[:, None] / np.pi) * (1 - np.cos(np.pi * (t - rise[:, None]) / length[:, None]))
    w = np.diff(cum, axis=1)
    return w / w.sum(axis=1, keepdims=True)


def hourly_table(hourly_csv, daily_csv, latitude, cfg=None):
    """One catchment's hourly model input: time (UTC, hour start), rain_mm (CEH-GEAR),
    rain_gradgb_mm, pet_mm (the daily column `pet` spread over daylight), temperature_c (the
    day's), flow_m3s, flow_mmh, flow_flag, level_m and level_flag. Flags are kept as given.
    Every evaporation product in the daily file is also spread and kept, as <product>_mm
    (pet_chess_mm, peti_chess_mm, pet_hydrope_mm, peti_hydrope_mm), so a calibration can
    compare them without preparing the data again."""
    st = settings(cfg)
    h = pd.read_csv(hourly_csv, dtype={"discharge_flag": str, "level_flag": str})
    d = pd.read_csv(daily_csv)
    for col in (st["pet"], st["temperature"]):
        if col not in d:
            raise ValueError(f"No column {col} in {Path(daily_csv).name}")
    time = pd.to_datetime(h["date"], utc=True)
    days = pd.to_datetime(d["date"])
    weights = daylight_weights(days, latitude)
    hours = pd.DatetimeIndex((days.to_numpy()[:, None] + np.arange(24) * np.timedelta64(1, "h"))
                             .ravel()).tz_localize("UTC")

    def spread(col):
        daily = d[col].to_numpy(dtype=float)[:, None]
        return pd.Series((daily * weights).ravel(), index=hours).reindex(time).to_numpy()

    temp = pd.Series(d[st["temperature"]].to_numpy(), index=days.dt.date)
    out = pd.DataFrame({
        "time": time,
        "rain_mm": h.get("precipitation_cehgear"),
        "rain_gradgb_mm": h.get("precipitation_gradgb"),
        "pet_mm": spread(st["pet"]),
        "temperature_c": temp.reindex(time.dt.date).to_numpy(),
        "flow_m3s": h.get("discharge_vol"),
        "flow_mmh": h.get("discharge_spec"),
        "flow_flag": h.get("discharge_flag"),
        "level_m": h.get("level"),
        "level_flag": h.get("level_flag"),
    })
    for product in PET_PRODUCTS:
        if product in d:
            out[f"{product}_mm"] = spread(product)
    return out


def _current(target, newest, daily_csv):
    """True if the stored table is newer than its sources and holds every evaporation product
    the daily file has: a table from before the products were kept is written again."""
    if not target.exists() or target.stat().st_mtime < newest:
        return False
    have = set(pq.read_schema(target).names)
    products = set(pd.read_csv(daily_csv, nrows=0).columns) & set(PET_PRODUCTS)
    return {f"{p}_mm" for p in products} <= have


def _read_tables(root, names):
    """The named attribute tables (camels_gb_v2_<name>_attributes.csv), joined on gauge_id."""
    parts = []
    for name in names:
        hits = list(Path(root).rglob(f"camels_gb_v2_{name}_attributes.csv"))
        if not hits:
            raise FileNotFoundError(f"No camels_gb_v2_{name}_attributes.csv under {root}")
        parts.append(pd.read_csv(hits[0], dtype={"gauge_id": str}).set_index("gauge_id"))
    out = parts[0]
    for p in parts[1:]:
        out = out.join(p.drop(columns=[c for c in p.columns if c in out.columns]), how="outer")
    return out.reset_index()


def attributes(root):
    """The attribute tables in three: descriptors, signatures and hydrometry."""
    return {"descriptors": _read_tables(root, DESCRIPTOR_TABLES),
            "signatures": _read_tables(root, SIGNATURE_TABLES),
            "hydrometry": _read_tables(root, HYDROMETRY_TABLES)}


def _flagged(values):
    """True for the ways a yes is written: 1, Y, yes, true."""
    text = pd.Series(values).astype(str).str.strip().str.lower()
    return text.isin(["1", "1.0", "y", "yes", "true"])


def screening(desc, hydrometry, cfg=None, signatures=None):
    """Which catchments suit a PDM calibration, how each is to be calibrated, and the reasons
    for leaving any out.

    Left out: an hourly flow record less complete than `min_hourly_complete`; snow above
    `max_frac_snow`; reservoir capacity above `max_reservoir_capacity` of the mean annual
    runoff; more than `max_urban` per cent urban (sewers and culverts the PDM does not
    represent); and, given the signatures, a runoff ratio above `max_runoff_ratio`: more
    water than the groundwater extension can bring in.

    `treatment` says how a kept catchment is calibrated, from its water balance. A catchment
    is groundwater-fed with a baseflow index of at least `groundwater_bfi`, or more than
    `max_high_perm` per cent on highly productive aquifers. Groundwater-fed and losing water
    (runoff ratio under `losing_runoff_ratio`): `abstraction`, the groundwater extension with
    an abstraction term. Groundwater-fed and gaining (runoff ratio above 1): `import`, springs
    free to bring water in. Not groundwater-fed with a runoff ratio of `rain_low_runoff_ratio`
    or more: `free_fc`, the rain is likely too low (gauge undercatch in the uplands), so the
    rainfall factor is freed. Otherwise `standard`. `pack` marks a snow share of at least
    `pack_frac_snow`, for PACK ahead of the PDM.

    The signatures choose catchments and treatments only; they go into no model. The CAMELS
    benchmark flag (near-natural catchments) is shown, not required. Missing values do not
    exclude, and give `standard`."""
    st = settings(cfg)
    t = desc.merge(hydrometry, on="gauge_id", how="left")
    if signatures is not None:
        keep = [c for c in ("gauge_id", "baseflow_index", "runoff_ratio") if c in signatures]
        t = t.merge(signatures[keep], on="gauge_id", how="left")
    missing = pd.Series(np.nan, index=t.index)
    high = t.get("frac_high_perc", 0).fillna(0) + t.get("inter_high_perc", 0).fillna(0)
    ratio = t.get("runoff_ratio", missing)
    rules = {
        "hourly record incomplete": t["hourly_flow_perc_complete"] < st["min_hourly_complete"],
        "snow": t["frac_snow"] > st["max_frac_snow"],
        "reservoirs": t.get("reservoir_normalised_upstream_capacity",
                            missing) > st["max_reservoir_capacity"],
        "urban": t.get("urban_perc_2015", missing) > st["max_urban"],
        "water from outside the catchment": ratio > st["max_runoff_ratio"],
    }
    reasons = pd.Series([""] * len(t), index=t.index)
    for name, hit in rules.items():
        hit = hit.fillna(False)
        reasons[hit] = reasons[hit].where(reasons[hit] == "", reasons[hit] + "; ") + name
    groundwater = ((t.get("baseflow_index", missing) >= st["groundwater_bfi"]).fillna(False)
                   | (high > st["max_high_perm"]))
    treatment = pd.Series(["standard"] * len(t), index=t.index)
    treatment[(groundwater & (ratio < st["losing_runoff_ratio"])).fillna(False)] = "abstraction"
    treatment[(groundwater & (ratio > 1)).fillna(False)] = "import"
    treatment[(~groundwater & (ratio >= st["rain_low_runoff_ratio"])).fillna(False)] = "free_fc"
    out = pd.DataFrame({
        "gauge_id": t["gauge_id"],
        "gauge_name": t.get("gauge_name"),
        "area_km2": t.get("area"),
        "benchmark": _flagged(t["benchmark_catch"]) if "benchmark_catch" in t else False,
        "hourly_complete_perc": t["hourly_flow_perc_complete"],
        "frac_snow": t["frac_snow"],
        "high_perm_perc": high,
        "urban_perc": t.get("urban_perc_2015", missing),
        "baseflow_index": t.get("baseflow_index", missing),
        "runoff_ratio": ratio,
        "use": reasons == "",
        "reasons": reasons,
        "treatment": treatment,
        "pack": (t["frac_snow"] >= st["pack_frac_snow"]).fillna(False),
    })
    return out.sort_values("gauge_id").reset_index(drop=True)


def run(cfg, gauges=None, refresh=False):
    """Writes every catchment's hourly table (only those whose source files are newer than
    the table, unless `refresh`), then the descriptors, signatures, hydrometry and screening.
    Returns the screening table."""
    st = settings(cfg)
    root = (cfg.get("camels", {}) or {}).get("root")
    if not root:
        raise ValueError("Set camels.root in the config to the CAMELS-GB v2 folder")
    out = out_dir(cfg)
    tables = attributes(root)
    for name, df in tables.items():
        store.write_parquet(df, str(out / f"{name}.parquet"))
    desc = tables["descriptors"].set_index("gauge_id")
    files = find_files(root)
    ids = [g for g in (gauges or sorted(files)) if g in files]
    for k, g in enumerate(ids, start=1):
        f = files[g]
        if "hourly" not in f or "daily" not in f:
            continue
        target = out / "hourly" / f"{g}.parquet"
        newest = max(f["hourly"].stat().st_mtime, f["daily"].stat().st_mtime)
        if not refresh and _current(target, newest, f["daily"]):
            continue
        lat = float(desc.at[g, "gauge_lat"]) if g in desc.index else 54.0
        store.write_parquet(hourly_table(f["hourly"], f["daily"], lat, st), str(target))
        if k % 50 == 0:
            print(f"{k}/{len(ids)} catchments", flush=True)
    screen = screening(tables["descriptors"], tables["hydrometry"], st, tables["signatures"])
    store.write_csv(screen, str(out / "screening.csv"))
    return screen
