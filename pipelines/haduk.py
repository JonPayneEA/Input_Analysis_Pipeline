# ============================================================
# Tool:         HadUK-Grid extraction
# Description:  Finds the HadUK-Grid daily rainfall files, and extracts daily series for given
#               points (each gauge's 1 km cell) and areas (the mean of the cells inside an
#               outline), one year at a time
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-17
# Tier:         1
# Inputs:       HadUK-Grid NetCDF files, 1 km daily (Met Office, Open Government Licence)
# Outputs:      Data frames of date, kind ("gauge" or "area"), id and rain_mm
# Dependencies: numpy, pandas, xarray, netCDF4
# ============================================================
"""HadUK-Grid daily rainfall at gauges and over areas.

The files are named like rainfall_hadukgrid_uk_1km_day_20230101-20230131.nc, one a month,
with the variable `rainfall` (mm) on `projection_x_coordinate` and `projection_y_coordinate`
(British National Grid, metres, cell centres). A HadUK rain day runs from 09:00 UTC to 09:00
UTC the next day; each day is labelled here by the date of the 09:00 it starts at, taken from
the file's time bounds where present.
"""

import re
from pathlib import Path

import numpy as np
import pandas as pd

_NAME = re.compile(r"_(\d{8})-(\d{8})\.nc$")


def find_files(root, variable="rainfall", freq="day"):
    """The files for one variable and frequency anywhere under `root`, sorted by date, as
    (start date, path) pairs."""
    pattern = f"**/{variable}_hadukgrid_uk_1km_{freq}_*.nc"
    out = []
    for p in Path(root).glob(pattern):
        hit = _NAME.search(p.name)
        if hit:
            out.append((pd.Timestamp(hit.group(1)), p))
    return sorted(out)


def _open(path):
    import xarray as xr
    return xr.open_dataset(path, decode_times=True)


def day_labels(ds):
    """The date each step of the file starts at 09:00 UTC: the lower time bound's date where
    the file has bounds, else the time's own date."""
    bounds = None
    for name in ("time_bnds", "time_bounds"):
        if name in ds:
            bounds = ds[name].values[:, 0]
    stamps = pd.to_datetime(bounds if bounds is not None else ds["time"].values)
    return stamps.normalize()


def nearest_index(axis, values):
    """Index of the nearest grid coordinate along an axis for each value."""
    axis = np.asarray(axis, dtype=float)
    return np.abs(axis[None, :] - np.asarray(values, dtype=float)[:, None]).argmin(axis=1)


def extract_file(path, points=None, areas=None, variable="rainfall"):
    """Daily series from one file. `points` maps an id to (easting, northing); `areas` maps an
    id to an (n, 2) array of cell centres. Returns date, kind, id, rain_mm; an area's value is
    the mean over its cells that hold data (sea and missing cells left out)."""
    ds = _open(path)
    try:
        x = ds["projection_x_coordinate"].values
        y = ds["projection_y_coordinate"].values
        data = ds[variable]
        dims = data.dims                      # (time, y, x) in HadUK files
        arr = data.transpose(dims[0], "projection_y_coordinate",
                             "projection_x_coordinate").values.astype(float)
        dates = day_labels(ds)
    finally:
        ds.close()
    frames = []
    for kind, items in (("gauge", points or {}), ("area", areas or {})):
        for key, where in items.items():
            where = np.atleast_2d(np.asarray(where, dtype=float))
            ix, iy = nearest_index(x, where[:, 0]), nearest_index(y, where[:, 1])
            values = arr[:, iy, ix]
            with np.errstate(all="ignore"):
                series = np.nanmean(values, axis=1) if values.shape[1] > 1 else values[:, 0]
            frames.append(pd.DataFrame({"date": dates, "kind": kind, "id": str(key),
                                        "rain_mm": series}))
    if not frames:
        return pd.DataFrame(columns=["date", "kind", "id", "rain_mm"])
    return pd.concat(frames, ignore_index=True)


def extract_year(files, year, points=None, areas=None):
    """The daily series for one calendar year, from that year's monthly files."""
    parts = [extract_file(p, points, areas) for start, p in files if start.year == year]
    if not parts:
        return pd.DataFrame(columns=["date", "kind", "id", "rain_mm"])
    return pd.concat(parts, ignore_index=True)
