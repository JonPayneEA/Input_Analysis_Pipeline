# ============================================================
# Tool:         areas
# Description:  Catchment and area outlines from GeoJSON on British National Grid, the 1 km
#               cells inside them, and Thiessen weights of rain gauges over those cells
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-17
# Tier:         1
# Inputs:       GeoJSON polygons in metres on British National Grid (EPSG:27700); gauge
#               eastings and northings
# Outputs:      Cell centres inside an outline; gauge weights summing to one
# Dependencies: numpy, matplotlib
# ============================================================
"""Outlines, cells and gauge weights, with no GIS library: matplotlib's Path tests points."""

import json
from pathlib import Path as FilePath

import numpy as np
from matplotlib.path import Path


def read_outline(path):
    """The rings of a GeoJSON outline (a Feature, FeatureCollection, Polygon or MultiPolygon)
    as a list of (exterior, holes) polygons, each ring an (n, 2) array in metres."""
    data = json.loads(FilePath(path).read_text())
    geoms = []
    if data.get("type") == "FeatureCollection":
        geoms = [f["geometry"] for f in data["features"]]
    elif data.get("type") == "Feature":
        geoms = [data["geometry"]]
    else:
        geoms = [data]
    polygons = []
    for g in geoms:
        parts = [g["coordinates"]] if g["type"] == "Polygon" else g["coordinates"]
        if g["type"] not in ("Polygon", "MultiPolygon"):
            raise ValueError(f"{path}: {g['type']} is not a polygon")
        for rings in parts:
            arrays = [np.asarray(r, dtype=float)[:, :2] for r in rings]
            polygons.append((arrays[0], arrays[1:]))
    if not polygons:
        raise ValueError(f"{path}: no polygons")
    first = polygons[0][0]
    if np.abs(first).max() < 1000:
        raise ValueError(f"{path}: coordinates look like degrees; British National Grid "
                         "metres are needed (EPSG:27700)")
    return polygons


def inside(polygons, x, y):
    """True for each point (x, y) inside the outline and outside its holes."""
    pts = np.column_stack([np.ravel(x), np.ravel(y)])
    hit = np.zeros(len(pts), dtype=bool)
    for exterior, holes in polygons:
        h = Path(exterior).contains_points(pts)
        for hole in holes:
            h &= ~Path(hole).contains_points(pts)
        hit |= h
    return hit.reshape(np.shape(x))


def cells_in(polygons, size=1000.0):
    """Centres of the `size` m cells, on the HadUK grid (centres at 500 m offsets), inside the
    outline; at least the single cell holding the outline's centre for a very small area."""
    allpts = np.vstack([e for e, _ in polygons])
    x0, y0 = np.floor(allpts.min(axis=0) / size) * size
    x1, y1 = np.ceil(allpts.max(axis=0) / size) * size
    xs = np.arange(x0 + size / 2, x1, size)
    ys = np.arange(y0 + size / 2, y1, size)
    gx, gy = np.meshgrid(xs, ys)
    keep = inside(polygons, gx, gy)
    if not keep.any():
        cx, cy = allpts.mean(axis=0)
        return np.array([[np.floor(cx / size) * size + size / 2,
                          np.floor(cy / size) * size + size / 2]])
    return np.column_stack([gx[keep], gy[keep]])


def thiessen_weights(cells, gauges):
    """Each gauge's share of the cells nearest to it: the Thiessen weights of the operational
    scheme, on a 1 km grid. `gauges` maps gauge id to (easting, northing). Gauges with no
    cell get no weight; the weights sum to one."""
    ids = list(gauges)
    xy = np.array([gauges[g] for g in ids], dtype=float)
    d2 = ((cells[:, None, :] - xy[None, :, :]) ** 2).sum(axis=2)
    nearest = d2.argmin(axis=1)
    counts = np.bincount(nearest, minlength=len(ids))
    return {ids[i]: counts[i] / len(cells) for i in range(len(ids)) if counts[i]}
