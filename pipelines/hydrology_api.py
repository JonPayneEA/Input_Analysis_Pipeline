# ============================================================
# Tool:         EA Hydrology API client
# Description:  Lists 15-minute rainfall measures and fetches readings in annual chunks, one
#               request at a time as the EA asks, with retries for passing failures
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       https://environment.data.gov.uk/hydrology (Open Government Licence v3.0)
# Outputs:      Lists of measure records; pandas data frames of readings
# Dependencies: requests, pandas
# ============================================================
"""The EA Hydrology API, as reach.io calls it (lookup.R, sync.R and router.R)."""

import re
import time
from datetime import date

import pandas as pd
import requests

BASE_URL = "https://environment.data.gov.uk/hydrology"
API_HARD_LIMIT = 2_000_000
PAUSE_S = 1.0            # reach.io's .pace(): one request at a time, a second apart
TIMEOUT_S = 300
RETRIES = 3

# The parameter, value type and period sit between hyphens after the station part, which is a
# UUID, sometimes with a suffix such as "_589359" (77 of the 995 rain measures in November 2026)
_FIELDS = re.compile(r"-([A-Za-z]+)-(m|min|max|i|t)-(\d+)-")
_TYPES = {"m": "mean", "min": "min", "max": "max", "i": "instantaneous", "t": "total"}
_PERIODS = {"900": "15min", "86400": "daily"}


def _get(path, params, session=None, sleep=time.sleep):
    """GET with retries on connection errors and 5xx; raises on anything else."""
    url = f"{BASE_URL}{path}"
    get = (session or requests).get
    for attempt in range(1, RETRIES + 1):
        try:
            resp = get(url, params=params, timeout=TIMEOUT_S)
            if resp.status_code < 500:
                resp.raise_for_status()
                return resp.json()
            err = requests.HTTPError(f"{resp.status_code} for {url}")
        except (requests.ConnectionError, requests.Timeout) as e:
            err = e
        if attempt < RETRIES:
            sleep(PAUSE_S * 10 * attempt)
    raise err


def parse_notation(notation):
    """Parameter, value type and period from a measure notation.

    "<uuid>-rainfall-t-900-mm-qualified" and "<uuid>_589359-rainfall-t-900-mm-qualified" both
    give ("rainfall", "total", "15min").
    """
    hit = _FIELDS.search(notation) if isinstance(notation, str) else None
    if not hit:
        return (None, None, None)
    param, vt, pd_ = hit.groups()
    return (param, _TYPES.get(vt, vt), _PERIODS.get(pd_, pd_))


def _wiski_id(station):
    if isinstance(station, dict):
        return station.get("wiskiID")
    if isinstance(station, list) and station and isinstance(station[0], dict):
        return station[0].get("wiskiID")
    return None


def rain_measures(session=None):
    """Every 15-minute rainfall measure: a list of {wiski_id, notation, label} dicts."""
    body = _get("/id/measures.json", {"observedProperty": "rainfall", "periodName": "15min",
                                       "_limit": 5000}, session)
    out = []
    for item in body.get("items", []):
        station = item.get("station")
        label = station.get("label") if isinstance(station, dict) else None
        out.append({"wiski_id": _wiski_id(station), "notation": item.get("notation"),
                    "label": label})
    return out


def _measure_notation(m):
    """A measure's notation: its `notation` field, else the last part of its `@id` (the
    stations endpoint lists measures by `@id` only)."""
    if not isinstance(m, dict):
        return m
    if m.get("notation"):
        return m["notation"]
    uri = m.get("@id") or ""
    return uri.rstrip("/").rsplit("/", 1)[-1] or None


def station_rain_measure(wiski_id, session=None):
    """The gauge's 15-minute rainfall measure notation, or None; the first if several."""
    body = _get("/id/stations.json", {"wiskiID": wiski_id}, session)
    for station in body.get("items", []):
        for m in station.get("measures", []) or []:
            notation = _measure_notation(m)
            param, _, period = parse_notation(notation)
            if param == "rainfall" and period == "15min":
                return notation
    return None


def date_chunks(from_date, to_date):
    """Annual (start, end) pairs, start inclusive and end exclusive, as make_date_chunks()."""
    start, end = date.fromisoformat(str(from_date)), date.fromisoformat(str(to_date))
    out = []
    s = start
    while s < end:
        try:
            nxt = s.replace(year=s.year + 1)
        except ValueError:               # 29 February
            nxt = s.replace(year=s.year + 1, day=28)
        out.append((s.isoformat(), min(nxt, end).isoformat()))
        s = nxt
    return out


def parse_timestamps(values):
    """UTC timestamps from the API's strings: the offset is dropped without converting, as in
    reach.io, because EA times are always UTC. Unparseable strings become NaT."""
    s = pd.Series(values, dtype="object").astype("string")
    s = s.str.replace(r"(Z|[+-]\d{2}:\d{2})$", "", regex=True).str.replace(" ", "T", n=1)
    return pd.to_datetime(s, format="%Y-%m-%dT%H:%M:%S", errors="coerce", utc=True)


def fetch_readings(notation, from_date, to_date, session=None):
    """Readings in [from_date, to_date) as a data frame with dateTime, value and quality."""
    body = _get(f"/id/measures/{notation}/readings.json",
                {"mineq-date": from_date, "max-date": to_date, "_limit": API_HARD_LIMIT,
                 "_view": "full"}, session)
    items = body.get("items", [])
    if not items:
        return pd.DataFrame(columns=["dateTime", "value", "quality"])
    df = pd.DataFrame(items)
    for col in ("dateTime", "value", "quality"):
        if col not in df:
            df[col] = None
    if len(df) >= API_HARD_LIMIT:
        raise RuntimeError(f"{notation} {from_date} to {to_date} hit the API row limit")
    return df[["dateTime", "value", "quality"]]


def reading_bounds(notation, session=None, sleep=time.sleep):
    """The dates of the measure's first and last readings, or (None, None) if it has none.
    Two one-reading requests, so a backfill can skip the years before a gauge opened and
    after it closed."""
    out = []
    for which in ("earliest", "latest"):
        items = _get(f"/id/measures/{notation}/readings.json", {which: ""}, session).get(
            "items", [])
        sleep(PAUSE_S)
        stamp = parse_timestamps([items[0].get("dateTime")])[0] if items else pd.NaT
        out.append(None if pd.isna(stamp) else stamp.date())
    return tuple(out)


def fetch_series(notation, start_date, end_date, session=None, sleep=time.sleep):
    """The whole series in annual chunks, pausing after every request. Any failed chunk
    raises after the loop, so a partial series is never stored as if it were whole."""
    parts, failed = [], []
    for a, b in date_chunks(start_date, end_date):
        try:
            df = fetch_readings(notation, a, b, session)
            if len(df):
                parts.append(df)
        except Exception as e:  # noqa: BLE001 - every chunk failure is reported below
            failed.append(f"{a} to {b}: {e}")
        sleep(PAUSE_S)
    if failed:
        raise RuntimeError(f"{len(failed)} chunk(s) failed: " + "; ".join(failed))
    if not parts:
        return pd.DataFrame(columns=["dateTime", "value", "quality"])
    return pd.concat(parts, ignore_index=True)
