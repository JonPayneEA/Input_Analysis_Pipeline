# ============================================================
# Tool:         rain gauge pipeline
# Description:  Builds the rain gauge registry, backfills gauges in checkpointed chunks and
#               keeps them current from each gauge's latest stored timestamp
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The configuration; the EA Hydrology API
# Outputs:      Bronze Parquet, the register, the registry and the run logs in the store
# Dependencies: pandas, pyarrow, requests
# ============================================================
"""The rain gauge jobs' logic: reach.io's backfill.R and incremental.R for one data type."""

import time
from datetime import date, datetime, timedelta, timezone

import pandas as pd

from pipelines import hydrology_api as api
from pipelines import store

BACKFILL_LOG = ["gauge_id", "data_type", "status", "rows", "elapsed_s", "error"]
INCREMENTAL_LOG = ["run_at", "gauge_id", "watermark", "end_date", "status", "rows", "revised",
                   "elapsed_s", "error"]


def gauge_registry(measures, lookup=None, id_col=None, catchment_col=None, today=None):
    """One registry row per WISKI ID with a 15-minute rainfall measure (the first notation in
    sort order if a gauge has several), in reach.io's registry columns plus the notation."""
    first = {}
    for m in sorted(measures, key=lambda m: str(m.get("notation"))):
        wid = (m.get("wiski_id") or "").strip()
        if wid and wid not in first:
            first[wid] = m.get("notation")
    ids = sorted(first)
    catchments = [None] * len(ids)
    if lookup is not None:
        if id_col not in lookup or catchment_col not in lookup:
            raise KeyError(f"lookup lacks {id_col} or {catchment_col}")
        key = dict(zip(lookup[id_col].astype(str).str.strip(), lookup[catchment_col], strict=True))
        catchments = [key.get(i) for i in ids]
    return pd.DataFrame({
        "gauge_id": ids, "source_system": "HDE", "data_type": "rainfall",
        "category": "hydrometric", "catchment": catchments, "ea_site_ref": ids,
        "active": True, "live": True, "date_added": today or store.utc_today(),
        "backfill_done": False, "notes": None, "measure_notation": [first[i] for i in ids],
    })


def to_do(registry):
    """Active gauges not yet marked done."""
    return registry[registry["active"].fillna(False).astype(bool)
                    & ~registry["backfill_done"].fillna(False).astype(bool)]


def mark_done(registry, log_rows):
    """Marks gauges done whose status was SUCCESS or EMPTY (the source has nothing for them).
    FAILED gauges stay to do, so the next run tries them again."""
    ok = {r["gauge_id"] for r in log_rows if r["status"] in ("SUCCESS", "EMPTY")}
    registry = registry.copy()
    registry.loc[registry["gauge_id"].isin(ok), "backfill_done"] = True
    return registry


def _notation(gauge, session):
    notation = gauge.get("measure_notation")
    if isinstance(notation, str) and notation:
        return notation
    notation = api.station_rain_measure(gauge["gauge_id"], session)
    if not notation:
        raise RuntimeError(f"No 15-minute rainfall measure for {gauge['gauge_id']}")
    return notation


def record_window(bounds, start_date, end_date):
    """The part of [start_date, end_date) the gauge has readings in, as ISO dates, or None.
    `bounds` are the dates of its first and last readings; the end stays exclusive."""
    first, last = bounds
    if first is None or last is None:
        return None
    start = max(date.fromisoformat(start_date), first)
    end = min(date.fromisoformat(end_date), last + timedelta(days=1))
    return (start.isoformat(), end.isoformat()) if start < end else None


def fetch_and_store(gauge, root, start_date, end_date, method, session=None, after=None,
                    sleep=time.sleep, received_by=None, trim=False, years=1, existing=None):
    """Fetches one gauge and writes its Bronze files. Returns (rows stored, register rows,
    the rows stored). `after` keeps only readings later than that timestamp (the incremental
    watermark). `existing`, the rows already stored for the period fetched, keeps only
    readings that are new or differ from them, so a look-back stores revisions, not copies.
    `trim` first asks for the gauge's first and last readings and fetches only the years
    between, so start_date can be early without requesting empty years."""
    notation = _notation(gauge, session)
    if trim:
        window = record_window(api.reading_bounds(notation, session, sleep), start_date,
                               end_date)
        if window is None:
            return 0, [], None
        start_date, end_date = window
    readings = api.fetch_series(notation, start_date, end_date, session, sleep, years)
    if not len(readings):
        return 0, [], None
    ds_id = store.dataset_id(gauge["gauge_id"])
    bronze = store.to_bronze(readings, ds_id, gauge["gauge_id"])
    if after is not None:
        bronze = bronze[bronze["timestamp"] > after]
    if existing is not None:
        bronze = store.changed_rows(bronze, existing)
    if not len(bronze):
        return 0, [], None
    files = store.write_bronze_by_year(bronze, root, "P", ds_id)
    return len(bronze), store.provenance_rows(files, ds_id, gauge["gauge_id"], "P", method,
                                              received_by), bronze


def backfill_one(gauge, root, start_date, end_date, **kw):
    """One gauge's backfill: a log row and its register rows; never raises."""
    t0 = time.monotonic()

    def row(status, rows=0, error=None):
        return {"gauge_id": gauge["gauge_id"], "data_type": "rainfall", "status": status,
                "rows": rows, "elapsed_s": round(time.monotonic() - t0, 2), "error": error}
    try:
        n, prov, _ = fetch_and_store(gauge, root, start_date, end_date, "HDE", trim=True,
                                     **kw)
        return (row("SUCCESS", n), prov) if n else (row("EMPTY"), [])
    except Exception as e:  # noqa: BLE001 - logged as FAILED and retried next run
        return row("FAILED", error=str(e)), []


def run_backfill(cfg, start_date, end_date, registry=None, log_name="backfill_log.csv",
                 chunk_size=None, mark=True, **kw):
    """Backfills the gauges still to do in chunks. After each chunk the register, the log
    and (if `mark`) the registry are written to the store, so a stopped run loses at most one
    chunk. Returns the log rows of this run."""
    full = registry if registry is not None else store.read_registry(cfg["registry_path"])
    todo = to_do(full)
    size = chunk_size or cfg["backfill"].get("chunk_size", 50)
    kw.setdefault("years", cfg["backfill"].get("years_per_request", 10))
    print(f"{len(todo)} of {len(full)} gauges to do")
    log_all = []
    for i in range(0, len(todo), size):
        log, prov = [], []
        for k, gauge in enumerate(todo.iloc[i:i + size].to_dict("records"), start=i + 1):
            r, p = backfill_one(gauge, cfg["store_root"], start_date, end_date, **kw)
            log.append(r)
            prov.extend(p)
            print(f"[{k}/{len(todo)}] {r['gauge_id']}: {r['status']}, {r['rows']:,} rows, "
                  f"{r['elapsed_s']:.0f} s" + (f" ({r['error']})" if r["error"] else ""),
                  flush=True)
        # Register first: a gauge marked done must already have its files registered
        store.add_rows_to_csv(prov, cfg["register_path"], store.REGISTER_COLUMNS)
        store.add_rows_to_csv(log, f"{cfg['logs_dir']}/{log_name}", BACKFILL_LOG)
        if mark:
            full = mark_done(full, log)
            store.write_registry(full, cfg["registry_path"])
        log_all.extend(log)
        counts = pd.Series([r["status"] for r in log]).value_counts().to_dict()
        print(f"Chunk done: {counts}; {len(to_do(full)) if mark else '-'} left")
    return log_all


def incremental_one(gauge, root, end_date, default_start, run_at, lookback_days=0, **kw):
    """One gauge's update: a log row and register rows.

    From the high watermark (the latest stored reading), or, with `lookback_days`, from that
    many days before it, so readings the EA has revised since they were stored come in again.
    Only readings that are new or differ from the stored ones are written; `revised` counts
    those at or before the watermark."""
    t0 = time.monotonic()
    hwm = store.high_watermark(root, gauge["gauge_id"])
    mark = None if hwm is None else hwm.strftime("%Y-%m-%d %H:%M:%S")

    def row(status, rows=0, revised=0, error=None):
        return {"run_at": run_at, "gauge_id": gauge["gauge_id"], "watermark": mark,
                "end_date": end_date, "status": status, "rows": rows, "revised": revised,
                "elapsed_s": round(time.monotonic() - t0, 2), "error": error}
    try:
        if hwm is None:
            # Nothing stored yet: start from the gauge's own first reading
            n, prov, _ = fetch_and_store(gauge, root, default_start, end_date,
                                         "HDE incremental", trim=True, **kw)
            return (row("SUCCESS", n), prov) if n else (row("EMPTY"), [])
        # Start on the watermark's own day (the API works in dates), less the look-back
        since = (hwm - pd.Timedelta(days=lookback_days)).normalize()
        if since.date() >= date.fromisoformat(end_date):
            return row("NO_NEW_DATA"), []
        existing = store.read_series(root, gauge["gauge_id"], since=since)
        n, prov, kept = fetch_and_store(gauge, root, since.date().isoformat(), end_date,
                                        "HDE incremental", existing=existing, **kw)
        if not n:
            return row("NO_NEW_DATA"), []
        revised = int((kept["timestamp"] <= hwm).sum())
        return row("SUCCESS", n, revised), prov
    except Exception as e:  # noqa: BLE001 - logged as FAILED and retried next run
        return row("FAILED", error=str(e)), []


def run_incremental(cfg, end_date=None, **kw):
    """Brings every live, active gauge up to `end_date` (exclusive; default tomorrow, so
    today's readings come in)."""
    reg = store.read_registry(cfg["registry_path"])
    reg = reg[reg["active"].fillna(False).astype(bool) & reg["live"].fillna(True).astype(bool)]
    end_date = end_date or (store.utc_today() + timedelta(days=1)).isoformat()
    run_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    default_start = cfg["incremental"].get("default_start", cfg["backfill"]["start_date"])
    kw.setdefault("lookback_days", cfg["incremental"].get("lookback_days", 90))
    log, prov = [], []
    for gauge in reg.to_dict("records"):
        r, p = incremental_one(gauge, cfg["store_root"], end_date, default_start, run_at, **kw)
        log.append(r)
        prov.extend(p)
    store.add_rows_to_csv(prov, cfg["register_path"], store.REGISTER_COLUMNS)
    store.add_rows_to_csv(log, f"{cfg['logs_dir']}/incremental_log.csv", INCREMENTAL_LOG)
    return log
