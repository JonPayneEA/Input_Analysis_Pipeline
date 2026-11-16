# ============================================================
# Tool:         Bronze store
# Description:  Writes and reads reach.io's Bronze layout: dataset IDs, year-partitioned
#               Parquet, the provenance register and the gauge registry, every file written
#               whole so a Unity Catalog volume never sees an append
# Flode Module: analytics_pipelines_r
# Author:       analytics_pipelines_r contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       Readings as pandas data frames; the store root
# Outputs:      bronze/<category>/<supplier>/<type>/<YYYY>/<dataset_id>.parquet; register CSV
# Dependencies: pandas, pyarrow
# ============================================================
"""reach.io's Bronze store in Python (schema.R, orchestration.R, registry.R, incremental.R).

Every file is written to a local temporary file and copied into place whole: volumes may
refuse appends and in-place rewrites, and a copy never leaves a half-written file behind.
"""

import getpass
import os
import re
import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

DATA_TYPE_CODES = {"flow": "Q", "level": "H", "rainfall": "P"}
SUPPLIER = "HDE"
SUPPLIER_CODE = "EA"
CATEGORY = "hydrometric"

BRONZE_SCHEMA = pa.schema([
    ("timestamp", pa.timestamp("us", tz="UTC")),
    ("value", pa.float64()),
    ("supplier_flag", pa.string()),
    ("dataset_id", pa.string()),
    ("site_id", pa.string()),
    ("data_type", pa.string()),
    ("period", pa.string()),
])

REGISTER_COLUMNS = [
    "dataset_id", "tier", "status", "supplier", "supplier_code", "site_id", "data_type",
    "time_period_start", "time_period_end", "temporal_resolution", "units",
    "file_format_received", "date_received", "received_by", "method_of_receipt",
    "supplier_quality_flags", "known_limitations", "file_path", "notes",
]

REGISTRY_COLUMNS = [
    "gauge_id", "source_system", "data_type", "category", "catchment", "ea_site_ref",
    "active", "live", "date_added", "backfill_done", "notes", "measure_notation",
]


def _copy_into_place(write, path):
    """Runs write(tmp) on a local temporary file, then copies it to `path` whole."""
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(suffix=Path(path).suffix)
    os.close(fd)
    try:
        write(tmp)
        shutil.copyfile(tmp, path)
    finally:
        os.unlink(tmp)


def write_parquet(df, path, schema=None):
    table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
    _copy_into_place(lambda tmp: pq.write_table(table, tmp), path)


def write_csv(df, path):
    _copy_into_place(lambda tmp: df.to_csv(tmp, index=False), path)


def utc_today():
    """Today's date in UTC, the store's time zone."""
    return datetime.now(timezone.utc).date()


def dataset_id(site_id, data_type="P", received=None):
    """EA_<site>_<type>_<YYYYMMDD>, the received date making Bronze append-only."""
    received = received or utc_today()
    return f"{SUPPLIER_CODE}_{site_id}_{data_type}_{received.strftime('%Y%m%d')}"


def bronze_path(root, data_type, ds_id, year):
    return (f"{root}/bronze/{CATEGORY}/{SUPPLIER_CODE}/{data_type}/{year}/"
            f"{ds_id}.parquet")


def to_bronze(readings, ds_id, site_id, data_type="P", period="15min"):
    """The Bronze schema from API readings; rows without a timestamp are dropped."""
    from pipelines.hydrology_api import parse_timestamps
    ts = parse_timestamps(readings["dateTime"])
    flag = readings["quality"].astype("object").where(readings["quality"].notna(), None)
    out = pd.DataFrame({
        "timestamp": ts,
        "value": pd.to_numeric(readings["value"], errors="coerce").astype("float64"),
        "supplier_flag": flag.map(lambda v: None if v is None else str(v)),
        "dataset_id": ds_id, "site_id": site_id, "data_type": data_type, "period": period,
    })
    return out[out["timestamp"].notna()].reset_index(drop=True)


def write_bronze_by_year(bronze, root, data_type, ds_id):
    """One file per observation year. A file already there (same series, same day) is merged,
    new rows winning on a repeated timestamp. Returns one row per file written."""
    rows = []
    years = bronze["timestamp"].dt.year
    for year in sorted(years.unique()):
        part = bronze[years == year]
        path = bronze_path(root, data_type, ds_id, int(year))
        merged = part
        if Path(path).exists():
            old = pq.read_table(path).to_pandas()
            merged = pd.concat([old, part], ignore_index=True)
            merged = merged.drop_duplicates("timestamp", keep="last")
        merged = merged.sort_values("timestamp").reset_index(drop=True)
        write_parquet(merged, path, BRONZE_SCHEMA)
        rows.append({"path": path, "rows": len(part),
                     "period_start": merged["timestamp"].min().strftime("%Y-%m-%d"),
                     "period_end": merged["timestamp"].max().strftime("%Y-%m-%d")})
    return rows


def provenance_rows(files, ds_id, site_id, data_type, method, received_by=None):
    """Register rows for the files written, in write_provenance_record()'s columns."""
    who = received_by or getpass.getuser()
    today = utc_today().isoformat()
    return [{
        "dataset_id": ds_id, "tier": "Bronze", "status": "Active", "supplier": SUPPLIER,
        "supplier_code": SUPPLIER_CODE, "site_id": site_id, "data_type": data_type,
        "time_period_start": f["period_start"], "time_period_end": f["period_end"],
        "temporal_resolution": "15min", "units": None, "file_format_received": "Parquet",
        "date_received": today, "received_by": who, "method_of_receipt": method,
        "supplier_quality_flags": None, "known_limitations": None,
        "file_path": f["path"], "notes": None,
    } for f in files]


def add_rows_to_csv(rows, path, columns=None):
    """Adds rows to a CSV by rewriting it whole; returns the number added."""
    if not rows:
        return 0
    new = pd.DataFrame(rows, columns=columns)
    if Path(path).exists():
        old = pd.read_csv(path, dtype=str, keep_default_na=False)
        new = pd.concat([old, new.astype("string").fillna("")], ignore_index=True)
    write_csv(new, path)
    return len(rows)


def read_registry(path):
    reg = pq.read_table(path).to_pandas()
    for col in ("active", "live", "backfill_done"):
        if col not in reg:
            reg[col] = col != "backfill_done"
    return reg


def write_registry(reg, path):
    schema = pa.schema([
        ("gauge_id", pa.string()), ("source_system", pa.string()), ("data_type", pa.string()),
        ("category", pa.string()), ("catchment", pa.string()), ("ea_site_ref", pa.string()),
        ("active", pa.bool_()), ("live", pa.bool_()), ("date_added", pa.date32()),
        ("backfill_done", pa.bool_()), ("notes", pa.string()),
        ("measure_notation", pa.string()),
    ])
    reg = reg.reindex(columns=REGISTRY_COLUMNS)
    reg["date_added"] = pd.to_datetime(reg["date_added"]).dt.date
    write_parquet(reg, path, schema)


_RECEIVED = re.compile(r"_(\d{8})\.parquet$")


def series_files(root, site_id, data_type="P"):
    """This gauge's Bronze files, oldest received first, matched on the whole name."""
    base = Path(root) / "bronze" / CATEGORY / SUPPLIER_CODE / data_type
    if not base.exists():
        return []
    pat = re.compile(rf"^{re.escape(SUPPLIER_CODE)}_{re.escape(site_id)}_"
                     rf"{re.escape(data_type)}(_.+)?_\d{{8}}\.parquet$")
    files = [p for p in base.glob("*/*.parquet") if pat.match(p.name)]
    return sorted(files, key=lambda p: (_RECEIVED.search(p.name).group(1), str(p)))


def high_watermark(root, site_id, data_type="P"):
    """The latest stored timestamp for a gauge, from its latest year folder; None if none."""
    files = [p for p in series_files(root, site_id, data_type) if p.parent.name.isdigit()]
    if not files:
        return None
    last = max(int(p.parent.name) for p in files)
    stamps = [pq.read_table(p, columns=["timestamp"]).column(0).to_pandas().max()
              for p in files if int(p.parent.name) == last]
    stamps = [s for s in stamps if pd.notna(s)]
    return max(stamps) if stamps else None


def read_series(root, site_id, data_type="P"):
    """All of a gauge's Bronze rows, the most recently received winning on a repeat."""
    files = series_files(root, site_id, data_type)
    if not files:
        return pd.DataFrame(columns=BRONZE_SCHEMA.names)
    df = pd.concat([pq.read_table(p).to_pandas() for p in files], ignore_index=True)
    return (df.drop_duplicates("timestamp", keep="last").sort_values("timestamp")
            .reset_index(drop=True))
