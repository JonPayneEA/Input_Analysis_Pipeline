# ============================================================
# Tool:         tests for the CAMELS-GB v2 preparation
# Description:  Evaporation spread over daylight, the hourly table, the split of attributes
#               into descriptors and signatures, the screening rules, and the run
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-18
# Tier:         1
# Inputs:       Small synthetic CAMELS-GB v2 folders built in each test
# Outputs:      Test results
# Dependencies: numpy, pandas, pytest
# ============================================================

import math

import numpy as np
import pandas as pd
import pytest

from pipelines import camels

EQUINOX = "2021-03-22"   # day 81 of 2021: declination sin(2 pi (284 + 81) / 365) = 0


def test_evaporation_at_the_equinox_is_a_half_sine_from_six_to_six():
    # Declination 0: sunrise 06:00 and sunset 18:00 at any latitude. The share of the day in
    # [t1, t2] is (cos(pi (t1 - 6) / 12) - cos(pi (t2 - 6) / 12)) / 2.
    w = camels.daylight_weights([EQUINOX], 52.0)[0]
    assert w[:6].sum() == 0 and w[18:].sum() == 0
    assert w[6] == pytest.approx((1 - math.cos(math.radians(15))) / 2)      # 06:00-07:00
    assert w[11] == pytest.approx(math.cos(math.radians(75)) / 2)           # 11:00-12:00
    assert w.sum() == pytest.approx(1.0)


def test_midsummer_daylight_at_two_latitudes():
    # 21 June (day 172): declination 23.44 deg x sin(2 pi 456 / 365) = 23.45 deg. Half the day
    # length is arccos(-tan(lat) tan(dec)) / 15 hours: 8.07 h at 50 N (sun up 03:56 to 20:04)
    # and 8.95 h at 58 N (03:03 to 20:57). Both touch 18 clock hours, 03:00 to 21:00; the share
    # of 11:00-12:00 is (cos(pi (11 - r) / L) - cos(pi (12 - r) / L)) / 2 for sunrise r and day
    # length L, smaller in the longer northern day.
    for lat, rise_hour in ((50.0, 3), (58.0, 3)):
        dec = math.radians(23.44) * math.sin(2 * math.pi * (284 + 172) / 365)
        half = math.degrees(math.acos(-math.tan(math.radians(lat)) * math.tan(dec))) / 15
        r, length = 12 - half, 2 * half
        w = camels.daylight_weights(["2021-06-21"], lat)[0]
        assert int(r) == rise_hour and (w > 0).sum() == 18
        assert w[11] == pytest.approx((math.cos(math.pi * (11 - r) / length)
                                       - math.cos(math.pi * (12 - r) / length)) / 2)
    assert camels.daylight_weights(["2021-06-21"], 58.0)[0][11] < \
        camels.daylight_weights(["2021-06-21"], 50.0)[0][11]


def write_camels(root, gauges=("1001",), days=2):
    """A small CAMELS-GB v2 folder: hourly and daily files and the attribute tables."""
    hourly = root / "Catchment_Timeseries" / "hydro-meteorological" / "hourly"
    daily = root / "Catchment_Timeseries" / "hydro-meteorological" / "daily"
    att = root / "Catchment_Attributes"
    for p in (hourly, daily, att):
        p.mkdir(parents=True, exist_ok=True)
    t = pd.date_range(EQUINOX, periods=24 * days, freq="h")
    for g in gauges:
        pd.DataFrame({"date": t.strftime("%Y-%m-%d %H:%M:%S"), "precipitation_cehgear": 0.5,
                      "precipitation_gradgb": np.nan, "discharge_spec": 0.04,
                      "discharge_vol": 3.0, "discharge_flag": "100", "level": 0.5,
                      "level_flag": "000"}).to_csv(
            hourly / f"camels_gb_v2_hydromet_hourly_timeseries_{g}_19901001-20221001.csv",
            index=False)
        d = pd.date_range(EQUINOX, periods=days, freq="D")
        pd.DataFrame({"date": d.strftime("%Y-%m-%d"), "pet_hydrope": [2.4, 1.2][:days],
                      "pet_chess": 3.0, "temperature_chess": [7.5, 9.0][:days]}).to_csv(
            daily / f"camels_gb_v2_hydromet_daily_timeseries_{g}_19701001-20220930.csv",
            index=False)
    return att


def write_attributes(att, rows):
    """rows: gauge_id -> dict of attribute values; each table gets the columns it owns."""
    owners = {
        "topographic": ["gauge_name", "gauge_lat", "area"],
        "climatic": ["frac_snow"],
        "soil": ["sand_perc"],
        "hydrogeology": ["frac_high_perc", "inter_high_perc"],
        "landcover": ["urban_perc_2015"],
        "humaninfluence": ["benchmark_catch", "reservoir_normalised_upstream_capacity"],
        "hydrologic": ["baseflow_index", "q_mean"],
        "hydrometry": ["hourly_flow_perc_complete", "max_gauging_flow"],
    }
    for table, cols in owners.items():
        pd.DataFrame([{"gauge_id": g, **{c: r.get(c, np.nan) for c in cols}}
                      for g, r in rows.items()]).to_csv(
            att / f"camels_gb_v2_{table}_attributes.csv", index=False)


BASE = {"gauge_name": "Test", "gauge_lat": 52.0, "area": 50.0, "frac_snow": 0.02,
        "sand_perc": 40.0, "frac_high_perc": 10.0, "inter_high_perc": 5.0,
        "urban_perc_2015": 2.0, "benchmark_catch": "Y",
        "reservoir_normalised_upstream_capacity": 0.0, "baseflow_index": 0.5, "q_mean": 1.2,
        "hourly_flow_perc_complete": 98.0, "max_gauging_flow": 40.0}


def test_hourly_table_keeps_each_days_evaporation(tmp_path):
    write_camels(tmp_path)
    f = camels.find_files(tmp_path)["1001"]
    t = camels.hourly_table(f["hourly"], f["daily"], 52.0)
    assert len(t) == 48 and str(t["time"].dt.tz) == "UTC"
    day1 = t[t["time"].dt.day == 22]
    # The day's 2.4 mm, kept exactly; 06:00-07:00 has 2.4 x (1 - cos 15 deg) / 2
    assert day1["pet_mm"].sum() == pytest.approx(2.4)
    assert day1["pet_mm"].iloc[6] == pytest.approx(2.4 * (1 - math.cos(math.radians(15))) / 2)
    assert day1["pet_mm"].iloc[:6].sum() == 0
    assert (day1["temperature_c"] == 7.5).all()
    assert (t["flow_m3s"] == 3.0).all() and (t["flow_flag"] == "100").all()
    # Every product in the daily file kept beside pet_mm, each day's total kept exactly
    assert day1["pet_chess_mm"].sum() == pytest.approx(3.0)
    assert (t["pet_hydrope_mm"] == t["pet_mm"]).all()
    assert "peti_chess_mm" not in t
    # Another evaporation product, by name
    other = camels.hourly_table(f["hourly"], f["daily"], 52.0, {"camels": {"pet": "pet_chess"}})
    assert other["pet_mm"].iloc[:24].sum() == pytest.approx(3.0)
    with pytest.raises(ValueError, match="No column peti_chess"):
        camels.hourly_table(f["hourly"], f["daily"], 52.0, {"camels": {"pet": "peti_chess"}})


def test_signatures_are_kept_apart_from_descriptors(tmp_path):
    att = write_camels(tmp_path)
    write_attributes(att, {"1001": BASE})
    tables = camels.attributes(tmp_path)
    # Flow statistics would leak the gauged flow into a model for ungauged catchments
    assert "baseflow_index" not in tables["descriptors"]
    assert "baseflow_index" in tables["signatures"]
    assert "hourly_flow_perc_complete" in tables["hydrometry"]
    assert {"gauge_lat", "frac_snow", "sand_perc", "frac_high_perc"} <= set(
        tables["descriptors"].columns)


def test_screening_rules(tmp_path):
    att = write_camels(tmp_path)
    rows = {
        "A": BASE,                                                 # suitable
        "B": {**BASE, "frac_snow": 0.3},                           # snow 0.3 > 0.15
        "C": {**BASE, "hourly_flow_perc_complete": 50.0},          # 50% < 80%
        "D": {**BASE, "frac_high_perc": 30.0, "inter_high_perc": 25.0},  # 55% > 50%
        "E": {**BASE, "reservoir_normalised_upstream_capacity": 0.5,     # 0.5 > 0.2, and snow
              "frac_snow": 0.2, "benchmark_catch": "N"},
        "F": {**BASE, "frac_snow": np.nan},                        # missing does not exclude
    }
    write_attributes(att, rows)
    t = camels.attributes(tmp_path)
    s = camels.screening(t["descriptors"], t["hydrometry"]).set_index("gauge_id")
    assert s["use"].to_dict() == {"A": True, "B": False, "C": False, "D": False, "E": False,
                                  "F": True}
    assert s.at["B", "reasons"] == "snow"
    assert s.at["C", "reasons"] == "hourly record incomplete"
    assert s.at["D", "high_perm_perc"] == 55.0
    assert s.at["E", "reasons"] == "snow; reservoirs"
    assert bool(s.at["A", "benchmark"]) and not bool(s.at["E", "benchmark"])


def test_run_writes_everything_and_skips_unchanged(tmp_path, monkeypatch):
    src = tmp_path / "camels"
    att = write_camels(src, gauges=("1001", "2002"))
    write_attributes(att, {"1001": BASE, "2002": {**BASE, "frac_snow": 0.4}})
    cfg = {"store_root": str(tmp_path / "store"), "camels": {"root": str(src)}}
    screen = camels.run(cfg)
    out = camels.out_dir(cfg)
    assert sorted(p.name for p in (out / "hourly").iterdir()) == ["1001.parquet",
                                                                  "2002.parquet"]
    for name in ("descriptors", "signatures", "hydrometry"):
        assert (out / f"{name}.parquet").exists()
    assert screen.set_index("gauge_id")["use"].to_dict() == {"1001": True, "2002": False}
    # Unchanged sources are not read again
    calls = []
    monkeypatch.setattr(camels, "hourly_table", lambda *a, **k: calls.append(a))
    camels.run(cfg)
    assert calls == []
    # A table written before the products were kept is written again, once
    target = out / "hourly" / "1001.parquet"
    pd.read_parquet(target).drop(columns=["pet_chess_mm"]).to_parquet(target)
    monkeypatch.undo()
    camels.run(cfg)
    assert "pet_chess_mm" in pd.read_parquet(target)
    with pytest.raises(ValueError, match="camels.root"):
        camels.run({"store_root": str(tmp_path)})
