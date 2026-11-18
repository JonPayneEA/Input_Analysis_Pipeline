"""Each rule on a small series built to trip it, or just miss it; counts by hand."""

import numpy as np
import pandas as pd

from pipelines import qc, rain_qc, store


def series(start, values, flags=None, step="15min"):
    t = pd.date_range(start, periods=len(values), freq=step, tz="UTC")
    s = pd.DataFrame({"timestamp": t, "value": values})
    s["supplier_flag"] = flags if flags is not None else "Good"
    return s


def rules(flagged):
    return sorted(flagged["rule"].tolist())


def test_a_clean_day_raises_nothing():
    s = series("2023-03-01 09:00", [0.0] * 40 + [0.2, 0.4, 0.2] + [0.0] * 53)
    assert qc.check_readings(s).empty
    assert qc.check_days(s, qc.check_readings(s)).empty


def test_off_grid_and_negative_readings_are_rejected():
    # Harley, January 2025: 145.2 mm at 09:50, between the 09:45 and 10:00 readings
    s = pd.concat([series("2025-01-27 09:00", [0.0] * 8),
                   series("2025-01-27 09:50", [145.2]), series("2025-01-27 12:00", [-0.2])])
    f = qc.check_readings(s)
    assert rules(f) == ["negative", "off_grid"]
    assert set(f["flag"]) == {qc.REJECTED}


def test_extreme_15min_readings_suspect_then_rejected():
    s = series("2023-07-01 15:00", [39.9, 40.1, 100.0, 100.1])
    f = qc.check_readings(s).set_index("value")
    assert f.loc[40.1, "flag"] == qc.SUSPECT and f.loc[100.0, "flag"] == qc.SUSPECT
    assert f.loc[100.1, "flag"] == qc.REJECTED
    assert 39.9 not in f.index


def test_stuck_runs():
    # 1.0 mm for 8 readings (2 hours) trips; 7 do not. 0.2 mm needs 48 (12 hours).
    eight = series("2023-01-01 00:00", [0.0] + [1.0] * 8 + [0.0])
    seven = series("2023-01-01 00:00", [0.0] + [1.0] * 7 + [0.0])
    drizzle = series("2023-01-01 00:00", [0.2] * 47 + [0.0])
    stuck = series("2023-01-01 00:00", [0.2] * 48)
    assert rules(qc.check_readings(eight)) == ["repeated"] * 8
    assert qc.check_readings(seven).empty
    assert qc.check_readings(drizzle).empty
    assert len(qc.check_readings(stuck)) == 48


def test_accumulation_after_an_outage():
    # 30 hours with no readings, then 6 mm: dumped. 6 mm after a normal 15-minute step: not.
    before = series("2023-01-01 00:00", [0.0] * 4)
    after = series("2023-01-02 07:00", [6.0, 6.0])
    f = qc.check_readings(pd.concat([before, after]))
    assert rules(f) == ["accumulation"]
    assert f["timestamp"].iloc[0] == pd.Timestamp("2023-01-02 07:00", tz="UTC")


def test_ea_suspect_can_be_followed_or_not():
    s = series("2023-01-01 00:00", [0.2, 0.2], flags=["Suspect", "Good"])
    assert rules(qc.check_readings(s)) == ["ea_suspect"]
    assert qc.check_readings(s, {"qc": {"use_ea_suspect": False}}).empty


def test_days_take_their_worst_reading_and_extreme_totals():
    # Day 1 (from 09:00 on 1 March): one suspect 50 mm reading, 50 mm in all. Day 2: 96
    # readings of 2.2 mm = 211.2 mm, above the 200 mm suspect line.
    d1 = series("2023-03-01 09:00", [50.0] + [0.0] * 95)
    d2 = series("2023-03-02 09:00", [2.2] * 96)
    s = pd.concat([d1, d2])
    cfg = {"qc": {"repeat_runs": []}}            # leave the 96 equal readings alone here
    days = qc.check_days(s, qc.check_readings(s, cfg), cfg).set_index("date")
    assert days.loc["2023-03-01", "rules"] == "extreme_15min"
    assert days.loc["2023-03-02", "rules"] == "extreme_day"
    assert set(days["flag"]) == {qc.SUSPECT}


def test_years_report_resolution_and_bias():
    # 2021: tips of 0.2 mm, spread over hours. 2022: tips of 0.5 mm, all at 06:00 UTC
    a = series("2021-01-01 00:00", [0.2, 0.4] * 48, step="6h")
    hours = pd.date_range("2022-01-01 06:00", periods=96, freq="D", tz="UTC")
    b = pd.DataFrame({"timestamp": hours, "value": 0.5, "supplier_flag": "Good"})
    y = qc.check_years(pd.concat([a, b]), {"qc": {"min_wet_readings": 90}}).set_index("year")
    assert (y.loc[2021, "resolution_mm"], y.loc[2022, "resolution_mm"]) == (0.2, 0.5)
    assert y.loc[2021, "share_multiples"] == 1.0
    assert bool(y.loc[2022, "resolution_change"]) and not bool(y.loc[2021, "resolution_change"])
    assert y.loc[2022, "max_hour_share"] == 1.0 and bool(y.loc[2022, "hour_bias"])
    # Every 6 hours, alternating 0.2 and 0.4 mm: 06:00 and 18:00 take the 0.4s, each holding
    # 0.4 / (0.2 + 0.4 + 0.2 + 0.4) = 1/3 of the rain
    assert y.loc[2021, "max_hour_share"] == 0.333


def test_weekend_ratio():
    # Rain every day for 4 weeks, but 3 times as much on weekdays: weekend share 2 / (5x3 + 2)
    days = pd.date_range("2023-01-02 12:00", periods=28, freq="D", tz="UTC")   # from a Monday
    v = np.where(days.dayofweek >= 5, 1.0, 3.0)
    y = qc.check_years(pd.DataFrame({"timestamp": days, "value": v}),
                       {"qc": {"min_wet_readings": 28}}).iloc[0]
    assert y["weekend_ratio"] == round((8.0 / 68.0) / (2 / 7), 2)
    assert bool(y["weekend_bias"])
    # With the default of 200 wet readings, 28 are too few to judge
    assert not bool(qc.check_years(pd.DataFrame({"timestamp": days, "value": v})).iloc[0][
        "weekend_bias"])


def test_run_qc_saves_skips_and_summarises(tmp_path, monkeypatch):
    root = str(tmp_path / "store")
    cfg = {"store_root": root, "registry_path": f"{root}/register/gauge_registry.parquet"}
    for gid, vals in (("A", [0.0] * 95 + [45.0]), ("B", [0.0] * 96)):
        s = series("2023-03-01 09:00", vals)
        ds = store.dataset_id(gid)
        store.write_bronze_by_year(store.to_bronze(pd.DataFrame(
            {"dateTime": s["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S"), "value": s["value"],
             "quality": "Good"}), ds, gid), root, "P", ds)
    summary = rain_qc.run_qc(cfg, gauges=["A", "B", "E"]).set_index("gauge_id")
    assert summary.loc["A", "n_extreme_15min"] == 1 and summary.loc["A", "days_suspect"] == 1
    assert summary.loc["B", "days_suspect"] == 0
    assert "E" not in summary.index                           # no files: left out
    assert (tmp_path / "store" / "gold" / "qc" / "summary.csv").exists()

    def no_bronze(*a, **k):
        raise AssertionError("checked again although nothing changed")
    monkeypatch.setattr(store, "read_series", no_bronze)
    again = rain_qc.run_qc(cfg, gauges=["A", "B"]).set_index("gauge_id")
    assert again.loc["A", "days_suspect"] == 1
    flags = rain_qc.day_flags(cfg, "A")
    assert flags["date"].tolist() == [pd.Timestamp("2023-03-01")]



def test_one_odd_small_reading_is_not_a_change_of_resolution():
    # 2021 and 2022 both tip in 0.2 mm; 2022 has one reading of 0.01 mm among 200 wet ones
    a = series("2021-01-01 00:00", [0.2] * 200, step="12h")
    b = series("2022-01-01 00:00", [0.2] * 199 + [0.01], step="12h")
    y = qc.check_years(pd.concat([a, b]), {"qc": {"repeat_runs": []}}).set_index("year")
    assert y.loc[2022, "resolution_mm"] == 0.2
    assert not bool(y.loc[2022, "resolution_change"])
