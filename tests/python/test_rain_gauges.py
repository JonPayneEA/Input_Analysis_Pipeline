"""Each fake gauge has a few readings at known times, so every count is by hand."""

from datetime import date

import pandas as pd
from conftest import FakeAPI, notation

from pipelines import rain_gauges, store


def make_cfg(tmp_path, chunk_size=2):
    root = str(tmp_path / "store")
    return {"store_root": root, "register_path": f"{root}/register/register.csv",
            "registry_path": f"{root}/register/gauge_registry.parquet",
            "logs_dir": f"{root}/logs", "backfill": {"start_date": "2022-01-01",
                                                     "chunk_size": chunk_size},
            "incremental": {}}


def test_registry_has_one_row_per_gauge_and_keeps_the_first_notation():
    measures = [{"wiski_id": "B1", "notation": "z"}, {"wiski_id": "A1", "notation": "y"},
                {"wiski_id": "B1", "notation": "x"}, {"wiski_id": None, "notation": "w"},
                {"wiski_id": " ", "notation": "v"}]
    reg = rain_gauges.gauge_registry(measures, today=date(2026, 11, 16))
    assert list(reg["gauge_id"]) == ["A1", "B1"]
    assert list(reg["measure_notation"]) == ["y", "x"]          # x sorts before z
    lookup = pd.DataFrame({"WISKI": ["B1 "], "Catchment": ["Avon"]})
    reg = rain_gauges.gauge_registry(measures, lookup, "WISKI", "Catchment")
    assert reg["catchment"].isna().tolist() == [True, False]
    assert reg["catchment"][1] == "Avon"


def test_registry_round_trips_through_parquet(tmp_path):
    reg = rain_gauges.gauge_registry([{"wiski_id": "A1", "notation": "n"}],
                                     today=date(2026, 11, 16))
    store.write_registry(reg, tmp_path / "r.parquet")
    back = store.read_registry(tmp_path / "r.parquet")
    assert back["gauge_id"].tolist() == ["A1"]
    assert back["date_added"].tolist() == [date(2026, 11, 16)]
    assert bool(back["active"][0]) and not bool(back["backfill_done"][0])


SERIES = {
    "G1": [("2022-03-01T00:00:00", 0.2, "Good"), ("2023-03-01T00:00:00", 0.4, "Good")],
    "G2": [],
    "G3": [("2022-05-01T00:00:00", 1.0, "Good")],
}


def registry_for(ids):
    reg = rain_gauges.gauge_registry([{"wiski_id": g, "notation": notation(g)} for g in ids])
    return reg


def test_backfill_checkpoints_and_leaves_failures_to_do(tmp_path, no_sleep):
    cfg = make_cfg(tmp_path)
    store.write_registry(registry_for(["G1", "G2", "G3"]), cfg["registry_path"])
    fake = FakeAPI(SERIES, broken={"G3"})
    log = rain_gauges.run_backfill(cfg, "2022-01-01", "2024-01-01", session=fake,
                                   sleep=no_sleep)
    assert [r["status"] for r in log] == ["SUCCESS", "EMPTY", "FAILED"]
    assert log[0]["rows"] == 2
    reg = store.read_registry(cfg["registry_path"])
    assert reg["backfill_done"].tolist() == [True, True, False]
    register = pd.read_csv(cfg["register_path"], dtype=str)
    assert len(register) == 2                                   # G1: one file per year
    assert len(pd.read_csv(f"{cfg['logs_dir']}/backfill_log.csv")) == 3
    # A rerun tries only G3, which now works
    fake.broken.clear()
    log = rain_gauges.run_backfill(cfg, "2022-01-01", "2024-01-01", session=fake,
                                   sleep=no_sleep)
    assert [(r["gauge_id"], r["status"]) for r in log] == [("G3", "SUCCESS")]
    assert store.read_registry(cfg["registry_path"])["backfill_done"].all()
    assert len(pd.read_csv(f"{cfg['logs_dir']}/backfill_log.csv")) == 4


def test_incremental_keeps_only_readings_after_the_watermark(tmp_path, no_sleep):
    cfg = make_cfg(tmp_path)
    store.write_registry(registry_for(["G1"]), cfg["registry_path"])
    fake = FakeAPI({"G1": [("2023-03-01T00:00:00", 0.4, "Good")]})
    rain_gauges.run_backfill(cfg, "2023-01-01", "2023-03-02", session=fake, sleep=no_sleep)
    fake.series["G1"] += [("2023-03-01T00:15:00", 0.6, "Good")]
    log = rain_gauges.run_incremental(cfg, end_date="2023-03-02", session=fake,
                                      sleep=no_sleep)
    assert log[0]["status"] == "SUCCESS" and log[0]["rows"] == 1  # 00:00 already stored
    assert log[0]["watermark"] == "2023-03-01 00:00:00"
    again = rain_gauges.run_incremental(cfg, end_date="2023-03-02", session=fake,
                                        sleep=no_sleep)
    assert again[0]["status"] == "NO_NEW_DATA"
    series = store.read_series(cfg["store_root"], "G1")
    assert series["value"].tolist() == [0.4, 0.6]


def readings_requests(fake):
    return [p for u, p in fake.calls if u.endswith("/readings.json") and "mineq-date" in p]


def test_backfill_fetches_only_the_years_a_gauge_has_readings(tmp_path, no_sleep):
    # G1 reads from March 1971 to May 1973: from a 1900 floor, three annual requests
    # (1971, 1972, 1973), not 126
    cfg = make_cfg(tmp_path)
    store.write_registry(registry_for(["G1"]), cfg["registry_path"])
    fake = FakeAPI({"G1": [("1971-03-01T00:00:00", 0.2, "Good"),
                           ("1973-05-01T09:00:00", 0.4, "Good")]})
    log = rain_gauges.run_backfill(cfg, "1900-01-01", "2026-01-01", session=fake,
                                   sleep=no_sleep)
    assert log[0]["status"] == "SUCCESS" and log[0]["rows"] == 2
    asked = readings_requests(fake)
    assert [(p["mineq-date"], p["max-date"]) for p in asked] == [
        ("1971-03-01", "1972-03-01"), ("1972-03-01", "1973-03-01"),
        ("1973-03-01", "1973-05-02")]


def test_record_window_clips_to_the_floor_and_ceiling():
    w = rain_gauges.record_window((date(1966, 1, 1), date(2026, 11, 16)), "2000-01-01",
                                  "2026-01-01")
    assert w == ("2000-01-01", "2026-01-01")
    w = rain_gauges.record_window((date(1985, 5, 1), date(2023, 2, 20)), "1900-01-01",
                                  "2026-01-01")
    assert w == ("1985-05-01", "2023-02-21")                  # end exclusive: the day after
    assert rain_gauges.record_window((None, None), "1900-01-01", "2026-01-01") is None
    assert rain_gauges.record_window((date(2026, 3, 1), date(2026, 9, 1)), "1900-01-01",
                                     "2026-01-01") is None     # opened after the ceiling


def test_a_gauge_with_no_readings_is_empty_without_fetching_years(tmp_path, no_sleep):
    cfg = make_cfg(tmp_path)
    store.write_registry(registry_for(["G2"]), cfg["registry_path"])
    fake = FakeAPI({"G2": []})
    log = rain_gauges.run_backfill(cfg, "1900-01-01", "2026-01-01", session=fake,
                                   sleep=no_sleep)
    assert log[0]["status"] == "EMPTY"
    assert readings_requests(fake) == []


def test_the_station_lookup_reads_measure_ids(no_sleep):
    # The stations endpoint lists measures by @id only; the 15-minute one must be found
    from pipelines import hydrology_api as api
    fake = FakeAPI({"G1": []})
    assert api.station_rain_measure("G1", fake) == notation("G1")
