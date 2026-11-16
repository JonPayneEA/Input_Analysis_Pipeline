"""Expected values are worked by hand from the small inputs in each test."""

from datetime import date

import pandas as pd
import pyarrow.parquet as pq
import pytest
import requests
from conftest import FakeAPI, Resp

from pipelines import hydrology_api as api
from pipelines import store


def test_parse_notation_maps_the_codes():
    n = "0a1b2c3d-1111-2222-3333-444455556666-rainfall-t-900-mm-qualified"
    assert api.parse_notation(n) == ("rainfall", "total", "15min")
    assert api.parse_notation("no-uuid-here") == (None, None, None)
    # A UUID with a suffix, as 77 real rain measures have
    suffixed = "34f4e53b-4616-4aaa-b8ad-170c22c808c4_589359-rainfall-t-900-mm-qualified"
    assert api.parse_notation(suffixed) == ("rainfall", "total", "15min")


def test_date_chunks_are_annual_with_an_exclusive_end():
    assert api.date_chunks("2020-01-01", "2022-06-01") == [
        ("2020-01-01", "2021-01-01"), ("2021-01-01", "2022-01-01"),
        ("2022-01-01", "2022-06-01")]
    assert api.date_chunks("2020-01-01", "2020-01-01") == []
    # Ten-year chunks: 1966 to 1976, 1976 to 1986, and the rest to the end
    assert api.date_chunks("1966-01-01", "1990-03-01", years=10) == [
        ("1966-01-01", "1976-01-01"), ("1976-01-01", "1986-01-01"),
        ("1986-01-01", "1990-03-01")]


def test_timestamps_drop_the_offset_without_converting():
    ts = api.parse_timestamps(["2023-01-01T09:15:00Z", "2023-01-01T09:15:00+01:00",
                               "2023-01-01 09:15:00", "junk"])
    assert list(ts[:3].dt.hour) == [9, 9, 9]
    assert pd.isna(ts[3])


def test_get_retries_server_errors_but_not_client_errors(no_sleep):
    class Flaky:
        def __init__(self, codes):
            self.codes = list(codes)

        def get(self, url, params=None, timeout=None):
            return Resp({"items": []}, self.codes.pop(0))
    assert api._get("/x", {}, Flaky([503, 200]), no_sleep) == {"items": []}
    with pytest.raises(requests.HTTPError):
        api._get("/x", {}, Flaky([404, 200]), no_sleep)


def test_a_failed_chunk_fails_the_whole_series(no_sleep):
    fake = FakeAPI({"G1": [("2023-06-01T00:00:00", 1.0, "Good")]}, broken={"G1"})
    with pytest.raises(RuntimeError, match="2 chunk"):
        api.fetch_series(api_notation("G1"), "2022-01-01", "2024-01-01", fake, no_sleep)


def api_notation(g):
    from conftest import notation
    return notation(g)


def test_dataset_id_and_path_follow_reach_io():
    ds = store.dataset_id("SX123", "P", date(2026, 11, 16))
    assert ds == "EA_SX123_P_20261116"
    assert store.bronze_path("/s", "P", ds, 2023) == (
        "/s/bronze/hydrometric/EA/P/2023/EA_SX123_P_20261116.parquet")


def readings(rows):
    return pd.DataFrame(rows, columns=["dateTime", "value", "quality"])


def test_bronze_splits_by_year_and_merges_a_same_day_rewrite(tmp_path):
    ds = "EA_G1_P_20261116"
    first = store.to_bronze(readings([("2022-12-31T23:45:00", 0.2, "Good"),
                                      ("2023-01-01T00:00:00", 0.4, None)]), ds, "G1")
    files = store.write_bronze_by_year(first, str(tmp_path), "P", ds)
    assert [f["rows"] for f in files] == [1, 1]                 # one row in each year
    again = store.to_bronze(readings([("2023-01-01T00:00:00", 0.6, "Good"),
                                      ("2023-01-01T00:15:00", 0.0, "Good")]), ds, "G1")
    store.write_bronze_by_year(again, str(tmp_path), "P", ds)
    t = pq.read_table(store.bronze_path(str(tmp_path), "P", ds, 2023))
    assert str(t.schema.field("timestamp").type) == "timestamp[us, tz=UTC]"
    df = t.to_pandas()
    assert list(df["value"]) == [0.6, 0.0]                      # the new 00:00 row won
    assert df["supplier_flag"].tolist() == ["Good", "Good"]


def test_register_rows_are_added_by_rewriting(tmp_path):
    path = tmp_path / "register.csv"
    assert store.add_rows_to_csv([{"dataset_id": "a"}], path, store.REGISTER_COLUMNS) == 1
    store.add_rows_to_csv([{"dataset_id": "b", "tier": "Bronze"}], path,
                          store.REGISTER_COLUMNS)
    out = pd.read_csv(path, dtype=str, keep_default_na=False)
    assert list(out["dataset_id"]) == ["a", "b"]
    assert list(out.columns) == store.REGISTER_COLUMNS


def test_watermark_comes_from_the_latest_year(tmp_path):
    for ds, rows in [("EA_G1_P_20261101", [("2023-05-01T00:00:00", 1, None)]),
                     ("EA_G1_P_20261102", [("2024-02-01T06:00:00", 1, None)]),
                     ("EA_G10_P_20261102", [("2025-01-01T00:00:00", 1, None)])]:
        store.write_bronze_by_year(store.to_bronze(readings(rows), ds, "G1"),
                                   str(tmp_path), "P", ds)
    hwm = store.high_watermark(str(tmp_path), "G1")              # G10 is another gauge
    assert hwm == pd.Timestamp("2024-02-01 06:00", tz="UTC")
    assert store.high_watermark(str(tmp_path), "G2") is None
