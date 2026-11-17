"""Expected values are worked by hand from the small series built in each test."""

import json

import numpy as np
import pandas as pd
import pytest

from pipelines import accumulation as acc
from pipelines import areas as ar
from pipelines import haduk, rain_charts, store


def readings(start, n, value=0.25, step="15min"):
    t = pd.date_range(start, periods=n, freq=step, tz="UTC")
    return pd.DataFrame({"timestamp": t, "value": value})


# ---- The 09:00 rain day ----------------------------------------------------------------------


def test_rain_day_runs_from_0900_and_is_named_by_its_start():
    # 96 readings of 0.25 mm from 09:00 on 1 March: one complete day of 24 mm, dated 1 March.
    # One more reading at 08:45 on 1 March belongs to 28 February.
    s = pd.concat([readings("2023-03-01 08:45", 1, 1.0), readings("2023-03-01 09:00", 96)])
    d = acc.daily_0900(s).set_index("date")
    assert d.loc["2023-03-01", "rain_mm"] == 24.0
    assert d.loc["2023-03-01", "complete"] == 1.0
    assert np.isnan(d.loc["2023-02-28", "rain_mm"])        # 1 of 96 readings: incomplete


def test_readings_off_the_15_minute_grid_are_left_out():
    # A full day of 0.25 mm on the grid (24 mm) plus a reading of 145.2 mm at 09:50, as in the
    # EA record at gauge 000008 in January 2025: the day stays 24 mm, one reading left out
    s = pd.concat([readings("2025-01-27 09:00", 96),
                   readings("2025-01-27 09:50", 1, 145.2)])
    d = acc.daily_0900(s)
    assert d["rain_mm"].iloc[0] == 24.0 and d["complete"].iloc[0] == 1.0
    assert d.attrs["off_grid"] == 1


def test_a_day_short_of_readings_gets_no_total():
    s = readings("2023-03-01 09:00", 90)                      # 90 / 96 = 0.9375 < 0.95
    assert np.isnan(acc.daily_0900(s)["rain_mm"].iloc[0])
    assert acc.daily_0900(s, min_complete=0.9)["rain_mm"].iloc[0] == 22.5


# ---- Running totals and reference lines ------------------------------------------------------


def daily_constant(years, month, rate):
    rows = []
    for y, r in zip(years, rate, strict=True):
        n = pd.Timestamp(year=y, month=month, day=1).days_in_month
        for day in range(1, n + 1):
            rows.append((pd.Timestamp(year=y, month=month, day=day), r))
    return pd.DataFrame(rows, columns=["date", "rain_mm"])


def test_running_totals_stop_at_the_first_gap():
    d = daily_constant([2023], 4, [2.0])
    d.loc[d["date"] == "2023-04-11", "rain_mm"] = np.nan
    m = acc.month_matrix(d, 4)
    assert m.loc[2023, 10] == 20.0
    assert m.loc[2023, 11:].isna().all()


def test_reference_lines_from_three_years():
    # April at 1, 2 and 3 mm a day in 1991-1993. By day 10: 10, 20, 30 mm. Mean over the
    # normal (all three) 20; lowest 10; highest 30; 5% by linear interpolation
    # 10 + 0.05 * 2 * 10 = 11; 95%: 30 - 1 = 29. The year plotted (1993) is left out when asked.
    m = acc.month_matrix(daily_constant([1991, 1992, 1993], 4, [1.0, 2.0, 3.0]), 4)
    ref = acc.reference_lines(m, normal=(1991, 2020))
    assert ref.loc[10, "mean"] == 20.0
    assert (ref.loc[10, "lowest"], ref.loc[10, "highest"]) == (10.0, 30.0)
    assert ref.loc[10, "p5"] == pytest.approx(11.0)
    assert ref.loc[10, "p95"] == pytest.approx(29.0)
    assert ref.attrs["years"] == 3
    without = acc.reference_lines(m, normal=(1991, 2020), exclude=1993)
    assert without.loc[10, "highest"] == 20.0 and without.attrs["years"] == 2


def test_a_february_of_28_days_counts_as_complete():
    # 2023 has 28 days and 2024 29; both complete, so both count
    m = acc.month_matrix(daily_constant([2023, 2024], 2, [1.0, 1.0]), 2)
    ref = acc.reference_lines(m, normal=(2023, 2024))
    assert ref.attrs["years"] == 2
    assert ref.loc[28, "mean"] == 28.0
    assert ref.loc[29, "mean"] == 29.0                       # 2024 alone reaches day 29


def test_a_record_outside_the_normal_takes_its_mean_from_all_years():
    m = acc.month_matrix(daily_constant([2010, 2012], 4, [1.0, 3.0]), 4)
    ref = acc.reference_lines(m, normal=(1991, 2000))
    assert ref.loc[10, "mean"] == 20.0 and ref.attrs["mean_label"] == "Mean of 2 years"


def test_a_month_with_a_gap_is_left_out_of_the_reference_lines():
    d = daily_constant([2021, 2022], 4, [1.0, 5.0])
    d.loc[d["date"] == "2022-04-20", "rain_mm"] = np.nan
    ref = acc.reference_lines(acc.month_matrix(d, 4))
    assert ref.attrs["years"] == 1 and ref.loc[30, "highest"] == 30.0


def test_standing_against_the_lines():
    ref = pd.Series({"lowest": 10, "p5": 11, "p10": 12, "p90": 28, "p95": 29, "highest": 30,
                     "mean": 20})
    assert acc.standing(9, ref) == "lowest on record"
    assert acc.standing(10.5, ref) == "below 5%"
    assert acc.standing(11.5, ref) == "below 10%"
    assert acc.standing(20, ref) == "normal range"
    assert acc.standing(29.5, ref) == "above 95%"
    assert acc.standing(31, ref) == "highest on record"


def test_the_chart_is_drawn_and_saved(tmp_path):
    m = acc.month_matrix(daily_constant([1991, 1992, 1993], 10, [1.0, 2.0, 3.0]), 10)
    ref = acc.reference_lines(m, exclude=1993)
    path = tmp_path / "chart.png"
    acc.accumulation_chart(m.loc[1993], ref, "Rainfall amount", "Source: test", path=path)
    assert path.stat().st_size > 10_000


# ---- Outlines, cells and Thiessen weights ------------------------------------------------------


def square(x0, y0, size, hole=None):
    ring = [[x0, y0], [x0 + size, y0], [x0 + size, y0 + size], [x0, y0 + size], [x0, y0]]
    rings = [ring] + ([hole] if hole else [])
    return {"type": "Feature", "geometry": {"type": "Polygon", "coordinates": rings}}


def test_cells_inside_an_outline_with_a_hole(tmp_path):
    # A 3 km square on the 1 km grid holds 9 cell centres; a hole round the middle one
    # leaves 8
    hole = [[401200, 501200], [401800, 501200], [401800, 501800], [401200, 501800],
            [401200, 501200]]
    path = tmp_path / "a.geojson"
    path.write_text(json.dumps(square(400000, 500000, 3000, hole)))
    cells = ar.cells_in(ar.read_outline(path))
    assert len(cells) == 8
    assert [401500.0, 501500.0] not in cells.tolist()


def test_degrees_are_refused(tmp_path):
    path = tmp_path / "b.geojson"
    path.write_text(json.dumps(square(-1.5, 51.0, 0.1)))
    with pytest.raises(ValueError, match="degrees"):
        ar.read_outline(path)


def test_thiessen_weights_split_the_cells_by_nearest_gauge():
    # Cells in a 4 x 1 row; gauge A sits over the first, B over the last: two cells each.
    # A third gauge far away gets none.
    cells = np.array([[500.0 + 1000 * i, 500.0] for i in range(4)])
    w = ar.thiessen_weights(cells, {"A": (500, 500), "B": (3500, 500), "C": (99999, 99999)})
    assert w == {"A": 0.5, "B": 0.5}


# ---- HadUK-Grid files --------------------------------------------------------------------------


def write_haduk(path, start, days, value_of_day):
    """A HadUK-shaped file: 3 x 3 cells from (400500, 500500), days starting 09:00."""
    import xarray as xr
    x = np.array([400500.0, 401500.0, 402500.0])
    y = np.array([500500.0, 501500.0, 502500.0])
    t0 = pd.date_range(start, periods=days, freq="D") + pd.Timedelta(hours=9)
    data = np.stack([np.full((3, 3), value_of_day(i)) for i in range(days)])
    data[:, 0, 0] = np.nan                                     # one sea cell
    ds = xr.Dataset(
        {"rainfall": (("time", "projection_y_coordinate", "projection_x_coordinate"), data),
         "time_bnds": (("time", "bnds"), np.column_stack([t0, t0 + pd.Timedelta(days=1)]))},
        coords={"time": t0 + pd.Timedelta(hours=12), "projection_x_coordinate": x,
                "projection_y_coordinate": y})
    ds.to_netcdf(path)


def test_haduk_files_found_and_read(tmp_path):
    folder = tmp_path / "1km" / "rainfall" / "day"
    folder.mkdir(parents=True)
    write_haduk(folder / "rainfall_hadukgrid_uk_1km_day_20230101-20230131.nc", "2023-01-01",
                31, lambda i: float(i + 1))
    write_haduk(folder / "rainfall_hadukgrid_uk_1km_day_20230201-20230228.nc", "2023-02-01",
                28, lambda i: 2.0)
    files = haduk.find_files(tmp_path)
    assert [d.month for d, _ in files] == [1, 2]
    df = haduk.extract_year(files, 2023, points={"G1": (401400, 501600)},
                            areas={"A": np.array([[400500, 500500], [401500, 500500]])})
    g = df[df["id"] == "G1"].set_index("date")["rain_mm"]
    assert g["2023-01-05"] == 5.0                            # day 5 of January holds 5 mm
    a = df[df["id"] == "A"].set_index("date")["rain_mm"]
    assert a["2023-02-10"] == 2.0                            # the sea cell is left out
    assert len(g) == 59


# ---- The whole path: store, locations, charts and the month table ------------------------------


def make_cfg(tmp_path):
    root = str(tmp_path / "store")
    return {"store_root": root, "accumulation": {"min_years": 3}, "areas": []}


def put_gauge(cfg, gid, years, month, mm_per_day):
    """Complete months of 15-minute readings, `mm_per_day[k]` mm a day in year k."""
    for y, r in zip(years, mm_per_day, strict=True):
        start = pd.Timestamp(year=y, month=month, day=1, hour=9)
        n = start.days_in_month * 96
        s = readings(start, n, r / 96)
        ds = store.dataset_id(gid)
        store.write_bronze_by_year(store.to_bronze(
            pd.DataFrame({"dateTime": s["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S"),
                          "value": s["value"], "quality": "Good"}), ds, gid), cfg["store_root"],
            "P", ds)


def test_gauge_chart_and_month_table(tmp_path):
    cfg = make_cfg(tmp_path)
    years = [2019, 2020, 2021, 2022, 2023]
    # Four gauges near each other with the same weather; D's last October reads a fifth of it
    for gid, last in (("A", 3.0), ("B", 3.0), ("C", 3.0), ("D", 0.6)):
        put_gauge(cfg, gid, years, 10, [1.0, 2.0, 1.5, 2.5, last])
    loc = pd.DataFrame({"wiski_id": list("ABCD"), "label": list("ABCD"),
                        "easting": [400000, 401000, 402000, 403000], "northing": 500000})
    store.write_csv(loc, rain_charts._paths(cfg)["locations"])
    fig, summary = rain_charts.gauge_chart(cfg, "A", 2023, 10)
    assert summary["total_mm"] == pytest.approx(93.0)          # 31 days at 3 mm
    assert summary["standing"] == "highest on record"
    assert summary["history"] == "gauge record" and summary["years"] == 4
    table = rain_charts.month_table(cfg, 2023, 10, neighbours=3).set_index("gauge_id")
    assert table.at["A", "rank"] == 1.0 and table.at["D", "rank"] == 0.0
    assert table.at["D", "odd_one_out"] and not table.at["A", "odd_one_out"]


def test_area_rain_from_gauges_shares_a_missing_gauges_weight(tmp_path):
    # Two gauges, each nearest half the cells. In October 2023 gauge B has no readings after
    # the 10th, so from the 11th the area takes A's rain alone.
    cfg = make_cfg(tmp_path)
    put_gauge(cfg, "A", [2023], 10, [2.0])
    start = pd.Timestamp("2023-10-01 09:00")
    s = readings(start, 10 * 96, 4.0 / 96)
    ds = store.dataset_id("B")
    store.write_bronze_by_year(store.to_bronze(pd.DataFrame(
        {"dateTime": s["timestamp"].dt.strftime("%Y-%m-%dT%H:%M:%S"), "value": s["value"],
         "quality": "Good"}), ds, "B"), cfg["store_root"], "P", ds)
    loc = pd.DataFrame({"label": ["A", "B"], "easting": [500.0, 3500.0],
                        "northing": [500.0, 500.0]}, index=pd.Index(["A", "B"],
                                                                     name="wiski_id"))
    cells = np.array([[500.0 + 1000 * i, 500.0] for i in range(4)])
    daily, weights = rain_charts.area_daily_from_gauges(cfg, cells, 2023, 10, loc)
    assert weights == {"A": 0.5, "B": 0.5}
    d = daily.set_index("date")["rain_mm"]
    assert d["2023-10-05"] == pytest.approx(3.0)              # (2 + 4) / 2
    assert d["2023-10-20"] == pytest.approx(2.0)              # A alone


def test_area_chart_from_haduk_history_and_gauges_this_month(tmp_path):
    # HadUK-Grid Octobers 2019-2022 at 1, 2, 3 and 4 mm a day over a 3 x 3 grid; the area is
    # the grid's top-right 2 x 2 cells. October 2023 is not in the files, so it comes from the
    # one gauge inside, at 5 mm a day: 155 mm, the highest on record.
    cfg = make_cfg(tmp_path)
    folder = tmp_path / "haduk"
    folder.mkdir()
    for k, y in enumerate([2019, 2020, 2021, 2022]):
        write_haduk(folder / f"rainfall_hadukgrid_uk_1km_day_{y}1001-{y}1031.nc", f"{y}-10-01",
                    31, lambda i, r=k + 1: float(r))
    outline = tmp_path / "area.geojson"
    outline.write_text(json.dumps(square(401000, 501000, 2000)))
    cfg["haduk"] = {"root": str(folder), "start_year": 2019}
    cfg["areas"] = [{"id": "tr", "name": "Top right", "geojson": str(outline)}]
    loc = pd.DataFrame({"wiski_id": ["A"], "label": ["A"], "easting": [402000.0],
                        "northing": [502000.0]})
    store.write_csv(loc, rain_charts._paths(cfg)["locations"])
    assert rain_charts.run_haduk_extract(cfg) == [2019, 2020, 2021, 2022]
    hist = rain_charts.read_haduk(cfg, "area", "tr").set_index("date")["rain_mm"]
    assert hist["2021-10-15"] == 3.0
    put_gauge(cfg, "A", [2023], 10, [5.0])
    fig, summary = rain_charts.area_chart(cfg, "tr", 2023, 10)
    assert summary["total_mm"] == pytest.approx(155.0)
    assert summary["standing"] == "highest on record"
    assert summary["history"] == "1 EA gauge by Thiessen weights"
    # A second extraction skips the years already done, except the latest
    assert rain_charts.run_haduk_extract(cfg) == [2022]


# ---- Longer periods and the selectors ------------------------------------------------------------


def test_water_years_of_366_and_365_days():
    # 1 mm every day from October 2019 to September 2021. Water year 2019 (Oct 2019 to Sep 2020)
    # holds 29 February 2020: 366 days, 366 mm. Water year 2020: 365. Both are complete.
    days = pd.date_range("2019-10-01", "2021-09-30")
    m = acc.period_matrix(pd.DataFrame({"date": days, "rain_mm": 1.0}), 10, 12)
    assert m.attrs["lengths"] == {2019: 366, 2020: 365}
    assert m.loc[2019, 366] == 366.0 and np.isnan(m.loc[2020, 366])
    ref = acc.reference_lines(m, normal=(2019, 2020))
    assert ref.attrs["years"] == 2
    assert ref.loc[365, "mean"] == 365.0


def test_a_winter_runs_across_the_new_year():
    # December 2022 to February 2023 at 2 mm a day: 31 + 31 + 28 = 90 days, 180 mm
    days = pd.date_range("2022-12-01", "2023-02-28")
    m = acc.period_matrix(pd.DataFrame({"date": days, "rain_mm": 2.0}), 12, 3)
    assert m.attrs["lengths"] == {2022: 90}
    assert m.loc[2022, 90] == 180.0


def test_period_names():
    assert rain_charts.period_label(2026, 10) == "October 2026"
    assert rain_charts.period_label(2025, 10, 12) == "Water year 2025-26 (Oct-Sep)"
    assert rain_charts.period_label(2026, 1, 12) == "2026"
    assert rain_charts.period_label(2025, 12, 3) == "Dec 2025 to Feb 2026"


def test_long_charts_mark_the_months(tmp_path):
    days = pd.date_range("2019-10-01", "2021-09-30")
    m = acc.period_matrix(pd.DataFrame({"date": days, "rain_mm": 1.0}), 10, 12)
    ref = acc.reference_lines(m)
    fig = acc.accumulation_chart(m.loc[2020], ref, "t", "s",
                                 period_start=pd.Timestamp("2020-10-01"))
    labels = [x.get_text() for x in fig.axes[0].get_xticklabels()]
    assert labels[:2] == ["Oct 2020", "Nov 2020"] and len(labels) == 12


def test_gauge_choices_sorted_by_name_and_read_back(tmp_path):
    cfg = make_cfg(tmp_path)
    cfg["registry_path"] = f"{cfg['store_root']}/register/gauge_registry.parquet"
    from pipelines import rain_gauges
    reg = rain_gauges.gauge_registry([{"wiski_id": g, "notation": "n"} for g in ("A1", "B2")])
    reg["backfill_done"] = True
    store.write_registry(reg, cfg["registry_path"])
    store.write_csv(pd.DataFrame({"wiski_id": ["A1", "B2"], "label": ["Zennor", "Alston"],
                                  "easting": 1.0, "northing": 1.0}),
                    rain_charts._paths(cfg)["locations"])
    choices = rain_charts.gauge_choices(cfg)
    assert choices == ["B2 - Alston", "A1 - Zennor"]
    assert rain_charts.choice_id(choices[0]) == "B2"


def test_gauge_chart_over_three_months(tmp_path):
    cfg = make_cfg(tmp_path)
    for y in (2019, 2020, 2021, 2022):
        for mth in (10, 11, 12):
            put_gauge(cfg, "A", [y], mth, [1.0])
    store.write_csv(pd.DataFrame({"wiski_id": ["A"], "label": ["A"], "easting": [1.0],
                                  "northing": [1.0]}), rain_charts._paths(cfg)["locations"])
    fig, s = rain_charts.gauge_chart(cfg, "A", 2022, 10, n_months=3)
    assert s["days"] == 92 and s["total_mm"] == pytest.approx(92.0)   # 31 + 30 + 31 days
