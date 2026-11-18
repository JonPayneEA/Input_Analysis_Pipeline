# ============================================================
# Tool:         tests for the gauge swap check
# Description:  Dating the swap from whole tips to finer readings, and the step at the swap
#               against unchanged neighbours, with its noise from earlier dates
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-18
# Tier:         1
# Inputs:       Synthetic readings and daily series built in each test
# Outputs:      Test results
# Dependencies: numpy, pandas, pytest
# ============================================================

import numpy as np
import pandas as pd
import pytest

from pipelines import qc_swap, rain_charts, rain_qc, store


def two_a_day(first_day, last_day, values):
    """Two wet readings each rain day, at 10:00 and 11:00 UTC, of `values` (a pair)."""
    days = pd.date_range(first_day, last_day)
    t = [d + pd.Timedelta(hours=h) for d in days for h in (10, 11)]
    v = [x for _ in days for x in values]
    return pd.DataFrame({"timestamp": pd.to_datetime(t, utc=True), "value": v})


def test_the_swap_is_dated_from_the_readings():
    # Whole tips (0.2 and 0.4 mm) all of 2024 except March, and from 10 July fine amounts (0.03
    # and 0.41). March is fine too, but of the ten months from March only seven are fine (March
    # and July to December): 0.7, under the hold of 0.8. July is fine (44 of its 62 wet
    # readings, 0.71) and so is every month after it: the swap is the first fine day, 10 July.
    s = pd.concat([two_a_day("2024-01-01", "2024-02-29", (0.2, 0.4)),
                   two_a_day("2024-03-01", "2024-03-31", (0.03, 0.41)),
                   two_a_day("2024-04-01", "2024-07-09", (0.2, 0.4)),
                   two_a_day("2024-07-10", "2024-12-31", (0.03, 0.41))])
    assert qc_swap.swap_date(s) == pd.Timestamp("2024-07-10")


def test_a_swap_late_in_a_month_is_dated_in_that_month():
    # Fine amounts from 25 June, and one stray fine reading at 10:00 on 3 June. June has 13
    # fine readings of 60 (0.22), not a fine month, so July is the swap month. From 25 June
    # every wet reading is fine: the date is 25 June, not 1 July. The stray is not the date:
    # from it to the end of July, 1 + 12 + 62 = 75 readings are fine and 1 + 42 = 43 are
    # tips (11:00 on 3 June, then 4 to 24 June), 75 of 118, 0.64, under the hold of 0.8.
    s = pd.concat([two_a_day("2024-01-01", "2024-06-24", (0.2, 0.4)),
                   two_a_day("2024-06-25", "2024-12-31", (0.03, 0.41))])
    s.loc[s["timestamp"] == pd.Timestamp("2024-06-03 10:00", tz="UTC"), "value"] = 0.03
    assert qc_swap.swap_date(s) == pd.Timestamp("2024-06-25")


def test_no_swap_without_a_change_from_tips():
    tips = two_a_day("2024-01-01", "2024-12-31", (0.2, 0.4))
    assert pd.isna(qc_swap.swap_date(tips))
    # Fine from the first month: the record never left tips, so it cannot be dated
    assert pd.isna(qc_swap.swap_date(two_a_day("2024-01-01", "2024-12-31", (0.03, 0.41))))
    # Fine readings off the 15-minute grid do not count
    odd = two_a_day("2024-06-01", "2024-12-31", (0.03, 0.41))
    odd["timestamp"] += pd.Timedelta(minutes=50)
    assert pd.isna(qc_swap.swap_date(pd.concat([tips, odd])))


def daily(first, last, mm):
    return pd.Series(mm, index=pd.date_range(first, last), dtype=float)


def test_a_step_is_the_ratio_of_ratios():
    # The reference holds 2 mm a day; the gauge 2 mm before 1 July 2020 and 2.2 after. Its
    # ratio is 1.0 before and 1.1 after: a step of 10%. A neighbour reading half as much
    # gives ratios of 2.0 and 2.2: the same step, its level cancels.
    r = daily("2018-07-01", "2022-06-30", 2.0)
    g = r.where(r.index < "2020-07-01", 2.2)
    assert qc_swap.usable_years(g, "2020-07-01") == 2
    assert qc_swap.step_at(g, r, "2020-07-01", 2) == pytest.approx(0.1)
    assert qc_swap.step_at(g, r / 2, "2020-07-01", 2) == pytest.approx(0.1)
    # A day short of two years after the swap: one year each side
    assert qc_swap.usable_years(g[:"2022-06-29"], "2020-07-01") == 1


def test_the_step_against_its_noise():
    # Eight years from 1 July 2013; year j (from 1 July) reads the reference (2 mm a day) times
    # 1.0 when j is even and 1.1 when odd, to j = 6; then 1.5 in year 7, after the swap on
    # 1 July 2020. One year each side of the swap (the record ends 30 June 2021).
    # The step: 1.5 / 1.0 - 1 = 0.5.
    # Placebos 1 to 6 years earlier: years 5 to 6 give 1.0 / 1.1 - 1 = -1/11, years 4 to 5 give
    # 1.1 / 1.0 - 1 = +0.1, and so on: three of each. (Seven years back has no year before.)
    # Their median is (0.1 - 1/11) / 2 = 1/220; every one lies 21/220 from it, so the noise is
    # 1.4826 x 21/220 and z = (0.5 - 1/220) / noise = 109 / (1.4826 x 21) = 3.50: up.
    r = daily("2013-07-01", "2021-06-30", 2.0)
    j = np.array([(d - pd.Timestamp("2013-07-01")).days for d in r.index])
    year = np.searchsorted([(pd.Timestamp("2013-07-01") + pd.DateOffset(years=k)
                             - pd.Timestamp("2013-07-01")).days for k in range(1, 8)],
                           j, side="right")
    f = np.where(year == 7, 1.5, np.where(year % 2 == 0, 1.0, 1.1))
    g = r * f
    refs = pd.DataFrame({"B": r, "C": r / 2})
    out = qc_swap.swap_step(g, refs, "2020-07-01")
    assert out["years"] == 1 and out["refs"] == 2
    assert out["step"] == pytest.approx(0.5)
    assert out["placebos"] == 6
    assert out["noise"] == pytest.approx(1.4826 * 21 / 220)
    assert out["z"] == pytest.approx(109 / (1.4826 * 21))
    assert out["flag"] == "up"


def put(root, gid, first, last, wet):
    """Complete 09:00 rain days: on each, ten 15-minute readings of wet(day) mm from 09:00 and
    zeros for the other 86."""
    days = pd.date_range(first, last)
    t = pd.date_range(pd.Timestamp(first) + pd.Timedelta(hours=9), periods=96 * len(days),
                      freq="15min", tz="UTC")
    v = np.concatenate([[wet(d)] * 10 + [0.0] * 86 for d in days])
    ds = store.dataset_id(gid)
    store.write_bronze_by_year(store.to_bronze(pd.DataFrame(
        {"dateTime": t.strftime("%Y-%m-%dT%H:%M:%S"), "value": v, "quality": "Good"}),
        ds, gid), root, "P", ds)


def test_run_swaps_uses_only_unchanged_neighbours(tmp_path, monkeypatch):
    # Four gauges 1 km apart, 2020 and 2021, each day ten readings: 0.2 mm (2 mm a day). On
    # 1 January 2021 A moves to 0.22 mm readings (2.2 mm a day) and D to 0.3 mm (3 mm). A's
    # neighbours that did not change are B and C: its step is 2.2 / 2.0 - 1 = 10%. D swapped
    # inside A's windows, so it is left out. D against B and C: 3.0 / 2.0 - 1 = 50%. One year
    # each side, no earlier years: no noise, no flag.
    root = str(tmp_path / "store")
    cfg = {"store_root": root, "registry_path": f"{root}/register/gauge_registry.parquet",
           "qc": {"repeat_runs": []}}
    from pipelines import rain_gauges
    reg = rain_gauges.gauge_registry([{"wiski_id": g, "notation": "n"} for g in "ABCD"])
    store.write_registry(reg, cfg["registry_path"])
    store.write_csv(pd.DataFrame({"wiski_id": list("ABCD"), "label": list("ABCD"),
                                  "easting": [0, 1000, 2000, 3000], "northing": 0}),
                    rain_charts._paths(cfg)["locations"])
    swap = pd.Timestamp("2021-01-01")
    for g, after in (("A", 0.22), ("B", 0.2), ("C", 0.2), ("D", 0.3)):
        put(root, g, "2020-01-01", "2021-12-31", lambda d, a=after: a if d >= swap else 0.2)
    rain_qc.run_qc(cfg)
    s = rain_qc.run_swaps(cfg).set_index("gauge_id")
    assert set(s.index) == {"A", "D"}
    assert s.at["A", "swap_date"] == swap.date() and s.at["A", "source"] == "readings"
    assert s.at["A", "years"] == 1 and s.at["A", "nb_refs"] == 2
    assert s.at["A", "nb_step_pct"] == 10.0 and s.at["D", "nb_step_pct"] == 50.0
    assert s.at["A", "nb_placebos"] == 0 and s.at["A", "nb_flag"] == ""
    assert s.at["A", "haduk_refs"] == 0
    # A second run reads the dates kept from the first, not the readings
    def no_reading(*a, **k):
        raise AssertionError("dates should come from gold/qc/swap/dates.csv")
    monkeypatch.setattr(rain_qc.qc_swap, "swap_date", no_reading)
    again = rain_qc.run_swaps(cfg).set_index("gauge_id")
    assert again.at["A", "nb_step_pct"] == 10.0
    # The install register beats the readings
    reg_csv = tmp_path / "register.csv"
    pd.DataFrame({"gauge_id": ["A"], "swap_date": ["2021-01-01"]}).to_csv(reg_csv, index=False)
    cfg["qc"]["swap_csv"] = str(reg_csv)
    s3 = rain_qc.run_swaps(cfg).set_index("gauge_id")
    assert s3.at["A", "source"] == "register" and s3.at["A", "nb_step_pct"] == 10.0
