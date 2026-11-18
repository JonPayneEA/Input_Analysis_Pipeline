"""Stage 2 on small hand-built networks; every expected value worked by hand."""

import numpy as np
import pandas as pd

from pipelines import qc2, rain_charts, rain_qc, store

DAYS = pd.date_range("2023-01-01", periods=10)


def test_nearest_neighbours_within_the_distance():
    # On a line at 0, 10, 20 and 50 km: A's two nearest are B and C; D, 30 km from C and 40
    # from B, keeps only C within 30 km
    loc = pd.DataFrame({"easting": [0, 10000, 20000, 50000], "northing": 0},
                       index=["A", "B", "C", "D"])
    nb = qc2.nearest_neighbours(loc, k=2, max_km=30)
    assert nb["A"] == ["B", "C"] and nb["D"] == ["C"]


def test_neighbour_median_needs_enough_reporting():
    m = pd.DataFrame({"G": 0.0, "A": [1.0, 1.0], "B": [3.0, np.nan], "C": [5.0, 5.0]},
                     index=DAYS[:2])
    med = qc2.neighbour_median(m, {"G": ["A", "B", "C"]}, min_n=3)
    assert med.loc[DAYS[0], "G"] == 3.0
    assert np.isnan(med.loc[DAYS[1], "G"])                     # only two report


def test_day_rules():
    # Day 0: gauge dry, reference 8 mm. Day 1: gauge 25 mm, reference 0.5. Day 2: 20 against
    # 6 (more than 3 times). Day 3: 10 against 6 (within). Day 4: 2 against 7 (gauge under
    # 5 mm: not judged on the factor).
    g = pd.Series([0.0, 25.0, 20.0, 10.0, 2.0], index=DAYS[:5])
    r = pd.Series([8.0, 0.5, 6.0, 6.0, 7.0], index=DAYS[:5])
    out = qc2.day_rules(g, r, "nb").set_index("date")["rule"]
    assert out.to_dict() == {DAYS[0]: "nb_dry", DAYS[1]: "nb_wet", DAYS[2]: "nb_factor"}


def test_month_ratios():
    # January: gauge 31 x 1 mm against 31 x 3 mm: ratio 0.333, below 0.5, reference above
    # 20 mm: suspect. February: 28 x 1 against 28 x 1.5: ratio 0.667, fine.
    days = pd.date_range("2023-01-01", "2023-02-28")
    g = pd.Series(1.0, index=days)
    r = pd.Series(np.where(days.month == 1, 3.0, 1.5), index=days)
    t = qc2.month_table(g, haduk=r).set_index("month")
    assert t.loc[1, "ratio"] == 0.333 and t.loc[1, "flag"] == qc2.SUSPECT
    assert t.loc[2, "ratio"] == 0.667 and t.loc[2, "flag"] == 1


def test_a_gauge_a_day_late_is_found():
    rng = np.random.default_rng(1)
    days = pd.date_range("2022-01-01", "2022-12-31")
    r = pd.Series(np.where(rng.random(len(days)) < 0.4, rng.gamma(0.8, 6, len(days)), 0.0),
                  index=days)
    late = r.shift(1)                       # the gauge books each day's rain a day later
    y = qc2.day_shift(late, r).iloc[0]
    assert y["best_lag"] == 1 and bool(y["shifted"]) and y["corr_best"] == 1.0
    on_time = qc2.day_shift(r, r).iloc[0]
    assert on_time["best_lag"] == 0 and not bool(on_time["shifted"])


def test_only_the_year_that_slipped_is_shifted():
    # Two years of rain; the gauge is on time in 2021 and a day late in 2022
    rng = np.random.default_rng(2)
    days = pd.date_range("2021-01-01", "2022-12-31")
    r = pd.Series(np.where(rng.random(len(days)) < 0.4, rng.gamma(0.8, 6, len(days)), 0.0),
                  index=days)
    g = r.where(days.year == 2021, r.shift(1))
    y = qc2.day_shift(g, r).set_index("year")
    assert not bool(y.loc[2021, "shifted"]) and y.loc[2021, "best_lag"] == 0
    assert bool(y.loc[2022, "shifted"]) and y.loc[2022, "best_lag"] == 1


def put(root, gid, values, start="2023-03-01 09:00"):
    """A gauge's 15-minute readings: values[d] mm on day d, spread evenly over its 96 steps."""
    t = pd.date_range(start, periods=96 * len(values), freq="15min", tz="UTC")
    v = np.repeat(np.asarray(values, dtype=float) / 96, 96)
    ds = store.dataset_id(gid)
    store.write_bronze_by_year(store.to_bronze(pd.DataFrame(
        {"dateTime": t.strftime("%Y-%m-%dT%H:%M:%S"), "value": v, "quality": "Good"}),
        ds, gid), root, "P", ds)


def test_run_stage2_finds_a_blocked_gauge(tmp_path):
    # Four gauges 1 km apart see the same rain: 10 mm a day for 31 days of March. Gauge D is
    # blocked from the 11th: zeros. D is dry on 21 days against a neighbour median of 10 mm;
    # its March total is 100 against 310, ratio 0.323: one month off.
    root = str(tmp_path / "store")
    cfg = {"store_root": root, "registry_path": f"{root}/register/gauge_registry.parquet",
           "qc": {"repeat_runs": []}}
    from pipelines import rain_gauges
    reg = rain_gauges.gauge_registry([{"wiski_id": g, "notation": "n"} for g in "ABCD"])
    store.write_registry(reg, cfg["registry_path"])
    store.write_csv(pd.DataFrame({"wiski_id": list("ABCD"), "label": list("ABCD"),
                                  "easting": [0, 1000, 2000, 3000], "northing": 0}),
                    rain_charts._paths(cfg)["locations"])
    for g in "ABC":
        put(root, g, [10.0] * 31)
    put(root, "D", [10.0] * 10 + [0.0] * 21)
    rain_qc.run_qc(cfg)
    s = rain_qc.run_stage2(cfg).set_index("gauge_id")
    assert s.loc["D", "n_nb_dry"] == 21 and s.loc["D", "months_off"] == 1
    assert s.loc["A", "n_nb_dry"] == 0 and s.loc["A", "months_off"] == 0
    flags = rain_qc.day_flags(cfg, "D").set_index("date")
    assert len(flags) == 21 and set(flags["rules"]) == {"nb_dry"}
    months = pd.read_parquet(f"{root}/gold/qc/stage2/months.parquet")
    d = months[(months["gauge_id"] == "D")].iloc[0]
    assert (d["gauge_mm"], d["ref_mm"], d["ratio"]) == (100.0, 310.0, 0.323)



def test_a_hill_gauge_is_compared_at_its_own_level():
    # A hill gauge takes 1.5 times its three valley neighbours' rain every day for two years.
    # Unscaled, its months are 1.5 times the median; scaled, exactly 1. A month it reads a
    # third of its usual (blocked) still stands out: 0.333 of the scaled reference.
    rng = np.random.default_rng(3)
    days = pd.date_range("2021-01-01", "2022-12-31")
    valley = pd.Series(np.where(rng.random(len(days)) < 0.5, rng.gamma(1.0, 4, len(days)), 0.0),
                       index=days)
    hill = 1.5 * valley
    hill[(days.year == 2022) & (days.month == 3)] *= 1 / 3
    m = pd.DataFrame({"H": hill, "A": valley, "B": valley, "C": valley})
    nb = {"H": ["A", "B", "C"]}
    f = qc2.level_factors(m, "H", ["A", "B", "C"])
    assert all(abs(v - hill.mean() / valley.mean()) < 1e-9 for v in f.values())
    raw = qc2.neighbour_median(m, nb, scale=False)["H"]
    scaled = qc2.neighbour_median(m, nb)["H"]
    jan = days[(days.year == 2021) & (days.month == 1)]
    assert abs(hill[jan].sum() / raw[jan].sum() - 1.5) < 1e-9
    t = qc2.month_table(hill, neighbours=scaled).set_index(["year", "month"])
    k = hill.mean() / valley.mean()                    # a little under 1.5: March is low
    assert t.loc[(2021, 1), "ratio"] == round(1.5 / k, 3)
    assert t.loc[(2022, 3), "ratio"] == round(0.5 / k, 3)
    assert t.loc[(2022, 3), "flag"] == qc2.SUSPECT and t.loc[(2021, 1), "flag"] == 1
