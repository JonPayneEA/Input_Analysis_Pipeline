"""Stage 3. The ratio cases are reach.forcing's own (tests/testthat/test-ratio_checks.R and
test-longterm_ratios.R): constructed series with exact answers, so both give the same values."""

import numpy as np
import pandas as pd
import pytest

from pipelines import qc3, rain_qc, store

YEAR = np.repeat(np.arange(2001, 2016), 12)
MONTH = np.tile([10, 11, 12, 1, 2, 3, 4, 5, 6, 7, 8, 9], 15)
REF = 50 + 10 * np.sin(np.arange(1, 181))          # positive, varied


def check(out, name):
    return out.set_index("check").loc[name]


def test_an_exact_factor():
    out = qc3.ratio_checks(REF * 0.88, REF, YEAR, MONTH)
    t = check(out, "typical_ratio")
    assert t["value"] == pytest.approx(0.88)
    assert (t["lower"], t["upper"]) == (pytest.approx(0.88), pytest.approx(0.88))
    assert t["flag"] == "low" and t["n"] == 180
    assert check(out, "spread")["value"] == pytest.approx(0, abs=1e-12)
    assert check(out, "winter_summer")["value"] == pytest.approx(1)
    assert check(out, "trend")["value"] == pytest.approx(0, abs=1e-9)
    assert check(out, "trend")["flag"] == "ok" and check(out, "change_point")["flag"] == "ok"


def test_a_steady_drift_is_the_trend():
    tr = check(qc3.ratio_checks(REF * np.exp(0.01 * (YEAR - 2001)), REF, YEAR, MONTH), "trend")
    assert tr["value"] == pytest.approx((np.exp(0.1) - 1) * 100)
    assert tr["flag"] == "flagged" and tr["n"] == 15


def test_a_step_is_found_sized_and_dated():
    # K = 8 x 7 = 56; p = 2 exp(-6 x 56^2 / (15^3 + 15^2)) = 0.011
    cp = check(qc3.ratio_checks(REF * np.where(YEAR >= 2009, 0.9, 1.0), REF, YEAR, MONTH),
               "change_point")
    assert cp["at"] == 2009 and cp["value"] == pytest.approx(-10)
    assert cp["flag"] == "flagged"


def test_pettitt_closed_form():
    index, p, k = qc3.pettitt([0.0] * 10 + [1.0] * 10)
    assert (index, k) == (10, 100.0)
    assert p == pytest.approx(2 * np.exp(-6 * 100 ** 2 / (20 ** 3 + 20 ** 2)))


def test_winter_undercatch():
    ws = check(qc3.ratio_checks(REF * np.where(np.isin(MONTH, [12, 1, 2]), 0.8, 1.0), REF,
                                YEAR, MONTH), "winter_summer")
    assert ws["value"] == pytest.approx(0.8) and ws["flag"] == "low"


def test_short_records_and_missing_months():
    k = slice(0, 12)
    out = qc3.ratio_checks(REF[k] * 0.5, REF[k], YEAR[k], MONTH[k])
    assert check(out, "typical_ratio")["value"] == pytest.approx(0.5)
    assert check(out, "typical_ratio")["flag"] == "too_few"
    assert check(out, "trend")["flag"] == "too_few"
    g = REF.copy()
    g[:3] = np.nan
    assert check(qc3.ratio_checks(g, REF, YEAR, MONTH), "typical_ratio")["n"] == 177


def test_a_seed_gives_a_repeatable_interval():
    g = REF * np.exp(np.repeat(np.linspace(-0.1, 0.1, 15), 12))
    a, b = qc3.ratio_checks(g, REF, YEAR, MONTH), qc3.ratio_checks(g, REF, YEAR, MONTH)
    pd.testing.assert_frame_equal(a, b)


def test_longterm_ratios_with_and_without_saar():
    years = range(2001, 2016)
    gauge, haduk = pd.Series(900.0, index=years), pd.Series(1050.0, index=years)
    r = qc3.longterm_ratios(gauge, haduk, haduk_baseline=1000, saar=980)
    assert (r["years"], r["c"]) == (15, 1.05)
    assert r["m"] == round(1000 / 980, 3) and r["m_flag"] == "ok"
    assert r["b"] == round(900 / (980 * 1.05), 3) and r["b_flag"] == "low"
    r = qc3.longterm_ratios(gauge, haduk, haduk_baseline=1000)
    assert r["b"] == round(900 / 1050, 3) and r["b_against"] == "HadUK-Grid"
    assert r["m_flag"] == "no_saar"


def test_run_stage3_from_stage2_months(tmp_path):
    # Gauge G reads 0.88 of HadUK-Grid in every month of 2001-2015, and steps down 10% against
    # its neighbours from water year 2009. HadUK-Grid at its cell is 2 mm every day of 1961-1980:
    # 15 years of 730 mm and 5 leap years of 732, a baseline of 730.5.
    root = str(tmp_path / "store")
    cfg = {"store_root": root, "qc": {}}
    years = np.repeat(np.arange(2000, 2016), 12)[9:-3]       # Oct 2000 to Sep 2015
    months = np.tile(np.arange(1, 13), 16)[9:-3]
    days = pd.to_datetime(dict(year=years, month=months, day=1)).dt.days_in_month.to_numpy()
    wy = years + (months >= 10)
    ref = 2.0 * days
    rows = []
    for name, g in (("haduk", 0.88 * ref), ("neighbours", ref * np.where(wy >= 2009, 0.9, 1))):
        rows.append(pd.DataFrame({"gauge_id": "G", "year": years, "month": months,
                                  "reference": name, "days": days, "gauge_mm": g,
                                  "ref_mm": ref, "ratio": g / ref, "flag": 1}))
    store.write_parquet(pd.concat(rows), f"{root}/gold/qc/stage2/months.parquet")
    d = pd.date_range("1961-01-01", "1980-12-31")
    for y in range(1961, 1981):
        dd = d[d.year == y]
        frame = pd.DataFrame({"date": dd, "kind": "gauge", "id": "G", "rain_mm": 2.0})
        store.write_parquet(frame, f"{root}/gold/haduk/rainfall_daily/{y}.parquet")
    s = rain_qc.run_stage3(cfg).set_index("gauge_id").loc["G"]
    assert s["typical_ratio"] == 0.88 and s["typical_flag"] == "low"
    assert s["step_nb_year"] == 2009 and s["step_nb_pct"] == -10.0
    assert s["step_nb_flag"] == "flagged"
    assert s["years"] == 15 and s["b"] == 0.88 and s["b_flag"] == "low"
    # c: HadUK over 2001-2015 (365 x 2 or 366 x 2 a water year) against the 730.5 baseline
    annual = pd.Series(ref).groupby(wy).sum()
    assert s["c"] == round(annual.mean() / 730.5, 3)
