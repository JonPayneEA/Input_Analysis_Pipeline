# ============================================================
# Tool:         tests for the notebook helpers
# Description:  An empty table is described, not displayed
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-19
# Tier:         1
# Inputs:       Small pandas tables
# Outputs:      Test results
# Dependencies: pandas, pytest
# ============================================================

import pandas as pd

from pipelines.notebook import show


def test_an_empty_table_is_described_not_displayed(capsys):
    # Databricks' display() fails on an empty pandas table (CANNOT_INFER_EMPTY_SCHEMA): the
    # stage 2 cell listing shifted years stopped there when no year was shifted
    shown = []
    years = pd.DataFrame({"gauge_id": ["A"], "year": [2008], "shifted": [False]})
    assert show(years[years["shifted"]], shown.append, "No year is shifted.") is False
    assert shown == []
    assert capsys.readouterr().out == "No year is shifted.\n"
    assert show(None, shown.append) is False


def test_a_table_with_rows_is_displayed():
    shown = []
    t = pd.DataFrame({"a": [1, 2]})
    assert show(t, shown.append) is True
    assert len(shown) == 1 and shown[0] is t


def test_rain_notebooks_show_tables_through_show():
    # A table passed straight to display() fails when it is empty, so the rain notebooks pass
    # tables to show() and give display() only figures. Two direct calls, on the odd ones out
    # in 07 and the stage 3 summary in 08, came back in a commit made in the Git folder.
    import re
    from pathlib import Path
    jobs = Path(__file__).resolve().parents[2] / "jobs" / "rain_gauges"
    direct = re.compile(r"(?<![\w.])display\((?!fig\))")
    found = [f"{p.name}:{n}" for p in sorted(jobs.glob("*.py"))
             for n, line in enumerate(p.read_text().splitlines(), start=1)
             if direct.search(line.split("#")[0])]
    assert found == []
