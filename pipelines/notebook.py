# ============================================================
# Tool:         notebook helpers
# Description:  Small helpers for the Databricks notebooks: showing a table that may be empty
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-19
# Tier:         1
# Inputs:       A pandas table and the notebook's display function
# Outputs:      The table shown, or a line saying there is nothing to show
# Dependencies: pandas
# ============================================================
"""Helpers for the notebooks."""


def show(table, display, empty="None found."):
    """Shows `table` with the notebook's `display`, or prints `empty` when it has no rows.

    Databricks' display() turns a pandas table into a Spark one, which cannot be done for an
    empty table: it stops with CANNOT_INFER_EMPTY_SCHEMA. An empty result is often the good
    news (no shifted years, no flagged gauges), so it is said in words instead."""
    if table is None or len(table) == 0:
        print(empty)
        return False
    display(table)
    return True
