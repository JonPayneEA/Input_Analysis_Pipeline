# Databricks notebook source
# ============================================================
# Tool:         01 set up the store
# Description:  Creates the store's folders under store_root; safe to rerun
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The configuration
# Outputs:      Folders for Bronze, the register and the logs
# Dependencies: pandas, pyarrow, requests, pyyaml (all in the Databricks runtime)
# ============================================================

# COMMAND ----------

import os
import sys
from pathlib import Path

_here = Path(os.getcwd()).resolve()
_root = os.environ.get("PIPELINES_ROOT") or next(
    (str(p) for p in (_here, *_here.parents) if (p / "pipelines" / "__init__.py").exists()),
    None)
if _root is None:
    raise RuntimeError("Cannot find the repository; set PIPELINES_ROOT")
if _root not in sys.path:
    sys.path.insert(0, _root)
# Forget any copy of the pipelines code loaded before a git pull, so the new code is used
for _name in [m for m in sys.modules if m == "pipelines" or m.startswith("pipelines.")]:
    del sys.modules[_name]

from pipelines.config import load_config

cfg = load_config(_root)

# COMMAND ----------

for sub in ("bronze/hydrometric/EA/P", "silver/hydrometric/P", "gold/hydrometric/P",
            "register", "logs"):
    Path(cfg["store_root"], sub).mkdir(parents=True, exist_ok=True)
print("Store ready at", cfg["store_root"])
