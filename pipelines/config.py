# ============================================================
# Tool:         pipeline configuration
# Description:  Finds the repository root and reads the YAML configuration, adding the
#               store's derived paths
# Flode Module: Input_Analysis_Pipeline
# Author:       Input_Analysis_Pipeline contributors
# Created:      2026-11-16
# Tier:         1
# Inputs:       The file PIPELINES_CONFIG names, else config/config.yml
# Outputs:      A configuration dictionary
# Dependencies: pyyaml
# ============================================================
"""Configuration for the jobs. The config file is never committed."""

import os
from pathlib import Path

import yaml

REQUIRED = ("store_root", "backfill", "trial", "incremental")


def repo_root(start=None):
    """PIPELINES_ROOT if set, else the first folder at or above `start` holding pipelines/."""
    env = os.environ.get("PIPELINES_ROOT", "")
    if env:
        return Path(env).resolve()
    here = Path(start or os.getcwd()).resolve()
    for folder in (here, *here.parents):
        if (folder / "pipelines" / "__init__.py").exists():
            return folder
    raise RuntimeError("Cannot find the repository root; set PIPELINES_ROOT")


def load_config(root=None, file=None):
    """Reads the configuration and adds the store's derived paths."""
    file = file or os.environ.get("PIPELINES_CONFIG", "")
    if not file:
        file = Path(root or repo_root()) / "config" / "config.yml"
    file = Path(file)
    if not file.exists():
        raise FileNotFoundError(
            f"No {file}: copy config/config.example.yml to it and fill it in")
    cfg = yaml.safe_load(file.read_text()) or {}
    missing = [k for k in REQUIRED if k not in cfg]
    if missing:
        raise KeyError("config lacks: " + ", ".join(missing))
    store = cfg["store_root"]
    cfg["register_dir"] = f"{store}/register"
    cfg["registry_path"] = f"{store}/register/gauge_registry.parquet"
    cfg["register_path"] = f"{store}/register/hydrometric_data_register.csv"
    cfg["logs_dir"] = f"{store}/logs"
    return cfg
