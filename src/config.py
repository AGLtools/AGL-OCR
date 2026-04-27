"""Centralized configuration loader."""
from __future__ import annotations
from pathlib import Path
import yaml

from .paths import resource_dir, app_data_dir

ROOT = resource_dir()                     # read-only bundled assets
CONFIG_DIR = ROOT / "config"

DATA_ROOT = app_data_dir()                # writable user data
DATA_DIR = DATA_ROOT / "data"
TEMPLATES_DIR = DATA_DIR / "templates"
EXPORTS_DIR = DATA_DIR / "exports"
CACHE_DIR = DATA_DIR / "cache"

for _d in (DATA_DIR, TEMPLATES_DIR, EXPORTS_DIR, CACHE_DIR):
    _d.mkdir(parents=True, exist_ok=True)


def load_yaml(path: Path) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def load_settings() -> dict:
    return load_yaml(CONFIG_DIR / "settings.yaml")


def load_fields() -> list[dict]:
    return load_yaml(CONFIG_DIR / "fields.yaml").get("fields", [])
