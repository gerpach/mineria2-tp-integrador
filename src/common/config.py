"""Carga de configuración externalizada: config/settings.yaml (si existe) o settings.example.yaml,
con overrides por variables de entorno. Las credenciales solo llegan por entorno (D8)."""
import os
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[2]


def _resolve(path_str: str) -> Path:
    p = Path(path_str)
    return p if p.is_absolute() else ROOT / p


def load_config(path: str | None = None) -> dict:
    if path is None:
        path = os.getenv("TP_CONFIG")
    if path is None:
        local = ROOT / "config" / "settings.yaml"
        path = local if local.exists() else ROOT / "config" / "settings.example.yaml"
    cfg = yaml.safe_load(Path(path).read_text(encoding="utf-8"))

    paths = cfg["paths"]
    paths["landing_dir"] = _resolve(os.getenv("LANDING_DIR", paths["landing_dir"]))
    paths["lake_root"] = _resolve(os.getenv("LAKE_ROOT", paths["lake_root"]))
    if os.getenv("WATERMARK"):
        cfg["streaming"]["watermark"] = os.environ["WATERMARK"]

    cas = cfg["cassandra"]
    cas["mode"] = os.getenv("CASSANDRA_MODE", cas.get("mode", "local"))
    if os.getenv("CASSANDRA_HOST"):
        cas["hosts"] = [os.environ["CASSANDRA_HOST"]]
    for k in ("secure_connect_bundle", "client_id", "client_secret"):
        v = os.path.expandvars(str(cas.get(k) or ""))
        cas[k] = None if (not v or v.startswith("${")) else v
    return cfg


def lake_path(cfg: dict, *parts: str) -> str:
    """Ruta dentro del Data Lake: lake_path(cfg, 'bronze', 'usage_events')."""
    return str(Path(cfg["paths"]["lake_root"]).joinpath(*parts))
