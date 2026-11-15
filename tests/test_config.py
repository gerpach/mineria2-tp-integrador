"""Los defaults de configuración deben coincidir con las decisiones cerradas (feedback E1)."""
import re
from pathlib import Path

import yaml

from src.common.config import load_config

ROOT = Path(__file__).resolve().parents[1]
EXAMPLE = ROOT / "config" / "settings.example.yaml"


def decision_row(decision_id: str) -> str:
    for line in (ROOT / "DECISIONS.md").read_text(encoding="utf-8").splitlines():
        if line.startswith(f"| {decision_id} |"):
            return line
    raise AssertionError(f"{decision_id} no está en DECISIONS.md")


def test_watermark_default_matches_d9():
    cfg = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))
    row = decision_row("D9")
    assert "Aprobada" in row
    days = int(re.search(r"(\d+)\s*d[ií]as", row).group(1))
    assert cfg["streaming"]["watermark"] == f"{days} days"


def test_coalesce_is_a_parameter_d10():
    cfg = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))
    assert "coalesce" in cfg["write"]          # parámetro, no política fija en el código
    assert "coalesce(1)" not in "".join(p.read_text(encoding="utf-8") for p in (ROOT / "src").rglob("*.py"))


def test_quality_threshold_matches_consigna():
    cfg = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))
    assert cfg["quality"]["min_cost_increment_usd"] == -0.01


def test_no_secrets_in_example():
    cas = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))["cassandra"]
    for k in ("secure_connect_bundle", "client_id", "client_secret"):
        assert str(cas[k]).startswith("${"), f"{k} debe venir de una variable de entorno"


def test_env_overrides(monkeypatch, tmp_path):
    monkeypatch.setenv("LANDING_DIR", str(tmp_path))
    monkeypatch.setenv("WATERMARK", "1 day")
    monkeypatch.delenv("ASTRA_CLIENT_SECRET", raising=False)
    cfg = load_config(str(EXAMPLE))
    assert cfg["paths"]["landing_dir"] == tmp_path
    assert cfg["streaming"]["watermark"] == "1 day"
    assert cfg["cassandra"]["client_secret"] is None
