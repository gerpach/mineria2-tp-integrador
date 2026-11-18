import logging

import pytest

from src.common.config import load_config
from src.pipeline import reset_lake


def test_reset_never_deletes_landing(tmp_path, monkeypatch):
    lake = tmp_path / "datalake"
    (lake / "bronze" / "landing").mkdir(parents=True)
    monkeypatch.setenv("LAKE_ROOT", str(lake))
    monkeypatch.setenv("LANDING_DIR", str(lake / "bronze" / "landing"))  # landing mal ubicado dentro de bronze
    with pytest.raises(RuntimeError):
        reset_lake(load_config(), logging.getLogger("t"))
    assert (lake / "bronze" / "landing").exists()


def test_reset_keeps_landing(tmp_path, monkeypatch):
    lake = tmp_path / "datalake"
    for z in ("landing", "bronze", "silver", "_checkpoints"):
        (lake / z).mkdir(parents=True)
    monkeypatch.setenv("LAKE_ROOT", str(lake))
    monkeypatch.setenv("LANDING_DIR", str(lake / "landing"))
    reset_lake(load_config(), logging.getLogger("t"))
    assert (lake / "landing").exists() and not (lake / "bronze").exists() and not (lake / "_checkpoints").exists()
