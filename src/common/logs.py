import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from .config import ROOT


def new_run_id() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def setup_logging(run_id: str, log_dir: Path | None = None) -> logging.Logger:
    log_dir = log_dir or ROOT / "evidence" / "entrega2" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s [%(name)s] %(message)s")
    root = logging.getLogger("tp")
    root.setLevel(logging.INFO)
    root.handlers.clear()
    for h in (logging.StreamHandler(sys.stdout), logging.FileHandler(log_dir / f"pipeline_{run_id}.log", encoding="utf-8")):
        h.setFormatter(fmt)
        root.addHandler(h)
    return root
