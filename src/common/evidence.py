"""Helpers para dejar evidencia regenerable (rutas, tamaños, cantidad de archivos, conteos)."""
import json
from pathlib import Path

from .config import ROOT

EVIDENCE_DIR = ROOT / "evidence" / "entrega2"


def dir_stats(path: str) -> dict:
    p = Path(path)
    files = [f for f in p.rglob("*.parquet")] if p.exists() else []
    parts = {f.parent for f in files}
    size = sum(f.stat().st_size for f in files)
    return {"parquet_files": len(files), "partitions": len(parts), "size_kb": round(size / 1024, 1),
            "avg_file_kb": round(size / 1024 / max(len(files), 1), 1)}


def write_json(name: str, data) -> Path:
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    out = EVIDENCE_DIR / name
    out.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return out


def md_table(rows: list[dict]) -> str:
    if not rows:
        return "_(sin filas)_\n"
    cols = list(rows[0].keys())
    lines = ["| " + " | ".join(cols) + " |", "|" + "---|" * len(cols)]
    for r in rows:
        lines.append("| " + " | ".join("" if r[c] is None else str(r[c]) for c in cols) + " |")
    return "\n".join(lines) + "\n"


def rel(path) -> str:
    """Ruta relativa a lake_root para no exponer rutas locales en la evidencia."""
    s = str(path).replace("\\", "/")
    return s[s.find("datalake"):] if "datalake" in s else s
