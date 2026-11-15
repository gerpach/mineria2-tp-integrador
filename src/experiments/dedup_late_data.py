"""Experimentos controlados de deduplicación y late data (feedback E1).

Arma entradas chicas y conocidas a partir de eventos reales de Landing (sin modificar Landing),
las procesa con la MISMA función de streaming del pipeline (bronze_stream.build_stream) y compara
lo esperado con lo obtenido. Escribe evidence/entrega2/experimentos.md.

Uso: python -m src.experiments.dedup_late_data
"""
import json
import os
import shutil
import sys
import time
from pathlib import Path

from pyspark.sql import functions as F

from ..common.config import lake_path, load_config
from ..common.evidence import EVIDENCE_DIR, md_table, write_json
from ..common.quality import dedupe_by_event_id
from ..common.spark import get_spark
from ..jobs.bronze_stream import run_stream


def load_events(landing: Path) -> list[dict]:
    rows = []
    for f in sorted((landing / "usage_events_stream").glob("*.jsonl")):
        with open(f, encoding="utf-8") as fh:
            rows += [json.loads(l) for l in fh if l.strip()]
    return rows


def write_files(folder: Path, files: list[tuple[str, list[dict]]]):
    """Escribe archivos JSONL con mtime creciente: el file source los toma en ese orden."""
    if folder.exists():
        shutil.rmtree(folder)
    folder.mkdir(parents=True)
    t0 = time.time() - 1000
    for i, (name, rows) in enumerate(files):
        p = folder / name
        p.write_text("\n".join(json.dumps(r) for r in rows) + "\n", encoding="utf-8")
        os.utime(p, (t0 + i * 10, t0 + i * 10))


def stream_once(spark, inp: Path, base: Path, tag: str, watermark: str, fresh: bool = True):
    out, ck = base / f"out_{tag}", base / f"ck_{tag}"
    if fresh:
        for p in (out, ck):
            shutil.rmtree(p, ignore_errors=True)
    progress = run_stream(spark, str(inp), str(out), str(ck), watermark, 1, f"exp_{tag}")
    return progress, spark.read.parquet(str(out))


def experiment_dedup(spark, events, base: Path, watermark: str) -> dict:
    a, b = events[:200], events[200:300]
    files = [("f1_original.jsonl", a),
             ("f2_reingesta_mismo_contenido.jsonl", a),                    # re-entrega completa del mismo archivo
             ("f3_solapado.jsonl", a[:50] + b + [b[0], b[1]])]             # solapamiento + duplicados dentro del archivo
    inp = base / "in_dedup"
    write_files(inp, files)
    n_lines = sum(len(r) for _, r in files)
    expected = len({r["event_id"] for _, rows in files for r in rows})
    progress, out = stream_once(spark, inp, base, "dedup", watermark)
    n_out, n_distinct = out.count(), out.select("event_id").distinct().count()

    # Re-ejecución con el mismo checkpoint: no debe volver a leer nada.
    progress_rerun, out2 = stream_once(spark, inp, base, "dedup", watermark, fresh=False)
    n_rerun = out2.count()

    # Se pierde el checkpoint (o el estado expira): Bronze recibe duplicados,
    # pero el dedupe de Silver por event_id (sin estado temporal, D9) los elimina.
    shutil.rmtree(base / "ck_dedup")
    stream_once(spark, inp, base, "dedup", watermark, fresh=False)
    bronze_after_loss = spark.read.parquet(str(base / "out_dedup"))
    n_bronze_loss = bronze_after_loss.count()
    n_silver = dedupe_by_event_id(bronze_after_loss).count()
    return {
        "watermark": watermark,
        "archivos": [{"archivo": n, "lineas": len(r)} for n, r in files],
        "lineas_entrada": n_lines, "event_id_unicos_esperados": expected,
        "micro_lotes": [{"batch": p["batch_id"], "filas_leidas": p["input_rows"]} for p in progress],
        "bronze_filas": n_out, "bronze_event_id_distintos": n_distinct,
        "reejecucion_mismo_checkpoint_filas_leidas": sum(p["input_rows"] for p in progress_rerun),
        "reejecucion_mismo_checkpoint_bronze_filas": n_rerun,
        "sin_checkpoint_bronze_filas": n_bronze_loss, "sin_checkpoint_silver_filas": n_silver,
        "ok": n_out == expected == n_distinct and n_rerun == n_out and n_silver == expected,
    }


def experiment_late(spark, events, base: Path, watermark: str, tag: str) -> dict:
    recent = [e for e in events if e["timestamp"] >= "2025-08-20"][:300]
    half = len(recent) // 2
    late_in_window = [e for e in events if "2025-07-05" <= e["timestamp"] < "2025-07-20"][:200]
    # 5 eventos sintéticos (copias con event_id nuevo) fechados 2025-06-15: más viejos que todo el rango.
    too_late = [dict(e, event_id=f"evt_exp_late_{i:05d}", timestamp="2025-06-15T12:00:00Z")
                for i, e in enumerate(events[:5])]
    # En Spark 3.5 el filtro de late data usa el watermark del lote ANTERIOR (watermark "for late events"),
    # por eso hay dos lotes de agosto antes de los tardíos: así el corte ya está activo cuando llegan.
    files = [("b1_agosto_parte1.jsonl", recent[:half]), ("b2_agosto_parte2.jsonl", recent[half:]),
             ("b3_tardios_julio.jsonl", late_in_window), ("b4_muy_tardios_junio.jsonl", too_late)]
    inp = base / f"in_late_{tag}"
    write_files(inp, files)
    progress, out = stream_once(spark, inp, base, f"late_{tag}", watermark)
    by_file = {r["f"]: r["count"] for r in
               out.withColumn("f", F.regexp_extract("source_file", r"([^/]+\.jsonl)$", 1)).groupBy("f").count().collect()}
    return {
        "watermark": watermark,
        "micro_lotes": [{"batch": p["batch_id"], "archivo": files[i][0] if i < len(files) else "-",
                         "filas_leidas": p["input_rows"], "descartadas_por_watermark": p["rows_dropped_by_watermark"],
                         "watermark_al_iniciar_lote": p["watermark"]} for i, p in enumerate(progress)],
        "escritas_por_archivo": {n: by_file.get(n, 0) for n, _ in files},
        "entrada_por_archivo": {n: len(r) for n, r in files},
    }


def main() -> int:
    cfg = load_config()
    spark = get_spark("tp-experimentos")
    base = Path(lake_path(cfg, "_experiments"))
    events = load_events(Path(cfg["paths"]["landing_dir"]))
    wm = cfg["streaming"]["watermark"]
    dedup = experiment_dedup(spark, events, base, wm)
    late_short = experiment_late(spark, events, base, "10 minutes", "10min")
    late_cfg = experiment_late(spark, events, base, wm, "config")
    write_json("experimentos.json", {"dedup": dedup, "late_10min": late_short, "late_config": late_cfg})
    write_markdown(dedup, late_short, late_cfg)
    print(json.dumps({"dedup_ok": dedup["ok"]}, ensure_ascii=False))
    return 0 if dedup["ok"] else 1


def write_markdown(dedup: dict, short: dict, conf: dict):
    md = ["# Experimentos controlados: deduplicación y late data\n",
          "Generado por `python -m src.experiments.dedup_late_data`. Entradas armadas con eventos reales de Landing "
          "(Landing no se modifica) en `datalake/_experiments/`, procesadas con la misma función de streaming del "
          "pipeline (`bronze_stream.run_stream`, `maxFilesPerTrigger=1` para forzar un micro-lote por archivo).\n",
          f"## Experimento 1: deduplicación por `event_id` (watermark `{dedup['watermark']}`)\n",
          md_table(dedup["archivos"]),
          md_table([{"medida": k, "valor": dedup[k]} for k in (
              "lineas_entrada", "event_id_unicos_esperados", "bronze_filas", "bronze_event_id_distintos",
              "reejecucion_mismo_checkpoint_filas_leidas", "reejecucion_mismo_checkpoint_bronze_filas",
              "sin_checkpoint_bronze_filas", "sin_checkpoint_silver_filas", "ok")]),
          "Lectura: el streaming descarta los duplicados entre micro-lotes y dentro del mismo archivo; re-ejecutar con el "
          "mismo checkpoint no relee archivos; si se pierde el checkpoint, Bronze recibe duplicados pero Silver deduplica "
          "por `event_id` sin depender del estado temporal (D9).\n",
          "## Experimento 2: late data con watermark de 10 minutos vs. configuración (D9)\n",
          "Orden de llegada: b1 y b2 (eventos reales del 20-31/08), b3 (eventos reales del 05-19/07: tardíos respecto de "
          "agosto pero dentro del rango del dataset), b4 (5 eventos sintéticos, copias con `event_id` nuevo fechadas el "
          "15/06, fuera de todo rango).\n\n"
          "Hallazgo: en Spark 3.5 el filtro de late data de un lote usa el watermark calculado hasta el lote anterior "
          "(se ve en la tabla: el lote 1 todavía no descarta). Por eso el experimento manda dos lotes de agosto antes "
          "de los tardíos.\n"]
    for res in (short, conf):
        md += [f"### watermark = `{res['watermark']}`\n", md_table(res["micro_lotes"]),
               md_table([{"archivo": k, "filas_entrada": res["entrada_por_archivo"][k],
                          "filas_escritas_en_bronze": v} for k, v in res["escritas_por_archivo"].items()])]
    md.append("Lectura: con 10 minutos, apenas el watermark llega al 31/08 Spark descarta "
              "como tardíos los eventos válidos de julio. Con 61 días se conservan todos los eventos del rango y solo se "
              "descartan los que están fuera de la ventana (política de late data: se pierden y quedan contados en "
              "`numRowsDroppedByWatermark` del progreso del stream).\n")
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    (EVIDENCE_DIR / "experimentos.md").write_text("\n".join(md), encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
