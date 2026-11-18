"""Orquestador del pipeline (Entrega 2).

Uso:
  python -m src.pipeline                      # todas las etapas
  python -m src.pipeline --steps bronze_batch,bronze_stream
  python -m src.pipeline --skip serving       # sin Cassandra
  python -m src.pipeline --reset              # borra bronze/silver/gold/quarantine/_checkpoints (NUNCA landing)
"""
import argparse
import logging
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

from .common.config import lake_path, load_config
from .common.evidence import EVIDENCE_DIR, dir_stats, md_table, rel, write_json
from .common.logs import new_run_id, setup_logging

STEPS = ["bronze_batch", "bronze_stream", "silver", "gold", "serving"]
LAKE_ZONES = ["bronze", "silver", "gold", "quarantine", "_checkpoints"]


def reset_lake(cfg: dict, log: logging.Logger):
    landing = Path(cfg["paths"]["landing_dir"]).resolve()
    for zone in LAKE_ZONES:
        p = Path(lake_path(cfg, zone)).resolve()
        if p == landing or landing.is_relative_to(p):
            raise RuntimeError(f"Reset abortado: {p} contiene Landing")
        if p.exists():
            shutil.rmtree(p)
            log.info("reset: borrado %s", rel(p))


def run_steps(steps: list[str], cfg: dict, run_id: str, log: logging.Logger) -> dict:
    from .common.spark import get_spark
    from .jobs import bronze_batch, bronze_stream, gold, serving, silver
    jobs = {"bronze_batch": bronze_batch, "bronze_stream": bronze_stream, "silver": silver,
            "gold": gold, "serving": serving}
    landing = Path(cfg["paths"]["landing_dir"])
    if not landing.exists():
        raise FileNotFoundError(f"Landing no encontrado: {landing}. Definir LANDING_DIR o paths.landing_dir")
    spark = get_spark()
    results = {}
    for step in steps:
        t0 = datetime.now(timezone.utc)
        log.info("=== etapa %s ===", step)
        results[step] = jobs[step].run(spark, cfg, run_id)
        results[step]["seconds"] = round((datetime.now(timezone.utc) - t0).total_seconds(), 1)
    return results


def write_run_summary(cfg: dict, run_id: str, results: dict):
    tables = [
        ("bronze/customers_orgs", lake_path(cfg, "bronze", "customers_orgs")),
        ("bronze/users", lake_path(cfg, "bronze", "users")),
        ("bronze/billing_monthly", lake_path(cfg, "bronze", "billing_monthly")),
        ("bronze/resources", lake_path(cfg, "bronze", "resources")),
        ("bronze/usage_events", lake_path(cfg, "bronze", "usage_events")),
        ("silver/customers_orgs", lake_path(cfg, "silver", "customers_orgs")),
        ("silver/resources", lake_path(cfg, "silver", "resources")),
        ("silver/usage_events", lake_path(cfg, "silver", "usage_events")),
        ("quarantine/usage_events", lake_path(cfg, "quarantine", "usage_events")),
        ("gold/org_daily_usage_by_service", lake_path(cfg, "gold", "org_daily_usage_by_service")),
    ]
    rows = [{"tabla": n, "ruta": rel(p), **dir_stats(p)} for n, p in tables]
    write_json(f"run_{run_id}.json", {"run_id": run_id, "landing": rel(cfg["paths"]["landing_dir"]),
                                     "watermark": cfg["streaming"]["watermark"], "results": results, "storage": rows})
    md = [f"# Resumen de corrida `{run_id}`\n",
          f"- Landing: `{rel(cfg['paths']['landing_dir'])}` | watermark: `{cfg['streaming']['watermark']}` | "
          f"maxFilesPerTrigger: {cfg['streaming']['max_files_per_trigger']} | coalesce: {cfg['write'].get('coalesce')}\n",
          "## Rutas, archivos y tamaños\n", md_table(rows), "## Conteos por etapa\n"]
    for step, r in results.items():
        flat = {k: v for k, v in r.items() if k not in ("progress", "queries")}
        md.append(f"### {step}\n```json\n{__import__('json').dumps(flat, indent=2, ensure_ascii=False, default=str)}\n```\n")
    (EVIDENCE_DIR / "run_summary.md").write_text("\n".join(md), encoding="utf-8")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--steps", default=",".join(STEPS))
    ap.add_argument("--skip", default="")
    ap.add_argument("--reset", action="store_true")
    ap.add_argument("--config", default=None)
    args = ap.parse_args(argv)

    run_id = new_run_id()
    log = setup_logging(run_id)
    cfg = load_config(args.config)
    log.info("run_id=%s landing=%s lake=%s", run_id, rel(cfg["paths"]["landing_dir"]), rel(cfg["paths"]["lake_root"]))
    try:
        if args.reset:
            reset_lake(cfg, log)
            return 0
        skip = {s for s in args.skip.split(",") if s}
        steps = [s for s in args.steps.split(",") if s and s not in skip]
        unknown = set(steps) - set(STEPS)
        if unknown:
            raise ValueError(f"Etapas desconocidas: {unknown}. Válidas: {STEPS}")
        results = run_steps(steps, cfg, run_id, log)
        write_run_summary(cfg, run_id, results)
        if "serving" in results:
            write_queries_evidence(results["serving"])
        log.info("OK run_id=%s", run_id)
        return 0
    except Exception:
        log.exception("Pipeline falló (run_id=%s)", run_id)
        return 1


def write_queries_evidence(serving: dict):
    md = ["# Consultas CQL sobre Cassandra\n",
          f"Tabla `{serving['keyspace']}.{serving['table']}`: filas antes de la carga {serving['rows_before']}, "
          f"cargadas {serving['rows_loaded']}, después {serving['rows_after']}.\n"]
    for q in serving["queries"]:
        md += [f"## {q['name']}\n", f"```sql\n{q['cql']};\n```\n", f"Parámetros: `{q['params']}`\n", md_table(q["rows"])]
    (EVIDENCE_DIR / "cassandra_queries.md").write_text("\n".join(md), encoding="utf-8")


if __name__ == "__main__":
    sys.exit(main())
