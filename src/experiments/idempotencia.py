"""Demostración de idempotencia: corre el pipeline completo dos veces seguidas y compara conteos
antes/después en cada capa y en Cassandra. Escribe evidence/entrega2/idempotencia.md.

Uso: python -m src.experiments.idempotencia            (agregar --skip-serving si no hay Cassandra)
"""
import argparse
import sys

from pyspark.sql import functions as F

from ..common.config import lake_path, load_config
from ..common.evidence import EVIDENCE_DIR, md_table, write_json
from ..common.logs import new_run_id, setup_logging
from ..common.spark import get_spark
from ..pipeline import STEPS, run_steps

TABLES = ["bronze/customers_orgs", "bronze/users", "bronze/billing_monthly", "bronze/resources",
          "bronze/usage_events", "silver/usage_events", "quarantine/usage_events", "gold/org_daily_usage_by_service"]


def snapshot(spark, cfg) -> dict:
    out = {}
    for t in TABLES:
        df = spark.read.parquet(lake_path(cfg, *t.split("/")))
        if t.startswith("bronze/") and "ingest_date" in df.columns:
            df = df.where(F.col("ingest_date") == F.current_date())    # foto vigente del maestro
        out[t] = df.count()
    out["gold: suma daily_cost_usd"] = round(spark.read.parquet(lake_path(cfg, "gold", "org_daily_usage_by_service"))
                                             .agg(F.sum("daily_cost_usd")).first()[0], 4)
    return out


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--skip-serving", action="store_true")
    args = ap.parse_args(argv)
    cfg = load_config()
    steps = [s for s in STEPS if not (args.skip_serving and s == "serving")]
    spark = get_spark()
    try:
        before = snapshot(spark, cfg)
    except Exception:                       # lake vacío: primero una corrida inicial
        run_id = new_run_id() + "_inicial"
        run_steps(steps, cfg, run_id, setup_logging(run_id))
        before = snapshot(spark, cfg)
    run_id = new_run_id() + "_reejecucion"
    log = setup_logging(run_id)
    res = run_steps(steps, cfg, run_id, log)
    after = snapshot(spark, cfg)
    rows = [{"tabla": k, "antes_de_reejecutar": before[k], "despues_de_reejecutar": after[k],
             "diferencia": round(after[k] - before[k], 4)} for k in before]
    cass = res.get("serving")
    if cass:
        rows.append({"tabla": "cassandra org_daily_usage_by_service (COUNT)", "antes_de_reejecutar": cass["rows_before"],
                     "despues_de_reejecutar": cass["rows_after"], "diferencia": cass["rows_after"] - cass["rows_before"]})
    stream = res["bronze_stream"]
    ok = all(r["diferencia"] == 0 for r in rows)
    write_json("idempotencia.json", {"rows": rows, "ok": ok, "stream_rows_read_on_rerun": stream["input_rows_this_run"],
                                     "cassandra_rows_reloaded": cass["rows_loaded"] if cass else None})
    md = ["# Idempotencia: re-ejecución completa del pipeline\n",
          "Generado por `python -m src.experiments.idempotencia`: toma conteos, vuelve a correr todas las etapas y "
          "compara. Mecanismos: checkpoint del streaming (no relee archivos ya procesados), foto diaria de maestros "
          "(partición `ingest_date` reemplazada), Silver/Gold recalculados con overwrite y Cassandra con INSERT sobre "
          "la misma clave primaria (upsert).\n",
          f"- Filas leídas por el streaming en la re-ejecución: **{stream['input_rows_this_run']}** "
          f"(micro-lotes: {stream['micro_batches']})\n"]
    if cass:
        md.append(f"- Filas re-enviadas a Cassandra: **{cass['rows_loaded']}** (upsert sobre las mismas claves)\n")
    md += ["\n", md_table(rows),
           f"\n**Resultado: {'sin duplicados (diferencia 0 en todas las capas)' if ok else 'HAY DIFERENCIAS'}**\n"]
    (EVIDENCE_DIR / "idempotencia.md").write_text("\n".join(md), encoding="utf-8")
    log.info("idempotencia ok=%s", ok)
    print("OK" if ok else "DIFERENCIAS")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
