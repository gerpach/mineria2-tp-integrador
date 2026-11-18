"""Gold -> AstraDB por la Data API (HTTPS 443). Alternativa al driver CQL (serving.py) para redes que
bloquean el puerto CQL de Astra (29042), como el entorno de nube donde se generó la evidencia (D19).
Crea la misma tabla que cql/01_schema.cql (createTable con la misma PK), carga desde Spark en lotes
de 100 con insertMany (upsert por PK) y resuelve las dos consultas con find.

Uso: python -m src.jobs.serving_astra_api    (requiere ASTRA_TOKEN, ASTRA_DB_ID, ASTRA_REGION en .env)
"""
import json
import logging
import os
import sys
import urllib.request
from collections import defaultdict
from datetime import timedelta

from pyspark.sql import functions as F

from ..common.config import lake_path, load_config
from ..common.evidence import EVIDENCE_DIR, md_table
from ..common.logs import new_run_id, setup_logging
from ..common.spark import get_spark
from .serving import COLUMNS, TABLE

log = logging.getLogger("tp.serving_astra_api")
TYPES = {"org_id": "text", "service": "text", "usage_date": "date", "daily_cost_usd": "double", "requests": "double",
         "cpu_hours": "double", "storage_gb_hours": "double", "genai_tokens": "bigint", "carbon_kg": "double",
         "events_count": "int", "spike_events": "int", "cost_robust_z": "double", "is_cost_anomaly": "boolean",
         "org_name": "text", "industry": "text", "plan_tier": "text", "gold_run_id": "text"}
SERVICES = ["analytics", "compute", "database", "genai", "networking", "storage"]


class DataAPI:
    def __init__(self, db_id: str, region: str, keyspace: str, token: str):
        self.base = f"https://{db_id}-{region}.apps.astra.datastax.com/api/json/v1/{keyspace}"
        self.token = token

    def call(self, payload: dict, table: str | None = None) -> dict:
        url = f"{self.base}/{table}" if table else self.base
        req = urllib.request.Request(url, data=json.dumps(payload, default=str).encode(), method="POST",
                                     headers={"Token": self.token, "Content-Type": "application/json"})
        with urllib.request.urlopen(req, timeout=60) as r:
            body = json.loads(r.read())
        if body.get("errors"):
            raise RuntimeError(f"Data API: {body['errors'][0].get('message', body['errors'])[:300]}")
        return body

    def find_all(self, table: str, filt: dict, projection: dict | None = None) -> list[dict]:
        rows, state = [], None
        while True:
            opts = {"pageState": state} if state else {}
            body = self.call({"find": {"filter": filt, "projection": projection or {}, "options": opts}}, table)
            rows += body["data"]["documents"]
            state = body["data"].get("nextPageState")
            if not state:
                return rows


def create_table(api: DataAPI):
    api.call({"createTable": {"name": TABLE, "definition": {
        "columns": TYPES,
        "primaryKey": {"partitionBy": ["org_id", "service"], "partitionSort": {"usage_date": -1}}},
        "options": {"ifNotExists": True}}})


def load(spark, api: DataAPI, cfg: dict) -> int:
    gold = spark.read.parquet(lake_path(cfg, "gold", TABLE)).select(*COLUMNS)
    batch, n = [], 0
    for row in gold.toLocalIterator():
        batch.append({k: (v.isoformat() if k == "usage_date" else v) for k, v in row.asDict().items()})
        if len(batch) == 100:
            api.call({"insertMany": {"documents": batch, "options": {"ordered": False}}}, TABLE)
            n, batch = n + len(batch), []
    if batch:
        api.call({"insertMany": {"documents": batch, "options": {"ordered": False}}}, TABLE)
        n += len(batch)
    return n


def count_rows(api: DataAPI) -> int:
    return len(api.find_all(TABLE, {}, {"org_id": 1}))


def main() -> int:
    run_id = new_run_id() + "_astra"
    setup_logging(run_id)
    cfg = load_config()
    token, db_id = os.getenv("ASTRA_TOKEN"), os.getenv("ASTRA_DB_ID")
    region = os.getenv("ASTRA_REGION", "us-east-2")
    if not (token and db_id):
        log.error("Definir ASTRA_TOKEN y ASTRA_DB_ID (en .env, nunca en el repo)")
        return 1
    api = DataAPI(db_id, region, cfg["cassandra"]["keyspace"], token)
    spark = get_spark("tp-serving-astra")
    create_table(api)
    before = count_rows(api)
    loaded = load(spark, api, cfg)
    after = count_rows(api)
    log.info("astra %s.%s: antes=%d cargadas=%d después=%d", cfg["cassandra"]["keyspace"], TABLE, before, loaded, after)

    gold = spark.read.parquet(lake_path(cfg, "gold", TABLE))
    last_day = gold.agg(F.max("usage_date")).first()[0]
    top = (gold.where(F.col("usage_date") > F.date_sub(F.lit(last_day), 14)).groupBy("org_id", "service")
               .agg(F.sum("daily_cost_usd").alias("c")).orderBy(F.desc("c")).first())
    d1, d2 = (last_day - timedelta(days=6)).isoformat(), (last_day - timedelta(days=13)).isoformat()
    q1 = api.find_all(TABLE, {"org_id": top["org_id"], "service": top["service"],
                              "usage_date": {"$gte": d1, "$lte": last_day.isoformat()}},
                      {"usage_date": 1, "daily_cost_usd": 1, "requests": 1, "cpu_hours": 1, "storage_gb_hours": 1,
                       "is_cost_anomaly": 1})
    agg = defaultdict(lambda: {"cost_14d_usd": 0.0, "requests_14d": 0.0})
    for s in SERVICES:   # una lectura por partición (org_id, service), igual que el IN de la consulta CQL
        for r in api.find_all(TABLE, {"org_id": top["org_id"], "service": s,
                                      "usage_date": {"$gte": d2, "$lte": last_day.isoformat()}},
                              {"daily_cost_usd": 1, "requests": 1}):
            agg[s]["cost_14d_usd"] += r.get("daily_cost_usd") or 0
            agg[s]["requests_14d"] += r.get("requests") or 0
    agg = {s: v for s, v in agg.items() if v["cost_14d_usd"] or v["requests_14d"]}
    q2 = sorted(({"service": s, "cost_14d_usd": round(v["cost_14d_usd"], 4), "requests_14d": v["requests_14d"]}
                 for s, v in agg.items()), key=lambda r: r["cost_14d_usd"], reverse=True)
    q1 = sorted(({k: r.get(k) for k in ("usage_date", "daily_cost_usd", "requests", "cpu_hours", "storage_gb_hours",
                                         "is_cost_anomaly")} for r in q1), key=lambda r: r["usage_date"], reverse=True)
    md = ["# AstraDB: carga de Gold y consultas\n",
          f"Base AstraDB serverless (AWS us-east-2), keyspace `{cfg['cassandra']['keyspace']}`, tabla `{TABLE}` con "
          "`PRIMARY KEY ((org_id, service), usage_date)` y `usage_date DESC` (igual que `cql/01_schema.cql`). "
          "Generado por `python -m src.jobs.serving_astra_api` (Data API por HTTPS, D19).\n",
          f"- Filas antes de la carga: **{before}** | cargadas: **{loaded}** | después: **{after}**\n",
          f"## Consulta 1: costos y requests diarios de `{top['org_id']}` / `{top['service']}` entre {d1} y {last_day}\n",
          md_table(q1),
          f"## Consulta 2: Top-N servicios por costo de `{top['org_id']}`, últimos 14 días ({d2} a {last_day})\n",
          md_table(q2),
          "El CQL equivalente está en `cql/02_queries.cql`; la captura de su ejecución en el CQL console de Astra se "
          "guarda en `evidence/entrega2/astra_cql_console.png`.\n"]
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    (EVIDENCE_DIR / "astra_queries.md").write_text("\n".join(md), encoding="utf-8")
    log.info("consulta1=%d filas consulta2=%d filas", len(q1), len(q2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
