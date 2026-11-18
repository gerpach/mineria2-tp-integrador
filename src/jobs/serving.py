"""Gold -> Cassandra/AstraDB. Crea keyspace/tabla (cql/01_schema.cql), carga desde Spark con el
driver de Python (INSERT = upsert por clave natural) y ejecuta las dos consultas de cql/02_queries.cql."""
import logging
import re
from datetime import date, timedelta

from pyspark.sql import SparkSession

from ..common.config import ROOT, lake_path

log = logging.getLogger("tp.serving")
TABLE = "org_daily_usage_by_service"
COLUMNS = ["org_id", "service", "usage_date", "daily_cost_usd", "requests", "cpu_hours", "storage_gb_hours",
           "genai_tokens", "carbon_kg", "events_count", "spike_events", "cost_robust_z", "is_cost_anomaly",
           "org_name", "industry", "plan_tier", "gold_run_id"]


def connect(cfg: dict):
    from cassandra.cluster import Cluster
    c = cfg["cassandra"]
    if c["mode"] == "astra":
        from cassandra.auth import PlainTextAuthProvider
        # Autenticación: token de aplicación (AstraCS:...) o el par client_id / client_secret.
        if c.get("token"):
            user, password = "token", c["token"]
        else:
            user, password = c["client_id"], c["client_secret"]
        if not (c["secure_connect_bundle"] and user and password):
            raise RuntimeError("Modo astra: definir ASTRA_BUNDLE_PATH y ASTRA_TOKEN "
                               "(o ASTRA_CLIENT_ID + ASTRA_CLIENT_SECRET) en el entorno o en .env")
        cluster = Cluster(cloud={"secure_connect_bundle": c["secure_connect_bundle"]},
                          auth_provider=PlainTextAuthProvider(user, password), protocol_version=4)
    else:
        cluster = Cluster(c["hosts"], port=int(c["port"]))
    return cluster, cluster.connect()


def cql_statements(file_name: str) -> list[str]:
    text = (ROOT / "cql" / file_name).read_text(encoding="utf-8")
    text = "\n".join(l for l in text.splitlines() if not l.strip().startswith("--"))
    return [s.strip() for s in text.split(";") if s.strip()]


def ensure_schema(session, cfg: dict):
    ks = cfg["cassandra"]["keyspace"]
    for stmt in cql_statements("01_schema.cql"):
        if stmt.upper().startswith("CREATE KEYSPACE") and cfg["cassandra"]["mode"] == "astra":
            continue
        session.execute(stmt.replace("cloud_analytics", ks))


def count_rows(session, ks: str) -> int:
    return session.execute(f"SELECT COUNT(*) FROM {ks}.{TABLE}").one()[0]


def load(spark: SparkSession, session, cfg: dict) -> int:
    from cassandra.concurrent import execute_concurrent_with_args
    ks = cfg["cassandra"]["keyspace"]
    insert = session.prepare(f"INSERT INTO {ks}.{TABLE} ({', '.join(COLUMNS)}) VALUES ({', '.join('?' * len(COLUMNS))})")
    gold = spark.read.parquet(lake_path(cfg, "gold", TABLE)).select(*COLUMNS)
    batch, n = [], 0
    for row in gold.toLocalIterator():
        batch.append(tuple(row))
        if len(batch) == 500:
            n += _flush(session, insert, batch, execute_concurrent_with_args)
            batch = []
    if batch:
        n += _flush(session, insert, batch, execute_concurrent_with_args)
    return n


def _flush(session, insert, batch, execute_concurrent_with_args) -> int:
    results = execute_concurrent_with_args(session, insert, batch, concurrency=50, raise_on_first_error=False)
    errors = [r.result_or_exc for r in results if not r.success]
    if errors:
        raise RuntimeError(f"{len(errors)} inserts fallaron; primero: {errors[0]}")
    return len(batch)


def run_queries(session, cfg: dict, org_id: str, service: str, last_day: date) -> list[dict]:
    ks = cfg["cassandra"]["keyspace"]
    q1, q2 = [s.replace("cloud_analytics", ks) for s in cql_statements("02_queries.cql")]
    p1 = {"org_id": org_id, "service": service, "date_from": last_day - timedelta(days=6), "date_to": last_day}
    p2 = {"org_id": org_id, "date_from": last_day - timedelta(days=13), "date_to": last_day}
    out = []
    for name, cql, params in (("Consulta 1: costos y requests diarios por org y servicio en un rango", q1, p1),
                              ("Consulta 2: Top-N servicios por costo, últimos 14 días", q2, p2)):
        rows = [r._asdict() for r in session.execute(_to_positional(cql, params), list(params.values()))]
        if "cost_14d_usd" in cql:
            rows.sort(key=lambda r: r["cost_14d_usd"] or 0, reverse=True)
        out.append({"name": name, "cql": cql, "params": params, "rows": rows})
        log.info("%s -> %d filas", name, len(rows))
    return out


def _to_positional(cql: str, params: dict) -> str:
    for k in params:
        cql = re.sub(rf":{k}\b", "%s", cql)
    return cql


def run(spark: SparkSession, cfg: dict, run_id: str) -> dict:
    from pyspark.sql import functions as F
    cluster, session = connect(cfg)
    try:
        ensure_schema(session, cfg)
        ks = cfg["cassandra"]["keyspace"]
        before = count_rows(session, ks)
        loaded = load(spark, session, cfg)
        after = count_rows(session, ks)
        gold = spark.read.parquet(lake_path(cfg, "gold", TABLE))
        last_day = gold.agg(F.max("usage_date")).first()[0]
        # Para la demo: la org con mayor costo en los últimos 14 días y su servicio principal.
        top = (gold.where(F.col("usage_date") > F.date_sub(F.lit(last_day), 14))
                   .groupBy("org_id", "service").agg(F.sum("daily_cost_usd").alias("c"))
                   .orderBy(F.desc("c")).first())
        queries = run_queries(session, cfg, top["org_id"], top["service"], last_day)
        log.info("cassandra %s.%s: antes=%d cargadas=%d después=%d", ks, TABLE, before, loaded, after)
        return {"keyspace": ks, "table": TABLE, "rows_before": before, "rows_loaded": loaded,
                "rows_after": after, "queries": queries}
    finally:
        cluster.shutdown()
