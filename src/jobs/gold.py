"""Silver -> Gold: mart FinOps org_daily_usage_by_service (grano org_id, usage_date, service)
con features diarias y score de anomalía MAD (componente analítico, D6/D15)."""
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..common.anomaly import add_mad_anomaly
from ..common.config import lake_path
from ..common.evidence import rel
from .silver import maybe_coalesce

log = logging.getLogger("tp.gold")

GRAIN = ["org_id", "usage_date", "service"]


def build_org_daily_usage(silver: DataFrame, threshold: float, min_days: int) -> DataFrame:
    daily = silver.groupBy(*GRAIN).agg(
        F.round(F.sum("cost_usd"), 4).alias("daily_cost_usd"),
        F.round(F.sum("requests"), 2).alias("requests"),
        F.round(F.sum("cpu_hours"), 4).alias("cpu_hours"),
        F.round(F.sum("storage_gb_hours"), 4).alias("storage_gb_hours"),
        F.sum("genai_tokens").alias("genai_tokens"),
        F.round(F.sum("carbon_kg"), 6).alias("carbon_kg"),
        F.count("*").alias("events_count"),
        F.sum(F.col("flag_cost_spike").cast("int")).alias("spike_events"),
        F.first("org_name", ignorenulls=True).alias("org_name"),
        F.first("industry", ignorenulls=True).alias("industry"),
        F.first("plan_tier", ignorenulls=True).alias("plan_tier"),
    )
    return add_mad_anomaly(daily, "daily_cost_usd", ["org_id", "service"], threshold, min_days)


def run(spark: SparkSession, cfg: dict, run_id: str) -> dict:
    silver = spark.read.parquet(lake_path(cfg, "silver", "usage_events"))
    a = cfg["anomaly"]
    gold = build_org_daily_usage(silver, float(a["threshold"]), int(a["min_days"])).withColumn("gold_run_id", F.lit(run_id))
    out = lake_path(cfg, "gold", "org_daily_usage_by_service")
    maybe_coalesce(gold, cfg).write.mode("overwrite").option(
        "partitionOverwriteMode", "static").partitionBy("usage_date").parquet(out)
    g = spark.read.parquet(out)
    n = g.count()
    n_keys = g.select(*GRAIN).distinct().count()
    n_anom = g.where("is_cost_anomaly").count()
    log.info("gold/org_daily_usage_by_service filas=%d claves únicas=%d anomalías=%d", n, n_keys, n_anom)
    write_anomaly_evidence(g, a)
    return {"rows": n, "distinct_grain_keys": n_keys, "anomalies": n_anom, "path": rel(out),
            "cost_total_usd": round(g.agg(F.sum("daily_cost_usd")).first()[0] or 0, 2)}


def write_anomaly_evidence(g: DataFrame, a: dict):
    """Métricas del componente analítico. No hay etiquetas reales: se usan los spikes >= umbral
    (eventos con costo extremo inyectados en el dataset) como pseudo-etiqueta para estimar cobertura."""
    from ..common.evidence import EVIDENCE_DIR, md_table
    scored = g.where(F.col("cost_robust_z").isNotNull())
    flagged = g.where("is_cost_anomaly")
    with_spike = g.where("spike_events > 0")
    tp = flagged.where("spike_events > 0").count()
    n_flag, n_spike = flagged.count(), with_spike.count()
    metrics = [
        {"métrica": "filas del mart (org, día, servicio)", "valor": g.count()},
        {"métrica": "filas con score (grupo con >= min_days y MAD > 0)", "valor": scored.count()},
        {"métrica": f"filas marcadas |robust_z| > {a['threshold']}", "valor": n_flag},
        {"métrica": "tasa de anomalías", "valor": f"{n_flag / max(g.count(), 1):.2%}"},
        {"métrica": "filas que contienen al menos un spike de costo", "valor": n_spike},
        {"métrica": "spikes detectados (cobertura sobre pseudo-etiqueta)", "valor": f"{tp} de {n_spike} ({tp / max(n_spike, 1):.0%})"},
        {"métrica": "anomalías que contienen un spike (precisión sobre pseudo-etiqueta)", "valor": f"{tp} de {n_flag} ({tp / max(n_flag, 1):.0%})"},
    ]
    top = [r.asDict() for r in flagged.select("org_id", "usage_date", "service", "daily_cost_usd",
                                                "cost_median_org_service", "cost_robust_z", "spike_events")
           .orderBy(F.desc("cost_robust_z")).limit(10).collect()]
    md = ["# Componente analítico: anomalías de costo (MAD robusto)\n",
          f"Método (D6): por cada `(org_id, service)` se calcula la mediana y el MAD del costo diario; "
          f"`robust_z = 0.6745 * (x - mediana) / MAD`; se marca `is_cost_anomaly` si `|robust_z| > {a['threshold']}` "
          f"y el grupo tiene al menos {a['min_days']} días. Columnas en Gold y en Cassandra: `cost_robust_z`, "
          "`is_cost_anomaly`, `cost_median_org_service` (contexto).\n",
          md_table(metrics), "## Top 10 anomalías por score\n", md_table(top),
          "## Limitaciones\n",
          "- No hay etiquetas reales de anomalía: la cobertura y precisión se estiman contra los spikes de costo "
          "(pseudo-etiqueta), que no son la única forma de anomalía (también hay caídas y días con muchos eventos).\n"
          "- Con ~60 días por grupo la mediana es estable, pero grupos con pocos días no se puntúan (`cost_robust_z` nulo).\n"
          "- Los costos negativos (Q2) quedan en quarantine y no entran al score; se analizan aparte.\n"]
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    (EVIDENCE_DIR / "anomalias.md").write_text("\n".join(md), encoding="utf-8")
