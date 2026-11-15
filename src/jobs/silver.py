"""Bronze -> Silver (batch sobre Bronze, D13). Conforma customers_orgs y resources, limpia y
deduplica eventos, aplica reglas Q0-Q9, separa quarantine, enriquece con joins y calcula features."""
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..common.config import lake_path
from ..common.evidence import rel
from ..common.quality import (add_usage_features, apply_event_rules, dedupe_by_event_id, normalize_events,
                              split_valid_quarantine)

log = logging.getLogger("tp.silver")


def latest_snapshot(spark: SparkSession, path: str) -> DataFrame:
    df = spark.read.parquet(path)
    last = df.agg(F.max("ingest_date")).first()[0]
    return df.where(F.col("ingest_date") == F.lit(last))


def maybe_coalesce(df: DataFrame, cfg: dict) -> DataFrame:
    n = cfg["write"].get("coalesce")
    return df.coalesce(int(n)) if n else df


def conform_customers(df: DataFrame) -> DataFrame:
    """Maestro conformado: texto normalizado y nps_score fuera de [-100, 100] -> NULL + flag."""
    out = df.drop("_corrupt_record")
    for c in ("org_id", "industry", "hq_region", "plan_tier", "lifecycle_stage", "marketing_source", "sales_rep"):
        out = out.withColumn(c, F.lower(F.trim(F.col(c))))
    bad_nps = (F.col("nps_score") < -100) | (F.col("nps_score") > 100)
    return (out.withColumn("flag_nps_out_of_range", F.coalesce(bad_nps, F.lit(False)))
               .withColumn("nps_score", F.when(bad_nps, None).otherwise(F.col("nps_score"))))


def conform_resources(df: DataFrame) -> DataFrame:
    out = df.drop("_corrupt_record")
    for c in ("resource_id", "org_id", "service", "region", "state"):
        out = out.withColumn(c, F.lower(F.trim(F.col(c))))
    return out.withColumn("tags", F.from_json("tags_json", "array<string>"))


def build_silver_events(events: DataFrame, customers: DataFrame, resources: DataFrame, cfg: dict):
    q = cfg["quality"]
    ev = dedupe_by_event_id(normalize_events(events))
    res = resources.select("resource_id", F.col("created_at").alias("resource_created_at"))
    ev = ev.join(res, "resource_id", "left")
    ev = apply_event_rules(ev, float(q["min_cost_increment_usd"]), float(q["spike_cost_usd"]), customers)
    valid, quarantine = split_valid_quarantine(ev)
    dims = customers.select("org_id", "org_name", "industry", "plan_tier", "is_enterprise", "hq_region")
    silver = add_usage_features(valid).join(dims, "org_id", "left").drop("_corrupt_record", "value")
    return silver, quarantine


def run(spark: SparkSession, cfg: dict, run_id: str) -> dict:
    customers = conform_customers(latest_snapshot(spark, lake_path(cfg, "bronze", "customers_orgs")))
    resources = conform_resources(latest_snapshot(spark, lake_path(cfg, "bronze", "resources")))
    maybe_coalesce(customers, cfg).write.mode("overwrite").parquet(lake_path(cfg, "silver", "customers_orgs"))
    maybe_coalesce(resources, cfg).write.mode("overwrite").parquet(lake_path(cfg, "silver", "resources"))

    bronze_events = spark.read.parquet(lake_path(cfg, "bronze", "usage_events"))
    n_bronze = bronze_events.count()
    silver, quarantine = build_silver_events(bronze_events, customers, resources, cfg)
    silver = silver.withColumn("silver_run_id", F.lit(run_id))

    out_silver = lake_path(cfg, "silver", "usage_events")
    out_q = lake_path(cfg, "quarantine", "usage_events")
    # Silver se recalcula completo desde Bronze: overwrite = re-ejecución idempotente.
    maybe_coalesce(silver, cfg).write.mode("overwrite").option(
        "partitionOverwriteMode", "static").partitionBy("usage_date").parquet(out_silver)
    quarantine.withColumn("quarantine_run_id", F.lit(run_id)).write.mode("overwrite").option(
        "partitionOverwriteMode", "static").partitionBy("rule_id").parquet(out_q)

    s = spark.read.parquet(out_silver)
    qq = spark.read.parquet(out_q)
    n_silver, n_q = s.count(), qq.count()
    n_distinct = bronze_events.where("event_id IS NOT NULL").select("event_id").distinct().count()
    n_null_id = bronze_events.where("event_id IS NULL").count()
    stats = {
        "bronze_rows": n_bronze,
        "duplicates_removed": n_bronze - n_distinct - n_null_id,
        "silver_rows": n_silver,
        "quarantine_rows": n_q,
        "quarantine_by_rule": {r["rule_id"]: r["count"] for r in qq.groupBy("rule_id").count().collect()},
        "flags": s.agg(*[F.sum(F.col(c).cast("int")).alias(c) for c in (
            "flag_value_cast_failed", "flag_cost_spike", "flag_event_before_resource",
            "flag_genai_tokens_out_of_rule")]).first().asDict(),
        "schema_versions": {r["schema_version"]: r["count"] for r in s.groupBy("schema_version").count().collect()},
        "path": rel(out_silver), "quarantine_path": rel(out_q),
    }
    write_quarantine_samples(qq, stats)
    log.info("silver/usage_events=%d quarantine=%d por regla=%s duplicados=%d",
             n_silver, n_q, stats["quarantine_by_rule"], stats["duplicates_removed"])
    return stats


def write_quarantine_samples(qq: DataFrame, stats: dict, n: int = 5):
    from ..common.evidence import EVIDENCE_DIR, md_table
    from ..common.quality import BLOCKING_RULES
    md = ["# Quarantine de eventos: conteos y muestras\n",
          f"Ruta: `{stats['quarantine_path'] if 'quarantine_path' in stats else 'datalake/quarantine/usage_events'}` "
          "(Parquet particionado por `rule_id`; `quarantine_reasons` lista todas las reglas que falló el registro).\n",
          md_table([{"rule_id": k, "descripción": v, "registros": stats["quarantine_by_rule"].get(k, 0)}
                    for k, v in BLOCKING_RULES.items()]),
          "\nFlags informativos en Silver (no bloquean): " + ", ".join(f"`{k}`={v}" for k, v in stats["flags"].items()) + "\n"]
    cols = ["event_id", "event_ts", "org_id", "service", "metric", "value", "unit", "cost_usd_increment",
            "quarantine_reasons", "source_file"]
    for rule in sorted(stats["quarantine_by_rule"]):
        rows = [r.asDict() for r in qq.where(F.col("rule_id") == rule).select(*cols).orderBy("event_id").limit(n).collect()]
        for r in rows:
            r["source_file"] = r["source_file"].rsplit("/", 1)[-1]
        md += [f"## {rule} (muestra de {len(rows)})\n", md_table(rows)]
    EVIDENCE_DIR.mkdir(parents=True, exist_ok=True)
    (EVIDENCE_DIR / "quarantine_muestras.md").write_text("\n".join(md), encoding="utf-8")
