"""Reglas de calidad de eventos (docs/DISENO.md §7.2). Funciones puras sobre DataFrames para poder
probarlas con datos chicos en tests/. Reglas que bloquean -> quarantine; reglas informativas -> flag."""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

from .schemas import CORRUPT

# Reglas que mandan el registro a quarantine (en orden: la primera que falla es el rule_id principal).
BLOCKING_RULES = {
    "Q0_parse_error": "línea JSON no parseable",
    "Q1_event_id_null": "event_id nulo",
    "Q2_cost_below_min": "cost_usd_increment < mínimo configurado (flag de anomalía)",
    "Q3_unit_null_with_value": "unit nulo cuando existe value",
    "Q6_org_orphan": "org_id inexistente en customers_orgs",
}


def normalize_events(df: DataFrame) -> DataFrame:
    """Normaliza texto y castea `value` con fallback controlado (Q4): si no es numérico queda NULL + flag."""
    out = df
    for c in ("service", "region", "metric", "unit", "org_id", "resource_id"):
        out = out.withColumn(c, F.lower(F.trim(F.col(c))))
    value_txt = F.trim(F.col("value"))
    value_num = value_txt.cast("double")
    return (out.withColumn("value_num", value_num)
               .withColumn("flag_value_cast_failed", value_txt.isNotNull() & value_num.isNull())
               .withColumn("usage_date", F.to_date("event_ts")))


def dedupe_by_event_id(df: DataFrame) -> DataFrame:
    """Q1 (unicidad) sin depender del estado temporal del streaming (D9): conserva la primera ingesta."""
    w = Window.partitionBy("event_id").orderBy(F.col("ingest_ts").asc(), F.col("source_file").asc())
    with_id = df.where(F.col("event_id").isNotNull())
    deduped = with_id.withColumn("_rn", F.row_number().over(w)).where("_rn = 1").drop("_rn")
    return deduped.unionByName(df.where(F.col("event_id").isNull()))


def apply_event_rules(df: DataFrame, min_cost: float, spike_cost: float, org_ids: DataFrame) -> DataFrame:
    """Agrega `quarantine_reasons` (array, vacío = válido) y los flags informativos Q8/Q9/spike.
    `df` debe traer `resource_created_at` (join con resources)."""
    known = org_ids.select(F.col("org_id").alias("_known_org")).distinct()
    df = df.join(known, df.org_id == known._known_org, "left")
    checks = [
        (F.col(CORRUPT).isNotNull(), "Q0_parse_error"),
        (F.col("event_id").isNull(), "Q1_event_id_null"),
        (F.col("cost_usd_increment") < F.lit(min_cost), "Q2_cost_below_min"),
        (F.col("value_num").isNotNull() & F.col("unit").isNull(), "Q3_unit_null_with_value"),
        (F.col("org_id").isNotNull() & F.col("_known_org").isNull(), "Q6_org_orphan"),
    ]
    reasons = F.array_compact(F.array(*[F.when(cond, F.lit(rid)) for cond, rid in checks]))
    return (df.withColumn("quarantine_reasons", reasons)
              .drop("_known_org")
              .withColumn("flag_cost_anomaly", F.coalesce(F.col("cost_usd_increment") < F.lit(min_cost), F.lit(False)))
              .withColumn("flag_cost_spike", F.coalesce(F.col("cost_usd_increment") >= F.lit(spike_cost), F.lit(False)))
              # Q8: flag, no descarte (D12)
              .withColumn("flag_event_before_resource",
                          F.coalesce(F.col("usage_date") < F.col("resource_created_at"), F.lit(False)))
              # Q9: genai_tokens solo para genai en v2
              .withColumn("flag_genai_tokens_out_of_rule",
                          F.col("genai_tokens").isNotNull()
                          & ~((F.col("service") == "genai") & (F.col("schema_version") == 2))))


def split_valid_quarantine(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    valid = df.where(F.size("quarantine_reasons") == 0).drop("quarantine_reasons")
    quarantine = (df.where(F.size("quarantine_reasons") > 0)
                    .withColumn("rule_id", F.col("quarantine_reasons")[0]))
    return valid, quarantine


def add_usage_features(df: DataFrame) -> DataFrame:
    """Features por evento (se suman en Gold): pivot de `metric` + costo, tokens y carbono."""
    def metric(name):
        return F.when(F.col("metric") == name, F.col("value_num"))
    return (df.withColumn("requests", metric("requests"))
              .withColumn("cpu_hours", metric("cpu_hours"))
              .withColumn("storage_gb_hours", metric("storage_gb_hours"))
              .withColumn("cost_usd", F.col("cost_usd_increment")))
