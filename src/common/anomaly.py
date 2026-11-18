"""D6: score robusto con MAD por (org_id, service) sobre el costo diario.
robust_z = 0.6745 * (x - mediana) / MAD. Robusto a los propios spikes que se quieren detectar."""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_mad_anomaly(df: DataFrame, value_col: str, group_cols: list[str], threshold: float, min_days: int) -> DataFrame:
    med = df.groupBy(*group_cols).agg(F.expr(f"percentile({value_col}, 0.5)").alias("_median"),
                                       F.count(value_col).alias("_n"))
    with_med = df.join(med, group_cols, "left").withColumn("_absdev", F.abs(F.col(value_col) - F.col("_median")))
    mad = with_med.groupBy(*group_cols).agg(F.expr("percentile(_absdev, 0.5)").alias("_mad"))
    out = with_med.join(mad, group_cols, "left")
    z = F.when((F.col("_mad") > 0) & (F.col("_n") >= min_days),
               F.lit(0.6745) * (F.col(value_col) - F.col("_median")) / F.col("_mad"))
    return (out.withColumn("cost_robust_z", F.round(z, 3))
               .withColumn("is_cost_anomaly", F.coalesce(F.abs(z) > F.lit(threshold), F.lit(False)))
               .withColumnRenamed("_median", "cost_median_org_service")
               .drop("_absdev", "_mad", "_n"))
