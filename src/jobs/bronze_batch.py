"""Batch Landing CSV -> Bronze Parquet (maestros). Esquema explícito, columnas técnicas,
dedupe por clave natural y partición por ingest_date (re-ejecutar el mismo día reemplaza la foto)."""
import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..common.config import lake_path
from ..common.evidence import rel
from ..common.schemas import CORRUPT, MASTERS

log = logging.getLogger("tp.bronze_batch")


def read_master(spark: SparkSession, csv_path: str, schema) -> DataFrame:
    return (spark.read.schema(schema).option("header", True).option("mode", "PERMISSIVE")
            # Las comillas internas vienen duplicadas ("[""env:prod""]"): escape = comilla doble.
            .option("quote", '"').option("escape", '"')
            .option("columnNameOfCorruptRecord", CORRUPT).csv(csv_path))


def to_bronze(df: DataFrame, keys: list[str], run_id: str) -> DataFrame:
    return (df.withColumn("source_file", F.input_file_name())
              .withColumn("ingest_ts", F.current_timestamp())
              .withColumn("ingest_date", F.current_date())
              .withColumn("pipeline_run_id", F.lit(run_id))
              .dropDuplicates(keys))


def run(spark: SparkSession, cfg: dict, run_id: str) -> dict:
    landing = Path(cfg["paths"]["landing_dir"])
    stats = {}
    for name, (schema, keys) in MASTERS.items():
        src = landing / f"{name}.csv"
        if not src.exists():
            raise FileNotFoundError(f"No existe {src}: revisar paths.landing_dir / LANDING_DIR")
        raw = read_master(spark, str(src), schema).cache()
        n_raw = raw.count()
        n_corrupt = raw.where(F.col(CORRUPT).isNotNull()).count()
        bronze = to_bronze(raw, keys, run_id)
        out = lake_path(cfg, "bronze", name)
        bronze.write.mode("overwrite").partitionBy("ingest_date").parquet(out)
        n_out = spark.read.parquet(out).where(F.col("ingest_date") == F.current_date()).count()
        raw.unpersist()
        stats[name] = {"landing_rows": n_raw, "corrupt_rows": n_corrupt, "bronze_rows": n_out,
                       "duplicates_removed": n_raw - n_out, "path": rel(out)}
        log.info("bronze/%s: landing=%d corruptas=%d bronze=%d", name, n_raw, n_corrupt, n_out)
    return stats
