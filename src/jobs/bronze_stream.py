"""Streaming Landing JSONL -> Bronze Parquet con Structured Streaming.
Esquema explícito, watermark parametrizable (D9: 61 días), dedupe por event_id dentro del watermark,
checkpoint (exactly-once del file sink) y partición por event_date."""
import json
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ..common.config import lake_path
from ..common.evidence import rel
from ..common.schemas import CORRUPT, USAGE_EVENTS

log = logging.getLogger("tp.bronze_stream")


def build_stream(spark: SparkSession, events_dir: str, watermark: str, max_files: int, run_id: str) -> DataFrame:
    raw = (spark.readStream.schema(USAGE_EVENTS)
           .option("maxFilesPerTrigger", max_files)
           .option("mode", "PERMISSIVE").option("columnNameOfCorruptRecord", CORRUPT)
           .json(events_dir))
    df = (raw.withColumnRenamed("timestamp", "event_ts")
             .withColumn("event_date", F.to_date("event_ts"))
             .withColumn("source_file", F.input_file_name())
             .withColumn("ingest_ts", F.current_timestamp())
             .withColumn("pipeline_run_id", F.lit(run_id))
             .withWatermark("event_ts", watermark))
    # Dedupe con estado acotado por el watermark: un event_id repetido dentro de la ventana se descarta.
    # Los eventos más viejos que el watermark se descartan como late data (ver experimentos.md).
    # Sin event_id (o línea corrupta) no hay clave: se usa un hash del registro crudo para no colapsar
    # todos los nulos en uno; Silver los manda a quarantine (Q0/Q1).
    payload = F.to_json(F.struct(*[F.col(c) for c in USAGE_EVENTS.fieldNames() if c != "timestamp"], "event_ts"))
    key = F.coalesce(F.col("event_id"), F.concat(F.lit("noid:"), F.sha2(payload, 256)))
    return (df.withColumn("_dedup_key", key)
              .dropDuplicatesWithinWatermark(["_dedup_key"])
              .drop("_dedup_key"))


def run_stream(spark: SparkSession, events_dir: str, out_path: str, checkpoint: str, watermark: str,
               max_files: int, run_id: str, coalesce: int | None = None) -> list[dict]:
    stream = build_stream(spark, events_dir, watermark, max_files, run_id)
    if coalesce:
        # D10: limita archivos por micro-lote (sin esto: particiones del estado x fechas x micro-lotes).
        stream = stream.coalesce(int(coalesce))
    q = (stream.writeStream.format("parquet").outputMode("append")
         .option("checkpointLocation", checkpoint)
         .partitionBy("event_date")
         .trigger(availableNow=True)
         .start(out_path))
    q.awaitTermination()
    if q.exception():
        raise RuntimeError(f"Streaming falló: {q.exception()}")
    progress = []
    for p in q.recentProgress:
        ops = p.get("stateOperators") or [{}]
        progress.append({
            "batch_id": p["batchId"],
            "input_rows": p.get("numInputRows", 0),
            "rows_dropped_by_watermark": sum(o.get("numRowsDroppedByWatermark", 0) for o in ops),
            "state_rows": sum(o.get("numRowsTotal", 0) for o in ops),
            "watermark": (p.get("eventTime") or {}).get("watermark"),
        })
    return progress


def run(spark: SparkSession, cfg: dict, run_id: str) -> dict:
    events_dir = str(cfg["paths"]["landing_dir"] / "usage_events_stream")
    out = lake_path(cfg, "bronze", "usage_events")
    ck = lake_path(cfg, "_checkpoints", "bronze_usage_events")
    s = cfg["streaming"]
    log.info("streaming desde %s | watermark=%s | maxFilesPerTrigger=%s", rel(events_dir), s["watermark"], s["max_files_per_trigger"])
    progress = run_stream(spark, events_dir, out, ck, s["watermark"], int(s["max_files_per_trigger"]), run_id,
                          cfg["write"].get("coalesce"))
    for p in progress:
        log.info("micro-lote %s", json.dumps(p))
    total_in = sum(p["input_rows"] for p in progress)
    n_bronze = spark.read.parquet(out).count()
    log.info("bronze/usage_events: leídas en esta corrida=%d micro-lotes=%d total en bronze=%d", total_in, len(progress), n_bronze)
    return {"micro_batches": len(progress), "input_rows_this_run": total_in,
            "dropped_by_watermark": sum(p["rows_dropped_by_watermark"] for p in progress),
            "bronze_rows_total": n_bronze, "path": rel(out), "checkpoint": rel(ck), "progress": progress}
