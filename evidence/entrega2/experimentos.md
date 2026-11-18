# Experimentos controlados: deduplicación y late data

Generado por `python -m src.experiments.dedup_late_data`. Entradas armadas con eventos reales de Landing (Landing no se modifica) en `datalake/_experiments/`, procesadas con la misma función de streaming del pipeline (`bronze_stream.run_stream`, `maxFilesPerTrigger=1` para forzar un micro-lote por archivo).

## Experimento 1: deduplicación por `event_id` (watermark `61 days`)

| archivo | lineas |
|---|---|
| f1_original.jsonl | 200 |
| f2_reingesta_mismo_contenido.jsonl | 200 |
| f3_solapado.jsonl | 152 |

| medida | valor |
|---|---|
| lineas_entrada | 552 |
| event_id_unicos_esperados | 300 |
| bronze_filas | 300 |
| bronze_event_id_distintos | 300 |
| reejecucion_mismo_checkpoint_filas_leidas | 0 |
| reejecucion_mismo_checkpoint_bronze_filas | 300 |
| sin_checkpoint_bronze_filas | 300 |
| sin_checkpoint_silver_filas | 300 |
| ok | True |

Lectura: el streaming descarta los duplicados entre micro-lotes y dentro del mismo archivo; re-ejecutar con el mismo checkpoint no relee archivos; si se pierde el checkpoint, Bronze recibe duplicados pero Silver deduplica por `event_id` sin depender del estado temporal (D9).

## Experimento 2: late data con watermark de 10 minutos vs. configuración (D9)

Orden de llegada: b1 y b2 (eventos reales del 20-31/08), b3 (eventos reales del 05-19/07: tardíos respecto de agosto pero dentro del rango del dataset), b4 (5 eventos sintéticos, copias con `event_id` nuevo fechadas el 15/06, fuera de todo rango).

Hallazgo: en Spark 3.5 el filtro de late data de un lote usa el watermark calculado hasta el lote anterior (se ve en la tabla: el lote 1 todavía no descarta). Por eso el experimento manda dos lotes de agosto antes de los tardíos.

### watermark = `10 minutes`

| batch | archivo | filas_leidas | descartadas_por_watermark | watermark_al_iniciar_lote |
|---|---|---|---|---|
| 0 | b1_agosto_parte1.jsonl | 150 | 0 | 1970-01-01T00:00:00.000Z |
| 1 | b2_agosto_parte2.jsonl | 150 | 0 | 2025-08-31T23:41:00.000Z |
| 2 | b3_tardios_julio.jsonl | 200 | 200 | 2025-08-31T23:41:00.000Z |
| 3 | b4_muy_tardios_junio.jsonl | 5 | 5 | 2025-08-31T23:41:00.000Z |

| archivo | filas_entrada | filas_escritas_en_bronze |
|---|---|---|
| b1_agosto_parte1.jsonl | 150 | 150 |
| b2_agosto_parte2.jsonl | 150 | 150 |
| b3_tardios_julio.jsonl | 200 | 0 |
| b4_muy_tardios_junio.jsonl | 5 | 0 |

### watermark = `61 days`

| batch | archivo | filas_leidas | descartadas_por_watermark | watermark_al_iniciar_lote |
|---|---|---|---|---|
| 0 | b1_agosto_parte1.jsonl | 150 | 0 | 1970-01-01T00:00:00.000Z |
| 1 | b2_agosto_parte2.jsonl | 150 | 0 | 2025-07-01T23:51:00.000Z |
| 2 | b3_tardios_julio.jsonl | 200 | 0 | 2025-07-01T23:51:00.000Z |
| 3 | b4_muy_tardios_junio.jsonl | 5 | 5 | 2025-07-01T23:51:00.000Z |

| archivo | filas_entrada | filas_escritas_en_bronze |
|---|---|---|
| b1_agosto_parte1.jsonl | 150 | 150 |
| b2_agosto_parte2.jsonl | 150 | 150 |
| b3_tardios_julio.jsonl | 200 | 200 |
| b4_muy_tardios_junio.jsonl | 5 | 0 |

Lectura: con 10 minutos, apenas el watermark llega al 31/08 Spark descarta como tardíos los eventos válidos de julio. Con 61 días se conservan todos los eventos del rango y solo se descartan los que están fuera de la ventana (política de late data: se pierden y quedan contados en `numRowsDroppedByWatermark` del progreso del stream).
