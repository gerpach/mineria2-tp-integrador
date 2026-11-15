# Gobierno preliminar (Entrega 2)

## Controles de calidad implementados

| Regla | Dónde | Acción | Resultado (120 archivos) |
|---|---|---|---|
| Q0 línea JSON no parseable | Bronze (`_corrupt_record`) → Silver | quarantine | 0 |
| Q1 `event_id` no nulo y único | streaming (`dropDuplicatesWithinWatermark`) + Silver (`row_number` por `event_id`) | nulo → quarantine; duplicado → se conserva la primera ingesta | 0 nulos, 0 duplicados (los experimentos prueban que se eliminan) |
| Q2 `cost_usd_increment >= -0.01` | Silver | quarantine + `flag_cost_anomaly` | 211 |
| Q3 `unit` no nulo si hay `value` | Silver | quarantine | 2 033 |
| Q4 `value` casteable | Silver | NULL + `flag_value_cast_failed` | 0 fallos (1 309 strings casteadas) |
| Q6 `org_id` existe en `customers_orgs` | Silver | quarantine | 0 |
| Q8 evento anterior al `created_at` del recurso | Silver | `flag_event_before_resource` | 7 006 en Silver |
| Q9 `genai_tokens` solo en genai v2 | Silver | `flag_genai_tokens_out_of_rule` | 0 |
| NPS fuera de [-100, 100] | Silver `customers_orgs` | NULL + `flag_nps_out_of_range` | 1 |
| Grano único del mart | Gold | conteo de claves en cada corrida | 11 050 = 11 050 |

Muestras de quarantine en `evidence/entrega2/quarantine_muestras.md`. Las reglas están cubiertas por `tests/test_quality.py`.

## Metadatos y linaje

| Columna | Capa | Para qué |
|---|---|---|
| `source_file` | Bronze → Silver → Quarantine | archivo de Landing de origen de cada fila |
| `ingest_ts`, `ingest_date` | Bronze | cuándo entró; `ingest_date` identifica la foto de cada maestro |
| `pipeline_run_id` | Bronze | corrida que escribió la fila (= nombre del log) |
| `silver_run_id`, `quarantine_run_id`, `gold_run_id` | Silver, Quarantine, Gold, Cassandra | corrida que generó cada capa |
| `quarantine_reasons`, `rule_id` | Quarantine | todas las reglas incumplidas y la principal |

Linaje de punta a punta: `landing/usage_events_stream/events_part_XXXX.jsonl` → `bronze/usage_events` (source_file, pipeline_run_id) → `silver/usage_events` (silver_run_id) → `gold/org_daily_usage_by_service` (gold_run_id) → Cassandra (`gold_run_id`). Cada corrida deja su log en `evidence/entrega2/logs/pipeline_<run_id>.log` y un resumen JSON `run_<run_id>.json`.

## Seguridad y acceso

- Sin credenciales en el repo: AstraDB solo por variables de entorno o un `.env` local ignorado por git (`ASTRA_TOKEN`, `ASTRA_BUNDLE_PATH`, `ASTRA_DB_ID`; plantilla en `.env.example`) (D8). `tests/test_config.py` falla si alguien escribe una credencial en el ejemplo.
- `config/settings.yaml`, `.env` y `secure-connect-*.zip` están en `.gitignore`.
- Landing es de solo lectura para el pipeline; `--reset` borra únicamente bronze/silver/gold/quarantine/_checkpoints y aborta si Landing quedara dentro de una de esas carpetas (test en `tests/test_pipeline.py`).
- `users.email` es dato personal: hoy solo llega a Bronze; no se publica en Silver/Gold ni en Cassandra. Enmascararlo si se construye `silver/users` (backlog).
- Acceso propuesto: escritura en el lake solo desde el pipeline; Cassandra con un rol de solo lectura para consumo (pendiente en AstraDB).

## Responsabilidades (a confirmar por el equipo)

| Área | Responsable |
|---|---|
| Ingesta batch y streaming (Bronze) | _a definir_ |
| Calidad, quarantine y gobierno | _a definir_ |
| Silver, Gold y anomalías | _a definir_ |
| Serving Cassandra/AstraDB y consultas | _a definir_ |
| Documentación, evidencias y defensa | _a definir_ |

## Observabilidad

- Log por corrida con conteos por etapa, por micro-lote (filas leídas, descartadas por watermark, filas en estado) y por regla de calidad.
- Código de salida distinto de 0 si una etapa falla, con el stack trace en el log.
