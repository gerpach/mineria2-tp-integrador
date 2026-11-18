# Diccionario de datos

## Landing (fuentes)
Columnas tomadas del perfil real de los CSV. Esquema de `usage_events_stream` obtenido del perfil de los 120 archivos (43 200 eventos).

| Fuente | Columna | Tipo propuesto (Bronze) | Nota |
|---|---|---|---|
| customers_orgs | org_id | string | PK |
| customers_orgs | org_name, industry, hq_region, plan_tier, sales_rep, lifecycle_stage, marketing_source | string | Conformar región en Silver |
| customers_orgs | is_enterprise | boolean | |
| customers_orgs | signup_date | date | |
| customers_orgs | nps_score | double | Nulos; 1 valor > 100 |
| users | user_id (PK), org_id, email, role | string | `email` es dato personal: enmascarar en Silver |
| users | active | boolean | |
| users | created_at, last_login | date | `last_login` nulo; 232 anteriores a `created_at` |
| resources | resource_id (PK), org_id, service, region, state | string | |
| resources | created_at | date | |
| resources | tags_json | string (JSON array) | Parsear a `array<string>` en Silver |
| support_tickets | ticket_id (PK), org_id, category, severity | string | |
| support_tickets | created_at, resolved_at | date | `resolved_at` nulo = abierto |
| support_tickets | csat | double | Esperado 1–5; hay 0, 6, 7 |
| support_tickets | sla_breached | boolean | |
| marketing_touches | touch_id (PK), org_id, campaign, channel | string | |
| marketing_touches | timestamp | date | |
| marketing_touches | clicked, converted | boolean | 96 `converted` sin `clicked` |
| nps_surveys | org_id, survey_date | string, date | Clave compuesta |
| nps_surveys | nps_score | double | Nulos 20.7 % |
| nps_surveys | comment | string | Nulos 10.9 % |
| billing_monthly | invoice_id (PK), org_id, currency | string | USD / EUR / ARS |
| billing_monthly | month | date | 2025-06/07/08 |
| billing_monthly | subtotal, credits, taxes, exchange_rate_to_usd | double | 13 subtotales negativos; `credits` 57 % nulo |
| usage_events | event_id | string | PK (`evt_` + 12 car.) |
| usage_events | timestamp | timestamp (UTC, ISO-8601 con `Z`) | Granularidad de minuto; desordenado entre archivos |
| usage_events | org_id, resource_id, service, region | string | FK a customers_orgs / resources; 6 servicios, 7 regiones |
| usage_events | metric | string | `requests`, `cpu_hours`, `storage_gb_hours` |
| usage_events | value | double | Llega como número, string numérico o null → cast con fallback |
| usage_events | unit | string | `count`, `hours`, `gb_hours`; nulo en 4,8 % |
| usage_events | cost_usd_increment | double | Puede ser negativo (hasta −154 USD); spikes hasta ~317 USD |
| usage_events | schema_version | int | 1 (hasta 17/07/2025) / 2 (desde 18/07/2025) |
| usage_events | carbon_kg | double | Solo v2; 0–0.0326 (2 641 ceros) |
| usage_events | genai_tokens | long | Solo v2 y servicio `genai` (3 132 eventos) |
| *todas (Bronze)* | ingest_ts | timestamp | Columna técnica |
| *todas (Bronze)* | source_file | string | Columna técnica |


## Bronze (Parquet)

| Tabla | Partición | Columnas | Nota |
|---|---|---|---|
| bronze/customers_orgs, users, billing_monthly, resources | `ingest_date` | columnas de Landing con el tipo de arriba + `_corrupt_record`, `source_file`, `ingest_ts`, `pipeline_run_id` | dedupe por clave natural; una foto por día |
| bronze/usage_events | `event_date` | columnas del evento (`timestamp` → `event_ts`; `value` como **string**) + `_corrupt_record`, `source_file`, `ingest_ts`, `pipeline_run_id` | `value` se castea en Silver (Q4) |

## Silver (Parquet)

| Tabla | Columna | Tipo | Nota |
|---|---|---|---|
| silver/usage_events | event_id, org_id, resource_id, service, region, metric, unit | string | texto normalizado (lower/trim) |
| | event_ts / usage_date | timestamp / date | partición `usage_date` (UTC) |
| | value_num | double | `value` casteado; NULL si no es numérico |
| | cost_usd_increment, cost_usd | double | `cost_usd` = feature de costo |
| | requests, cpu_hours, storage_gb_hours | double | features: pivot de `metric` |
| | carbon_kg, genai_tokens | double, long | NULL en v1 (compatibilidad v1/v2) |
| | schema_version | int | 1 o 2 |
| | resource_created_at | date | join con resources |
| | org_name, industry, plan_tier, is_enterprise, hq_region | — | join con customers_orgs |
| | flag_value_cast_failed, flag_cost_anomaly, flag_cost_spike, flag_event_before_resource, flag_genai_tokens_out_of_rule | boolean | flags de calidad (no bloquean) |
| | source_file, ingest_ts, pipeline_run_id, silver_run_id | — | linaje |
| silver/customers_orgs | columnas del maestro + `flag_nps_out_of_range` | — | `nps_score` fuera de [-100, 100] → NULL |
| silver/resources | columnas del maestro + `tags` (array<string>) | — | `tags_json` parseado |
| quarantine/usage_events | columnas del evento + `quarantine_reasons` (array), `rule_id`, `quarantine_run_id` | — | partición `rule_id` |

## Gold: `org_daily_usage_by_service` (grano `org_id, usage_date, service`)

| Columna | Tipo | Cálculo |
|---|---|---|
| org_id, usage_date, service | string, date, string | clave del grano |
| daily_cost_usd | double | Σ `cost_usd` del día |
| requests, cpu_hours, storage_gb_hours | double | Σ de cada métrica |
| genai_tokens | long | Σ tokens (solo genai v2) |
| carbon_kg | double | Σ carbono (solo v2) |
| events_count, spike_events | long | eventos del día y cuántos con costo ≥ 100 USD |
| cost_median_org_service | double | mediana del costo diario de la org+servicio |
| cost_robust_z, is_cost_anomaly | double, boolean | score MAD y flag (`|z| > 3.5`) |
| org_name, industry, plan_tier | string | contexto |
| gold_run_id | string | linaje |

## Cassandra: `cloud_analytics.org_daily_usage_by_service`

Mismas columnas que Gold (sin `cost_median_org_service`). `PRIMARY KEY ((org_id, service), usage_date)`, `CLUSTERING ORDER BY (usage_date DESC)`. Ver `cql/01_schema.cql`.
