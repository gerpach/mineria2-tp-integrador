# Diccionario de datos inicial (Landing)
Columnas tomadas del perfil real de los CSV. Esquema de `usage_events_stream` obtenido de una muestra de 20 de 120 archivos (7 200 eventos).

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
| usage_events | unit | string | `count`, `hours`, `gb_hours`; nulo en 5 % |
| usage_events | cost_usd_increment | double | Puede ser negativo; spikes hasta ~195 USD |
| usage_events | schema_version | int | 1 (hasta 17/07/2025) / 2 (desde 18/07/2025) |
| usage_events | carbon_kg | double | Solo v2; 0–0.032 |
| usage_events | genai_tokens | long | Solo v2 y servicio `genai` |
| *todas (Bronze)* | ingest_ts | timestamp | Columna técnica |
| *todas (Bronze)* | source_file | string | Columna técnica |
