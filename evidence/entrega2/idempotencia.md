# Idempotencia: re-ejecución completa del pipeline

Generado por `python -m src.experiments.idempotencia`: toma conteos, vuelve a correr todas las etapas y compara. Mecanismos: checkpoint del streaming (no relee archivos ya procesados), foto diaria de maestros (partición `ingest_date` reemplazada), Silver/Gold recalculados con overwrite y Cassandra con INSERT sobre la misma clave primaria (upsert).

- Filas leídas por el streaming en la re-ejecución: **0** (micro-lotes: 1)

- Filas re-enviadas a Cassandra: **11050** (upsert sobre las mismas claves)



| tabla | antes_de_reejecutar | despues_de_reejecutar | diferencia |
|---|---|---|---|
| bronze/customers_orgs | 80 | 80 | 0 |
| bronze/users | 800 | 800 | 0 |
| bronze/billing_monthly | 240 | 240 | 0 |
| bronze/resources | 400 | 400 | 0 |
| bronze/usage_events | 43200 | 43200 | 0 |
| silver/usage_events | 40956 | 40956 | 0 |
| quarantine/usage_events | 2244 | 2244 | 0 |
| gold/org_daily_usage_by_service | 11050 | 11050 | 0 |
| gold: suma daily_cost_usd | 141241.4284 | 141241.4284 | 0.0 |
| cassandra org_daily_usage_by_service (COUNT) | 11050 | 11050 | 0 |


**Resultado: sin duplicados (diferencia 0 en todas las capas)**
