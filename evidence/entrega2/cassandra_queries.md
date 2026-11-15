# Consultas CQL sobre Cassandra

Tabla `cloud_analytics.org_daily_usage_by_service`: filas antes de la carga 0, cargadas 11050, después 11050.

## Consulta 1: costos y requests diarios por org y servicio en un rango

```sql
SELECT usage_date, daily_cost_usd, requests, cpu_hours, storage_gb_hours, is_cost_anomaly
FROM cloud_analytics.org_daily_usage_by_service
WHERE org_id = :org_id AND service = :service
  AND usage_date >= :date_from AND usage_date <= :date_to;
```

Parámetros: `{'org_id': 'org_ktakpuxq', 'service': 'compute', 'date_from': datetime.date(2025, 8, 25), 'date_to': datetime.date(2025, 8, 31)}`

| usage_date | daily_cost_usd | requests | cpu_hours | storage_gb_hours | is_cost_anomaly |
|---|---|---|---|---|---|
| 2025-08-31 | 27.9866 | 344.0 | 5.3458 | 8.0952 | False |
| 2025-08-30 | 21.9982 | 225.0 | 3.5113 | 34.3998 | False |
| 2025-08-29 | 10.2654 | 123.0 | 2.6513 | 3.928 | False |
| 2025-08-28 | 49.3702 | 469.0 | 4.2299 | 72.4386 | False |
| 2025-08-27 | 65.1642 | 715.0 | 1.7885 | 7.3014 | False |
| 2025-08-26 | 69.17 | 828.0 | 1.8858 | 54.9735 | False |
| 2025-08-25 | 28.2485 | 338.0 | 4.835 | 22.3314 | False |

## Consulta 2: Top-N servicios por costo, últimos 14 días

```sql
SELECT service, SUM(daily_cost_usd) AS cost_14d_usd, SUM(requests) AS requests_14d
FROM cloud_analytics.org_daily_usage_by_service
WHERE org_id = :org_id
  AND service IN ('analytics', 'compute', 'database', 'genai', 'networking', 'storage')
  AND usage_date >= :date_from AND usage_date <= :date_to
GROUP BY org_id, service;
```

Parámetros: `{'org_id': 'org_ktakpuxq', 'date_from': datetime.date(2025, 8, 18), 'date_to': datetime.date(2025, 8, 31)}`

| service | cost_14d_usd | requests_14d |
|---|---|---|
| compute | 626.9616 | 6990.0 |
| genai | 140.6273 | 1115.0 |
| database | 59.3881 | 1180.0 |
| networking | 8.2631 | 733.0 |
