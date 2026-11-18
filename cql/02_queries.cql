-- Consulta 1 (consigna 7.4 #1): costos y requests diarios de una organización y servicio en un rango de fechas.
SELECT usage_date, daily_cost_usd, requests, cpu_hours, storage_gb_hours, is_cost_anomaly
FROM cloud_analytics.org_daily_usage_by_service
WHERE org_id = :org_id AND service = :service
  AND usage_date >= :date_from AND usage_date <= :date_to;

-- Consulta 2 (consigna 7.4 #2): Top-N servicios por costo acumulado en los últimos 14 días de una organización.
-- IN sobre la última columna de la partition key + GROUP BY de la partition key: lectura de 6 particiones
-- conocidas, sin ALLOW FILTERING. El orden por costo (Top-N) se hace en el cliente (Cassandra no ordena por agregados).
SELECT service, SUM(daily_cost_usd) AS cost_14d_usd, SUM(requests) AS requests_14d
FROM cloud_analytics.org_daily_usage_by_service
WHERE org_id = :org_id
  AND service IN ('analytics', 'compute', 'database', 'genai', 'networking', 'storage')
  AND usage_date >= :date_from AND usage_date <= :date_to
GROUP BY org_id, service;
