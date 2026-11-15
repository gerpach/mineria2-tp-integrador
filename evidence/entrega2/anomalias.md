# Componente analítico: anomalías de costo (MAD robusto)

Método (D6): por cada `(org_id, service)` se calcula la mediana y el MAD del costo diario; `robust_z = 0.6745 * (x - mediana) / MAD`; se marca `is_cost_anomaly` si `|robust_z| > 3.5` y el grupo tiene al menos 7 días. Columnas en Gold y en Cassandra: `cost_robust_z`, `is_cost_anomaly`, `cost_median_org_service` (contexto).

| métrica | valor |
|---|---|
| filas del mart (org, día, servicio) | 11050 |
| filas con score (grupo con >= min_days y MAD > 0) | 11050 |
| filas marcadas |robust_z| > 3.5 | 123 |
| tasa de anomalías | 1.11% |
| filas que contienen al menos un spike de costo | 46 |
| spikes detectados (cobertura sobre pseudo-etiqueta) | 46 de 46 (100%) |
| anomalías que contienen un spike (precisión sobre pseudo-etiqueta) | 46 de 123 (37%) |

## Top 10 anomalías por score

| org_id | usage_date | service | daily_cost_usd | cost_median_org_service | cost_robust_z | spike_events |
|---|---|---|---|---|---|---|
| org_x7eedtjv | 2025-07-09 | compute | 311.255 | 12.72345 | 41.845 | 1 |
| org_okep7y6w | 2025-07-03 | compute | 202.646 | 11.1426 | 40.468 | 1 |
| org_pht0hl9x | 2025-08-25 | analytics | 257.042 | 11.3187 | 38.675 | 1 |
| org_w3zp08j3 | 2025-07-05 | networking | 27.4203 | 1.4849 | 38.388 | 0 |
| org_okep7y6w | 2025-08-07 | compute | 191.8968 | 11.1426 | 38.196 | 1 |
| org_w3zp08j3 | 2025-07-16 | networking | 26.4157 | 1.4849 | 36.901 | 0 |
| org_ujv6oh9s | 2025-07-19 | database | 142.7731 | 7.54265 | 32.564 | 1 |
| org_tvhhpbmy | 2025-07-10 | compute | 204.9628 | 10.4238 | 31.564 | 1 |
| org_d14ve92m | 2025-07-20 | analytics | 181.7988 | 10.3891 | 29.725 | 1 |
| org_874frhoc | 2025-07-17 | networking | 27.651 | 1.708 | 28.852 | 0 |

## Limitaciones

- No hay etiquetas reales de anomalía: la cobertura y precisión se estiman contra los spikes de costo (pseudo-etiqueta), que no son la única forma de anomalía (también hay caídas y días con muchos eventos).
- Con ~60 días por grupo la mediana es estable, pero grupos con pocos días no se puntúan (`cost_robust_z` nulo).
- Los costos negativos (Q2) quedan en quarantine y no entran al score; se analizan aparte.
