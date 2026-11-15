# Backlog priorizado para el MVP final (09/12/2026)

Estado al 18/11/2026. Prioridades según la consigna 6.2 (obligatorio / deseable / fuera de alcance).

## Obligatorio (consigna 7.2 a 7.6)

| # | Ítem | Criterio de terminado |
|---|---|---|
| O1 | Mart `revenue_by_org_month` (billing → USD con créditos, impuestos y FX; aplica D4 y D5) + tabla Cassandra | Consulta 4 responde desde Cassandra |
| O2 | Mart `tickets_by_org_date` (Silver de `support_tickets`, CSAT fuera de rango → NULL + flag) + tabla | Consulta 3: tickets críticos y SLA breach por día, últimos 30 días |
| O3 | Mart `genai_tokens_by_org_date` + tabla | Consulta 5: tokens y costo estimado por día |
| O4 | Mart `cost_anomaly_mart` con contexto (ya existe el score en Gold) | Tabla consultable por org y fecha |
| O5 | Consulta 2 en tabla propia si el `IN` + `GROUP BY` resulta limitado en AstraDB | Top-N sin agregación en cliente pesada |
| O6 | Correr el serving por driver CQL + secure connect bundle desde Colab contra AstraDB (en E2 la carga a Astra se hizo por Data API, D19) | Log de `serving.py` en modo astra |
| O7 | Validar el Quickstart desde un entorno limpio (Colab y una PC) | Checklist ejecutado por otro integrante |
| O8 | Presentación ejecutiva, video y guion de defensa | Artefactos entregados |
| O9 | Completar responsables en `docs/GOBIERNO.md` y roles en el README | Tabla sin "a definir" |

## Deseable

| # | Ítem |
|---|---|
| D1 | Imputar `unit` desde `metric` (relación 1:1) en lugar de quarantine para Q3 (2 033 eventos), con flag |
| D2 | SCD tipo 2 para `plan_tier` / `lifecycle_stage` usando las fotos diarias de Bronze (D18) |
| D3 | `silver/users` con email enmascarado |
| D4 | Compactación periódica de Bronze eventos (hoy 1 440 archivos chicos) |
| D5 | Spark-Cassandra-Connector si el volumen crece (D16) |
| D6 | Tests de integración de serving contra Cassandra en CI |

## Fuera de alcance

- Orquestador (Airflow, etc.), despliegue en la nube y monitoreo con alertas.
- Modelos de ML supervisados (no hay etiquetas); el componente analítico es el score MAD.
- Particionar por `service` o por mes mientras el volumen sea el del dataset académico (D3, D10).
