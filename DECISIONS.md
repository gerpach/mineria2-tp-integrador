# Registro de decisiones

| ID | Fecha | Decisión | Alternativas | Justificación | Estado |
|---|---|---|---|---|---|
| D1 | 2026-10 | Patrón **Lambda** | Kappa, híbrido | Fuentes con naturaleza distinta (maestros batch vs. eventos continuos); mapea al requisito invariable | Aprobada |
| D2 | 2026-10 | Parquet (snappy) particionado por fecha en eventos | CSV, Delta | Requisito de la consigna; columnar y eficiente | Aprobada |
| D3 | 2026-10 | No particionar por `service` inicialmente | Fecha + servicio | Solo 6 valores y bajo volumen: evita archivos pequeños | Abierta |
| D4 | 2026-10 | `credits` nulo = 0 para revenue (con flag) | Descartar fila | 57 % nulos; descartar perdería facturación | Abierta (a validar) |
| D5 | 2026-10 | Subtotal negativo: conservar con flag, no eliminar | Eliminar / valor absoluto | Puede ser nota de crédito legítima | Abierta |
| D6 | 2026-10 | Anomalías con MAD robusto por org+servicio | z-score, percentiles | Resiste outliers y spikes | Abierta |
| D7 | 2026-10 | Cassandra modelada query-first: una tabla por consulta | Tabla única | Lecturas por partición sin ALLOW FILTERING | Aprobada |
| D8 | 2026-10 | Credenciales solo por variables de entorno | Archivo en repo | Criterio de aceptación: repo sin secretos | Aprobada |
| D9 | 2026-10 | Watermark parametrizable y largo (≥ 61 días) para la demo; dedupe de `event_id` sin depender del estado temporal | Watermark de 10 min | Cada archivo cubre todo el rango temporal: un watermark corto descartaría eventos válidos | Abierta |
| D10 | 2026-10 | Partición diaria en Silver con `coalesce(1)` | Partición mensual; por servicio | ~720 eventos/día → archivos diminutos; la consulta típica es por rango de fechas | Abierta |
| D11 | 2026-10 | `unit` nulo con `value`: quarantine (Q3 de la consigna; 2 038 casos) y evaluar imputación desde `metric` | Imputar siempre | La consigna exige la regla; la relación metric→unit es 1:1 | Abierta |
| D12 | 2026-10 | Eventos anteriores al `created_at` del recurso: flag, no descarte | Descartar | 7 371 eventos (17,1 %); descartar sesgaría costos | Abierta |
