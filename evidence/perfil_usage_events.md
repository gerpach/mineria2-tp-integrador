# Perfil de usage_events_stream (Landing)

- Archivos: **20** | Tamaño total: **2.15 MB** | Filas: **7200** | Líneas no parseables: **0**
- event_id duplicados (claves repetidas): **0** | event_id nulo: **0**
- Costos negativos: **37** (< -0.01, incumplen Q2: **36**) | costos >= 100 USD (spikes): **9**
- `value` como string: **223** (no casteables: **0**) | `unit` nulo con `value` informado (incumple Q3): **361**
- Archivos cuyo rango de timestamps supera 1 día: **20 de 20** (eventos NO ordenados por archivo)
- Rango temporal (texto crudo): **2025-07-03T00:02:00Z  ->  2025-08-31T23:56:00Z**
- Filas por schema_version: {2: 5396, 1: 1804}

## Claves por schema_version

**schema_version=2** (5396 filas): `carbon_kg`(5396), `cost_usd_increment`(5396), `event_id`(5396), `genai_tokens`(518), `metric`(5396), `org_id`(5396), `region`(5396), `resource_id`(5396), `schema_version`(5396), `service`(5396), `timestamp`(5396), `unit`(5396), `value`(5396)
**schema_version=1** (1804 filas): `cost_usd_increment`(1804), `event_id`(1804), `metric`(1804), `org_id`(1804), `region`(1804), `resource_id`(1804), `schema_version`(1804), `service`(1804), `timestamp`(1804), `unit`(1804), `value`(1804)

## Tipos observados por campo (detecta números como texto)

| campo | tipos | nulos |
|---|---|---|
| carbon_kg | {'int': 460, 'float': 4936} | 0 |
| cost_usd_increment | {'float': 7200} | 0 |
| event_id | {'str': 7200} | 0 |
| genai_tokens | {'int': 518} | 0 |
| metric | {'str': 7200} | 0 |
| org_id | {'str': 7200} | 0 |
| region | {'str': 7200} | 0 |
| resource_id | {'str': 7200} | 0 |
| schema_version | {'int': 7200} | 0 |
| service | {'str': 7200} | 0 |
| timestamp | {'str': 7200} | 0 |
| unit | {'str': 6828, 'NoneType': 372} | 372 |
| value | {'NoneType': 160, 'float': 6817, 'str': 223} | 160 |

## Valores de campos categóricos (detecta variantes a conformar)

- **service** (6 distintos): {'compute': 2079, 'storage': 1263, 'database': 1251, 'networking': 1119, 'analytics': 799, 'genai': 689}
- **region** (7 distintos): {'ap-northeast': 1363, 'us-east': 1247, 'us-west': 1076, 'sa-east': 1021, 'ap-south': 928, 'eu-west': 793, 'eu-central': 772}
- **unit** (4 distintos): {'count': 3152, 'gb_hours': 2023, 'hours': 1653, None: 372}
- **metric** (3 distintos): {'requests': 3319, 'storage_gb_hours': 2143, 'cpu_hours': 1738}

## Ejemplo de registro por versión

schema_version=2:
```json
{
  "event_id": "evt_nwlh5boz2guf",
  "timestamp": "2025-08-17T01:55:00Z",
  "org_id": "org_cvs4f8cg",
  "resource_id": "res_i8kcmm7d",
  "service": "networking",
  "region": "sa-east",
  "metric": "requests",
  "value": null,
  "unit": "count",
  "cost_usd_increment": 0.9561,
  "schema_version": 2,
  "carbon_kg": 0
}
```
schema_version=1:
```json
{
  "event_id": "evt_xozb632zko0g",
  "timestamp": "2025-07-16T11:20:00Z",
  "org_id": "org_rixa11dp",
  "resource_id": "res_nmk7yyz6",
  "service": "database",
  "region": "ap-northeast",
  "metric": "requests",
  "value": null,
  "unit": "count",
  "cost_usd_increment": 5.6521,
  "schema_version": 1
}
```