# Perfil de usage_events_stream (Landing)

- Archivos: **120** | Tamaño total: **12.93 MB** | Filas: **43200** | Líneas no parseables: **0**
- event_id duplicados (claves repetidas): **0** | event_id nulo: **0**
- Costos negativos: **216** (< -0.01, incumplen Q2: **211**) | costos >= 100 USD (spikes): **48**
- `value` como string: **1309** (no casteables: **0**) | `unit` nulo con `value` informado (incumple Q3): **2038**
- Archivos cuyo rango de timestamps supera 1 día: **120 de 120** (eventos NO ordenados por archivo)
- Rango temporal (texto crudo): **2025-07-03T00:02:00Z  ->  2025-08-31T23:58:00Z**
- Filas por schema_version: {2: 32400, 1: 10800}
- `value` nulo: **877** | `unit` nulo (total): **2075**
- cost_usd_increment: min **-154.4608**, mediana **1.0044**, max **317.4308**
- carbon_kg: min **0**, max **0.0326**, ceros **2641**
- genai_tokens fuera de regla Q9 (no genai o no v2): **0**
- Integridad contra resources.csv: resource_id huérfano **0**, org distinta **0**, service distinto **0**, region distinta **0**; **eventos anteriores al created_at del recurso (Q8): 7371** (17.1%)
## Claves por schema_version

**schema_version=2** (32400 filas): `carbon_kg`(32400), `cost_usd_increment`(32400), `event_id`(32400), `genai_tokens`(3132), `metric`(32400), `org_id`(32400), `region`(32400), `resource_id`(32400), `schema_version`(32400), `service`(32400), `timestamp`(32400), `unit`(32400), `value`(32400)
**schema_version=1** (10800 filas): `cost_usd_increment`(10800), `event_id`(10800), `metric`(10800), `org_id`(10800), `region`(10800), `resource_id`(10800), `schema_version`(10800), `service`(10800), `timestamp`(10800), `unit`(10800), `value`(10800)

## Tipos observados por campo (detecta números como texto)

| campo | tipos | nulos |
|---|---|---|
| carbon_kg | {'int': 2638, 'float': 29762} | 0 |
| cost_usd_increment | {'float': 43200} | 0 |
| event_id | {'str': 43200} | 0 |
| genai_tokens | {'int': 3132} | 0 |
| metric | {'str': 43200} | 0 |
| org_id | {'str': 43200} | 0 |
| region | {'str': 43200} | 0 |
| resource_id | {'str': 43200} | 0 |
| schema_version | {'int': 43200} | 0 |
| service | {'str': 43200} | 0 |
| timestamp | {'str': 43200} | 0 |
| unit | {'str': 41125, 'NoneType': 2075} | 2075 |
| value | {'NoneType': 877, 'float': 41014, 'str': 1309} | 877 |

## Valores de campos categóricos (detecta variantes a conformar)

- **service** (6 distintos): {'compute': 12498, 'storage': 7614, 'database': 7419, 'networking': 6921, 'analytics': 4590, 'genai': 4158}
- **region** (7 distintos): {'ap-northeast': 7773, 'us-east': 7566, 'us-west': 6384, 'sa-east': 6270, 'ap-south': 5943, 'eu-west': 4854, 'eu-central': 4410}
- **unit** (4 distintos): {'count': 18596, 'gb_hours': 12342, 'hours': 10187, None: 2075}
- **metric** (3 distintos): {'requests': 19512, 'storage_gb_hours': 12996, 'cpu_hours': 10692}

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