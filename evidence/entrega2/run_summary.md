# Resumen de corrida `20261115T182626Z`

- Landing: `datalake/landing` | watermark: `61 days` | maxFilesPerTrigger: 5 | coalesce: 1

## Rutas, archivos y tamaños

| tabla | ruta | parquet_files | partitions | size_kb | avg_file_kb |
|---|---|---|---|---|---|
| bronze/customers_orgs | datalake/bronze/customers_orgs | 1 | 1 | 6.9 | 6.9 |
| bronze/users | datalake/bronze/users | 1 | 1 | 23.5 | 23.5 |
| bronze/billing_monthly | datalake/bronze/billing_monthly | 1 | 1 | 12.0 | 12.0 |
| bronze/resources | datalake/bronze/resources | 1 | 1 | 11.8 | 11.8 |
| bronze/usage_events | datalake/bronze/usage_events | 1440 | 60 | 10894.9 | 7.6 |
| silver/customers_orgs | datalake/silver/customers_orgs | 1 | 1 | 7.2 | 7.2 |
| silver/resources | datalake/silver/resources | 1 | 1 | 12.9 | 12.9 |
| silver/usage_events | datalake/silver/usage_events | 60 | 60 | 3220.9 | 53.7 |
| quarantine/usage_events | datalake/quarantine/usage_events | 8 | 2 | 219.1 | 27.4 |
| gold/org_daily_usage_by_service | datalake/gold/org_daily_usage_by_service | 60 | 60 | 895.1 | 14.9 |

## Conteos por etapa

### bronze_batch
```json
{
  "customers_orgs": {
    "landing_rows": 80,
    "corrupt_rows": 0,
    "bronze_rows": 80,
    "duplicates_removed": 0,
    "path": "datalake/bronze/customers_orgs"
  },
  "users": {
    "landing_rows": 800,
    "corrupt_rows": 0,
    "bronze_rows": 800,
    "duplicates_removed": 0,
    "path": "datalake/bronze/users"
  },
  "billing_monthly": {
    "landing_rows": 240,
    "corrupt_rows": 0,
    "bronze_rows": 240,
    "duplicates_removed": 0,
    "path": "datalake/bronze/billing_monthly"
  },
  "resources": {
    "landing_rows": 400,
    "corrupt_rows": 0,
    "bronze_rows": 400,
    "duplicates_removed": 0,
    "path": "datalake/bronze/resources"
  },
  "seconds": 7.6
}
```

### bronze_stream
```json
{
  "micro_batches": 24,
  "input_rows_this_run": 43200,
  "dropped_by_watermark": 0,
  "bronze_rows_total": 43200,
  "path": "datalake/bronze/usage_events",
  "checkpoint": "datalake/_checkpoints/bronze_usage_events",
  "seconds": 31.8
}
```

### silver
```json
{
  "bronze_rows": 43200,
  "duplicates_removed": 0,
  "silver_rows": 40956,
  "quarantine_rows": 2244,
  "quarantine_by_rule": {
    "Q3_unit_null_with_value": 2033,
    "Q2_cost_below_min": 211
  },
  "flags": {
    "flag_value_cast_failed": 0,
    "flag_cost_spike": 46,
    "flag_event_before_resource": 7006,
    "flag_genai_tokens_out_of_rule": 0
  },
  "schema_versions": {
    "2": 30755,
    "1": 10201
  },
  "path": "datalake/silver/usage_events",
  "quarantine_path": "datalake/quarantine/usage_events",
  "seconds": 15.4
}
```

### gold
```json
{
  "rows": 11050,
  "distinct_grain_keys": 11050,
  "anomalies": 123,
  "path": "datalake/gold/org_daily_usage_by_service",
  "cost_total_usd": 141241.43,
  "seconds": 4.5
}
```

### serving
```json
{
  "keyspace": "cloud_analytics",
  "table": "org_daily_usage_by_service",
  "rows_before": 0,
  "rows_loaded": 11050,
  "rows_after": 11050,
  "seconds": 3.8
}
```
