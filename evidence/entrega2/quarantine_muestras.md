# Quarantine de eventos: conteos y muestras

Ruta: `datalake/quarantine/usage_events` (Parquet particionado por `rule_id`; `quarantine_reasons` lista todas las reglas que falló el registro).

| rule_id | descripción | registros |
|---|---|---|
| Q0_parse_error | línea JSON no parseable | 0 |
| Q1_event_id_null | event_id nulo | 0 |
| Q2_cost_below_min | cost_usd_increment < mínimo configurado (flag de anomalía) | 211 |
| Q3_unit_null_with_value | unit nulo cuando existe value | 2033 |
| Q6_org_orphan | org_id inexistente en customers_orgs | 0 |


Flags informativos en Silver (no bloquean): `flag_value_cast_failed`=0, `flag_cost_spike`=46, `flag_event_before_resource`=7006, `flag_genai_tokens_out_of_rule`=0

## Q2_cost_below_min (muestra de 5)

| event_id | event_ts | org_id | service | metric | value | unit | cost_usd_increment | quarantine_reasons | source_file |
|---|---|---|---|---|---|---|---|---|---|
| evt_05rs1cd7uxv6 | 2025-08-05 15:21:00 | org_1n13jcat | compute | requests | 119.0 | count | -7.7014 | ['Q2_cost_below_min'] | events_part_0009.jsonl |
| evt_07y0qnbewq73 | 2025-08-23 17:57:00 | org_kdgigatj | database | storage_gb_hours | 11.6425 | gb_hours | -0.506 | ['Q2_cost_below_min'] | events_part_0063.jsonl |
| evt_0buvvhr9d2og | 2025-07-06 01:55:00 | org_5935a0l7 | compute | requests | 133.0 | count | -10.99 | ['Q2_cost_below_min'] | events_part_0037.jsonl |
| evt_0mv15gk8xuga | 2025-08-08 23:47:00 | org_53lc58dr | genai | cpu_hours | 1.5755 | hours | -0.1854 | ['Q2_cost_below_min'] | events_part_0014.jsonl |
| evt_0s3r8qeziv84 | 2025-07-08 00:00:00 | org_5lxo6qji | database | requests | 121.0 | count | -5.1627 | ['Q2_cost_below_min'] | events_part_0071.jsonl |

## Q3_unit_null_with_value (muestra de 5)

| event_id | event_ts | org_id | service | metric | value | unit | cost_usd_increment | quarantine_reasons | source_file |
|---|---|---|---|---|---|---|---|---|---|
| evt_003mc7ncrk0z | 2025-08-04 11:40:00 | org_w3zp08j3 | genai | requests | 120.0 |  | 11.5385 | ['Q3_unit_null_with_value'] | events_part_0097.jsonl |
| evt_01xb4ljijao2 | 2025-08-08 11:33:00 | org_axeqbnhl | compute | cpu_hours | 1.7567 |  | 0.1357 | ['Q3_unit_null_with_value'] | events_part_0104.jsonl |
| evt_02fpkgce46vv | 2025-08-25 08:55:00 | org_kdgigatj | analytics | cpu_hours | 1.1257 |  | 0.0927 | ['Q3_unit_null_with_value'] | events_part_0090.jsonl |
| evt_02lwdvmyd06y | 2025-07-25 11:35:00 | org_0lvsnujz | storage | requests | 121.0 |  | 1.7358 | ['Q3_unit_null_with_value'] | events_part_0064.jsonl |
| evt_042dwzutl991 | 2025-08-29 18:22:00 | org_1swjckjl | database | cpu_hours | 2.0187 |  | 0.092 | ['Q3_unit_null_with_value'] | events_part_0036.jsonl |
