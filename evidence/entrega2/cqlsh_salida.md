# Salida de cqlsh (Cassandra 4.1 local, infra/docker-compose.yml)

```

CREATE TABLE cloud_analytics.org_daily_usage_by_service (
    org_id text,
    service text,
    usage_date date,
    carbon_kg double,
    cost_robust_z double,
    cpu_hours double,
    daily_cost_usd double,
    events_count int,
    genai_tokens bigint,
    gold_run_id text,
    industry text,
    is_cost_anomaly boolean,
    org_name text,
    plan_tier text,
    requests double,
    spike_events int,
    storage_gb_hours double,
    PRIMARY KEY ((org_id, service), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND memtable = 'default'
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';

 count
-------
 11050

(1 rows)



 usage_date | daily_cost_usd | requests | is_cost_anomaly
------------+----------------+----------+-----------------
 2025-08-31 |        27.9866 |      344 |           False
 2025-08-30 |        21.9982 |      225 |           False
 2025-08-29 |        10.2654 |      123 |           False
 2025-08-28 |        49.3702 |      469 |           False
 2025-08-27 |        65.1642 |      715 |           False
 2025-08-26 |          69.17 |      828 |           False
 2025-08-25 |        28.2485 |      338 |           False

(7 rows)

 service    | cost_14d_usd
------------+--------------
    compute |     626.9616
   database |      59.3881
      genai |     140.6273
 networking |       8.2631

(4 rows)


```
