-- Keyspace (solo Cassandra local; en AstraDB el keyspace se crea desde la consola).
CREATE KEYSPACE IF NOT EXISTS cloud_analytics
  WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Query-first (D7, D14): la partición (org_id, service) responde "una org y un servicio";
-- usage_date como clustering DESC permite rangos de fechas ordenados sin ALLOW FILTERING.
-- La clave primaria es el grano del mart: re-cargar = upsert (idempotencia).
CREATE TABLE IF NOT EXISTS cloud_analytics.org_daily_usage_by_service (
  org_id            text,
  service           text,
  usage_date        date,
  daily_cost_usd    double,
  requests          double,
  cpu_hours         double,
  storage_gb_hours  double,
  genai_tokens      bigint,
  carbon_kg         double,
  events_count      int,
  spike_events      int,
  cost_robust_z     double,
  is_cost_anomaly   boolean,
  org_name          text,
  industry          text,
  plan_tier         text,
  gold_run_id       text,
  PRIMARY KEY ((org_id, service), usage_date)
) WITH CLUSTERING ORDER BY (usage_date DESC);
