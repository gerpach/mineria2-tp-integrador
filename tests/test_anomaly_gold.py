from datetime import date, timedelta

from pyspark.sql import Row

from src.common.anomaly import add_mad_anomaly
from src.jobs.gold import build_org_daily_usage


def test_mad_flags_only_the_spike(spark):
    costs = [10, 11, 9, 10, 12, 10, 11, 9, 10, 300]
    df = spark.createDataFrame([Row(org_id="o", service="s", d=i, daily_cost_usd=float(c)) for i, c in enumerate(costs)])
    out = add_mad_anomaly(df, "daily_cost_usd", ["org_id", "service"], 3.5, 7).orderBy("d").collect()
    assert [r.is_cost_anomaly for r in out] == [False] * 9 + [True]


def test_mad_needs_min_days(spark):
    df = spark.createDataFrame([Row(org_id="o", service="s", daily_cost_usd=float(c)) for c in (1, 2, 100)])
    out = add_mad_anomaly(df, "daily_cost_usd", ["org_id", "service"], 3.5, 7).collect()
    assert not any(r.is_cost_anomaly for r in out) and all(r.cost_robust_z is None for r in out)


def test_gold_grain_and_sums(spark):
    base = date(2025, 7, 1)
    rows = []
    for day in range(8):
        for metric, val in (("requests", 100.0), ("cpu_hours", 2.0)):
            rows.append(Row(org_id="o1", usage_date=base + timedelta(days=day), service="compute", cost_usd=1.5,
                            requests=val if metric == "requests" else None,
                            cpu_hours=val if metric == "cpu_hours" else None, storage_gb_hours=None,
                            genai_tokens=None, carbon_kg=0.01, flag_cost_spike=False, org_name="O", industry="x",
                            plan_tier="pro"))
    schema = ("org_id string, usage_date date, service string, cost_usd double, requests double, cpu_hours double, "
              "storage_gb_hours double, genai_tokens long, carbon_kg double, flag_cost_spike boolean, org_name string, "
              "industry string, plan_tier string")
    gold = build_org_daily_usage(spark.createDataFrame(rows, schema), 3.5, 7)
    assert gold.count() == gold.select("org_id", "usage_date", "service").distinct().count() == 8
    r = gold.where(f"usage_date = '{base}'").first()
    assert (r.daily_cost_usd, r.requests, r.cpu_hours, r.events_count) == (3.0, 100.0, 2.0, 2)
