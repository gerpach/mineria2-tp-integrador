from datetime import date, datetime

from pyspark.sql import Row

from src.common.quality import (add_usage_features, apply_event_rules, dedupe_by_event_id, normalize_events,
                                split_valid_quarantine)


EV_SCHEMA = ("event_id string, event_ts timestamp, org_id string, resource_id string, service string, region string, "
             "metric string, value string, unit string, cost_usd_increment double, schema_version int, carbon_kg double, "
             "genai_tokens long, _corrupt_record string, ingest_ts timestamp, source_file string")


def df_of(spark, rows):
    return spark.createDataFrame(rows, EV_SCHEMA)


def ev(event_id="evt_1", value="1.5", unit="count", cost=1.0, org="org_a", metric="requests", service="compute",
       version=2, tokens=None, ts=datetime(2025, 7, 10, 12, 0), ingest=datetime(2026, 11, 1), src="f1", corrupt=None):
    return Row(event_id=event_id, event_ts=ts, org_id=org, resource_id="res_1", service=service, region="us-east",
               metric=metric, value=value, unit=unit, cost_usd_increment=cost, schema_version=version,
               carbon_kg=None if version == 1 else 0.01, genai_tokens=tokens, _corrupt_record=corrupt,
               ingest_ts=ingest, source_file=src)


def run_rules(spark, rows, resource_created=date(2025, 7, 1)):
    df = normalize_events(df_of(spark, rows))
    res = spark.createDataFrame([Row(resource_id="res_1", resource_created_at=resource_created)])
    orgs = spark.createDataFrame([Row(org_id="org_a")])
    return apply_event_rules(df.join(res, "resource_id", "left"), -0.01, 100.0, orgs)


def test_value_cast_with_controlled_fallback(spark):
    out = normalize_events(df_of(spark, [ev(value=" 12.5 "), ev(event_id="e2", value="abc"),
                                                ev(event_id="e3", value=None)])).orderBy("event_id").collect()
    by_id = {r.event_id: r for r in out}
    assert by_id["evt_1"].value_num == 12.5 and not by_id["evt_1"].flag_value_cast_failed
    assert by_id["e2"].value_num is None and by_id["e2"].flag_value_cast_failed
    assert by_id["e3"].value_num is None and not by_id["e3"].flag_value_cast_failed


def test_blocking_rules_go_to_quarantine(spark):
    rows = [ev(event_id="ok"),
            ev(event_id="limite", cost=-0.01),                 # justo en el umbral: válido
            ev(event_id="q2", cost=-5.0),
            ev(event_id="q3", unit=None),
            ev(event_id="q3_sin_value", value=None, unit=None),  # sin value no aplica Q3
            ev(event_id="q6", org="org_fantasma"),
            ev(event_id=None),
            ev(event_id=None, corrupt="{roto")]
    valid, quarantine = split_valid_quarantine(run_rules(spark, rows))
    assert sorted(r.event_id for r in valid.collect()) == ["limite", "ok", "q3_sin_value"]
    rules = sorted(r.rule_id for r in quarantine.collect())
    assert rules == ["Q0_parse_error", "Q1_event_id_null", "Q2_cost_below_min", "Q3_unit_null_with_value", "Q6_org_orphan"]
    q2 = quarantine.where("event_id = 'q2'").first()
    assert q2.flag_cost_anomaly


def test_informative_flags(spark):
    rows = [ev(event_id="antes", ts=datetime(2025, 6, 20)), ev(event_id="spike", cost=250.0),
            ev(event_id="tokens_mal", tokens=10, service="compute"), ev(event_id="tokens_ok", tokens=10, service="genai")]
    out = {r.event_id: r for r in run_rules(spark, rows).collect()}
    assert out["antes"].flag_event_before_resource and not out["spike"].flag_event_before_resource
    assert out["spike"].flag_cost_spike
    assert out["tokens_mal"].flag_genai_tokens_out_of_rule and not out["tokens_ok"].flag_genai_tokens_out_of_rule


def test_dedupe_keeps_first_ingest_and_does_not_collapse_nulls(spark):
    rows = [ev(event_id="a", cost=2.0, ingest=datetime(2026, 11, 2), src="f2"),
            ev(event_id="a", cost=1.0, ingest=datetime(2026, 11, 1), src="f1"),
            ev(event_id=None), ev(event_id=None, value="9")]
    out = dedupe_by_event_id(normalize_events(df_of(spark, rows))).collect()
    assert len(out) == 3
    assert [r.cost_usd_increment for r in out if r.event_id == "a"] == [1.0]


def test_features_pivot_metric(spark):
    rows = [ev(event_id="r", metric="requests", value="10"), ev(event_id="c", metric="cpu_hours", value="2", unit="hours")]
    out = {r.event_id: r for r in add_usage_features(normalize_events(df_of(spark, rows))).collect()}
    assert out["r"].requests == 10 and out["r"].cpu_hours is None
    assert out["c"].cpu_hours == 2 and out["c"].requests is None
