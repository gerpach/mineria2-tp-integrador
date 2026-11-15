"""Streaming a Bronze con datos chicos: esquema v1/v2, dedupe, línea corrupta y checkpoint."""
import json

from src.jobs.bronze_stream import run_stream

V1 = {"event_id": "evt_a", "timestamp": "2025-07-10T10:00:00Z", "org_id": "org_a", "resource_id": "res_1",
      "service": "compute", "region": "us-east", "metric": "requests", "value": "12", "unit": "count",
      "cost_usd_increment": 1.0, "schema_version": 1}
V2 = dict(V1, event_id="evt_b", schema_version=2, carbon_kg=0.02, value=3.5, service="genai", genai_tokens=500)


def write(path, lines):
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def test_stream_bronze(spark, tmp_path):
    inp = tmp_path / "in"
    inp.mkdir()
    write(inp / "p1.jsonl", [json.dumps(V1), json.dumps(V2), json.dumps(V1), "{esto no es json"])
    out, ck = str(tmp_path / "out"), str(tmp_path / "ck")
    run_stream(spark, str(inp), out, ck, "61 days", 5, "test")
    df = spark.read.parquet(out)
    rows = {r.event_id: r for r in df.collect()}
    assert df.count() == 3                                  # evt_a deduplicado + evt_b + línea corrupta
    assert rows["evt_a"].carbon_kg is None and rows["evt_a"].genai_tokens is None   # v1
    assert rows["evt_b"].genai_tokens == 500 and rows["evt_b"].value == "3.5"       # v2, value como texto
    assert rows[None]._corrupt_record == "{esto no es json"
    assert {"ingest_ts", "source_file", "event_date"} <= set(df.columns)

    # Re-ejecución con el mismo checkpoint: no duplica.
    progress = run_stream(spark, str(inp), out, ck, "61 days", 5, "test")
    assert sum(p["input_rows"] for p in progress) == 0
    assert spark.read.parquet(out).count() == 3
