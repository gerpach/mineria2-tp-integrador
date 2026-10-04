"""Perfila usage_events_stream/*.jsonl SIN depender de Spark (solo lectura de Landing).
Descubre el esquema real (claves, tipos por campo, schema_version), nulos, duplicados de event_id,
variantes de service/region, costos negativos y rango temporal. Escribe evidence/perfil_usage_events.md.

Uso (Windows):  python src/exploration/profile_usage_events.py "F:\\Istea\\Año2\\MineríaDatos2\\Proyecto\\datalake\\landing\\usage_events_stream"
"""
import sys, os, json, glob, collections as C
from pathlib import Path

root = Path(sys.argv[1] if len(sys.argv) > 1 else os.getenv("EVENTS_DIR", "datalake/landing/usage_events_stream"))
files = sorted(glob.glob(str(root / "*.jsonl")) + glob.glob(str(root / "*.json")))
out = Path("evidence/perfil_usage_events.md")

n_rows = n_bad = 0
keys_by_ver = C.defaultdict(C.Counter)          # schema_version -> clave -> apariciones
types = C.defaultdict(C.Counter)                # campo -> tipo python -> apariciones
nulls = C.Counter(); rows_by_ver = C.Counter()
ids = C.Counter(); cat = C.defaultdict(C.Counter)
neg_cost = neg_cost_strict = spikes100 = unit_null_with_value = value_str = value_str_bad = 0
samples = {}; ts_vals = []; file_span = {}
bytes_total = sum(os.path.getsize(f) for f in files)

def first(d, *names):
    for k in names:
        if k in d: return d[k]
    return None

for f in files:
    with open(f, encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line: continue
            try: r = json.loads(line)
            except Exception: n_bad += 1; continue
            n_rows += 1
            ver = r.get("schema_version"); rows_by_ver[ver] += 1
            for k, v in r.items():
                keys_by_ver[ver][k] += 1; types[k][type(v).__name__] += 1
                if v is None: nulls[k] += 1
            ids[r.get("event_id")] += 1
            for c in ("service", "region", "unit", "metric"):
                if c in r: cat[c][r[c]] += 1
            cost = first(r, "cost_usd_increment", "cost_usd")
            try:
                if cost is not None and float(cost) < 0: neg_cost += 1
                if cost is not None and float(cost) < -0.01: neg_cost_strict += 1
                if cost is not None and float(cost) >= 100: spikes100 += 1
            except (TypeError, ValueError): pass
            if r.get("value") is not None and r.get("unit") is None: unit_null_with_value += 1
            if isinstance(r.get("value"), str):
                value_str += 1
                try: float(r["value"])
                except ValueError: value_str_bad += 1
            ts = first(r, "timestamp", "ts", "event_time", "event_ts")
            if ts:
                ts_vals.append(str(ts)); lo, hi = file_span.get(f, (str(ts), str(ts)))
                file_span[f] = (min(lo, str(ts)), max(hi, str(ts)))
            samples.setdefault(ver, r)

dups = sum(1 for k, v in ids.items() if v > 1)
L = ["# Perfil de usage_events_stream (Landing)\n",
     f"- Archivos: **{len(files)}** | Tamaño total: **{bytes_total/1e6:.2f} MB** | Filas: **{n_rows}** | Líneas no parseables: **{n_bad}**",
     f"- event_id duplicados (claves repetidas): **{dups}** | event_id nulo: **{ids.get(None, 0)}**",
     f"- Costos negativos: **{neg_cost}** (< -0.01, incumplen Q2: **{neg_cost_strict}**) | costos >= 100 USD (spikes): **{spikes100}**",
     f"- `value` como string: **{value_str}** (no casteables: **{value_str_bad}**) | `unit` nulo con `value` informado (incumple Q3): **{unit_null_with_value}**",
     f"- Archivos cuyo rango de timestamps supera 1 día: **{sum(1 for lo, hi in file_span.values() if lo[:10] != hi[:10])} de {len(file_span)}** (eventos NO ordenados por archivo)",
     f"- Rango temporal (texto crudo): **{min(ts_vals) if ts_vals else 'n/d'}  ->  {max(ts_vals) if ts_vals else 'n/d'}**",
     f"- Filas por schema_version: {dict(rows_by_ver)}\n", "## Claves por schema_version\n"]
for ver, kc in keys_by_ver.items():
    L.append(f"**schema_version={ver}** ({rows_by_ver[ver]} filas): " + ", ".join(f"`{k}`({v})" for k, v in sorted(kc.items())))
L += ["\n## Tipos observados por campo (detecta números como texto)\n", "| campo | tipos | nulos |", "|---|---|---|"]
for k, tc in sorted(types.items()): L.append(f"| {k} | {dict(tc)} | {nulls[k]} |")
L.append("\n## Valores de campos categóricos (detecta variantes a conformar)\n")
for c, cc in cat.items(): L.append(f"- **{c}** ({len(cc)} distintos): {dict(cc.most_common(25))}")
L.append("\n## Ejemplo de registro por versión\n")
for ver, r in samples.items(): L.append(f"schema_version={ver}:\n```json\n{json.dumps(r, ensure_ascii=False, indent=2)}\n```")
out.parent.mkdir(exist_ok=True); out.write_text("\n".join(L), encoding="utf-8"); print(f"OK -> {out}")
