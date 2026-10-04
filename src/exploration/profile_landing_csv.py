"""Perfila los CSV de Landing (solo lectura) y escribe evidence/perfil_fuentes_csv.md.
Uso: python src/exploration/profile_landing_csv.py [ruta_landing]
Si no se pasa ruta, usa la variable de entorno LANDING_DIR o 'datalake/landing'."""
import sys, os, pandas as pd
from pathlib import Path

landing = Path(sys.argv[1] if len(sys.argv) > 1 else os.getenv("LANDING_DIR", "datalake/landing"))
out = Path("evidence/perfil_fuentes_csv.md")
lines = ["# Perfil de fuentes CSV (Landing)\n"]

def prof(df):
    return pd.DataFrame({"dtype": df.dtypes.astype(str), "nulos": df.isna().sum(),
                         "nulos_%": (df.isna().mean() * 100).round(1), "distintos": df.nunique()})

dfs = {}
for f in sorted(landing.glob("*.csv")):
    df = pd.read_csv(f); dfs[f.stem] = df
    lines += [f"## {f.name}  ({len(df)} filas x {df.shape[1]} columnas)\n", prof(df).to_markdown(), "",
              f"Filas duplicadas exactas: {int(df.duplicated().sum())}\n"]

c = dfs["customers_orgs"]; b = dfs["billing_monthly"]; t = dfs["support_tickets"]
u = dfs["users"]; m = dfs["marketing_touches"]; n = dfs["nps_surveys"]
checks = {
    "billing: subtotal < 0": int((b.subtotal < 0).sum()),
    "billing: credits nulo": int(b.credits.isna().sum()),
    "billing: monedas": b.currency.value_counts().to_dict(),
    "customers: nps_score fuera de [-100,100]": int(((c.nps_score > 100) | (c.nps_score < -100)).sum()),
    "tickets: csat fuera de [1,5]": int(((t.csat < 1) | (t.csat > 5)).sum()),
    "tickets: csat informado sin resolved_at": int((t.resolved_at.isna() & t.csat.notna()).sum()),
    "tickets: tasa sla_breached": round(float(t.sla_breached.mean()), 3),
    "users: last_login < created_at": int((pd.to_datetime(u.last_login) < pd.to_datetime(u.created_at)).sum()),
    "marketing: converted sin clicked": int((m.converted & ~m.clicked).sum()),
    "nps_surveys: nps_score nulo": int(n.nps_score.isna().sum()),
}
orgs = set(c.org_id)
for name, df in dfs.items():
    if "org_id" in df and name != "customers_orgs":
        checks[f"integridad: {name} con org_id huerfano"] = int((~df.org_id.isin(orgs)).sum())
lines += ["## Chequeos de calidad / integridad\n"] + [f"- **{k}**: {v}" for k, v in checks.items()]
out.parent.mkdir(exist_ok=True); out.write_text("\n".join(lines), encoding="utf-8")
print(f"OK -> {out}")
