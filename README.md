# Trabajo Práctico Integrador (Minería de Datos II · ISTEA 2C 2026)

Pipeline ETL + Streaming + Serving para un proveedor de nube: PySpark, Structured Streaming, Parquet y Cassandra/AstraDB. Dominios: **FinOps, Soporte y Producto/Usage**.

> **Estado: Primera entrega (07/10/2026) — diseño y fundación.** Todavía no hay pipeline ejecutable; se entrega diseño, perfil de datos y estructura del repositorio.

## Documentación
| Documento | Contenido |
|---|---|
| [docs/DISENO.md](docs/DISENO.md) | Documento de diseño: problema, 5V, fuentes, arquitectura, patrón, Data Lake, MapReduce, riesgos, esfuerzo |
| [docs/diccionario_datos.md](docs/diccionario_datos.md) | Diccionario de datos inicial |
| [DECISIONS.md](DECISIONS.md) | Registro de decisiones |
| [evidence/](evidence/) | Perfiles de datos generados por los scripts |

## Estructura
```
README.md  DECISIONS.md  requirements.txt  .gitignore
docs/        diseño, diagrama, diccionario
data/sample/ CSV de muestra + 1 archivo JSONL de eventos (el stream completo no se versiona)
src/exploration/  scripts de perfilado (solo lectura)
config/      settings.example.yaml (sin credenciales)
notebooks/ tests/ infra/   reservados para próximas entregas
evidence/    salidas de ejecución
```
Data Lake (fuera del repo, `.gitignore`): `datalake/{landing,bronze,silver,gold,quarantine,_checkpoints}/`. **Landing es inmutable.**

## Quickstart (evidencia de exploración)
```bash
python -m venv .venv && .venv\Scripts\activate        # Windows
pip install -r requirements.txt

# 1) Perfil de los CSV (usa data/sample o la carpeta landing)
python src/exploration/profile_landing_csv.py "F:\Istea\Año2\MineríaDatos2\Proyecto\datalake\landing"

# 2) Perfil de los 120 JSONL de eventos
python src/exploration/profile_usage_events.py "F:\Istea\Año2\MineríaDatos2\Proyecto\datalake\landing\usage_events_stream"
```
Salidas: `evidence/perfil_fuentes_csv.md` y `evidence/perfil_usage_events.md`. Ambos scripts solo **leen** Landing.

> La evidencia de eventos corresponde a los **120 archivos** de Landing (43 200 eventos).

## Convenciones
snake_case en tablas y columnas · Parquet snappy · columnas técnicas `ingest_ts` y `source_file` en Bronze · sin secretos en el repo (variables de entorno, ver `config/settings.example.yaml`) · una decisión relevante = una fila en `DECISIONS.md`.

## Próximos pasos
Perfilar eventos y fijar esquema → Bronze batch (3 maestros) → Bronze streaming → Silver y calidad → Gold → Cassandra/AstraDB.
