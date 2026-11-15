## Trabajo Práctico Integrador (Minería de Datos II · ISTEA 2C 2026)

Pipeline ETL + Streaming + Serving para un proveedor de nube: PySpark, Structured Streaming, Parquet y Cassandra/AstraDB. Dominios: **FinOps, Soporte y Producto/Usage**.

> **Estado: Segunda entrega (18/11/2026) — implementación técnica.** Pipeline ejecutable Landing → Bronze → Silver → Gold → Cassandra, probado con los 120 archivos de eventos. La Primera entrega (diseño) sigue en `docs/DISENO.md`.

## Documentación
| Documento | Contenido |
|---|---|
| [docs/ARQUITECTURA_v2.md](docs/ARQUITECTURA_v2.md) | **Arquitectura implementada (v2)**, cambios respecto de v1, correspondencia con el código |
| [docs/PLAN_CORRECCIONES.md](docs/PLAN_CORRECCIONES.md) | Feedback de E1 y cómo se corrigió |
| [docs/GOBIERNO.md](docs/GOBIERNO.md) | Controles de calidad, metadatos, linaje, seguridad, responsabilidades |
| [docs/BACKLOG.md](docs/BACKLOG.md) | Backlog priorizado para el MVP final |
| [docs/DISENO.md](docs/DISENO.md) | Documento de diseño de la Primera entrega (v1) |
| [docs/diccionario_datos.md](docs/diccionario_datos.md) | Diccionario de datos: Landing, Bronze, Silver, Gold, Cassandra |
| [DECISIONS.md](DECISIONS.md) | Registro de decisiones (D1–D18) |
| [evidence/entrega2/](evidence/entrega2/) | Evidencias de ejecución de E2 (ver tabla abajo) |

## Equipo
| Integrante | Rol principal | Rol de apoyo |
|---|---|---|
| Giuliano Gambino | A definir | A definir |
| Gabriela Romero | A definir | A definir |
| Mayra Garate | A definir | A definir |
| Germán Pacheco | A definir | A definir |

Roles y responsables por área: completar en `docs/GOBIERNO.md` (pendiente del equipo).

## Estructura
```
README.md  DECISIONS.md  requirements.txt  .gitignore
config/      settings.example.yaml (defaults = decisiones cerradas, sin credenciales)
src/common/  config, sesión Spark, esquemas, reglas de calidad, anomalías, logs, evidencias
src/jobs/    bronze_batch · bronze_stream · silver · gold · serving
src/pipeline.py        orquestador (todas las etapas, --steps, --skip, --reset)
src/experiments/       experimentos de dedup/late data e idempotencia
src/exploration/       scripts de perfilado de E1 (solo lectura)
cql/         01_schema.cql (keyspace y tabla) · 02_queries.cql (2 consultas)
tests/       pytest (config, calidad, streaming, anomalías, Gold, reset)
infra/       docker-compose.yml con Cassandra 4.1 local
notebooks/   entrega2_colab.ipynb (correr todo en Google Colab)
data/sample/ CSV completos + 1 archivo JSONL de eventos (para correr rápido)
docs/  evidence/
```
Data Lake (fuera del repo, `.gitignore`): `datalake/{landing,bronze,silver,gold,quarantine,_checkpoints}/`. **Landing es inmutable.**

## Quickstart

### Requisitos
- Python 3.10 o 3.11, Java 17 (o 11) y `pip install -r requirements.txt` (PySpark 3.5.3, cassandra-driver 3.29+, pytest).
- Linux, macOS, WSL o Google Colab. En Windows nativo PySpark necesita además `winutils.exe` (`HADOOP_HOME`) para escribir Parquet: recomendamos WSL o Colab.
- Para el serving: Docker (Cassandra local) **o** una base AstraDB.

### 1. Instalar
```bash
git clone https://github.com/gerpach/mineria2-tp-integrador.git && cd mineria2-tp-integrador
python -m venv .venv && source .venv/bin/activate        # Windows/WSL: igual dentro de WSL
pip install -r requirements.txt
```

### 2. Levantar Cassandra (opción local)
```bash
docker compose -f infra/docker-compose.yml up -d          # esperar ~60 s
```
Opción AstraDB: `export CASSANDRA_MODE=astra ASTRA_BUNDLE_PATH=/ruta/secure-connect.zip ASTRA_CLIENT_ID=... ASTRA_CLIENT_SECRET=...` y crear el keyspace `cloud_analytics` desde la consola de Astra.

### 3. Correr el pipeline
```bash
# a) Con la muestra versionada (data/sample, ~1 minuto)
python -m src.pipeline

# b) Con los datos completos (120 archivos): apuntar a la carpeta landing del equipo
export LANDING_DIR="/ruta/datalake/landing"   LAKE_ROOT="/ruta/datalake"
python -m src.pipeline

# Sin Cassandra disponible:
python -m src.pipeline --skip serving
```
Cada corrida escribe el log `evidence/entrega2/logs/pipeline_<run_id>.log`, `evidence/entrega2/run_summary.md` (rutas, archivos, tamaños, conteos) y `evidence/entrega2/cassandra_queries.md`.

### 4. Experimentos e idempotencia
```bash
python -m src.experiments.dedup_late_data     # dedup + late data (watermark 10 min vs 61 días)
python -m src.experiments.idempotencia        # re-ejecuta todo y compara conteos (agregar --skip-serving sin Cassandra)
```

### 5. Tests
```bash
python -m pytest -q          # 16 tests, ~20 s, no necesitan Cassandra ni los datos completos
```

### 6. Limpieza / reinicio
```bash
python -m src.pipeline --reset                     # borra bronze, silver, gold, quarantine y _checkpoints (nunca landing)
docker compose -f infra/docker-compose.yml down    # baja Cassandra
```

### Configuración
`config/settings.example.yaml` (o una copia en `config/settings.yaml`, que no se versiona). Variables de entorno que la pisan: `LANDING_DIR`, `LAKE_ROOT`, `WATERMARK`, `CASSANDRA_MODE`, `CASSANDRA_HOST`, `ASTRA_*`, `TP_CONFIG`.

| Clave | Default | Decisión |
|---|---|---|
| `streaming.watermark` | `61 days` | D9 |
| `streaming.max_files_per_trigger` | 5 | micro-lotes de 1 800 eventos |
| `write.coalesce` | 1 | D10 (parámetro de la demo, no política) |
| `quality.min_cost_increment_usd` | -0.01 | Q2 |
| `anomaly.threshold` / `min_days` | 3.5 / 7 | D6, D15 |
| `cassandra.mode` | local | D17 |

### Solución de problemas
- `Landing no encontrado`: revisar `LANDING_DIR` (debe contener los CSV y `usage_events_stream/`).
- `NoHostAvailable` en serving: Cassandra todavía está arrancando; esperar y reintentar, o usar `--skip serving`.
- Se cambió el watermark o los archivos de entrada y el streaming "no lee nada": el checkpoint recuerda lo procesado; correr `--reset`.
- `java.lang.UnsupportedClassVersionError` / errores de Java: usar Java 17.

## Evidencias de la Segunda entrega (120 archivos, 43 200 eventos)
| Archivo | Qué muestra |
|---|---|
| [run_summary.md](evidence/entrega2/run_summary.md) | Rutas, cantidad de archivos, tamaños y conteos por capa |
| [quarantine_muestras.md](evidence/entrega2/quarantine_muestras.md) | Conteos por regla y muestras de quarantine |
| [experimentos.md](evidence/entrega2/experimentos.md) | Dedup y late data con experimentos controlados |
| [idempotencia.md](evidence/entrega2/idempotencia.md) | Re-ejecución completa: diferencia 0 en todas las capas y en Cassandra |
| [anomalias.md](evidence/entrega2/anomalias.md) | Componente analítico (MAD): métricas y limitaciones |
| [cassandra_queries.md](evidence/entrega2/cassandra_queries.md), [cqlsh_salida.md](evidence/entrega2/cqlsh_salida.md) | CQL y resultados de las dos consultas |
| [logs/](evidence/entrega2/logs/) | Logs de las corridas |

Resultados principales: 43 200 eventos en Bronze (0 duplicados, 0 descartados por watermark) → 40 956 en Silver + 2 244 en quarantine (Q2: 211, Q3: 2 033) → 11 050 filas en Gold (grano único) → 11 050 filas en Cassandra; 123 anomalías de costo.

## Evidencia de exploración (Primera entrega)
```bash
python src/exploration/profile_landing_csv.py "<ruta>/datalake/landing"
python src/exploration/profile_usage_events.py "<ruta>/datalake/landing/usage_events_stream"
```
Salidas: `evidence/perfil_fuentes_csv.md` y `evidence/perfil_usage_events.md`.

## Convenciones
snake_case en tablas y columnas · Parquet snappy · columnas técnicas `ingest_ts`, `source_file` y `pipeline_run_id` en Bronze · sin secretos en el repo (variables de entorno) · una decisión relevante = una fila en `DECISIONS.md` · cada regla de calidad con su test.

## Próximos pasos
Ver [docs/BACKLOG.md](docs/BACKLOG.md): marts de revenue, soporte y GenAI con sus tablas y las 5 consultas, corrida contra AstraDB, presentación y video.

## Licencia
Licencia: MIT (ver LICENSE). El dataset es sintético y fue provisto por la cátedra de Minería de Datos II.