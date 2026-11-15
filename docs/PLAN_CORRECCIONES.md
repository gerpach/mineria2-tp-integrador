# Plan de correcciones posterior a la Primera entrega

Feedback de la cátedra sobre E1 (10/10) y cómo se resolvió en E2.

| # | Observación | Corrección | Evidencia |
|---|---|---|---|
| 1 | `settings.example.yaml` decía watermark "10 minutes" y `DECISIONS.md` ≥ 61 días | Config en `"61 days"`, D9 Aprobada, advertencia en el YAML y test que compara ambos archivos | `config/settings.example.yaml`, `tests/test_config.py::test_watermark_default_matches_d9` |
| 2 | `coalesce(1)` no debe ser política general | Parámetro `write.coalesce` (D10); el código no tiene `coalesce(1)` fijo (lo verifica un test) | `src/jobs/silver.py::maybe_coalesce`, `tests/test_config.py::test_coalesce_is_a_parameter_d10` |
| 3 | Faltaban tests reales | 16 tests con pytest: config, reglas de calidad, dedupe, features, MAD, grano de Gold, streaming con checkpoint y reset | `tests/` |
| 4 | Defaults de configuración = decisiones cerradas | Todas las claves de config remiten a una decisión; tests de consistencia | `tests/test_config.py` |
| 5 | Demostrar dedup y late data con experimentos controlados | Script reproducible con entradas armadas a partir de eventos reales | `src/experiments/dedup_late_data.py`, `evidence/entrega2/experimentos.md` |
| 6 | No sumar complejidad | Se mantuvo Lambda; streaming solo hasta Bronze y Silver/Gold batch (D13); un solo mart y una tabla para E2 | `docs/ARQUITECTURA_v2.md` |
