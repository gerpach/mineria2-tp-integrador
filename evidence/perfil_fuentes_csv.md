# Perfil de fuentes CSV (Landing)

## billing_monthly.csv  (240 filas x 8 columnas)

|                      | dtype   |   nulos |   nulos_% |   distintos |
|:---------------------|:--------|--------:|----------:|------------:|
| invoice_id           | str     |       0 |       0   |         240 |
| org_id               | str     |       0 |       0   |          80 |
| month                | str     |       0 |       0   |           3 |
| subtotal             | float64 |       0 |       0   |         240 |
| credits              | float64 |     137 |      57.1 |         100 |
| taxes                | float64 |       0 |       0   |         240 |
| currency             | str     |       0 |       0   |           3 |
| exchange_rate_to_usd | float64 |       0 |       0   |         208 |

Filas duplicadas exactas: 0

## customers_orgs.csv  (80 filas x 11 columnas)

|                  | dtype   |   nulos |   nulos_% |   distintos |
|:-----------------|:--------|--------:|----------:|------------:|
| org_id           | str     |       0 |       0   |          80 |
| org_name         | str     |       0 |       0   |          80 |
| industry         | str     |       0 |       0   |          10 |
| hq_region        | str     |       0 |       0   |           7 |
| plan_tier        | str     |       0 |       0   |           4 |
| is_enterprise    | bool    |       0 |       0   |           2 |
| signup_date      | str     |       0 |       0   |          40 |
| sales_rep        | str     |       0 |       0   |           5 |
| lifecycle_stage  | str     |       0 |       0   |           5 |
| marketing_source | str     |       0 |       0   |           5 |
| nps_score        | float64 |      11 |      13.8 |          45 |

Filas duplicadas exactas: 0

## marketing_touches.csv  (1500 filas x 7 columnas)

|           | dtype   |   nulos |   nulos_% |   distintos |
|:----------|:--------|--------:|----------:|------------:|
| touch_id  | str     |       0 |         0 |        1500 |
| org_id    | str     |       0 |         0 |          80 |
| campaign  | str     |       0 |         0 |           6 |
| channel   | str     |       0 |         0 |           4 |
| timestamp | str     |       0 |         0 |         120 |
| clicked   | bool    |       0 |         0 |           2 |
| converted | bool    |       0 |         0 |           2 |

Filas duplicadas exactas: 0

## nps_surveys.csv  (92 filas x 4 columnas)

|             | dtype   |   nulos |   nulos_% |   distintos |
|:------------|:--------|--------:|----------:|------------:|
| org_id      | str     |       0 |       0   |          60 |
| survey_date | str     |       0 |       0   |          57 |
| nps_score   | float64 |      19 |      20.7 |          41 |
| comment     | str     |      10 |      10.9 |           6 |

Filas duplicadas exactas: 0

## resources.csv  (400 filas x 7 columnas)

|             | dtype   |   nulos |   nulos_% |   distintos |
|:------------|:--------|--------:|----------:|------------:|
| resource_id | str     |       0 |       0   |         400 |
| org_id      | str     |       0 |       0   |          80 |
| service     | str     |       0 |       0   |           6 |
| region      | str     |       0 |       0   |           7 |
| created_at  | str     |       0 |       0   |         106 |
| state       | str     |       0 |       0   |           3 |
| tags_json   | str     |      83 |      20.8 |         159 |

Filas duplicadas exactas: 0

## support_tickets.csv  (1000 filas x 8 columnas)

|              | dtype   |   nulos |   nulos_% |   distintos |
|:-------------|:--------|--------:|----------:|------------:|
| ticket_id    | str     |       0 |       0   |        1000 |
| org_id       | str     |       0 |       0   |          80 |
| category     | str     |       0 |       0   |           6 |
| severity     | str     |       0 |       0   |           4 |
| created_at   | str     |       0 |       0   |         115 |
| resolved_at  | str     |     240 |      24   |         125 |
| csat         | float64 |     254 |      25.4 |           8 |
| sla_breached | bool    |       0 |       0   |           2 |

Filas duplicadas exactas: 0

## users.csv  (800 filas x 7 columnas)

|            | dtype   |   nulos |   nulos_% |   distintos |
|:-----------|:--------|--------:|----------:|------------:|
| user_id    | str     |       0 |       0   |         800 |
| org_id     | str     |       0 |       0   |          80 |
| email      | str     |       0 |       0   |         800 |
| role       | str     |       0 |       0   |           6 |
| active     | bool    |       0 |       0   |           2 |
| created_at | str     |       0 |       0   |         100 |
| last_login | str     |     139 |      17.4 |         110 |

Filas duplicadas exactas: 0

## Chequeos de calidad / integridad

- **billing: subtotal < 0**: 13
- **billing: credits nulo**: 137
- **billing: monedas**: {'USD': 160, 'ARS': 51, 'EUR': 29}
- **customers: nps_score fuera de [-100,100]**: 1
- **tickets: csat fuera de [1,5]**: 40
- **tickets: csat informado sin resolved_at**: 172
- **tickets: tasa sla_breached**: 0.095
- **users: last_login < created_at**: 232
- **marketing: converted sin clicked**: 96
- **nps_surveys: nps_score nulo**: 19
- **integridad: billing_monthly con org_id huerfano**: 0
- **integridad: marketing_touches con org_id huerfano**: 0
- **integridad: nps_surveys con org_id huerfano**: 0
- **integridad: resources con org_id huerfano**: 0
- **integridad: support_tickets con org_id huerfano**: 0
- **integridad: users con org_id huerfano**: 0