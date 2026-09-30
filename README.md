# Shop Data Platform

A small but realistic data platform for learning and for writing about Apache Superset: a simulated production database, incremental ingestion, a dbt-style transformation layer, Airflow orchestration, and Superset dashboards for both the business and the pipeline itself.

Read `docs/STATUS.md` first. It says plainly what was run and what was not.

```
source-db --(load)--> warehouse raw --(dbt)--> staging --> marts --> Superset
                          |                                   ^
                          +--> ops (audit, drift, quality) ----+
Airflow runs load, checks and dbt. A simulator makes the source change every day.
```

## Quick start (Docker)
```
make setup && make up        # slow the first time
make demo                    # load, check, build, dashboards
make tick                    # advance one day and run the whole pipeline
```
Superset http://localhost:8088 (admin/admin) · Airflow http://localhost:8080

## Without Docker
You need Postgres 16, psql and Python 3.11+ with pyyaml.
```
export TEST_PG_ADMIN_DSN=postgresql://postgres@localhost:5432/postgres
make test
```

## Layout
| Path | What |
|---|---|
| `source/` | fake production DB, seed, day simulator, fault injection |
| `warehouse/` | roles, schemas, raw change log, ops tables |
| `pipeline/` | extract/load, schema drift, reconciliation (stdlib Python) |
| `dbt/` | staging, marts, tests, contracts, exposures |
| `airflow/dags/` | `shop_ingest`, `shop_transform` |
| `provisioning/` | Superset dashboards as code |
| `tests/` | 50 tests |
| `docs/` | ARCHITECTURE, LABS, INTERVIEW_GUIDE, STATUS |

## Suggested path
1. `make demo`, then open both dashboards.
2. Read `docs/ARCHITECTURE.md`.
3. Do every lab in `docs/LABS.md`, writing down what you see.
4. Read `pipeline/extract_load.py` top to bottom.
5. Use `docs/INTERVIEW_GUIDE.md` to practise out loud.
