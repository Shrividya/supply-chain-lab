# Shop Data Platform

A small but realistic data platform for learning and for writing about Apache Superset: a simulated production database, incremental ingestion, a dbt-style transformation layer, Airflow orchestration, and Superset dashboards for both the business and the pipeline itself.

## Quick start (Docker)
```
make setup && make up        # slow the first time
make demo                    # load, check, build, dashboards
make tick                    # advance one day and run the whole pipeline
```
Superset http://localhost:8088 (admin/admin) · Airflow http://localhost:8080



