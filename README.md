# Shop Data Platform

A small but realistic data platform for learning and for writing about Apache Superset: a simulated production database, incremental ingestion, a dbt-style transformation layer, Airflow orchestration, and Superset dashboards for both the business and the pipeline itself.

## Quick start (Docker)
- Create a .env file and add the keys SUPERSET_SECRET_KEY and AIRFLOW_JWT_SECRET
- Makefile has been configured with the required commands. Use the following commands for a quick start.
```
make up        # slow the first time
make tick      # advance one day and run the whole pipeline
```
Superset http://localhost:8088 (admin/admin) · Airflow http://localhost:8080



