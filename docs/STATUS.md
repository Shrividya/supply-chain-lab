# What was run, and what was not

This project was built in a sandbox with Postgres 16, psql and Python 3.11, and no Docker, no pip, and no access to the Airflow, dbt or Superset packages. That shapes what can honestly be claimed.

## Run and passing (50 tests)

| Area | How it was checked |
|---|---|
| Source simulator, seed, fault injection | Real Postgres 16 |
| Warehouse roles, schemas, partitions, ops tables | Real Postgres 16 |
| Ingestion (`pipeline/`): 29 tests | Idempotency, overlap window, atomic failure, schema drift, late events, partitions, least privilege, CLI exit codes |
| SQL models: 15 tests | Run through `tools/mini_dbt.py`. Incremental equals full rebuild, SCD2 is point-in-time correct, each injected fault turns the intended test red |
| Dashboard definitions: 6 tests | Every dataset metric, chart column, dashboard filter and SQL exercise runs against the real marts as the read-only role |

## Written but never run

- `docker-compose.yml`, `docker/*.Dockerfile`: parsed as YAML only.
- `airflow/dags/*.py`: they compile, nothing more. The logic they call is tested.
- `dbt/` with the real dbt: `mini_dbt.py` is a stand-in that understands the subset of Jinja these models use. Real dbt may disagree. Expect to fix small things on first run.
- `provisioning/provision.py` against a live Superset 6.1: the API calls are carried over from an earlier version that was also never run against a live instance. The cohort heatmap and funnel chart parameters are the likeliest to need a tweak.

## Why that is still useful

The hard parts of a data platform are the data decisions, and those were tested against a real database. The glue that was not run is thin and is written to fail loudly. When you run it, note what broke and how you fixed it. That is real experience you can talk about in an interview.
