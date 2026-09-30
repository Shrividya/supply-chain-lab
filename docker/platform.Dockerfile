# Airflow image with what the DAGs need:
#   * psql          pipeline/ shells out to it
#   * a dbt venv    dbt's dependencies stay out of Airflow's own Python environment

FROM apache/airflow:3.3.2

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends postgresql-client \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir /opt/dbt-venv && chown airflow /opt/dbt-venv

USER airflow
RUN python -m venv /opt/dbt-venv \
 && /opt/dbt-venv/bin/pip install --no-cache-dir "dbt-postgres==1.11.0"

ENV PYTHONPATH=/opt/airflow/platform
