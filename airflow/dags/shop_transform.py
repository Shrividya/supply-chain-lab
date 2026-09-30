"""Transformation DAG: runs `dbt build` whenever ingestion publishes new raw data.
"""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import Asset, Param, dag

RAW_SHOP = Asset("postgres://warehouse:5432/warehouse/raw/shop")
DBT = "/opt/dbt-venv/bin/dbt"


@dag(
    dag_id="shop_transform",
    schedule=[RAW_SHOP],
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={"full_refresh": Param(False, type="boolean", description="rebuild incremental models from scratch")},
    default_args={"retries": 1, "retry_delay": timedelta(minutes=1)},
    tags=["shop", "dbt"],
)
def shop_transform():
    env = {"DBT_PROFILES_DIR": "/opt/airflow/dbt"}
    flag = "{{ '--full-refresh' if params.full_refresh else '' }}"

    freshness = BashOperator(
        task_id="source_freshness",
        bash_command=f"cd /opt/airflow/dbt && {DBT} source freshness",
        env=env, append_env=True)
    build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd /opt/airflow/dbt && {DBT} build {flag}",
        env=env, append_env=True)
    freshness >> build


shop_transform()
