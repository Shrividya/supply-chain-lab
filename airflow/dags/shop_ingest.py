"""Ingestion DAG: source system -> raw layer, then data quality checks."""
from __future__ import annotations

from datetime import timedelta

import pendulum
from airflow.sdk import Asset, Param, dag, task

RAW_SHOP = Asset("postgres://warehouse:5432/warehouse/raw/shop")

TABLES = ["customers", "products", "orders", "order_items", "web_events"]


@dag(
    dag_id="shop_ingest",
    schedule="@hourly",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    params={
        "overlap_seconds": Param(3600, type="integer", minimum=0, description="re-read this much history"),
        "full_refresh": Param(False, type="boolean", description="ignore watermarks and re-read everything"),
    },
    default_args={
        "retries": 3,
        "retry_delay": timedelta(seconds=30),
        "retry_exponential_backoff": True,
        "max_retry_delay": timedelta(minutes=10),
    },
    tags=["shop", "ingestion"],
)
def shop_ingest():
    @task(max_active_tis_per_dag=5)
    def load(table: str, params=None, run_id=None) -> dict:
        from pipeline import extract_load
        from pipeline.config import TABLES_BY_NAME

        r = extract_load.load_table(
            TABLES_BY_NAME[table], run_id,
            full=bool(params["full_refresh"]), overlap_seconds=int(params["overlap_seconds"]))
        return {"table": r.table, "extracted": r.rows_extracted, "inserted": r.rows_inserted}

    @task(outlets=[RAW_SHOP], retries=0)
    def reconcile(loads: list, run_id=None) -> None:
        from pipeline import reconcile as rc

        rc.run_checks(run_id)

    reconcile(load.expand(table=TABLES))


shop_ingest()
