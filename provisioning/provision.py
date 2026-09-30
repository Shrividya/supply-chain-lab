#!/usr/bin/env python3
"""
Provision Superset through its REST API: one database connection, datasets on the
dbt marts and the ops tables, metrics, charts, and two dashboards:

    Shop Overview    business view (revenue, funnel, cohorts)   <- marts.*
    Pipeline Health  engineering view (loads, data quality)     <- ops.*

The script is idempotent: run it again and it updates what exists.
tests/test_provisioning.py runs every dataset table, column and metric expression
here against the real Postgres marts and ops tables.

Environment: SUPERSET_URL (default http://superset:8088), SUPERSET_ADMIN_USER,
SUPERSET_ADMIN_PASSWORD
"""
from __future__ import annotations

import json
import os
import sys
import time
from typing import Any

import requests

BASE = os.environ.get("SUPERSET_URL", "http://superset:8088").rstrip("/")
USER = os.environ.get("SUPERSET_ADMIN_USER", "admin")
PASSWORD = os.environ.get("SUPERSET_ADMIN_PASSWORD", "admin")

DB_NAME = "Shop Warehouse"
DB_URI = "postgresql+psycopg2://superset_ro:superset_ro@warehouse:5432/warehouse"


def date_filter(column: str = "order_date") -> dict[str, Any]:
    return {"expressionType": "SIMPLE", "clause": "WHERE", "subject": column,
            "operator": "TEMPORAL_RANGE", "comparator": "No filter"}


def metric(name: str, label: str, expression: str, fmt: str, description: str) -> dict[str, Any]:
    return {"metric_name": name, "verbose_name": label, "expression": expression,
            "d3format": fmt, "description": description}


# Datasets. Business logic lives in dbt models; metrics here are only aggregations.
DATASETS: dict[str, dict[str, Any]] = {
    "orders": {
        "schema": "marts", "table": "fct_orders", "main_dttm_col": "order_date", "cache_timeout": 300,
        "description": "One row per order (dbt mart). Country/segment are as of the order date.",
        "metrics": [
            metric("revenue", "Revenue", "SUM(CASE WHEN is_completed THEN net_revenue ELSE 0 END)", "$,.0f",
                   "Net revenue of completed orders."),
            metric("orders", "Orders", "COUNT(CASE WHEN is_completed THEN 1 END)", ",d", "Completed orders."),
            metric("aov", "Average order value",
                   "SUM(CASE WHEN is_completed THEN net_revenue END) / NULLIF(COUNT(CASE WHEN is_completed THEN 1 END), 0)",
                   "$,.2f", "Revenue divided by completed orders."),
            metric("gross_margin_pct", "Gross margin %",
                   "SUM(CASE WHEN is_completed THEN gross_margin END) / NULLIF(SUM(CASE WHEN is_completed THEN net_revenue END), 0)",
                   ".1%", "Gross margin as a share of net revenue."),
            metric("active_customers", "Active customers",
                   "COUNT(DISTINCT CASE WHEN is_completed THEN customer_id END)", ",d",
                   "Distinct customers with a completed order."),
            metric("refund_rate", "Refund rate", "AVG(CASE WHEN status = 'refunded' THEN 1.0 ELSE 0.0 END)", ".1%",
                   "Share of all orders that were refunded."),
        ],
    },
    "daily": {
        "schema": "marts", "table": "mart_daily_channel", "main_dttm_col": "order_date", "cache_timeout": 1800,
        "description": "Pre-aggregated completed orders per day and channel (dbt mart).",
        "metrics": [metric("mart_revenue", "Revenue", "SUM(net_revenue)", "$,.0f", "Net revenue from the daily mart.")],
    },
    "funnel": {
        "schema": "marts", "table": "mart_funnel_daily", "main_dttm_col": "event_date", "cache_timeout": 900,
        "description": "Sessions reaching each funnel stage per day and device (incremental dbt mart).",
        "metrics": [metric("sessions", "Sessions", "SUM(sessions)", ",d", "Sessions that reached the stage.")],
    },
    "retention": {
        "schema": "marts", "table": "mart_cohort_retention", "cache_timeout": 3600,
        "description": "Share of each signup-month cohort that ordered again N months later.",
        "metrics": [metric("avg_retention", "Retention", "AVG(retention_rate)", ".1%", "Average retention rate.")],
    },
    "rfm": {
        "schema": "marts", "table": "mart_customer_rfm", "cache_timeout": 3600,
        "description": "One row per customer with recency, frequency, monetary value and a segment.",
        "metrics": [metric("customers", "Customers", "COUNT(*)", ",d", "Customers in the segment.")],
    },
    "loads": {
        "schema": "ops", "table": "load_audit", "main_dttm_col": "started_at", "cache_timeout": 60,
        "description": "Every ingestion attempt: what ran, how long, how many rows, whether it worked.",
        "metrics": [
            metric("rows_loaded", "Rows inserted", "COALESCE(SUM(rows_inserted), 0)", ",d", "New raw rows."),
            metric("failed_loads", "Failed loads", "COUNT(CASE WHEN status = 'failed' THEN 1 END)", ",d", "Failed attempts."),
            metric("avg_seconds", "Average load seconds", "AVG(duration_seconds)", ",.2f", "Mean load duration."),
        ],
    },
    "dq": {
        "schema": "ops", "table": "dq_results", "main_dttm_col": "checked_at", "cache_timeout": 60,
        "description": "Result of every data quality check, every run.",
        "metrics": [
            metric("checks", "Checks", "COUNT(*)", ",d", "Checks run."),
            metric("failing_checks", "Failing checks", "COUNT(CASE WHEN status = 'fail' THEN 1 END)", ",d", "Checks that failed."),
        ],
    },
    "drift": {
        "schema": "ops", "table": "schema_drift", "main_dttm_col": "detected_at", "cache_timeout": 60,
        "description": "Schema changes seen in the source system.",
        "metrics": [metric("drift_events", "Drift events", "COUNT(*)", ",d", "Detected schema changes.")],
    },
}


def kpi(m: str, fmt: str, ts: str = "order_date") -> dict[str, Any]:
    return {"viz_type": "big_number_total", "metric": m, "adhoc_filters": [date_filter(ts)],
            "y_axis_format": fmt, "header_font_size": 0.4, "subheader_font_size": 0.15}


def line(ds: str, m: str, ts: str, grain: str, groupby: list[str], fmt: str) -> dict[str, Any]:
    return {"viz_type": "echarts_timeseries_line", "x_axis": ts, "time_grain_sqla": grain, "metrics": [m],
            "groupby": groupby, "adhoc_filters": [date_filter(ts)], "row_limit": 10000,
            "y_axis_format": fmt, "rich_tooltip": True, "zoomable": True}


def table(groupby: list[str], metrics: list[str], ts: str | None) -> dict[str, Any]:
    return {"viz_type": "table", "query_mode": "aggregate", "groupby": groupby, "metrics": metrics,
            "all_columns": [], "percent_metrics": [], "adhoc_filters": [date_filter(ts)] if ts else [],
            "order_desc": True, "row_limit": 200, "page_length": 10, "include_search": True}


DASHBOARDS: dict[str, dict[str, Any]] = {
    "overview": {
        "title": "Shop Overview", "slug": "shop-overview",
        "intro": ("## Shop Overview\nBuilt on the dbt marts (`marts.*`). Revenue means **completed orders only**, "
                  "defined once as a dataset metric. Country and segment are as they were when the order was placed."),
        "filters": [("Channel", "channel", "daily"), ("Country", "country", "orders")],
        "charts": [
            {"name": "Revenue", "dataset": "orders", "row": 1, "width": 2, "height": 18, "params": kpi("revenue", "$,.0f")},
            {"name": "Orders", "dataset": "orders", "row": 1, "width": 2, "height": 18, "params": kpi("orders", ",d")},
            {"name": "Average order value", "dataset": "orders", "row": 1, "width": 2, "height": 18, "params": kpi("aov", "$,.2f")},
            {"name": "Gross margin %", "dataset": "orders", "row": 1, "width": 2, "height": 18, "params": kpi("gross_margin_pct", ".1%")},
            {"name": "Active customers", "dataset": "orders", "row": 1, "width": 2, "height": 18, "params": kpi("active_customers", ",d")},
            {"name": "Refund rate", "dataset": "orders", "row": 1, "width": 2, "height": 18, "params": kpi("refund_rate", ".1%")},
            {"name": "Revenue by week and channel", "dataset": "daily", "row": 2, "width": 8, "height": 45,
             "params": line("daily", "mart_revenue", "order_date", "P1W", ["channel"], "$,.0f")},
            {"name": "Web funnel", "dataset": "funnel", "row": 2, "width": 4, "height": 45,
             "params": {"viz_type": "funnel", "groupby": ["stage"], "metric": "sessions", "adhoc_filters": [date_filter("event_date")],
                        "row_limit": 10, "sort_by_metric": True, "show_labels": True}},
            {"name": "Cohort retention", "dataset": "retention", "row": 3, "width": 6, "height": 45,
             "params": {"viz_type": "heatmap_v2", "x_axis": "months_since_signup", "groupby": "cohort_month",
                        "metric": "avg_retention", "adhoc_filters": [], "row_limit": 1000, "normalize_across": "heatmap"}},
            {"name": "Customers by RFM segment", "dataset": "rfm", "row": 3, "width": 6, "height": 45,
             "params": {"viz_type": "pie", "groupby": ["rfm_segment"], "metric": "customers", "adhoc_filters": [],
                        "row_limit": 20, "donut": True, "show_labels": True, "label_type": "key_percent"}},
            {"name": "Performance by country and segment", "dataset": "orders", "row": 4, "width": 12, "height": 45,
             "params": table(["country", "segment"], ["revenue", "orders", "aov", "gross_margin_pct", "refund_rate"], "order_date")},
        ],
    },
    "health": {
        "title": "Pipeline Health", "slug": "pipeline-health",
        "intro": ("## Pipeline Health\nBuilt on `ops.*`, the platform's own bookkeeping. If a number on the business "
                  "dashboard looks wrong, look here first: did the load run, did it finish, did the checks pass?"),
        "filters": [("Table", "table_name", "loads")],
        "charts": [
            {"name": "Failed loads", "dataset": "loads", "row": 1, "width": 3, "height": 18, "params": kpi("failed_loads", ",d", "started_at")},
            {"name": "Failing checks", "dataset": "dq", "row": 1, "width": 3, "height": 18, "params": kpi("failing_checks", ",d", "checked_at")},
            {"name": "Schema drift events", "dataset": "drift", "row": 1, "width": 3, "height": 18, "params": kpi("drift_events", ",d", "detected_at")},
            {"name": "Average load seconds", "dataset": "loads", "row": 1, "width": 3, "height": 18, "params": kpi("avg_seconds", ",.2f", "started_at")},
            {"name": "Rows loaded per day", "dataset": "loads", "row": 2, "width": 6, "height": 45,
             "params": line("loads", "rows_loaded", "started_at", "P1D", ["table_name"], ",d")},
            {"name": "Data quality results", "dataset": "dq", "row": 2, "width": 6, "height": 45,
             "params": {"viz_type": "echarts_timeseries_bar", "x_axis": "checked_at", "time_grain_sqla": "P1D", "metrics": ["checks"],
                        "groupby": ["status"], "adhoc_filters": [date_filter("checked_at")], "row_limit": 10000, "stack": "Stack"}},
            {"name": "Recent load attempts", "dataset": "loads", "row": 3, "width": 12, "height": 45,
             "params": {"viz_type": "table", "query_mode": "raw",
                        "all_columns": ["batch_id", "table_name", "status", "started_at", "rows_extracted", "rows_inserted", "duration_seconds", "error"],
                        "groupby": [], "metrics": [], "percent_metrics": [], "adhoc_filters": [], "order_by_cols": ["[\"batch_id\", false]"],
                        "row_limit": 200, "page_length": 10, "include_search": True}},
            {"name": "Schema drift log", "dataset": "drift", "row": 4, "width": 12, "height": 35,
             "params": {"viz_type": "table", "query_mode": "raw",
                        "all_columns": ["detected_at", "table_name", "kind", "column_name", "severity", "detail"],
                        "groupby": [], "metrics": [], "percent_metrics": [], "adhoc_filters": [],
                        "order_by_cols": ["[\"detected_at\", false]"], "row_limit": 200, "page_length": 10}},
        ],
    },
}


# API client
class Superset:
    def __init__(self) -> None:
        self.s = requests.Session()
        self.h: dict[str, str] = {}

    def wait_until_ready(self, attempts: int = 60) -> None:
        for i in range(attempts):
            try:
                if requests.get(f"{BASE}/health", timeout=3).ok:
                    return
            except requests.RequestException:
                pass
            time.sleep(2)
        sys.exit(f"Superset never became healthy at {BASE}")

    def login(self) -> None:
        r = self.s.post(
            f"{BASE}/api/v1/security/login",
            json={"username": USER, "password": PASSWORD, "provider": "db", "refresh": True},
            timeout=30,
        )
        self._check(r, "login")
        token = r.json()["access_token"]
        self.h = {"Authorization": f"Bearer {token}", "Referer": BASE}
        c = self.s.get(f"{BASE}/api/v1/security/csrf_token/", headers=self.h, timeout=30)
        self._check(c, "csrf token")
        self.h["X-CSRFToken"] = c.json()["result"]

    @staticmethod
    def _check(r: requests.Response, what: str) -> None:
        if not r.ok:
            print(f"!! {what} failed: HTTP {r.status_code}\n{r.text}", file=sys.stderr)
            r.raise_for_status()

    def get(self, path: str, **kw: Any) -> dict[str, Any]:
        r = self.s.get(f"{BASE}{path}", headers=self.h, timeout=60, **kw)
        self._check(r, f"GET {path}")
        return r.json()

    def post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        r = self.s.post(f"{BASE}{path}", headers=self.h, json=body, timeout=120)
        self._check(r, f"POST {path}")
        return r.json()

    def put(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        r = self.s.put(f"{BASE}{path}", headers=self.h, json=body, timeout=120)
        self._check(r, f"PUT {path}")
        return r.json()

    def list_all(self, path: str) -> list[dict[str, Any]]:
        """Walk every page of a list endpoint."""
        out: list[dict[str, Any]] = []
        page = 0
        while True:
            data = self.get(path, params={"q": f"(page:{page},page_size:100)"})
            batch = data.get("result", [])
            out.extend(batch)
            if len(batch) < 100:
                return out
            page += 1


def step(msg: str) -> None:
    print(f">> {msg}", flush=True)


# Build steps
def ensure_database(api: Superset) -> int:
    existing = next((d for d in api.list_all("/api/v1/database/") if d["database_name"] == DB_NAME), None)
    body = {
        "database_name": DB_NAME,
        "sqlalchemy_uri": DB_URI,
        "expose_in_sqllab": True,
        "allow_run_async": True,   # lets SQL Lab hand long queries to the Celery worker
        "allow_ctas": False,
        "allow_cvas": False,
        "allow_dml": False,
        "cache_timeout": 300,
    }
    if existing:
        step(f"Database '{DB_NAME}' exists (id {existing['id']}), updating")
        api.put(f"/api/v1/database/{existing['id']}", body)
        return existing["id"]
    step(f"Creating database '{DB_NAME}'")
    return api.post("/api/v1/database/", body)["id"]


def ensure_dataset(api: Superset, db_id: int, spec: dict[str, Any]) -> int:
    existing = next(
        (
            d for d in api.list_all("/api/v1/dataset/")
            if d["table_name"] == spec["table"]
            and d.get("schema") == spec["schema"]
            and d["database"]["id"] == db_id
        ),
        None,
    )
    if existing:
        return existing["id"]

    step(f"Creating dataset {spec['schema']}.{spec['table']}")
    base = {"database": db_id, "schema": spec["schema"], "table_name": spec["table"]}
    try:
        return api.post("/api/v1/dataset/", base)["id"]
    except requests.HTTPError:
        if "virtual_sql" not in spec:
            raise
        # Some Superset/SQLAlchemy combinations do not reflect materialized
        # views as physical tables. A virtual dataset is the same thing with
        # one extra line of SQL, so fall back to that.
        step(f"   physical dataset failed, creating {spec['table']} as a virtual dataset")
        return api.post("/api/v1/dataset/", {**base, "sql": spec["virtual_sql"]})["id"]


def configure_dataset(api: Superset, ds_id: int, spec: dict[str, Any]) -> None:
    current = api.get(f"/api/v1/dataset/{ds_id}")["result"]
    by_name = {m["metric_name"]: m for m in current.get("metrics", [])}
    wanted = {m["metric_name"] for m in spec["metrics"]}

    metrics: list[dict[str, Any]] = []
    for m in spec["metrics"]:
        item = dict(m)
        if m["metric_name"] in by_name:
            item["id"] = by_name[m["metric_name"]]["id"]  # update in place, never duplicate
        metrics.append(item)
    # Keep anything Superset or a human added that this script does not manage
    # (the auto-created COUNT(*), for example).
    for name, m in by_name.items():
        if name not in wanted:
            metrics.append({"id": m["id"], "metric_name": name, "expression": m["expression"]})

    body: dict[str, Any] = {
        "description": spec["description"],
        "cache_timeout": spec["cache_timeout"],
        "metrics": metrics,
    }
    if spec.get("main_dttm_col"):
        body["main_dttm_col"] = spec["main_dttm_col"]
    api.put(f"/api/v1/dataset/{ds_id}", body)


def smoke_test_dataset(api: Superset, ds_id: int, spec: dict[str, Any]) -> None:
    """Run every metric once so a typo in an expression fails here, loudly."""
    payload = {
        "datasource": {"id": ds_id, "type": "table"},
        "queries": [{
            "metrics": [m["metric_name"] for m in spec["metrics"]],
            "columns": [],
            "row_limit": 1,
        }],
        "result_format": "json",
        "result_type": "full",
    }
    result = api.post("/api/v1/chart/data", payload)["result"][0]
    if result.get("error"):
        sys.exit(f"Metric smoke test failed on {spec['table']}: {result['error']}")
    print(f"   ok  {spec['table']}: {result['data'][0] if result.get('data') else '(no rows)'}")


def ensure_dashboard(api: Superset, title: str, slug: str) -> int:
    existing = next((d for d in api.list_all("/api/v1/dashboard/") if d.get("slug") == slug), None)
    if existing:
        return existing["id"]
    step(f"Creating dashboard '{title}'")
    return api.post("/api/v1/dashboard/", {"dashboard_title": title, "slug": slug, "published": True})["id"]


def ensure_chart(api: Superset, spec: dict[str, Any], ds_id: int, dash_id: int, existing: dict[str, int]) -> int:
    params = {**spec["params"], "datasource": f"{ds_id}__table"}
    body = {
        "slice_name": spec["name"],
        "viz_type": spec["params"]["viz_type"],
        "datasource_id": ds_id,
        "datasource_type": "table",
        "params": json.dumps(params),
        "description": spec.get("description", ""),
        "dashboards": [dash_id],
    }
    if spec["name"] in existing:
        chart_id = existing[spec["name"]]
        api.put(f"/api/v1/chart/{chart_id}", body)
        return chart_id
    step(f"Creating chart '{spec['name']}'")
    return api.post("/api/v1/chart/", body)["id"]



def build_layout(title: str, intro: str, charts: list[dict[str, Any]], chart_ids: dict[str, int]) -> dict[str, Any]:
    """Superset's dashboard layout is a tree: ROOT > GRID > ROW > CHART, 12 columns wide."""
    layout: dict[str, Any] = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        "GRID_ID": {"type": "GRID", "id": "GRID_ID", "children": ["ROW-0"], "parents": ["ROOT_ID"]},
        "HEADER_ID": {"type": "HEADER", "id": "HEADER_ID", "meta": {"text": title}},
        "ROW-0": {"type": "ROW", "id": "ROW-0", "children": ["MARKDOWN-1"], "parents": ["ROOT_ID", "GRID_ID"],
                  "meta": {"background": "BACKGROUND_TRANSPARENT"}},
        "MARKDOWN-1": {"type": "MARKDOWN", "id": "MARKDOWN-1", "children": [], "parents": ["ROOT_ID", "GRID_ID", "ROW-0"],
                       "meta": {"width": 12, "height": 12, "code": intro}},
    }
    rows: dict[int, list[dict[str, Any]]] = {}
    for c in charts:
        rows.setdefault(c["row"], []).append(c)
    for row_no in sorted(rows):
        row_id = f"ROW-{row_no}"
        layout[row_id] = {"type": "ROW", "id": row_id, "children": [], "parents": ["ROOT_ID", "GRID_ID"],
                          "meta": {"background": "BACKGROUND_TRANSPARENT"}}
        layout["GRID_ID"]["children"].append(row_id)
        for c in rows[row_no]:
            cid = chart_ids[c["name"]]
            node = f"CHART-{cid}"
            layout[node] = {"type": "CHART", "id": node, "children": [], "parents": ["ROOT_ID", "GRID_ID", row_id],
                            "meta": {"width": c["width"], "height": c["height"], "chartId": cid, "sliceName": c["name"]}}
            layout[row_id]["children"].append(node)
    return layout


def build_metadata(filters: list[tuple[str, str, str]], ds_ids: dict[str, int]) -> dict[str, Any]:
    scope = {"rootPath": ["ROOT_ID"], "excluded": []}
    defaults = {"filterState": {}, "extraFormData": {}}
    native = [{"id": "NATIVE_FILTER-time", "name": "Time range", "filterType": "filter_time", "targets": [{}],
               "defaultDataMask": defaults, "controlValues": {}, "cascadeParentIds": [], "scope": scope,
               "type": "NATIVE_FILTER", "description": ""}]
    for name, column, ds in filters:
        native.append({
            "id": f"NATIVE_FILTER-{column}", "name": name, "filterType": "filter_select",
            "targets": [{"datasetId": ds_ids[ds], "column": {"name": column}}], "defaultDataMask": defaults,
            "controlValues": {"enableEmptyFilter": False, "defaultToFirstItem": False, "multiSelect": True,
                              "searchAllOptions": False, "inverseSelection": False},
            "cascadeParentIds": [], "scope": scope, "type": "NATIVE_FILTER", "description": ""})
    return {"native_filter_configuration": native, "cross_filters_enabled": True, "refresh_frequency": 0,
            "color_scheme": "supersetColors", "expanded_slices": {}, "label_colors": {},
            "timed_refresh_immune_slices": [], "default_filters": "{}"}


def main() -> None:
    api = Superset()
    step(f"Waiting for Superset at {BASE}")
    api.wait_until_ready()
    api.login()
    db_id = ensure_database(api)

    ds_ids: dict[str, int] = {}
    for key, spec in DATASETS.items():
        ds_ids[key] = ensure_dataset(api, db_id, spec)
        configure_dataset(api, ds_ids[key], spec)
    step("Smoke-testing every metric against the warehouse")
    for key, spec in DATASETS.items():
        smoke_test_dataset(api, ds_ids[key], spec)

    for dash in DASHBOARDS.values():
        dash_id = ensure_dashboard(api, dash["title"], dash["slug"])
        existing = {c["slice_name"]: c["id"] for c in api.list_all("/api/v1/chart/")}
        chart_ids = {c["name"]: ensure_chart(api, c, ds_ids[c["dataset"]], dash_id, existing) for c in dash["charts"]}
        step(f"Laying out '{dash['title']}'")
        api.put(f"/api/v1/dashboard/{dash_id}", {
            "dashboard_title": dash["title"], "slug": dash["slug"], "published": True,
            "position_json": json.dumps(build_layout(dash["title"], dash["intro"], dash["charts"], chart_ids)),
            "json_metadata": json.dumps(build_metadata(dash["filters"], ds_ids))})
        print(f"   {BASE.replace('superset', 'localhost')}/superset/dashboard/{dash['slug']}/")


if __name__ == "__main__":
    main()
