"""Do the Superset definitions in provisioning/provision.py match the real warehouse?

We cannot start Superset here, but the most common failure of a dashboard-as-code
script is boring: a column was renamed in a dbt model and a chart still points at
the old name. These tests catch that by running every dataset metric as SQL
against the real marts, as the read-only role Superset uses, and by checking that
every column and metric a chart names exists.
"""
from __future__ import annotations

import sys
import types
import unittest

from helpers import ROOT
from test_transformations import DbtCase

from pipeline import db

if "requests" not in sys.modules:                      # the API client is not exercised here
    try:
        import requests  # noqa: F401
    except ImportError:
        stub = types.ModuleType("requests")
        stub.Session = object
        stub.Response = object
        stub.RequestException = Exception
        stub.HTTPError = Exception
        sys.modules["requests"] = stub
sys.path.insert(0, str(ROOT / "provisioning"))
import provision  # noqa: E402


class Provisioning(DbtCase):
    def setUp(self) -> None:
        super().setUp()
        self.build_ok()
        self.load()                                    # ops.* needs at least one audited load
        from pipeline import reconcile
        reconcile.run_checks("t", raise_on_fail=False)

    def columns(self, schema: str, table: str) -> set[str]:
        rows = db.query(self.bi, f"SELECT column_name FROM information_schema.columns "
                                 f"WHERE table_schema = '{schema}' AND table_name = '{table}'")
        return {r[0] for r in rows}

    def test_every_metric_runs_as_the_read_only_role(self):
        for key, spec in provision.DATASETS.items():
            for m in spec["metrics"]:
                with self.subTest(dataset=key, metric=m["metric_name"]):
                    db.scalar(self.bi, f"SELECT {m['expression']} FROM {spec['schema']}.{spec['table']}")

    def test_every_chart_refers_to_real_columns_and_metrics(self):
        for dash in provision.DASHBOARDS.values():
            for chart in dash["charts"]:
                ds = provision.DATASETS[chart["dataset"]]
                cols = self.columns(ds["schema"], ds["table"])
                metrics = {m["metric_name"] for m in ds["metrics"]}
                p = chart["params"]
                wanted_cols: list[str] = []
                g = p.get("groupby") or []
                wanted_cols += [g] if isinstance(g, str) else list(g)
                wanted_cols += p.get("all_columns") or []
                wanted_cols += [p["x_axis"]] if p.get("x_axis") else []
                wanted_cols += [f["subject"] for f in p.get("adhoc_filters", [])]
                used_metrics = list(p.get("metrics") or []) + ([p["metric"]] if p.get("metric") else [])
                with self.subTest(chart=chart["name"]):
                    self.assertFalse(set(wanted_cols) - cols, f"missing columns {set(wanted_cols) - cols} in {ds['table']}")
                    self.assertFalse(set(used_metrics) - metrics, f"unknown metrics {set(used_metrics) - metrics}")

    def test_dashboard_filters_point_at_real_columns(self):
        for dash in provision.DASHBOARDS.values():
            for name, column, dskey in dash["filters"]:
                ds = provision.DATASETS[dskey]
                with self.subTest(dashboard=dash["title"], filter=name):
                    self.assertIn(column, self.columns(ds["schema"], ds["table"]))

    def test_layout_is_a_consistent_tree(self):
        for dash in provision.DASHBOARDS.values():
            ids = {c["name"]: i + 100 for i, c in enumerate(dash["charts"])}
            layout = {k: v for k, v in provision.build_layout(
                dash["title"], dash["intro"], dash["charts"], ids).items() if isinstance(v, dict)}
            for node_id, node in layout.items():
                for child in node.get("children", []):
                    self.assertIn(child, layout, f"{node_id} points at missing {child}")
            for node in layout.values():
                if node.get("type") == "ROW":
                    width = sum(layout[c]["meta"]["width"] for c in node["children"])
                    self.assertLessEqual(width, 12)

    def test_every_sql_exercise_runs(self):
        text = (ROOT / "sql" / "exercises.sql").read_text()
        statements = [s.strip() for s in text.split(";\n") if "SELECT" in s.upper()]
        self.assertGreaterEqual(len(statements), 7)
        for i, stmt in enumerate(statements, 1):
            with self.subTest(exercise=i):
                db.query(self.bi, stmt.rstrip(";"))

    def test_chart_names_are_unique_across_dashboards(self):
        names = [c["name"] for d in provision.DASHBOARDS.values() for c in d["charts"]]
        self.assertEqual(len(names), len(set(names)))


if __name__ == "__main__":
    unittest.main()
