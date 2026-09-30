"""Do the dbt models compute the right thing, and do their tests actually catch problems?

These tests run the models through tools/mini_dbt.py (a stand-in for `dbt build`, see its
docstring) against a real Postgres. They check two different things:

  * properties of the models   (incremental == full rebuild, SCD2 is point-in-time correct)
  * properties of the tests    (each injected fault turns the intended check red)

The second group is the important one. A green test suite only means something if you
have seen it go red for the right reason.
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest

from helpers import ROOT, PlatformTestCase

from pipeline import db, extract_load

sys.path.insert(0, str(ROOT / "tools"))
import mini_dbt  # noqa: E402


class DbtCase(PlatformTestCase):
    preload = True

    def dbt_cmd(self, command: str, *extra: str) -> tuple[int, str]:
        r = subprocess.run([sys.executable, str(ROOT / "tools" / "mini_dbt.py"), command, "--dsn", self.dbt, *extra],
                           cwd=ROOT, capture_output=True, text=True, env=os.environ)
        return r.returncode, r.stdout + r.stderr

    def build_ok(self, *extra: str) -> str:
        code, out = self.dbt_cmd("build", *extra)
        self.assertEqual(code, 0, out)
        return out

    def failures(self, out: str) -> set[str]:
        return {line.split("FAIL", 1)[1].split(":")[0].strip() for line in out.splitlines() if "  FAIL  " in line}

    def load(self) -> None:
        extract_load.load_all("t")

    def same_rows(self, left: str, right: str) -> bool:
        a = db.scalar(self.dbt, f"SELECT count(*) FROM (SELECT * FROM {left} EXCEPT SELECT * FROM {right}) x")
        b = db.scalar(self.dbt, f"SELECT count(*) FROM (SELECT * FROM {right} EXCEPT SELECT * FROM {left}) x")
        return a == "0" and b == "0"


class ModelProperties(DbtCase):
    def test_first_build_is_green(self):
        out = self.build_ok()
        self.assertIn("0 failed", out)

    def test_incremental_result_equals_a_full_rebuild(self):
        """The defining property of an incremental model: same answer, less work."""
        self.build_ok()
        self.advance(3)                      # new orders, settled orders, refunds, late events
        self.load()
        out = self.build_ok()                # incremental run
        self.assertIn("incremental (merged)", out)
        db.execute(self.dbt, "CREATE TABLE marts._snap_orders AS SELECT * FROM marts.fct_orders;"
                             "CREATE TABLE marts._snap_funnel AS SELECT * FROM marts.mart_funnel_daily")
        self.build_ok("--full-refresh")      # rebuild from scratch
        self.assertTrue(self.same_rows("marts._snap_orders", "marts.fct_orders"), "fct_orders drifted")
        self.assertTrue(self.same_rows("marts._snap_funnel", "marts.mart_funnel_daily"), "funnel drifted")

    def test_a_refund_is_picked_up_as_an_update_to_an_old_order(self):
        self.build_ok()
        self.advance(3)
        self.load()
        self.build_ok()
        refunded = db.scalar(self.dbt, "SELECT count(*) FROM marts.fct_orders WHERE status = 'refunded' "
                                       "AND order_date >= '2026-09-15'")
        self.assertGreater(int(refunded), 0)
        self.assertEqual(db.scalar(self.dbt, "SELECT count(*) FROM marts.fct_orders WHERE is_completed "
                                             "AND status <> 'completed'"), "0")

    def test_history_is_point_in_time_correct(self):
        """An order keeps the country the customer lived in when the order was placed."""
        self.build_ok()
        self.advance(3)
        self.load()
        self.build_ok("--full-refresh")
        rows = db.query(self.dbt, """
            SELECT h1.customer_id, h1.country AS first_country, h2.country AS later_country, h2.valid_from
            FROM marts.dim_customers_history h1
            JOIN marts.dim_customers_history h2
              ON h1.customer_id = h2.customer_id AND h1.version_number = 1 AND h2.version_number = 2
            WHERE h1.country <> h2.country LIMIT 1""")
        self.assertTrue(rows, "the simulator should have moved at least one customer")
        customer, first_country, later_country, moved_at = rows[0]
        self.assertEqual(db.scalar(self.dbt, f"SELECT count(*) FROM marts.fct_orders WHERE customer_id = {customer} "
                                             f"AND order_ts < '{moved_at}' AND country <> '{first_country}'"), "0")
        self.assertEqual(db.scalar(self.dbt, f"SELECT count(*) FROM marts.fct_orders WHERE customer_id = {customer} "
                                             f"AND order_ts >= '{moved_at}' AND country <> '{later_country}'"), "0")

    def test_money_adds_up_across_layers(self):
        self.build_ok()
        gross = db.scalar(self.dbt, "SELECT round(sum(gross_revenue), 2) FROM marts.fct_orders")
        lines = db.scalar(self.dbt, "SELECT round(sum(line_revenue), 2) FROM marts.fct_order_items")
        raw = db.scalar(self.dbt, "SELECT round(sum(quantity * unit_price), 2) FROM staging.stg_order_items")
        self.assertEqual(gross, lines)
        self.assertEqual(gross, raw)

    def test_staging_keeps_exactly_the_newest_version(self):
        self.build_ok()
        self.advance(2)
        self.load()
        self.build_ok()
        self.assertEqual(db.scalar(self.dbt, "SELECT count(*) FROM staging.stg_orders"),
                         db.scalar(self.dbt, "SELECT count(DISTINCT order_id) FROM raw.orders"))
        self.assertEqual(db.scalar(self.dbt, """
            SELECT count(*) FROM staging.stg_orders s
            JOIN (SELECT order_id, max(updated_at) AS u FROM raw.orders GROUP BY order_id) m USING (order_id)
            WHERE s.updated_at <> m.u"""), "0")

    def test_no_personal_data_leaves_the_raw_schema(self):
        self.build_ok()
        leaked = db.scalar(self.dbt, """
            SELECT count(*) FROM information_schema.columns
            WHERE table_schema IN ('staging', 'marts') AND column_name IN ('email', 'full_name_raw')""")
        self.assertEqual(leaked, "0")
        self.assertEqual(db.scalar(self.dbt, "SELECT count(*) FROM staging.stg_customers WHERE length(email_hash) <> 32"), "0")

    def test_dashboards_can_read_marts_but_not_raw(self):
        self.build_ok()
        self.assertEqual(db.scalar(self.bi, "SELECT count(*) > 0 FROM marts.fct_orders"), "t")
        self.assertEqual(db.scalar(self.bi, "SELECT count(*) > 0 FROM marts.mart_daily_channel"), "t")
        with self.assertRaises(db.PsqlError):
            db.scalar(self.bi, "SELECT count(*) FROM raw.orders")

    def test_the_contract_check_notices_a_changed_column_type(self):
        self.build_ok()
        db.execute(self.dbt, "ALTER TABLE marts.fct_orders ALTER COLUMN units TYPE bigint")
        _, _, by_name = mini_dbt.load_project()
        problems = mini_dbt.check_contract(self.dbt, by_name["fct_orders"], mini_dbt.schema_yaml()["fct_orders"])
        self.assertTrue(any("units" in p for p in problems), problems)


class TheTestsCatchProblems(DbtCase):
    """Inject one fault, run the suite, expect exactly the intended checks to fail."""

    def setUp(self) -> None:
        super().setUp()
        self.build_ok()                       # a known-good baseline

    def inject_and_rebuild(self, kind: str, n: int = 20) -> str:
        self.src(f"SELECT sim.inject('{kind}', {n})")
        self.load()
        code, out = self.dbt_cmd("build")
        self.assertEqual(code, 1, f"a {kind} fault should fail the build:\n{out}")
        return out

    def test_negative_prices(self):
        failed = self.failures(self.inject_and_rebuild("bad_amount"))
        self.assertIn("singular(assert_no_negative_amounts)", failed)

    def test_missing_channel(self):
        failed = self.failures(self.inject_and_rebuild("null_channel"))
        self.assertIn("not_null(stg_orders.channel)", failed)

    def test_orders_for_customers_that_do_not_exist(self):
        failed = self.failures(self.inject_and_rebuild("orphan_order", 5))
        self.assertIn("relationships(stg_orders.customer_id)", failed)
        self.assertIn("not_null(fct_orders.country)", failed)        # the as-of join found nobody

    def test_events_later_than_the_reprocessing_window_are_caught_then_fixed_by_a_full_refresh(self):
        failed = self.failures(self.inject_and_rebuild("late_events", 6))
        self.assertIn("singular(assert_funnel_matches_events)", failed)
        self.assertEqual(self.dbt_cmd("build", "--full-refresh")[0], 0)   # the documented remedy works

    def test_events_inside_the_window_need_no_intervention(self):
        self.src("SELECT sim.inject('late_events', 2)")
        self.load()
        self.assertEqual(self.dbt_cmd("build")[0], 0)

    def test_a_source_fix_heals_the_warehouse_without_a_full_refresh(self):
        """Correct the bad rows upstream; the next incremental run repairs the marts.

        This only works because fct_orders watches its line items as well as the order row.
        """
        self.src("SELECT sim.inject('bad_amount', 10)")
        self.load()
        self.assertEqual(self.dbt_cmd("build")[0], 1)
        self.src("UPDATE app.order_items SET unit_price = abs(unit_price), "
                 "updated_at = (SELECT max(updated_at) FROM app.orders) + interval '2 hours' WHERE unit_price < 0")
        self.load()
        code, out = self.dbt_cmd("build")
        self.assertEqual(code, 0, out)


if __name__ == "__main__":
    unittest.main()
