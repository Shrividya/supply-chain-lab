"""Behaviour of the ingestion layer, tested against real Postgres databases.

Each test states a property a data engineer should be able to defend in an
interview: idempotency, atomicity, late data, schema drift, least privilege.
"""
from __future__ import annotations

import os
import subprocess
import sys
import unittest

from helpers import ROOT, PlatformTestCase

from pipeline import db, extract_load, reconcile, schema_drift
from pipeline.config import TABLES, TABLES_BY_NAME


class InitialLoad(PlatformTestCase):
    def test_full_load_copies_everything_and_moves_watermarks(self):
        results = extract_load.load_all("t")
        self.assertEqual({r.table for r in results}, {t.name for t in TABLES})
        for spec in TABLES:
            source_keys = self.src(f"SELECT count(*) FROM {spec.source}")
            raw_keys = self.wh(f"SELECT count(DISTINCT {spec.pk[0]}) FROM {spec.raw}")
            self.assertEqual(source_keys, raw_keys, spec.name)
            wm = self.wh(f"SELECT high_watermark FROM ops.watermarks WHERE table_name = '{spec.name}'")
            src_max = self.src(f"SELECT max({spec.cursor}) FROM {spec.source}")
            self.assertEqual(wm, src_max, spec.name)
        audit = self.wh("SELECT count(*) FROM ops.load_audit WHERE status = 'success'")
        self.assertEqual(audit, str(len(TABLES)))

    def test_lineage_columns_are_filled(self):
        extract_load.load_table(TABLES_BY_NAME["orders"], "t")
        self.assertEqual(self.wh("SELECT count(*) FROM raw.orders WHERE _batch_id IS NULL OR _loaded_at IS NULL"), "0")
        batch = self.wh("SELECT batch_id FROM ops.load_audit WHERE table_name = 'orders'")
        self.assertEqual(self.wh("SELECT count(DISTINCT _batch_id) FROM raw.orders"), "1")
        self.assertEqual(self.wh("SELECT max(_batch_id) FROM raw.orders"), batch)

    def test_events_land_in_monthly_partitions_not_the_default(self):
        extract_load.load_table(TABLES_BY_NAME["web_events"], "t")
        self.assertEqual(self.wh("SELECT count(*) FROM raw.web_events_default"), "0")
        partitions = int(self.wh("SELECT count(*) FROM pg_inherits WHERE inhparent = 'raw.web_events'::regclass"))
        self.assertGreaterEqual(partitions, 21)         # Jan 2025 .. Sep 2026, plus the default one


class Incremental(PlatformTestCase):
    preload = True

    def test_rerunning_a_load_is_harmless(self):
        before = self.wh("SELECT count(*) FROM raw.orders")
        for _ in range(2):
            results = extract_load.load_all("t")
            self.assertTrue(all(r.rows_inserted == 0 for r in results), results)
        self.assertEqual(self.wh("SELECT count(*) FROM raw.orders"), before)

    def test_overlap_window_rereads_recent_rows_but_the_key_ignores_them(self):
        r = extract_load.load_table(TABLES_BY_NAME["orders"], "t", overlap_seconds=3600)
        self.assertGreater(r.rows_extracted, 0)
        self.assertEqual(r.rows_inserted, 0)
        self.assertEqual(r.rows_ignored, r.rows_extracted)

    def test_no_overlap_means_nothing_to_read_when_nothing_changed(self):
        r = extract_load.load_table(TABLES_BY_NAME["orders"], "t", overlap_seconds=0)
        self.assertEqual((r.rows_extracted, r.rows_inserted), (0, 0))

    def test_full_refresh_rereads_everything_and_still_adds_nothing(self):
        before = self.wh("SELECT count(*) FROM raw.customers")
        r = extract_load.load_table(TABLES_BY_NAME["customers"], "t", full=True)
        self.assertEqual(str(r.rows_extracted), self.src("SELECT count(*) FROM app.customers"))
        self.assertEqual(r.rows_inserted, 0)
        self.assertEqual(self.wh("SELECT count(*) FROM raw.customers"), before)

    def test_changed_rows_are_appended_as_new_versions_not_overwritten(self):
        old_wm = self.wh("SELECT high_watermark FROM ops.watermarks WHERE table_name = 'orders'")
        before = int(self.wh("SELECT count(*) FROM raw.orders"))
        self.advance(1)
        extract_load.load_all("t")
        after = int(self.wh("SELECT count(*) FROM raw.orders"))
        distinct_orders_now = int(self.wh("SELECT count(DISTINCT order_id) FROM raw.orders"))
        self.assertGreater(after, before)
        self.assertGreater(after, distinct_orders_now, "settled orders should have more than one version")
        new_wm = self.wh("SELECT high_watermark FROM ops.watermarks WHERE table_name = 'orders'")
        self.assertEqual(self.wh(f"SELECT '{new_wm}'::timestamptz > '{old_wm}'::timestamptz"), "t")

    def test_latest_version_in_raw_matches_the_source(self):
        self.advance(2)
        extract_load.load_all("t")
        changed = db.query(self.source_admin,
                           "SELECT order_id, status FROM app.orders WHERE status <> 'pending' "
                           "AND updated_at > created_at ORDER BY updated_at DESC LIMIT 5")
        self.assertTrue(changed)
        for order_id, status in changed:
            latest = self.wh(f"SELECT status FROM raw.orders WHERE order_id = {order_id} "
                             "ORDER BY updated_at DESC LIMIT 1")
            self.assertEqual(latest, status, order_id)

    def test_late_arriving_events_are_captured_because_the_cursor_is_ingestion_time(self):
        self.advance(1)                                   # includes events for yesterday that arrive today
        extract_load.load_table(TABLES_BY_NAME["web_events"], "t")
        self.assertEqual(self.src("SELECT count(*) FROM app.web_events"),
                         self.wh("SELECT count(*) FROM raw.web_events"))
        late = self.src("SELECT count(*) FROM app.web_events WHERE ingested_at::date > event_ts::date")
        self.assertGreater(int(late), 0)

    def test_new_month_creates_its_partition_automatically(self):
        self.advance(9)                                    # template ends 2026-09-23, so this crosses into October
        extract_load.load_table(TABLES_BY_NAME["web_events"], "t")
        self.assertIsNotNone(self.wh("SELECT to_regclass('raw.web_events_2026_10')"))
        self.assertEqual(self.wh("SELECT count(*) FROM raw.web_events_default"), "0")


class Atomicity(PlatformTestCase):
    preload = True

    def test_a_failed_load_leaves_no_partial_data_and_no_watermark_move(self):
        self.advance(1)
        count_before = self.wh("SELECT count(*) FROM raw.orders")
        wm_before = self.wh("SELECT high_watermark FROM ops.watermarks WHERE table_name = 'orders'")
        # New orders arrive as 'pending'; this constraint (NOT VALID: it ignores rows already
        # in the table) makes the insert fail part way through the batch.
        self.wh("ALTER TABLE raw.orders ADD CONSTRAINT boom CHECK (status <> 'pending') NOT VALID")

        with self.assertRaises(db.PsqlError):
            extract_load.load_table(TABLES_BY_NAME["orders"], "t")

        self.assertEqual(self.wh("SELECT count(*) FROM raw.orders"), count_before)
        self.assertEqual(self.wh("SELECT high_watermark FROM ops.watermarks WHERE table_name = 'orders'"), wm_before)
        status, error = db.query(self.warehouse,
                                 "SELECT status, error FROM ops.load_audit WHERE table_name = 'orders' "
                                 "ORDER BY batch_id DESC LIMIT 1")[0]
        self.assertEqual(status, "failed")
        self.assertIn("boom", error)

        # Fix the cause and simply run it again: nothing was lost, nothing is duplicated.
        self.wh("ALTER TABLE raw.orders DROP CONSTRAINT boom")
        r = extract_load.load_table(TABLES_BY_NAME["orders"], "t")
        self.assertGreater(r.rows_inserted, 0)
        extract_load.load_all("t")
        reconcile.run_checks("t")                          # parity holds again


class SchemaDrift(PlatformTestCase):
    preload = True

    def test_an_added_column_warns_and_is_ignored(self):
        self.src("SELECT sim.inject('schema_add_column')")
        r = extract_load.load_table(TABLES_BY_NAME["orders"], "t")
        self.assertIsNotNone(r.watermark_to)
        kind, severity, column = db.query(self.warehouse,
            "SELECT kind, severity, column_name FROM ops.schema_drift ORDER BY id DESC LIMIT 1")[0]
        self.assertEqual((kind, severity, column), ("column_added", "warn", "coupon_code"))
        self.assertEqual(self.wh("SELECT count(*) FROM information_schema.columns "
                                 "WHERE table_schema = 'raw' AND table_name = 'orders' AND column_name = 'coupon_code'"), "0")

    def test_a_renamed_column_stops_the_load_before_anything_is_written(self):
        self.advance(1)
        count_before = self.wh("SELECT count(*) FROM raw.orders")
        wm_before = self.wh("SELECT high_watermark FROM ops.watermarks WHERE table_name = 'orders'")
        self.src("SELECT sim.inject('schema_rename_column')")

        with self.assertRaises(schema_drift.SchemaDriftError) as ctx:
            extract_load.load_table(TABLES_BY_NAME["orders"], "t")
        self.assertIn("channel", str(ctx.exception))
        self.assertEqual(self.wh("SELECT count(*) FROM raw.orders"), count_before)
        self.assertEqual(self.wh("SELECT high_watermark FROM ops.watermarks WHERE table_name = 'orders'"), wm_before)
        kinds = {k for (k,) in db.query(self.warehouse, "SELECT kind FROM ops.schema_drift WHERE severity = 'error'")}
        self.assertEqual(kinds, {"column_missing"})

        self.src("SELECT sim.reset_faults()")             # revert, then the same load just works
        self.assertGreater(extract_load.load_table(TABLES_BY_NAME["orders"], "t").rows_inserted, 0)


class DataQuality(PlatformTestCase):
    preload = True

    def statuses(self, results):
        return {(r.name, r.table): r.status for r in results}

    def test_everything_passes_right_after_a_clean_load(self):
        results = reconcile.run_checks("t")
        self.assertEqual({r.status for r in results}, {"pass"}, [r for r in results if r.status != "pass"])
        self.assertEqual(self.wh("SELECT count(*) FROM ops.dq_results WHERE run_label = 't'"), str(len(results)))

    def test_lost_rows_are_detected(self):
        self.wh("DELETE FROM raw.orders WHERE order_id IN (SELECT order_id FROM raw.orders LIMIT 10)")
        with self.assertRaises(reconcile.DataQualityError) as ctx:
            reconcile.run_checks("t")
        self.assertIn("rowcount_parity[orders]", str(ctx.exception))

    def test_a_shifted_amount_is_detected(self):
        self.wh("UPDATE raw.order_items SET unit_price = unit_price + 1 WHERE order_item_id % 500 = 0")
        with self.assertRaises(reconcile.DataQualityError) as ctx:
            reconcile.run_checks("t")
        self.assertIn("amount_parity", str(ctx.exception))

    def test_comparison_is_skipped_not_failed_when_the_source_is_ahead(self):
        self.advance(1)                                   # source moves on, the warehouse has not loaded it
        results = reconcile.run_checks("t")               # must not raise
        s = self.statuses(results)
        self.assertEqual(s[("rowcount_parity", "orders")], "skipped")
        self.assertEqual(s[("watermark_lag", "orders")], "warn")

    def test_null_rate_check_catches_a_bad_release(self):
        self.src("SELECT sim.inject('null_channel', 60)")
        extract_load.load_all("t")
        with self.assertRaises(reconcile.DataQualityError) as ctx:
            reconcile.run_checks("t")
        self.assertIn("null_rate_channel", str(ctx.exception))

    def test_events_in_the_default_partition_are_flagged(self):
        self.wh("INSERT INTO raw.web_events (event_id, session_id, event_ts, event_type, device, ingested_at, _batch_id) "
                "VALUES (9999999, 1, '2035-01-01', 'page_view', 'mobile', now(), 0)")
        with self.assertRaises(reconcile.DataQualityError) as ctx:
            reconcile.run_checks("t")
        self.assertIn("default_partition_empty", str(ctx.exception))

    def test_a_feed_that_stops_is_caught_by_the_volume_check(self):
        day = self.wh("SELECT (SELECT max(event_ts)::date FROM raw.web_events) - 1")
        self.wh(f"DELETE FROM raw.web_events WHERE event_ts::date = '{day}' AND event_id % 10 <> 0")
        with self.assertRaises(reconcile.DataQualityError) as ctx:
            reconcile.run_checks("t")
        self.assertIn("daily_volume", str(ctx.exception))

    def test_failures_are_recorded_even_though_the_check_raises(self):
        self.wh("DELETE FROM raw.customers WHERE customer_id = 1")
        with self.assertRaises(reconcile.DataQualityError):
            reconcile.run_checks("recorded")
        self.assertEqual(self.wh("SELECT count(*) FROM ops.dq_results WHERE run_label = 'recorded' AND status = 'fail'"), "1")


class LeastPrivilege(PlatformTestCase):
    preload = True

    def assertDenied(self, dsn, sql):
        with self.assertRaises(db.PsqlError) as ctx:
            db.execute(dsn, sql)
        self.assertIn("permission denied", str(ctx.exception))

    def test_the_pipeline_cannot_write_to_the_source(self):
        self.assertDenied(self.source, "UPDATE app.orders SET status = 'completed'")
        self.assertDenied(self.source, "SELECT sim.advance_day()")

    def test_the_loader_cannot_touch_business_tables(self):
        self.assertDenied(self.warehouse, "CREATE TABLE marts.sneaky (a int)")

    def test_dbt_can_read_raw_but_not_change_it(self):
        self.assertEqual(db.scalar(self.dbt, "SELECT count(*) > 0 FROM raw.orders"), "t")
        self.assertDenied(self.dbt, "DELETE FROM raw.orders")

    def test_bi_can_read_ops_but_not_raw(self):
        self.assertEqual(db.scalar(self.bi, "SELECT count(*) > 0 FROM ops.load_audit"), "t")
        self.assertDenied(self.bi, "SELECT * FROM raw.orders LIMIT 1")


class CommandLine(PlatformTestCase):
    def run_cli(self, *args):
        env = {**os.environ, "SOURCE_DSN": self.source, "WAREHOUSE_DSN": self.warehouse,
               "SOURCE_ADMIN_DSN": self.source_admin}
        return subprocess.run([sys.executable, "-m", "pipeline", *args], cwd=ROOT, env=env,
                              capture_output=True, text=True)

    def test_load_reconcile_status_round_trip(self):
        self.assertEqual(self.run_cli("load", "--label", "cli").returncode, 0)
        self.assertEqual(self.run_cli("reconcile", "--label", "cli").returncode, 0)
        out = self.run_cli("status")
        self.assertEqual(out.returncode, 0)
        for spec in TABLES:
            self.assertIn(spec.name, out.stdout)

    def test_exit_codes_distinguish_data_problems_from_usage_errors(self):
        self.run_cli("load")
        self.wh("DELETE FROM raw.customers WHERE customer_id = 1")
        self.assertEqual(self.run_cli("reconcile").returncode, 1)                 # data problem
        self.assertEqual(self.run_cli("reconcile", "--no-fail").returncode, 0)    # recorded, not enforced
        self.assertEqual(self.run_cli("load", "--tables", "nope").returncode, 2)  # usage error

    def test_passwords_do_not_leak_into_error_messages(self):
        env = {**os.environ, "SOURCE_DSN": self.source, "WAREHOUSE_DSN": self.warehouse.replace("loader:loader", "loader:wrongpw"),
               "SOURCE_ADMIN_DSN": self.source_admin}
        # Trust auth on the scratch server would accept any password, so test the redaction function directly too.
        self.assertNotIn("hunter2", db._redact("could not connect to postgresql://bob:hunter2@host/db"))
        r = subprocess.run([sys.executable, "-m", "pipeline", "status"], cwd=ROOT, env=env, capture_output=True, text=True)
        self.assertNotIn("wrongpw", r.stdout + r.stderr)


if __name__ == "__main__":
    unittest.main()
