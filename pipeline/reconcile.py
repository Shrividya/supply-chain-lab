"""Data quality checks that compare the warehouse against the source and against itself.

These are different from dbt tests. dbt tests ask "is the transformed data sane?".
These ask "did ingestion lose or duplicate anything?", which dbt cannot see
because it has no access to the source.

Every check writes a row to ops.dq_results (so Superset can chart pass/fail over
time) and returns a CheckResult. Statuses:
  pass     the check ran and the data is fine
  fail     the check ran and the data is wrong (the pipeline should stop)
  warn     worth a look, not worth stopping for
  skipped  the check could not give a trustworthy answer right now, and says why
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from . import db
from .config import TABLES, TableSpec, source_dsn, warehouse_dsn

log = logging.getLogger(__name__)

NULL_RATE_LIMIT = 0.01           # more than 1% null channels in recent orders is a fail
VOLUME_TOLERANCE = 0.5           # a day more than 50% away from its trailing average is a fail
MIN_HISTORY_DAYS = 8


class DataQualityError(RuntimeError):
    pass


@dataclass(frozen=True)
class CheckResult:
    name: str
    table: str
    status: str
    expected: Optional[str] = None
    actual: Optional[str] = None
    details: str = ""


def run_checks(run_label: str | None = None, *, raise_on_fail: bool = True) -> list[CheckResult]:
    results: list[CheckResult] = []
    for spec in TABLES:
        results.append(_watermark_lag(spec))
        results.append(_rowcount_parity(spec))
    results.append(_amount_parity())
    results.append(_default_partition_empty())
    results.append(_null_rate_recent_orders())
    results.append(_daily_volume_anomaly())
    _record(results, run_label)
    failed = [r for r in results if r.status == "fail"]
    for r in results:
        log.info("%-7s %-24s %-12s expected=%s actual=%s %s", r.status, r.name, r.table,
                 r.expected, r.actual, r.details)
    if failed and raise_on_fail:
        raise DataQualityError("; ".join(f"{r.name}[{r.table}]: expected {r.expected}, got {r.actual}"
                                         for r in failed))
    return results


# checks
def _state(spec: TableSpec) -> tuple[Optional[str], Optional[str]]:
    wm = db.scalar(warehouse_dsn(),
                   f"SELECT high_watermark FROM ops.watermarks WHERE table_name = {db.sql_literal(spec.name)}")
    src_max = db.scalar(source_dsn(), f"SELECT max({spec.cursor}) FROM {spec.source}")
    return wm, src_max


def _ts_le(a: str, b: str) -> bool:
    """a <= b, compared by the database rather than as strings."""
    return db.scalar(warehouse_dsn(),
                     f"SELECT {db.timestamp_literal(a)} <= {db.timestamp_literal(b)}") == "t"


def _watermark_lag(spec: TableSpec) -> CheckResult:
    wm, src_max = _state(spec)
    if wm is None:
        return CheckResult("watermark_lag", spec.name, "fail", "a watermark", "none", "never loaded")
    if src_max is None or _ts_le(src_max, wm):
        return CheckResult("watermark_lag", spec.name, "pass", "0 rows behind", "0 rows behind")
    behind = db.scalar(source_dsn(),
                       f"SELECT count(*) FROM {spec.source} WHERE {spec.cursor} > {db.timestamp_literal(wm)}")
    return CheckResult("watermark_lag", spec.name, "warn", "0 rows behind", f"{behind} rows behind",
                       "the source has changes the warehouse has not loaded yet")


def _rowcount_parity(spec: TableSpec) -> CheckResult:
    """Distinct keys in the source == distinct keys in raw, when nothing changed since the load."""
    wm, src_max = _state(spec)
    if wm is None:
        return CheckResult("rowcount_parity", spec.name, "fail", "loaded rows", "none", "never loaded")
    if src_max is not None and not _ts_le(src_max, wm):
        return CheckResult("rowcount_parity", spec.name, "skipped", details=(
            "the source is ahead of the watermark, so an exact comparison would be a false alarm; "
            "run it again right after a load"))
    pk = spec.pk[0]
    s = db.query(source_dsn(), f"SELECT count(*), coalesce(sum({pk}::numeric), 0) FROM {spec.source}")[0]
    w = db.query(warehouse_dsn(),
                 f"SELECT count(*), coalesce(sum({pk}::numeric), 0) FROM "
                 f"(SELECT DISTINCT {pk} FROM {spec.raw}) k")[0]
    expected, actual = f"{s[0]} keys, checksum {s[1]}", f"{w[0]} keys, checksum {w[1]}"
    return CheckResult("rowcount_parity", spec.name, "pass" if s == w else "fail", expected, actual)


def _amount_parity() -> CheckResult:
    """Money must add up. Uses order_items because it never changes after creation."""
    spec = next(t for t in TABLES if t.name == "order_items")
    wm, src_max = _state(spec)
    if wm is None:
        return CheckResult("amount_parity", "order_items", "fail", "loaded rows", "none", "never loaded")
    if src_max is not None and not _ts_le(src_max, wm):
        return CheckResult("amount_parity", "order_items", "skipped",
                           details="the source is ahead of the watermark")
    s = db.scalar(source_dsn(), "SELECT coalesce(sum(quantity * unit_price), 0) FROM app.order_items")
    w = db.scalar(warehouse_dsn(),
                  "SELECT coalesce(sum(quantity * unit_price), 0) FROM ("
                  "SELECT DISTINCT ON (order_item_id) quantity, unit_price FROM raw.order_items "
                  "ORDER BY order_item_id, updated_at DESC) latest")
    return CheckResult("amount_parity", "order_items", "pass" if s == w else "fail", str(s), str(w))


def _default_partition_empty() -> CheckResult:
    n = int(db.scalar(warehouse_dsn(), "SELECT count(*) FROM raw.web_events_default") or 0)
    return CheckResult("default_partition_empty", "web_events", "pass" if n == 0 else "fail",
                       "0 rows", f"{n} rows",
                       "" if n == 0 else "events landed in the DEFAULT partition: a month partition is missing")


def _null_rate_recent_orders() -> CheckResult:
    row = db.query(warehouse_dsn(), """
        WITH latest AS (
            SELECT DISTINCT ON (order_id) order_id, order_ts, channel
            FROM raw.orders ORDER BY order_id, updated_at DESC
        ), recent AS (
            SELECT * FROM latest WHERE order_ts >= (SELECT max(order_ts) FROM latest) - interval '7 days'
        )
        SELECT count(*), count(*) FILTER (WHERE channel IS NULL) FROM recent""")[0]
    total, nulls = int(row[0]), int(row[1])
    if total == 0:
        return CheckResult("null_rate_channel", "orders", "skipped", details="no recent orders")
    rate = nulls / total
    return CheckResult("null_rate_channel", "orders", "pass" if rate <= NULL_RATE_LIMIT else "fail",
                       f"<= {NULL_RATE_LIMIT:.1%}", f"{rate:.1%} ({nulls} of {total})")


def _daily_volume_anomaly() -> CheckResult:
    """Yesterday's event count vs the trailing 7-day average (a cheap 'did the feed break?' alarm)."""
    rows = db.query(warehouse_dsn(), """
        SELECT event_ts::date AS d, count(*) FROM raw.web_events
        GROUP BY 1 ORDER BY 1 DESC LIMIT 9""")
    if len(rows) < MIN_HISTORY_DAYS:
        return CheckResult("daily_volume", "web_events", "skipped", details="not enough history yet")
    counts = [int(c) for _, c in rows]
    # rows[0] is the newest (possibly partial) day; judge the day before it against the 7 before that
    day, latest = rows[1][0], counts[1]
    trailing = counts[2:9]
    mean = sum(trailing) / len(trailing)
    ok = abs(latest - mean) <= VOLUME_TOLERANCE * mean
    return CheckResult("daily_volume", "web_events", "pass" if ok else "fail",
                       f"{mean:.0f} +/- {VOLUME_TOLERANCE:.0%}", f"{latest} on {day}")


# persistence
def _record(results: list[CheckResult], run_label: str | None) -> None:
    def lit(v: Optional[str]) -> str:
        return "NULL" if v is None else db.sql_literal(v)
    values = ", ".join(
        f"({lit(run_label or '')}, {lit(r.name)}, {lit(r.table)}, {lit(r.status)}, "
        f"{lit(r.expected)}, {lit(r.actual)}, {lit(r.details)})" for r in results)
    db.execute(warehouse_dsn(),
               "INSERT INTO ops.dq_results (run_label, check_name, table_name, status, expected, actual, details) "
               f"VALUES {values}")
