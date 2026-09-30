"""Incremental, idempotent, atomic loads from the source into the raw layer.

The algorithm for one table, and why each step is there:

  1. open an audit row (status 'running')            -> every attempt is visible
  2. schema drift check                              -> break loudly, before writing
  3. read the watermark, note the source's newest    -> `upper` is fixed NOW, so rows
     cursor value (`upper`)                             arriving mid-extract are not
                                                        half-counted
  4. stream rows with  lower < cursor <= upper       -> `lower` = watermark minus an
     into a staging table                               overlap, to catch late commits
  5. ONE transaction: insert staged rows (dupes      -> data and watermark move
     ignored), advance the watermark, close audit       together or not at all
  6. on any error: mark the audit row 'failed'       -> the watermark did not move,
                                                        so a retry re-reads the range

Properties this gives you: re-running is harmless (idempotent), a crash never
leaves half a batch, and a failed load can simply be run again.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Optional

from . import db, schema_drift
from .config import DEFAULT_OVERLAP_SECONDS, TABLES, TableSpec, source_dsn, warehouse_dsn

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class LoadResult:
    table: str
    batch_id: int
    rows_extracted: int
    rows_inserted: int
    watermark_from: Optional[str]
    watermark_to: Optional[str]

    @property
    def rows_ignored(self) -> int:
        return self.rows_extracted - self.rows_inserted


def load_table(spec: TableSpec, run_label: str | None = None, *,
               full: bool = False, overlap_seconds: int = DEFAULT_OVERLAP_SECONDS) -> LoadResult:
    wh = warehouse_dsn()
    batch_id = int(db.scalar(
        wh,
        "INSERT INTO ops.load_audit (table_name, run_label, status) "
        f"VALUES ({db.sql_literal(spec.name)}, {db.sql_literal(run_label or '')}, 'running') "
        "RETURNING batch_id",
    ) or 0)
    try:
        result = _load(spec, batch_id, full, overlap_seconds, run_label)
        log.info("%-12s batch %s: extracted %s, inserted %s, ignored %s duplicates, watermark -> %s",
                 spec.name, batch_id, result.rows_extracted, result.rows_inserted,
                 result.rows_ignored, result.watermark_to)
        return result
    except Exception as exc:
        # Best effort: the original error matters more than a failed bookkeeping write.
        try:
            db.execute(
                wh,
                "UPDATE ops.load_audit SET status = 'failed', finished_at = now(), "
                f"error = {db.sql_literal(str(exc)[:1500])} WHERE batch_id = {batch_id}",
            )
        except Exception:                                    # pragma: no cover
            log.exception("could not record failure for batch %s", batch_id)
        raise


def _load(spec: TableSpec, batch_id: int, full: bool, overlap_seconds: int,
          run_label: str | None) -> LoadResult:
    src, wh = source_dsn(), warehouse_dsn()

    schema_drift.check(spec, run_label)
    columns = schema_drift.raw_columns(spec)

    watermark = None if full else db.scalar(
        wh, f"SELECT high_watermark FROM ops.watermarks WHERE table_name = {db.sql_literal(spec.name)}")
    upper = db.scalar(src, f"SELECT max({spec.cursor}) FROM {spec.source}")
    if upper is None:                                        # empty source table
        _finish(batch_id)
        return LoadResult(spec.name, batch_id, 0, 0, watermark, None)

    where = f"{spec.cursor} <= {db.timestamp_literal(upper)}"
    if watermark is not None:
        where = (f"{spec.cursor} > {db.timestamp_literal(watermark)} - "
                 f"interval '{int(overlap_seconds)} seconds' AND " + where)

    db.execute(wh, f"CREATE UNLOGGED TABLE IF NOT EXISTS {spec.stage} AS "
                   f"SELECT {', '.join(columns)} FROM {spec.raw} WHERE false; "
                   f"TRUNCATE {spec.stage}")
    db.copy_between(src, f"SELECT {', '.join(columns)} FROM {spec.source} WHERE {where}",
                    wh, spec.stage, columns)
    extracted = int(db.scalar(wh, f"SELECT count(*) FROM {spec.stage}") or 0)

    if spec.partition_column and extracted:
        db.scalar(wh, f"SELECT ops.ensure_event_partitions(min({spec.partition_column})::date, "
                      f"max({spec.partition_column})::date) FROM {spec.stage}")

    cols = ", ".join(columns)
    sql = f"""
    WITH ins AS (
        INSERT INTO {spec.raw} ({cols}, _batch_id)
        SELECT {cols}, {batch_id} FROM {spec.stage}
        ON CONFLICT DO NOTHING
        RETURNING 1
    ), wm AS (
        INSERT INTO ops.watermarks (table_name, high_watermark)
        VALUES ({db.sql_literal(spec.name)}, {db.timestamp_literal(upper)})
        ON CONFLICT (table_name) DO UPDATE
            SET high_watermark = GREATEST(ops.watermarks.high_watermark, EXCLUDED.high_watermark),
                updated_at = now()
        RETURNING 1
    )
    UPDATE ops.load_audit
       SET status = 'success', finished_at = now(),
           watermark_from = {db.timestamp_literal(watermark) if watermark else 'NULL'},
           watermark_to = {db.timestamp_literal(upper)},
           rows_extracted = {extracted},
           rows_inserted = (SELECT count(*) FROM ins)
     WHERE batch_id = {batch_id}
    """
    db.execute(wh, sql)
    inserted = int(db.scalar(wh, f"SELECT rows_inserted FROM ops.load_audit WHERE batch_id = {batch_id}") or 0)
    return LoadResult(spec.name, batch_id, extracted, inserted, watermark, upper)


def _finish(batch_id: int) -> None:
    """Close the audit row for a load that found nothing to do."""
    db.execute(
        warehouse_dsn(),
        "UPDATE ops.load_audit SET status = 'success', finished_at = now(), "
        f"rows_extracted = 0, rows_inserted = 0 WHERE batch_id = {batch_id}",
    )


def load_all(run_label: str | None = None, *, tables=TABLES, full: bool = False,
             overlap_seconds: int = DEFAULT_OVERLAP_SECONDS) -> list[LoadResult]:
    return [load_table(t, run_label, full=full, overlap_seconds=overlap_seconds) for t in tables]
