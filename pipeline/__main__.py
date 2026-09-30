"""Command line entry point:  python -m pipeline <command>

    load [--tables a,b] [--full] [--label L] [--overlap-seconds N]
    reconcile [--label L] [--no-fail]
    status
    simulate [--days N]        dev only: advance the fake source system

Exit codes: 0 ok, 1 data problem (drift or quality), 2 anything else.
"""
from __future__ import annotations

import argparse
import logging
import sys

from . import db, extract_load, reconcile, schema_drift
from .config import DEFAULT_OVERLAP_SECONDS, TABLES, TABLES_BY_NAME, source_admin_dsn, warehouse_dsn


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="pipeline")
    sub = p.add_subparsers(dest="command", required=True)

    ld = sub.add_parser("load", help="incremental load into raw")
    ld.add_argument("--tables", help="comma separated; default all")
    ld.add_argument("--full", action="store_true", help="ignore the watermark and re-read everything")
    ld.add_argument("--label", default="", help="run label written to the audit tables")
    ld.add_argument("--overlap-seconds", type=int, default=DEFAULT_OVERLAP_SECONDS)

    rc = sub.add_parser("reconcile", help="run data quality checks")
    rc.add_argument("--label", default="")
    rc.add_argument("--no-fail", action="store_true", help="record results but always exit 0")

    sub.add_parser("status", help="show watermarks and the latest load per table")

    sm = sub.add_parser("simulate", help="advance the fake source system (dev only)")
    sm.add_argument("--days", type=int, default=1)

    args = p.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    try:
        if args.command == "load":
            names = args.tables.split(",") if args.tables else [t.name for t in TABLES]
            unknown = [n for n in names if n not in TABLES_BY_NAME]
            if unknown:
                p.error(f"unknown table(s): {', '.join(unknown)}")
            extract_load.load_all(args.label, tables=[TABLES_BY_NAME[n] for n in names],
                                  full=args.full, overlap_seconds=args.overlap_seconds)
        elif args.command == "reconcile":
            reconcile.run_checks(args.label, raise_on_fail=not args.no_fail)
        elif args.command == "status":
            _status()
        elif args.command == "simulate":
            for _ in range(args.days):
                day = db.scalar(source_admin_dsn(), "SELECT sim.advance_day()")
                logging.info("source advanced to %s", day)
        return 0
    except (schema_drift.SchemaDriftError, reconcile.DataQualityError) as exc:
        logging.error("%s: %s", type(exc).__name__, exc)
        return 1
    except Exception as exc:                                          # noqa: BLE001
        logging.error("%s: %s", type(exc).__name__, exc)
        return 2


def _status() -> None:
    rows = db.query(warehouse_dsn(), """
        SELECT w.table_name, w.high_watermark,
               (SELECT status FROM ops.load_audit a WHERE a.table_name = w.table_name
                ORDER BY batch_id DESC LIMIT 1),
               (SELECT rows_inserted FROM ops.load_audit a WHERE a.table_name = w.table_name
                ORDER BY batch_id DESC LIMIT 1)
        FROM ops.watermarks w ORDER BY 1""")
    print(f"{'table':<14}{'watermark':<32}{'last load':<10}{'rows inserted'}")
    for table, wm, status, inserted in rows:
        print(f"{table:<14}{wm:<32}{status:<10}{inserted}")
    if not rows:
        print("(nothing loaded yet)")


if __name__ == "__main__":
    sys.exit(main())
