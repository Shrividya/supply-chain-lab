"""Detect schema changes in the source before they corrupt (or silently lose) data.

Policy, and the reasoning behind it:
  column added in the source    WARN   the loader only copies columns raw already
                                       has, so nothing breaks. Someone should
                                       decide whether the new column matters.
  column missing from source    ERROR  it was renamed or dropped; the extract
                                       would fail, or worse, load nulls.
  column type changed           ERROR  values may no longer fit raw's type.
Errors stop the load for that table BEFORE anything is written, and the
watermark does not move, so the next run after a fix picks up where it left off.
"""
from __future__ import annotations

from dataclasses import dataclass

from . import db
from .config import META_COLUMNS, TableSpec, source_dsn, warehouse_dsn


class SchemaDriftError(RuntimeError):
    pass


@dataclass(frozen=True)
class Drift:
    table: str
    kind: str          # column_added | column_missing | type_changed
    column: str
    detail: str
    severity: str      # warn | error


def _columns(dsn: str, schema: str, table: str) -> dict[str, tuple[int, str]]:
    rows = db.query(
        dsn,
        "SELECT column_name, ordinal_position, data_type FROM information_schema.columns "
        f"WHERE table_schema = {db.sql_literal(schema)} AND table_name = {db.sql_literal(table)} "
        "ORDER BY ordinal_position",
    )
    return {name: (int(pos), dtype) for name, pos, dtype in rows}


def raw_columns(spec: TableSpec) -> list[str]:
    """The columns the loader copies: whatever raw defines, minus lineage columns."""
    cols = _columns(warehouse_dsn(), spec.raw_schema, spec.name)
    return [c for c, _ in sorted(cols.items(), key=lambda kv: kv[1][0]) if c not in META_COLUMNS]


def detect(spec: TableSpec) -> list[Drift]:
    src = _columns(source_dsn(), spec.source_schema, spec.name)
    raw = {c: v for c, v in _columns(warehouse_dsn(), spec.raw_schema, spec.name).items()
           if c not in META_COLUMNS}
    found: list[Drift] = []
    for col in sorted(set(src) - set(raw)):
        found.append(Drift(spec.name, "column_added", col,
                           f"new source column of type {src[col][1]}; ignored by the loader", "warn"))
    for col in sorted(set(raw) - set(src)):
        found.append(Drift(spec.name, "column_missing", col,
                           "raw expects this column but the source no longer has it "
                           "(renamed or dropped?)", "error"))
    for col in sorted(set(raw) & set(src)):
        if raw[col][1] != src[col][1]:
            found.append(Drift(spec.name, "type_changed", col,
                               f"source is {src[col][1]}, raw is {raw[col][1]}", "error"))
    return found


def record(drifts: list[Drift], run_label: str | None) -> None:
    if not drifts:
        return
    values = ", ".join(
        f"({db.sql_literal(run_label or '')}, {db.sql_literal(d.table)}, {db.sql_literal(d.kind)}, "
        f"{db.sql_literal(d.column)}, {db.sql_literal(d.detail)}, {db.sql_literal(d.severity)})"
        for d in drifts
    )
    db.execute(
        warehouse_dsn(),
        "INSERT INTO ops.schema_drift (run_label, table_name, kind, column_name, detail, severity) "
        f"VALUES {values}",
    )


def check(spec: TableSpec, run_label: str | None = None) -> list[Drift]:
    """Detect, record, and raise SchemaDriftError if any drift is an error."""
    drifts = detect(spec)
    record(drifts, run_label)
    errors = [d for d in drifts if d.severity == "error"]
    if errors:
        summary = "; ".join(f"{d.table}.{d.column}: {d.kind} ({d.detail})" for d in errors)
        raise SchemaDriftError(summary)
    return drifts
