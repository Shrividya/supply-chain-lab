"""Thin wrappers around the `psql` command line client.

Why psql and not a Python driver?
  * `\\copy` streams rows between two servers at the speed of COPY, with no
    Python objects per row. That is the fast path in real pipelines too.
  * nothing to install: this code runs anywhere psql does, including CI.
In a bigger system you would replace this module with a driver, an Airbyte or
Fivetran connector, or Spark; the modules above it would not change.

Everything takes a libpq connection string (postgresql://user:pass@host:port/db).
"""
from __future__ import annotations

import csv
import io
import re
import subprocess
from typing import Iterable, Sequence

NULL = "\\N"          # what a SQL NULL looks like in query() results


class PsqlError(RuntimeError):
    pass


def _redact(text: str) -> str:
    return re.sub(r"(://[^:/@\s]+:)[^@\s]+@", r"\1***@", text)


def _base(dsn: str) -> list[str]:
    return ["psql", dsn, "-X", "-q", "-v", "ON_ERROR_STOP=1"]


def execute(dsn: str, sql: str) -> None:
    """Run one or more statements. Multiple statements run as ONE transaction."""
    r = subprocess.run(_base(dsn) + ["-1", "-f", "-"], input=sql, text=True, capture_output=True)
    if r.returncode != 0:
        raise PsqlError(_redact(r.stderr.strip() or f"psql exited with {r.returncode}"))


def query(dsn: str, sql: str) -> list[list[str]]:
    """Return rows as lists of strings. NULL comes back as db.NULL."""
    r = subprocess.run(
        _base(dsn) + ["--csv", "-P", f"null={NULL}", "-c", sql],
        text=True, capture_output=True,
    )
    if r.returncode != 0:
        raise PsqlError(_redact(r.stderr.strip() or f"psql exited with {r.returncode}"))
    rows = list(csv.reader(io.StringIO(r.stdout)))
    return rows[1:]


def scalar(dsn: str, sql: str) -> str | None:
    rows = query(dsn, sql)
    if not rows:
        return None
    value = rows[0][0]
    return None if value == NULL else value


def copy_between(src_dsn: str, select_sql: str, dst_dsn: str, dst_table: str,
                 columns: Sequence[str]) -> None:
    """Stream the result of `select_sql` on the source into `dst_table`.

    Two psql processes joined by a pipe: nothing is held in Python memory.
    """
    select_one_line = " ".join(select_sql.split())      # \\copy needs a single line
    cols = ", ".join(columns)
    src = subprocess.Popen(
        _base(src_dsn) + ["-c", f"\\copy ({select_one_line}) TO STDOUT WITH (FORMAT csv)"],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    dst = subprocess.Popen(
        _base(dst_dsn) + ["-c", f"\\copy {dst_table} ({cols}) FROM STDIN WITH (FORMAT csv)"],
        stdin=src.stdout, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
    )
    assert src.stdout is not None and src.stderr is not None
    src.stdout.close()                                   # let dst see EOF / SIGPIPE
    _, dst_err = dst.communicate()
    src_err = src.stderr.read()
    src.stderr.close()
    src.wait()
    if src.returncode != 0 or dst.returncode != 0:
        msg = (src_err + b"\n" + dst_err).decode(errors="replace").strip()
        raise PsqlError(_redact(msg) or "copy failed")


def sql_literal(value: str) -> str:
    """Quote a string for use inside SQL. Only for values we generated ourselves."""
    return "'" + value.replace("'", "''") + "'"


_TIMESTAMP = re.compile(r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d+)?([+-]\d{2}(:?\d{2})?)?$")


def timestamp_literal(value: str) -> str:
    """A timestamptz literal built from a value that came out of the database."""
    if not _TIMESTAMP.match(value):
        raise ValueError(f"not a timestamp: {value!r}")
    return f"timestamptz '{value}'"


def in_list(values: Iterable[str]) -> str:
    return ", ".join(sql_literal(v) for v in values)
