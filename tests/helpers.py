"""Shared test fixtures: real databases, no mocks.

Set TEST_PG_ADMIN_DSN to a superuser connection string for a scratch Postgres
server (the tests create and drop their own databases), for example:

    TEST_PG_ADMIN_DSN=postgresql://postgres@localhost:5432/postgres

The source database is built once per test run (schema + seed + a few simulated
days) and cloned with CREATE DATABASE ... TEMPLATE for each test, which takes a
fraction of a second. The warehouse is rebuilt from the real init scripts for
every test, so the tests exercise exactly what ships.
"""
from __future__ import annotations

import atexit
import os
import pathlib
import re
import subprocess
import sys
import unittest
import uuid

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from pipeline import db  # noqa: E402

ADMIN_DSN = os.environ.get("TEST_PG_ADMIN_DSN", "postgresql://postgres@localhost:5432/postgres")

_SUFFIX = uuid.uuid4().hex[:8]
_created: list[str] = []
_template_name = f"pf_src_template_{_SUFFIX}"
_loaded_src_template = f"pf_src_loaded_{_SUFFIX}"     # source + warehouse pair that has been loaded once
_loaded_wh_template = f"pf_wh_loaded_{_SUFFIX}"


def _with_db(dsn: str, dbname: str) -> str:
    """Swap the database name inside a postgresql:// URI, keeping user and query string."""
    m = re.match(r"^(postgresql://[^/]*/)([^?]*)(\?.*)?$", dsn)
    if not m:
        raise ValueError(f"unsupported DSN shape: {dsn}")
    return f"{m.group(1)}{dbname}{m.group(3) or ''}"


def _as_user(dsn: str, user: str, password: str) -> str:
    m = re.match(r"^postgresql://[^@/]*(@?)([^/]*)/(.*)$", dsn)
    if not m:
        raise ValueError(f"unsupported DSN shape: {dsn}")
    hostpart = m.group(2)
    return f"postgresql://{user}:{password}@{hostpart}/{m.group(3)}"


def _admin(sql: str) -> None:
    """CREATE/DROP DATABASE cannot run inside a transaction, so avoid db.execute (which wraps one)."""
    db.query(ADMIN_DSN, sql)


def _drop_all() -> None:
    for name in _created:
        try:
            _admin(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
        except Exception:
            pass


atexit.register(_drop_all)


def _run_file(dsn: str, path: pathlib.Path) -> None:
    r = subprocess.run(["psql", dsn, "-X", "-q", "-v", "ON_ERROR_STOP=1", "-f", str(path)],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"{path.name} failed: {r.stderr.strip()}")


def _new_db(prefix: str, template: str | None = None) -> str:
    name = f"{prefix}_{uuid.uuid4().hex[:8]}"
    tpl = f' TEMPLATE "{template}"' if template else ""
    _admin(f'CREATE DATABASE "{name}"{tpl}')
    _created.append(name)
    return name


def _ensure_template() -> None:
    if _template_name in _created:
        return
    _admin(f'CREATE DATABASE "{_template_name}"')
    _created.append(_template_name)
    dsn = _with_db(ADMIN_DSN, _template_name)
    for f in ("00_schema.sql", "01_seed.sql", "02_simulator.sql"):
        _run_file(dsn, ROOT / "source" / f)
    for _ in range(3):
        db.scalar(dsn, "SELECT sim.advance_day()")
    db.execute(dsn, "GRANT SELECT ON ALL TABLES IN SCHEMA app TO pipeline_ro")


def _init_warehouse(dsn: str) -> None:
    for f in ("00_roles_schemas.sql", "01_raw.sql", "02_ops.sql"):
        _run_file(dsn, ROOT / "warehouse" / f)


def _ensure_loaded_pair() -> None:
    """One source + warehouse pair that has already had a full initial load. Cloned per test."""
    if _loaded_wh_template in _created:
        return
    _ensure_template()
    _admin(f'CREATE DATABASE "{_loaded_src_template}" TEMPLATE "{_template_name}"')
    _created.append(_loaded_src_template)
    _admin(f'CREATE DATABASE "{_loaded_wh_template}"')
    _created.append(_loaded_wh_template)
    wh_admin = _with_db(ADMIN_DSN, _loaded_wh_template)
    _init_warehouse(wh_admin)
    env = {**os.environ,
           "SOURCE_DSN": _as_user(_with_db(ADMIN_DSN, _loaded_src_template), "pipeline_ro", "pipeline_ro"),
           "WAREHOUSE_DSN": _as_user(wh_admin, "loader", "loader")}
    r = subprocess.run([sys.executable, "-m", "pipeline", "load", "--label", "template"],
                       cwd=ROOT, env=env, capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(f"initial load for the test template failed:\n{r.stderr}")


class PlatformTestCase(unittest.TestCase):
    """A fresh source and a fresh warehouse per test.

    preload = False (default): empty warehouse, so the test can watch the first load.
    preload = True: a warehouse that already holds one complete load of its source.
    """

    preload = False

    source_admin: str
    source: str
    warehouse_admin: str
    warehouse: str

    def setUp(self) -> None:
        if self.preload:
            _ensure_loaded_pair()
            src_name = _new_db("pf_src", template=_loaded_src_template)
            wh_name = _new_db("pf_wh", template=_loaded_wh_template)
        else:
            _ensure_template()
            src_name = _new_db("pf_src", template=_template_name)
            wh_name = _new_db("pf_wh")
        self.source_admin = _with_db(ADMIN_DSN, src_name)
        self.warehouse_admin = _with_db(ADMIN_DSN, wh_name)
        if not self.preload:
            _init_warehouse(self.warehouse_admin)
        self.source = _as_user(self.source_admin, "pipeline_ro", "pipeline_ro")
        self.warehouse = _as_user(self.warehouse_admin, "loader", "loader")
        self.dbt = _as_user(self.warehouse_admin, "dbt_user", "dbt_user")
        self.bi = _as_user(self.warehouse_admin, "superset_ro", "superset_ro")
        os.environ["SOURCE_DSN"] = self.source
        os.environ["WAREHOUSE_DSN"] = self.warehouse
        os.environ["SOURCE_ADMIN_DSN"] = self.source_admin

    def tearDown(self) -> None:
        for dsn in (self.source_admin, self.warehouse_admin):
            name = re.search(r"/([^/?]+)(\?|$)", dsn).group(1)
            try:
                _admin(f'DROP DATABASE IF EXISTS "{name}" WITH (FORCE)')
            except Exception:
                pass

    def wh(self, sql: str) -> str | None:
        return db.scalar(self.warehouse, sql)

    def src(self, sql: str) -> str | None:
        return db.scalar(self.source_admin, sql)

    def advance(self, days: int = 1) -> None:
        for _ in range(days):
            db.scalar(self.source_admin, "SELECT sim.advance_day()")
