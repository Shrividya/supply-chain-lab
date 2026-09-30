#!/usr/bin/env python3
"""mini_dbt: a tiny stand-in for `dbt build`, used ONLY to check this repo's SQL
in places where dbt itself cannot be installed.

It is NOT dbt. It understands just the subset this project uses:
  Jinja      {{ ref() }} {{ source() }} {{ this }} {{ var() }} {{ config() }}
             {% if is_incremental() %}...{% endif %}   (anything else is an error)
  models     view, table, incremental (delete+insert on unique_key)
  tests      unique, not_null, accepted_values, relationships, singular tests
  contracts  declared column types are compared with what was built

What it does NOT do: real dbt compilation, packages, snapshots, seeds, hooks,
source freshness, the dbt DAG runner, or dbt's exact adapter behaviour. Passing
here means the SQL is valid and the logic holds on Postgres. It does not mean
`dbt build` will succeed, so run the real thing when you can.

    python tools/mini_dbt.py build --dsn postgresql://dbt_user:dbt_user@localhost/warehouse
    python tools/mini_dbt.py build --dsn ... --full-refresh --vars funnel_lookback_days=3
    python tools/mini_dbt.py test  --dsn ...
"""
from __future__ import annotations

import argparse
import ast
import pathlib
import re
import sys

import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
from pipeline import db  # noqa: E402

DBT = ROOT / "dbt"


class MiniDbtError(RuntimeError):
    pass


# project
class Model:
    def __init__(self, path: pathlib.Path, project_cfg: dict):
        self.path = path
        self.name = path.stem
        self.raw = path.read_text()
        rel = path.relative_to(DBT / "models").parts[:-1]
        cfg = dict(project_cfg)
        node = project_cfg.get("models", {}).get("shop", {})
        for part in rel:
            node = node.get(part, {})
            cfg.update({k.lstrip("+"): v for k, v in node.items() if k.startswith("+")})
        cfg.update(_parse_config(self.raw))
        self.config = {k: v for k, v in cfg.items() if k not in ("models", "vars", "name", "version")}
        self.materialized = self.config.get("materialized", "view")
        self.schema = self.config.get("schema", "staging")
        self.refs = re.findall(r"ref\(\s*'([^']+)'\s*\)", self.raw)

    @property
    def fqn(self) -> str:
        return f"{self.schema}.{self.name}"


def _parse_config(text: str) -> dict:
    m = re.search(r"\{\{\s*config\((.*?)\)\s*\}\}", text, re.S)
    if not m:
        return {}
    # Jinja writes true/false; Python wants True/False.
    args = re.sub(r"\btrue\b", "True", re.sub(r"\bfalse\b", "False", m.group(1)))
    return eval(compile(ast.parse(f"dict({args})", mode="eval"), "<config>", "eval"),  # noqa: S307
                {"__builtins__": {}, "dict": dict})


def load_project():
    cfg = yaml.safe_load((DBT / "dbt_project.yml").read_text())
    models = [Model(p, cfg) for p in sorted((DBT / "models").rglob("*.sql"))]
    by_name = {m.name: m for m in models}
    return cfg, models, by_name


def topo(models: list[Model], by_name: dict[str, Model]) -> list[Model]:
    done, order = set(), []

    def visit(m: Model, stack=()):
        if m.name in done:
            return
        if m.name in stack:
            raise MiniDbtError(f"cycle through {m.name}")
        for r in m.refs:
            if r not in by_name:
                raise MiniDbtError(f"{m.name} refs unknown model {r}")
            visit(by_name[r], stack + (m.name,))
        done.add(m.name)
        order.append(m)

    for m in models:
        visit(m)
    return order


# rendering
def render(text: str, by_name: dict[str, Model], *, this: str | None, incremental: bool, variables: dict) -> str:
    text = re.sub(r"\{#.*?#\}", "", text, flags=re.S)
    text = re.sub(r"\{\{\s*config\(.*?\)\s*\}\}", "", text, flags=re.S)

    def block(m: re.Match) -> str:
        return m.group(1) if incremental else ""
    text = re.sub(r"\{%\s*if is_incremental\(\)\s*%\}(.*?)\{%\s*endif\s*%\}", block, text, flags=re.S)

    def ref(m):
        name = m.group(1)
        if name not in by_name:
            raise MiniDbtError(f"unknown ref {name}")
        return by_name[name].fqn

    text = re.sub(r"\{\{\s*ref\(\s*'([^']+)'\s*\)\s*\}\}", ref, text)
    text = re.sub(r"\{\{\s*source\(\s*'([^']+)'\s*,\s*'([^']+)'\s*\)\s*\}\}", r"\1.\2", text)
    text = re.sub(r"\{\{\s*this\s*\}\}", lambda _: this or "", text)

    def var(m):
        name, default = m.group(1), m.group(2)
        return str(variables.get(name, default))
    text = re.sub(r"\{\{\s*var\(\s*'([^']+)'\s*(?:,\s*([^)]+?))?\s*\)\s*\}\}", var, text)

    leftover = re.search(r"\{[{%].*?[}%]\}", text, re.S)
    if leftover:
        raise MiniDbtError(f"unsupported Jinja left in the SQL: {leftover.group(0)[:80]}")
    return text


# build
def exists(dsn: str, fqn: str) -> bool:
    return db.scalar(dsn, f"SELECT to_regclass('{fqn}') IS NOT NULL") == "t"


def build_model(dsn: str, m: Model, by_name, variables, full_refresh: bool) -> str:
    fqn = m.fqn
    if m.materialized == "view":
        sql = render(m.raw, by_name, this=fqn, incremental=False, variables=variables)
        db.execute(dsn, f"CREATE OR REPLACE VIEW {fqn} AS\n{sql}")
        return "view"
    if m.materialized == "table":
        sql = render(m.raw, by_name, this=fqn, incremental=False, variables=variables)
        db.execute(dsn, f"DROP TABLE IF EXISTS {fqn} CASCADE; CREATE TABLE {fqn} AS\n{sql}")
        return "table"
    if m.materialized == "incremental":
        first = full_refresh or not exists(dsn, fqn)
        sql = render(m.raw, by_name, this=fqn, incremental=not first, variables=variables)
        if first:
            db.execute(dsn, f"DROP TABLE IF EXISTS {fqn} CASCADE; CREATE TABLE {fqn} AS\n{sql}")
            return "incremental (created)"
        key = m.config["unique_key"]
        tmp = f"{m.name}__tmp"
        db.execute(dsn, f"DROP TABLE IF EXISTS {tmp}; CREATE TEMP TABLE {tmp} AS\n{sql};\n"
                        f"DELETE FROM {fqn} WHERE {key} IN (SELECT {key} FROM {tmp});\n"
                        f"INSERT INTO {fqn} SELECT * FROM {tmp};")
        return "incremental (merged)"
    raise MiniDbtError(f"{m.name}: unsupported materialization {m.materialized}")


def check_contract(dsn: str, m: Model, model_yaml: dict) -> list[str]:
    if not (model_yaml.get("config", {}).get("contract", {}) or {}).get("enforced") and \
            not (m.config.get("contract") or {}).get("enforced"):
        return []
    declared = [(c["name"], c.get("data_type")) for c in model_yaml.get("columns", [])]
    schema, name = m.fqn.split(".")
    actual = [(r[0], r[1]) for r in db.query(
        dsn, "SELECT column_name, data_type FROM information_schema.columns "
             f"WHERE table_schema = '{schema}' AND table_name = '{name}' ORDER BY ordinal_position")]
    problems = []
    if [d[0] for d in declared] != [a[0] for a in actual]:
        problems.append(f"{m.name}: contract columns {[d[0] for d in declared]} != built {[a[0] for a in actual]}")
    else:
        for (col, want), (_, got) in zip(declared, actual):
            if want != got:
                problems.append(f"{m.name}.{col}: contract says {want}, built {got}")
    return problems


# tests
def schema_yaml() -> dict[str, dict]:
    out: dict[str, dict] = {}
    for p in (DBT / "models").rglob("*.yml"):
        doc = yaml.safe_load(p.read_text()) or {}
        for model in doc.get("models", []):
            out[model["name"]] = model
    return out


def generic_tests(models_yaml: dict[str, dict], by_name: dict[str, Model]):
    """Yield (label, severity, sql) for every column test declared in YAML."""
    for mname, spec in models_yaml.items():
        if mname not in by_name:
            continue
        fqn = by_name[mname].fqn
        for col in spec.get("columns", []):
            for t in col.get("data_tests", col.get("tests", [])):
                name, cfg = (t, {}) if isinstance(t, str) else next(iter(t.items()))
                args = (cfg or {}).get("arguments", cfg or {})
                severity = ((cfg or {}).get("config") or {}).get("severity", "error")
                c = col["name"]
                label = f"{name}({mname}.{c})"
                if name == "unique":
                    sql = f"SELECT {c} FROM {fqn} WHERE {c} IS NOT NULL GROUP BY {c} HAVING count(*) > 1"
                elif name == "not_null":
                    sql = f"SELECT * FROM {fqn} WHERE {c} IS NULL"
                elif name == "accepted_values":
                    vals = ", ".join("'" + str(v).replace("'", "''") + "'" for v in args["values"])
                    sql = f"SELECT {c} FROM {fqn} WHERE {c} IS NOT NULL AND {c} NOT IN ({vals})"
                elif name == "relationships":
                    target = re.match(r"ref\('([^']+)'\)", args["to"]).group(1)
                    sql = (f"SELECT child.{c} FROM {fqn} child LEFT JOIN {by_name[target].fqn} parent "
                           f"ON child.{c} = parent.{args['field']} "
                           f"WHERE child.{c} IS NOT NULL AND parent.{args['field']} IS NULL")
                else:
                    raise MiniDbtError(f"unsupported generic test {name}")
                yield label, severity, sql


def run_tests(dsn: str, by_name, variables) -> tuple[int, int, int]:
    passed = failed = warned = 0
    cases = list(generic_tests(schema_yaml(), by_name))
    for p in sorted((DBT / "tests").glob("*.sql")):
        sql = render(p.read_text(), by_name, this=None, incremental=False, variables=variables)
        cases.append((f"singular({p.stem})", "error", sql))
    for label, severity, sql in cases:
        n = int(db.scalar(dsn, f"SELECT count(*) FROM ({sql.rstrip().rstrip(';')}) t") or 0)
        if n == 0:
            passed += 1
            print(f"  PASS  {label}")
        elif severity == "warn":
            warned += 1
            print(f"  WARN  {label}: {n} rows")
        else:
            failed += 1
            print(f"  FAIL  {label}: {n} rows")
    return passed, warned, failed


# cli
def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("command", choices=["build", "run", "test"])
    ap.add_argument("--dsn", required=True)
    ap.add_argument("--full-refresh", action="store_true")
    ap.add_argument("--vars", default="", help="a=1,b=2")
    args = ap.parse_args(argv)
    variables = {k: v for k, v in (kv.split("=", 1) for kv in args.vars.split(",") if kv)}

    cfg, models, by_name = load_project()
    variables = {**cfg.get("vars", {}), **variables}
    order = topo(models, by_name)
    problems: list[str] = []

    if args.command in ("build", "run"):
        yml = schema_yaml()
        for m in order:
            kind = build_model(args.dsn, m, by_name, variables, args.full_refresh)
            print(f"  OK    {m.fqn:<34} {kind}")
            problems += check_contract(args.dsn, m, yml.get(m.name, {}))
    failed = 0
    if args.command in ("build", "test"):
        passed, warned, failed = run_tests(args.dsn, by_name, variables)
        print(f"\n{passed} passed, {warned} warned, {failed} failed")
    for p in problems:
        print(f"  CONTRACT  {p}")
    return 1 if (failed or problems) else 0


if __name__ == "__main__":
    sys.exit(main())
