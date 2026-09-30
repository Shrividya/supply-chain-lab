"""What to load, and how. Connection strings come from the environment."""
from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

# Lineage columns the loader adds to every raw table. They are never read from
# the source, and the drift check ignores them.
META_COLUMNS = ("_batch_id", "_loaded_at")

# Re-read this much history on every run. Rows that commit late (a transaction
# that started before the last extraction but finished after it) carry an older
# cursor value; without overlap they would be skipped forever. Duplicates from
# the overlap are discarded by the raw table's primary key.
DEFAULT_OVERLAP_SECONDS = 3600


@dataclass(frozen=True)
class TableSpec:
    name: str
    pk: tuple[str, ...]
    cursor: str                                  # column that only ever moves forward
    source_schema: str = "app"
    raw_schema: str = "raw"
    partition_column: Optional[str] = None       # raw table is range-partitioned by month on this

    @property
    def source(self) -> str:
        return f"{self.source_schema}.{self.name}"

    @property
    def raw(self) -> str:
        return f"{self.raw_schema}.{self.name}"

    @property
    def stage(self) -> str:
        return f"{self.raw_schema}._stg_{self.name}"


TABLES: tuple[TableSpec, ...] = (
    TableSpec("customers",   ("customer_id",),   "updated_at"),
    TableSpec("products",    ("product_id",),    "updated_at"),
    TableSpec("orders",      ("order_id",),      "updated_at"),
    TableSpec("order_items", ("order_item_id",), "updated_at"),
    # Immutable and append-only, so the cursor is ingestion time, not event time.
    # Late-arriving events have an old event_ts but a new ingested_at.
    TableSpec("web_events",  ("event_id",),      "ingested_at", partition_column="event_ts"),
)

TABLES_BY_NAME = {t.name: t for t in TABLES}


def source_dsn() -> str:
    return _env("SOURCE_DSN")


def warehouse_dsn() -> str:
    return _env("WAREHOUSE_DSN")


def source_admin_dsn() -> str:
    """Only the simulator uses this; the pipeline itself never writes to the source."""
    return _env("SOURCE_ADMIN_DSN")


def _env(name: str) -> str:
    try:
        return os.environ[name]
    except KeyError:
        raise RuntimeError(f"environment variable {name} is not set") from None
