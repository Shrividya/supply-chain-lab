-- SOURCE SYSTEM: the shop's application database (the OLTP Postgres
-- behind a web store). The data platform never writes here.
--
-- Rows are mutable: an order is created "pending" and later becomes
-- "completed" or "cancelled", or is refunded days afterwards. Customers move
-- country. That is exactly what makes ingestion hard, and it is what the
-- warehouse side has to cope with.
--
-- Known gaps:
--   * no foreign keys (many real systems have none, so orphans happen)
--   * hard deletes are NOT modelled, see docs/ARCHITECTURE.md ("what this cannot see")
--   * every mutable table carries updated_at, which is what incremental
--     extraction relies on

DO $$
BEGIN
    EXECUTE format('ALTER DATABASE %I SET timezone TO ''UTC''', current_database());
END $$;
SET timezone TO 'UTC';

CREATE SCHEMA IF NOT EXISTS app;
CREATE SCHEMA IF NOT EXISTS sim;

CREATE TABLE app.customers (
    customer_id          integer      PRIMARY KEY,
    full_name            text         NOT NULL,
    email                text         NOT NULL,
    country              text         NOT NULL,
    segment              text         NOT NULL,
    acquisition_channel  text         NOT NULL,
    signup_date          date         NOT NULL,
    created_at           timestamptz  NOT NULL,
    updated_at           timestamptz  NOT NULL
);

CREATE TABLE app.products (
    product_id    integer        PRIMARY KEY,
    product_name  text           NOT NULL,
    category      text           NOT NULL,
    unit_price    numeric(10,2)  NOT NULL,
    unit_cost     numeric(10,2)  NOT NULL,
    created_at    timestamptz    NOT NULL,
    updated_at    timestamptz    NOT NULL
);

CREATE TABLE app.orders (
    order_id      integer        PRIMARY KEY,
    customer_id   integer        NOT NULL,          -- no FK on purpose
    order_ts      timestamptz    NOT NULL,
    status        text           NOT NULL,          -- pending | completed | cancelled | refunded
    channel       text,                             -- nullable: the app forgets sometimes
    discount_pct  numeric(4,2)   NOT NULL DEFAULT 0,
    created_at    timestamptz    NOT NULL,
    updated_at    timestamptz    NOT NULL
);

CREATE TABLE app.order_items (
    order_item_id  integer        PRIMARY KEY,
    order_id       integer        NOT NULL,
    product_id     integer        NOT NULL,
    quantity       integer        NOT NULL,
    unit_price     numeric(10,2)  NOT NULL,
    created_at     timestamptz    NOT NULL,
    updated_at     timestamptz    NOT NULL
);

-- Append-only clickstream. ingested_at is when the row reached this database,
-- which can be later than event_ts (mobile clients batch and retry).
CREATE TABLE app.web_events (
    event_id     bigint       PRIMARY KEY,
    session_id   bigint       NOT NULL,
    customer_id  integer,
    event_ts     timestamptz  NOT NULL,
    event_type   text         NOT NULL,
    device       text         NOT NULL,
    ingested_at  timestamptz  NOT NULL
);

CREATE INDEX ix_customers_updated  ON app.customers   (updated_at);
CREATE INDEX ix_products_updated   ON app.products    (updated_at);
CREATE INDEX ix_orders_updated     ON app.orders      (updated_at);
CREATE INDEX ix_items_updated      ON app.order_items (updated_at);
CREATE INDEX ix_events_ingested    ON app.web_events  (ingested_at);

-- The account the pipeline connects with. Read-only, on purpose.
DO $$
BEGIN
    IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'pipeline_ro') THEN
        CREATE ROLE pipeline_ro LOGIN PASSWORD 'pipeline_ro';
    END IF;
END $$;
GRANT USAGE ON SCHEMA app TO pipeline_ro;
GRANT SELECT ON ALL TABLES IN SCHEMA app TO pipeline_ro;
ALTER DEFAULT PRIVILEGES IN SCHEMA app GRANT SELECT ON TABLES TO pipeline_ro;
