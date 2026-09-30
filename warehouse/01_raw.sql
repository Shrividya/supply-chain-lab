-- RAW layer: what the source looked like, as it was extracted.
--
-- Design choice: raw is an APPEND-ONLY CHANGE LOG, not a mirror. Every version
-- of a row that the pipeline observed is kept, keyed by (primary key,
-- updated_at). Consequences:
--   + re-running a load is harmless (duplicates are ignored)
--   + history exists, so customers' past countries can be reconstructed (SCD2)
--   + a bug in a downstream model never requires re-extracting from the source
--   - storage grows with change volume
--   - "the current row" needs a de-duplication step (staging does it)
--   - polling can miss intermediate states: if a row changes twice between two
--     extractions, only the last version is seen. Only CDC avoids that.
--
-- Every table also carries lineage columns: _batch_id (which load brought it
-- in) and _loaded_at.
SET timezone TO 'UTC';
SET ROLE loader;

CREATE TABLE raw.customers (
    customer_id          integer      NOT NULL,
    full_name            text         NOT NULL,
    email                text         NOT NULL,
    country              text         NOT NULL,
    segment              text         NOT NULL,
    acquisition_channel  text         NOT NULL,
    signup_date          date         NOT NULL,
    created_at           timestamptz  NOT NULL,
    updated_at           timestamptz  NOT NULL,
    _batch_id            bigint       NOT NULL,
    _loaded_at           timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (customer_id, updated_at)
);

CREATE TABLE raw.products (
    product_id    integer        NOT NULL,
    product_name  text           NOT NULL,
    category      text           NOT NULL,
    unit_price    numeric(10,2)  NOT NULL,
    unit_cost     numeric(10,2)  NOT NULL,
    created_at    timestamptz    NOT NULL,
    updated_at    timestamptz    NOT NULL,
    _batch_id     bigint         NOT NULL,
    _loaded_at    timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (product_id, updated_at)
);

CREATE TABLE raw.orders (
    order_id      integer        NOT NULL,
    customer_id   integer        NOT NULL,
    order_ts      timestamptz    NOT NULL,
    status        text           NOT NULL,
    channel       text,
    discount_pct  numeric(4,2)   NOT NULL,
    created_at    timestamptz    NOT NULL,
    updated_at    timestamptz    NOT NULL,
    _batch_id     bigint         NOT NULL,
    _loaded_at    timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (order_id, updated_at)
);

CREATE TABLE raw.order_items (
    order_item_id  integer        NOT NULL,
    order_id       integer        NOT NULL,
    product_id     integer        NOT NULL,
    quantity       integer        NOT NULL,
    unit_price     numeric(10,2)  NOT NULL,
    created_at     timestamptz    NOT NULL,
    updated_at     timestamptz    NOT NULL,
    _batch_id      bigint         NOT NULL,
    _loaded_at     timestamptz    NOT NULL DEFAULT now(),
    PRIMARY KEY (order_item_id, updated_at)
);

-- Events are immutable and large, so they are partitioned by month. Postgres
-- requires the partition key inside every unique constraint, which is why the
-- key is (event_id, event_ts) and not event_id alone. A DEFAULT partition
-- catches rows for months nobody created; a data quality check alerts when it
-- is not empty.
CREATE TABLE raw.web_events (
    event_id     bigint       NOT NULL,
    session_id   bigint       NOT NULL,
    customer_id  integer,
    event_ts     timestamptz  NOT NULL,
    event_type   text         NOT NULL,
    device       text         NOT NULL,
    ingested_at  timestamptz  NOT NULL,
    _batch_id    bigint       NOT NULL,
    _loaded_at   timestamptz  NOT NULL DEFAULT now(),
    PRIMARY KEY (event_id, event_ts)
) PARTITION BY RANGE (event_ts);

CREATE TABLE raw.web_events_default PARTITION OF raw.web_events DEFAULT;

CREATE INDEX ix_raw_events_session ON raw.web_events (session_id);
CREATE INDEX ix_raw_events_ingested ON raw.web_events (ingested_at);

CREATE OR REPLACE FUNCTION ops.ensure_event_partitions(p_from date, p_to date)
RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    m       date := date_trunc('month', p_from)::date;
    part    text;
    created integer := 0;
BEGIN
    WHILE m <= p_to LOOP
        part := format('web_events_%s', to_char(m, 'YYYY_MM'));
        IF to_regclass(format('raw.%I', part)) IS NULL THEN
            EXECUTE format(
                'CREATE TABLE raw.%I PARTITION OF raw.web_events FOR VALUES FROM (%L) TO (%L)',
                part, m::timestamptz, (m + interval '1 month')::timestamptz);
            created := created + 1;
        END IF;
        m := (m + interval '1 month')::date;
    END LOOP;
    RETURN created;
END;
$$;

RESET ROLE;
