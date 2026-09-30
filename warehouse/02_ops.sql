-- OPS: the platform's own bookkeeping. These tables are what make a pipeline
-- operable: you can answer "what loaded, when, how much, and did it pass?"
-- without reading logs.
SET timezone TO 'UTC';
SET ROLE loader;

-- Where each source table was read up to. It only ever moves forward, and it
-- moves in the SAME transaction that inserts the data, so a crash can never
-- leave "data loaded but watermark not advanced" or the reverse.
CREATE TABLE ops.watermarks (
    table_name      text         PRIMARY KEY,
    high_watermark  timestamptz  NOT NULL,
    updated_at      timestamptz  NOT NULL DEFAULT now()
);

-- One row per load attempt.
CREATE TABLE ops.load_audit (
    batch_id                bigserial    PRIMARY KEY,
    table_name              text         NOT NULL,
    run_label               text,
    started_at              timestamptz  NOT NULL DEFAULT now(),
    finished_at             timestamptz,
    status                  text         NOT NULL CHECK (status IN ('running', 'success', 'failed')),
    watermark_from          timestamptz,
    watermark_to            timestamptz,
    rows_extracted          bigint,
    rows_inserted           bigint,
    duration_seconds        numeric GENERATED ALWAYS AS (extract(epoch FROM finished_at - started_at)) STORED,
    error                   text
);
CREATE INDEX ix_load_audit_table ON ops.load_audit (table_name, started_at DESC);

-- Differences between the source's columns and raw's columns.
CREATE TABLE ops.schema_drift (
    id           bigserial    PRIMARY KEY,
    detected_at  timestamptz  NOT NULL DEFAULT now(),
    run_label    text,
    table_name   text         NOT NULL,
    kind         text         NOT NULL CHECK (kind IN ('column_added', 'column_missing', 'type_changed')),
    column_name  text         NOT NULL,
    detail       text,
    severity     text         NOT NULL CHECK (severity IN ('warn', 'error'))
);

-- Data quality check results, one row per check per run.
CREATE TABLE ops.dq_results (
    id          bigserial    PRIMARY KEY,
    run_label   text,
    checked_at  timestamptz  NOT NULL DEFAULT now(),
    check_name  text         NOT NULL,
    table_name  text         NOT NULL,
    status      text         NOT NULL CHECK (status IN ('pass', 'fail', 'warn', 'skipped')),
    expected    text,
    actual      text,
    details     text
);
CREATE INDEX ix_dq_results_run ON ops.dq_results (run_label, checked_at DESC);

RESET ROLE;
