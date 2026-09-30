# Architecture and the decisions behind it

```
 source-db (app.*)          warehouse                                     consumers
 customers, products  --->  raw.*  (append-only change log)
 orders, order_items        ops.*  (watermarks, audit, drift, dq) ------> Superset: Pipeline Health
 web_events (clicks)        staging.* (views: newest version, cleaned)
                            marts.*   (dbt tables) ---------------------> Superset: Shop Overview

 Airflow:  shop_ingest (hourly)  --asset-->  shop_transform (dbt build)
```

## Decisions

**Polling extraction, not CDC.** Ingestion reads rows whose `updated_at` moved past a watermark. It is simple and needs nothing from the source database beyond read access. The cost: if a row changes twice between polls you only see the last state, and hard deletes are invisible. Log-based CDC (Debezium, logical replication) fixes both and costs real operational complexity. Choose polling until a business question needs the intermediate states.

**Raw is an append-only change log.** Primary key is `(id, updated_at)`. Every version of a row is kept, so history is free, reloads are harmless (`ON CONFLICT DO NOTHING`), and a bug in a model never needs a re-extract.

**Watermark with an overlap window.** Each run re-reads the last hour. A transaction that started before the last extract but committed after it carries an older timestamp and would be skipped forever without overlap. The primary key throws away the duplicates.

**Upper bound fixed at the start of the extract.** Rows arriving mid-extract cannot be half-counted and the watermark can never run ahead of the data copied.

**One transaction moves data, watermark and audit together.** A crash leaves nothing half-done. The atomicity test proves it by making the insert fail and checking that the watermark did not move.

**Schema drift policy.** A new source column is a warning (nothing breaks). A missing column or changed type stops the load before writing. Silent nulls are worse than a failed task.

**Web events use ingestion time as the cursor.** Late events have an old `event_ts` but a new `ingested_at`, so they are found. Raw is partitioned monthly with a DEFAULT partition, and a check fails if anything lands there (a missing partition).

**Reconciliation is separate from dbt tests.** dbt sees only the warehouse. Only the pipeline can compare row counts and checksums against the source. When the source is ahead of the watermark the check reports `skipped`, not a false failure.

**SCD2 built from the change log.** `dim_customers_history` turns raw versions into validity ranges, so orders get the country the customer had at order time. The first version starts at 1970 so early orders still join.

**Incremental models with a lookback window.** `fct_orders` and `mart_funnel_daily` reprocess recent data using delete+insert. Two lessons recorded by tests:
- `fct_orders` must watch order *and* line item changes, or a fixed price never reaches the mart.
- The funnel must filter sessions *after* grouping, so a session that straddles the window edge keeps its true start date. (A bug found here, fixed, and pinned by a test.)

**Events later than the window are caught, not hidden.** A test compares the funnel with the raw events. The remedy is a full refresh, documented in LABS.

**Least privilege.** `pipeline_ro` reads the source. `loader` writes raw and ops. `dbt_user` reads raw, owns staging and marts. `superset_ro` reads marts and ops, with a 60 second statement timeout, and cannot read raw.

**Orchestration on assets.** `shop_transform` runs when `shop_ingest` publishes the raw asset, and only if the quality checks passed. Retries back off for infrastructure errors. The reconcile task does not retry, because bad data will not fix itself.

## Known limits

- No hard-delete handling.
- Polling can miss intermediate states.
- LocalExecutor, single node: fine for a lab, not for scale.
- Airflow uses its simple auth manager with open access for the lab. Do not expose it.
