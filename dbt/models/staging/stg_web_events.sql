{{ config(materialized='view') }}

-- Events are immutable, so there is nothing to de-duplicate: the raw table's
-- primary key already guarantees one row per event.
select
    event_id,
    session_id,
    customer_id,
    event_ts,
    event_ts::date as event_date,
    event_type,
    device,
    ingested_at
from {{ source('raw', 'web_events') }}
