{{ config(materialized='view') }}

-- The current state of each order. Orders change status after creation
-- (pending -> completed / cancelled, completed -> refunded), so the raw layer
-- holds several versions per order and this model keeps the newest.
with ranked as (
    select
        *,
        row_number() over (partition by order_id order by updated_at desc, _loaded_at desc) as rn
    from {{ source('raw', 'orders') }}
)

select
    order_id,
    customer_id,
    order_ts,
    order_ts::date as order_date,
    status,
    channel,
    discount_pct,
    updated_at
from ranked
where rn = 1
