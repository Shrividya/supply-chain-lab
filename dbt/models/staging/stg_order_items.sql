{{ config(materialized='view') }}

with ranked as (
    select
        *,
        row_number() over (partition by order_item_id order by updated_at desc, _loaded_at desc) as rn
    from {{ source('raw', 'order_items') }}
)

select order_item_id, order_id, product_id, quantity, unit_price, updated_at
from ranked
where rn = 1
