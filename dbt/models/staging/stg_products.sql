{{ config(materialized='view') }}

with ranked as (
    select
        *,
        row_number() over (partition by product_id order by updated_at desc, _loaded_at desc) as rn
    from {{ source('raw', 'products') }}
)

select product_id, product_name, category, unit_price, unit_cost, updated_at
from ranked
where rn = 1
