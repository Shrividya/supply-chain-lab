{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='delete+insert',
        on_schema_change='fail',
        contract={'enforced': true}
    )
}}

-- One row per order, with revenue and margin computed once, here, so every chart
-- and every analyst gets the same numbers.
--
-- Incremental: on each run only orders that changed since the last run are
-- recomputed, then swapped in by order_id. "Changed" means the order row OR any
-- of its line items has a newer version than anything already in this table, so
-- a refund three days later and a corrected line-item price are handled the same
-- way. (Watching only the order row is a classic incremental-model bug: child
-- tables change and the parent never notices.)
with item_changes as (
    select order_id, max(updated_at) as items_updated_at
    from {{ ref('stg_order_items') }}
    group by order_id
),

orders as (
    select
        o.*,
        greatest(o.updated_at, coalesce(ic.items_updated_at, o.updated_at)) as changed_at
    from {{ ref('stg_orders') }} o
    left join item_changes ic using (order_id)
    {% if is_incremental() %}
    where greatest(o.updated_at, coalesce(ic.items_updated_at, o.updated_at))
          > (select max(order_updated_at) from {{ this }})
    {% endif %}
),

items as (
    select
        oi.order_id,
        count(*)                          as items_count,
        sum(oi.quantity)                  as units,
        sum(oi.quantity * oi.unit_price)  as gross_revenue,
        sum(oi.quantity * p.unit_cost)    as cogs
    from {{ ref('stg_order_items') }} oi
    join {{ ref('stg_products') }} p using (product_id)
    where oi.order_id in (select order_id from orders)
    group by oi.order_id
)

select
    o.order_id::integer                                                            as order_id,
    o.customer_id::integer                                                         as customer_id,
    o.order_ts                                                                     as order_ts,
    o.order_date                                                                   as order_date,
    o.status::text                                                                 as status,
    o.channel::text                                                                as channel,
    o.discount_pct::numeric                                                        as discount_pct,
    -- as of the moment of the order, not today (see dim_customers_history)
    c.country::text                                                                as country,
    c.segment::text                                                                as segment,
    coalesce(i.items_count, 0)::integer                                            as items_count,
    coalesce(i.units, 0)::integer                                                  as units,
    coalesce(i.gross_revenue, 0)::numeric                                          as gross_revenue,
    round(coalesce(i.gross_revenue, 0) * o.discount_pct, 2)::numeric               as discount_amount,
    round(coalesce(i.gross_revenue, 0) * (1 - o.discount_pct), 2)::numeric         as net_revenue,
    coalesce(i.cogs, 0)::numeric                                                   as cogs,
    round(coalesce(i.gross_revenue, 0) * (1 - o.discount_pct) - coalesce(i.cogs, 0), 2)::numeric as gross_margin,
    (o.status = 'completed')::boolean                                              as is_completed,
    o.changed_at                                                                   as order_updated_at
from orders o
left join items i using (order_id)
left join {{ ref('dim_customers_history') }} c
       on c.customer_id = o.customer_id
      and o.order_ts >= c.valid_from
      and o.order_ts <  c.valid_to
