-- Incremental models can silently drop rows or leave them stale when their filter is wrong.
-- Compare the incremental table with what its sources say right now.
with expected as (
    select o.order_id,
           greatest(o.updated_at, coalesce(ic.items_updated_at, o.updated_at)) as changed_at
    from {{ ref('stg_orders') }} o
    left join (
        select order_id, max(updated_at) as items_updated_at
        from {{ ref('stg_order_items') }}
        group by order_id
    ) ic using (order_id)
)

select 'missing from fct_orders' as problem, e.order_id
from expected e
where e.order_id not in (select order_id from {{ ref('fct_orders') }})
union all
select 'stale in fct_orders', e.order_id
from expected e
join {{ ref('fct_orders') }} f using (order_id)
where f.order_updated_at <> e.changed_at
