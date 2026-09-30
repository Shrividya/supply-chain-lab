-- Completed revenue per day and channel. Small, pre-aggregated, and what the
-- revenue charts read, so dashboards stay fast however much history grows.
select
    order_date,
    coalesce(channel, 'unknown') as channel,
    count(*)                     as orders,
    sum(net_revenue)             as net_revenue,
    sum(gross_margin)            as gross_margin
from {{ ref('fct_orders') }}
where is_completed
group by 1, 2
