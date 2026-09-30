-- The pre-aggregated mart must add up to the fact table it summarises.
with mart as (
    select order_date, sum(net_revenue) as revenue from {{ ref('mart_daily_channel') }} group by order_date
),
fact as (
    select order_date, sum(net_revenue) as revenue from {{ ref('fct_orders') }} where is_completed group by order_date
)
select coalesce(m.order_date, f.order_date) as order_date, m.revenue as mart_revenue, f.revenue as fact_revenue
from mart m
full join fact f using (order_date)
where abs(coalesce(m.revenue, 0) - coalesce(f.revenue, 0)) > 0.005
