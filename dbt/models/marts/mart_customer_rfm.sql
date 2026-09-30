-- Recency / Frequency / Monetary scoring, the classic customer segmentation.
-- "Recency" is measured against the newest order in the data, not today's date,
-- so the result is reproducible.
with orders as (
    select customer_id, order_date, net_revenue
    from {{ ref('fct_orders') }}
    where is_completed
),

as_of as (
    select max(order_date) as as_of_date from orders
),

per_customer as (
    select
        o.customer_id,
        (select as_of_date from as_of) - max(o.order_date) as recency_days,
        count(*)                                           as frequency,
        sum(o.net_revenue)                                 as monetary
    from orders o
    group by o.customer_id
),

scored as (
    select
        *,
        ntile(5) over (order by recency_days desc) as r_score,   -- 5 = most recent
        ntile(5) over (order by frequency)         as f_score,
        ntile(5) over (order by monetary)          as m_score
    from per_customer
)

select
    customer_id,
    recency_days,
    frequency,
    monetary,
    r_score,
    f_score,
    m_score,
    case
        when r_score >= 4 and f_score >= 4 then 'Champions'
        when r_score >= 3 and f_score >= 3 then 'Loyal'
        when r_score >= 4                  then 'Recent'
        when r_score <= 2 and f_score >= 3 then 'At risk'
        else 'Needs attention'
    end as rfm_segment
from scored
