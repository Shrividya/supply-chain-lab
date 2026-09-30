-- Line level view for category analysis. A plain table (rebuilt every run)
-- rather than incremental, on purpose: it carries the ORDER's status, which
-- changes after the line item was created, and a full rebuild of ~100k rows is
-- cheaper than the bookkeeping needed to keep it incrementally correct.
select
    oi.order_item_id,
    oi.order_id,
    o.order_date,
    o.status,
    o.channel,
    o.country,
    p.product_id,
    p.product_name,
    p.category,
    oi.quantity,
    (oi.quantity * oi.unit_price) as line_revenue,
    (oi.quantity * p.unit_cost)   as line_cost
from {{ ref('stg_order_items') }} oi
join {{ ref('fct_orders') }}  o using (order_id)
join {{ ref('stg_products') }} p using (product_id)
