-- The order-level revenue must equal the sum of its line items in the line-level model.
-- Two models computing the same number two ways is the cheapest reconciliation there is.
select o.order_id, o.gross_revenue, l.line_total
from {{ ref('fct_orders') }} o
join (
    select order_id, sum(line_revenue) as line_total
    from {{ ref('fct_order_items') }}
    group by order_id
) l using (order_id)
where abs(o.gross_revenue - l.line_total) > 0.005
