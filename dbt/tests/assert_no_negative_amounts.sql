-- A line item with a negative price or zero quantity is a source bug, and it would
-- quietly shrink revenue. (Lab: SELECT sim.inject('bad_amount');)
select order_item_id, order_id, quantity, unit_price
from {{ ref('stg_order_items') }}
where unit_price < 0 or quantity <= 0
