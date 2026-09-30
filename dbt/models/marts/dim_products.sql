select product_id, product_name, category, unit_price, unit_cost
from {{ ref('stg_products') }}
