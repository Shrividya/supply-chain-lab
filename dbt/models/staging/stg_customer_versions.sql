{{ config(materialized='view') }}

-- Every version of every customer that the pipeline observed.
-- dim_customers_history turns this into a slowly changing dimension.
select
    customer_id,
    country,
    segment,
    signup_date,
    updated_at,
    _loaded_at
from {{ source('raw', 'customers') }}
