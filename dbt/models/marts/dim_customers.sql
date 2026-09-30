-- One row per customer, current attributes.
select
    customer_id,
    full_name,
    email_hash,
    country,
    segment,
    acquisition_channel,
    signup_date,
    updated_at
from {{ ref('stg_customers') }}
