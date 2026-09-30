{{ config(materialized='view') }}

-- The current state of each customer: the newest version wins.
-- Email is hashed here so nothing downstream ever sees personal data.
with ranked as (
    select
        *,
        row_number() over (
            partition by customer_id
            order by updated_at desc, _loaded_at desc
        ) as rn
    from {{ source('raw', 'customers') }}
)

select
    customer_id,
    full_name,
    md5(lower(email))   as email_hash,
    country,
    segment,
    acquisition_channel,
    signup_date,
    created_at,
    updated_at
from ranked
where rn = 1
