-- Slowly changing dimension, type 2: one row per period during which a customer
-- had a given (country, segment). Orders join to it "as of" the order time, so
-- last year's revenue stays attributed to the country the customer lived in then.
--
-- Built from the raw change log, which is why raw keeps every version.
--
-- Assumption: the first version the pipeline ever saw is
-- treated as true since the beginning of time (valid_from = 1970). If a customer
-- moved before we started observing, we cannot know.
with versions as (
    select
        customer_id,
        country,
        segment,
        updated_at,
        lag(country) over w as prev_country,
        lag(segment) over w as prev_segment
    from {{ ref('stg_customer_versions') }}
    window w as (partition by customer_id order by updated_at)
),

changes as (
    -- keep only versions where something we track actually changed
    select
        customer_id,
        country,
        segment,
        updated_at,
        (prev_country is null) as is_first
    from versions
    where prev_country is null
       or country is distinct from prev_country
       or segment is distinct from prev_segment
)

select
    customer_id,
    country,
    segment,
    case when is_first then timestamptz '1970-01-01 00:00:00+00' else updated_at end as valid_from,
    coalesce(lead(updated_at) over w, timestamptz '9999-12-31 00:00:00+00')          as valid_to,
    (lead(updated_at) over w is null)                                                as is_current,
    row_number() over w                                                              as version_number
from changes
window w as (partition by customer_id order by updated_at)
