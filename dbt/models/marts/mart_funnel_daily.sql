{{
    config(
        materialized='incremental',
        unique_key='funnel_key',
        incremental_strategy='delete+insert'
    )
}}

-- Sessions reaching each funnel stage, per day and device.
--
-- Incremental with a re-read window: each run recomputes the last N days
-- (var funnel_lookback_days), not just "new" days, because events arrive late.
-- An event that shows up more than N days late is NOT picked up until a full
-- refresh; assert_funnel_matches_events exists to catch exactly that.
-- Assumes a session does not cross midnight UTC.
with sessions as (
    select
        session_id,
        min(event_ts)::date as event_date,
        min(device)         as device,
        max(case event_type
                when 'page_view'      then 1
                when 'product_view'   then 2
                when 'add_to_cart'    then 3
                when 'begin_checkout' then 4
                when 'purchase'       then 5
            end)            as furthest_stage
    from {{ ref('stg_web_events') }}
    group by session_id
    {% if is_incremental() %}
    -- Filter AFTER grouping: a session's date is its earliest event, so every event of the
    -- session must be visible when that is computed. Filtering events first would give a
    -- session that straddles the window edge a wrong (later) date.
    having min(event_ts)::date >= (select max(event_date) from {{ this }}) - {{ var('funnel_lookback_days', 3) }}
    {% endif %}
),

stages as (
    select * from (values
        (1, 'Page view'), (2, 'Product view'), (3, 'Add to cart'), (4, 'Begin checkout'), (5, 'Purchase')
    ) as t (stage_order, stage)
)

select
    md5(s.event_date::text || '|' || s.device || '|' || st.stage) as funnel_key,
    s.event_date,
    s.device,
    st.stage,
    st.stage_order,
    count(*) as sessions
from sessions s
join stages st on s.furthest_stage >= st.stage_order
group by s.event_date, s.device, st.stage, st.stage_order
