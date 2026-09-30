-- Every day in the funnel mart must agree with the events it was built from.
-- The mart re-reads only the last few days, so events that arrive later than that
-- are missed until a full refresh; this test is what notices.
-- (Lab: SELECT sim.inject('late_events', 6);)
with expected as (
    select min_day as event_date, count(*) as sessions
    from (
        select session_id, min(event_ts)::date as min_day
        from {{ ref('stg_web_events') }}
        group by session_id
    ) s
    group by min_day
),

actual as (
    select event_date, sum(sessions) as sessions
    from {{ ref('mart_funnel_daily') }}
    where stage = 'Page view'
    group by event_date
)

select e.event_date, e.sessions as sessions_in_events, coalesce(a.sessions, 0) as sessions_in_mart
from expected e
left join actual a using (event_date)
where coalesce(a.sessions, 0) <> e.sessions
