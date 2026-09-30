-- Monthly acquisition cohorts: of the customers who signed up in month M, how
-- many placed a completed order in month M+k?
with cohorts as (
    select customer_id, date_trunc('month', signup_date)::date as cohort_month
    from {{ ref('dim_customers') }}
),

sizes as (
    select cohort_month, count(*) as cohort_size
    from cohorts
    group by cohort_month
),

activity as (
    select
        c.cohort_month,
        ((extract(year  from o.order_date) - extract(year  from c.cohort_month)) * 12
         + extract(month from o.order_date) - extract(month from c.cohort_month))::integer as months_since_signup,
        count(distinct o.customer_id) as active_customers
    from {{ ref('fct_orders') }} o
    join cohorts c using (customer_id)
    where o.is_completed
    group by 1, 2
)

select
    a.cohort_month,
    a.months_since_signup,
    s.cohort_size,
    a.active_customers,
    round(a.active_customers::numeric / s.cohort_size, 4) as retention_rate
from activity a
join sizes s using (cohort_month)
where a.months_since_signup >= 0
