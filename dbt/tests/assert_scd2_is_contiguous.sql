-- A slowly changing dimension must have no gaps and no overlaps, and exactly one open
-- period per customer. Otherwise an order can match zero or two countries.
with gaps as (
    select customer_id
    from (
        select
            customer_id,
            valid_to,
            lead(valid_from) over (partition by customer_id order by valid_from) as next_from
        from {{ ref('dim_customers_history') }}
    ) t
    where next_from is not null and next_from <> valid_to
),

open_periods as (
    select customer_id
    from {{ ref('dim_customers_history') }}
    group by customer_id
    having count(*) filter (where is_current) <> 1
)

select customer_id, 'gap or overlap' as problem from gaps
union all
select customer_id, 'not exactly one current row' from open_periods
