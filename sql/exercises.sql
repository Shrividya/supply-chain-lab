-- SQL Lab exercises against the marts and ops tables. Every statement here is
-- executed by tests/test_provisioning.py, so none of them is broken.
-- Connect as superset_ro (SQL Lab does this for you).

-- 1. Revenue by month, completed orders only.
SELECT date_trunc('month', order_date)::date AS month, sum(net_revenue) AS revenue, count(*) AS orders
FROM marts.fct_orders WHERE is_completed GROUP BY 1 ORDER BY 1;

-- 2. Which channel has the best margin? (NULL channel means the source did not send one.)
SELECT coalesce(channel, 'unknown') AS channel, round(sum(gross_margin) / nullif(sum(net_revenue), 0), 3) AS margin_pct
FROM marts.fct_orders WHERE is_completed GROUP BY 1 ORDER BY 2 DESC;

-- 3. Point-in-time correctness: customers who moved, and what their orders were attributed to.
SELECT h.customer_id, h.country, h.valid_from, h.valid_to, h.is_current
FROM marts.dim_customers_history h
WHERE h.customer_id IN (SELECT customer_id FROM marts.dim_customers_history GROUP BY 1 HAVING count(DISTINCT country) > 1)
ORDER BY h.customer_id, h.valid_from LIMIT 30;

-- 4. Funnel conversion per stage, last 14 days of data.
SELECT stage, sum(sessions) AS sessions,
       round(sum(sessions)::numeric / first_value(sum(sessions)) OVER (ORDER BY stage_order), 3) AS of_page_views
FROM marts.mart_funnel_daily
WHERE event_date > (SELECT max(event_date) FROM marts.mart_funnel_daily) - 14
GROUP BY stage, stage_order ORDER BY stage_order;

-- 5. Cohort retention triangle (first six months).
SELECT cohort_month, months_since_signup, retention_rate
FROM marts.mart_cohort_retention WHERE months_since_signup <= 6 ORDER BY 1, 2 LIMIT 60;

-- 6. How long do loads take, and how many rows do they move?
SELECT table_name, count(*) AS runs, round(avg(duration_seconds), 2) AS avg_seconds, sum(rows_inserted) AS rows_inserted
FROM ops.load_audit WHERE status = 'success' GROUP BY 1 ORDER BY 1;

-- 7. Latest data quality verdict per check.
SELECT DISTINCT ON (check_name, table_name) check_name, table_name, status, expected, actual, checked_at
FROM ops.dq_results ORDER BY check_name, table_name, checked_at DESC;
