-- Seed the source with history up to 2026-09-20 (deterministic).
-- After this, `sim.advance_day()` (02_simulator.sql) adds one day at a time.
--
--   3,000 customers   60 products   ~30,000 orders   ~75,000 items
--   ~120,000 sessions of clickstream (~290,000 events)
SET timezone TO 'UTC';
SELECT setseed(0.42);

-- Customers
INSERT INTO app.customers
SELECT
    c.g,
    'Customer ' || lpad(c.g::text, 5, '0'),
    'customer' || c.g || '@example.com',
    (ARRAY['United States','United Kingdom','Germany','India','Brazil','Canada','Australia'])
        [1 + floor(c.r_country * 7)::int],
    CASE WHEN c.r_seg < 0.62 THEN 'consumer'
         WHEN c.r_seg < 0.85 THEN 'small_business'
         ELSE 'enterprise' END,
    (ARRAY['organic','paid_search','email','social','affiliate'])
        [1 + floor(c.r_chan * 5)::int],
    date '2025-01-01' + floor(power(c.r_date, 0.8) * 618)::int,      -- up to 2026-09-01
    (date '2025-01-01' + floor(power(c.r_date, 0.8) * 618)::int)::timestamptz,
    (date '2025-01-01' + floor(power(c.r_date, 0.8) * 618)::int)::timestamptz
FROM (
    SELECT g, random() AS r_country, random() AS r_seg, random() AS r_chan, random() AS r_date
    FROM generate_series(1, 3000) AS g
) AS c;

-- Products
INSERT INTO app.products
SELECT
    p.g,
    (ARRAY['Aero','Nimbus','Vertex','Summit','Orbit','Pulse','Atlas','Nova','Ember','Drift'])[1 + (p.g % 10)]
      || ' ' ||
    (ARRAY['Backpack','Headphones','Desk Lamp','Water Bottle','Keyboard','Notebook'])[1 + (p.g % 6)]
      || ' ' || p.g,
    (ARRAY['Bags','Audio','Home Office','Outdoors','Accessories','Stationery'])[1 + (p.g % 6)],
    p.price,
    round((p.price * (0.45 + p.r_cost * 0.2))::numeric, 2),
    timestamptz '2025-01-01',
    timestamptz '2025-01-01'
FROM (
    SELECT g, round((12 + random() * 190)::numeric, 2) AS price, random() AS r_cost
    FROM generate_series(1, 60) AS g
) AS p;

-- Orders
-- A minority of customers order a lot (power law). Each order lands between the
-- customer's signup and 2026-09-20, so nobody buys before they exist.
INSERT INTO app.orders
SELECT
    o.order_id,
    o.customer_id,
    o.order_ts,
    o.status,
    CASE WHEN o.r_ch < 0.30 THEN 'organic'
         WHEN o.r_ch < 0.55 THEN 'paid_search'
         WHEN o.r_ch < 0.75 THEN 'email'
         WHEN o.r_ch < 0.90 THEN 'social'
         ELSE 'affiliate' END,
    CASE WHEN o.r_d < 0.70 THEN 0
         WHEN o.r_d < 0.90 THEN 0.10
         WHEN o.r_d < 0.97 THEN 0.20
         ELSE 0.30 END,
    o.order_ts,
    -- orders that ended up refunded/cancelled changed a few days after purchase
    CASE WHEN o.status = 'completed' THEN o.order_ts
         ELSE least(o.order_ts + o.r_lag * interval '5 days', timestamptz '2026-09-20 23:59:59')
    END
FROM (
    SELECT
        s.order_id,
        s.customer_id,
        c.signup_date::timestamptz
          + s.r_ts * (timestamptz '2026-09-20 23:59:59' - c.signup_date::timestamptz) AS order_ts,
        CASE WHEN s.r_s < 0.90 THEN 'completed'
             WHEN s.r_s < 0.95 THEN 'refunded'
             ELSE 'cancelled' END AS status,
        s.r_ch, s.r_d, s.r_lag
    FROM (
        SELECT g AS order_id,
               1 + floor(power(random(), 1.6) * 3000)::int AS customer_id,
               random() AS r_ts, random() AS r_s, random() AS r_ch, random() AS r_d, random() AS r_lag
        FROM generate_series(1, 30000) AS g
    ) AS s
    JOIN app.customers c ON c.customer_id = s.customer_id
) AS o;

-- Order items
INSERT INTO app.order_items
SELECT
    row_number() OVER (ORDER BY li.order_id, li.n),
    li.order_id,
    p.product_id,
    li.qty,
    round((p.unit_price * li.jitter)::numeric, 2),
    li.created_at,
    li.created_at
FROM (
    SELECT o.order_id, n, o.order_ts AS created_at,
           1 + floor(random() * 60)::int AS pid,
           1 + floor(random() * 3)::int  AS qty,
           0.95 + random() * 0.10        AS jitter
    FROM (
        SELECT order_id, order_ts, 1 + floor(random() * 4)::int AS n_items FROM app.orders
    ) AS o
    CROSS JOIN LATERAL generate_series(1, o.n_items) AS n
) AS li
JOIN app.products p ON p.product_id = li.pid;

-- Clickstream
-- A session walks a funnel and may drop out at each step:
--   page_view -> product_view -> add_to_cart -> begin_checkout -> purchase
INSERT INTO app.web_events
SELECT
    row_number() OVER (ORDER BY s.session_id, e.n),
    s.session_id,
    s.customer_id,
    s.start_ts + (e.n - 1) * s.r_gap * interval '4 minutes',
    (ARRAY['page_view','product_view','add_to_cart','begin_checkout','purchase'])[e.n],
    s.device,
    s.start_ts + (e.n - 1) * s.r_gap * interval '4 minutes' + interval '5 minutes'
FROM (
    SELECT
        g AS session_id,
        CASE WHEN random() < 0.35 THEN 1 + floor(random() * 3000)::int END AS customer_id,
        timestamptz '2025-01-01' + random() * (timestamptz '2026-09-20 23:00:00' - timestamptz '2025-01-01') AS start_ts,
        CASE WHEN d < 0.55 THEN 'mobile' WHEN d < 0.90 THEN 'desktop' ELSE 'tablet' END AS device,
        random() AS r_gap,
        CASE WHEN r1 >= 0.75 THEN 1
             WHEN r2 >= 0.45 THEN 2
             WHEN r3 >= 0.60 THEN 3
             WHEN r4 >= 0.70 THEN 4
             ELSE 5 END AS reach
    FROM (
        SELECT g, random() AS d, random() AS r1, random() AS r2, random() AS r3, random() AS r4
        FROM generate_series(1, 120000) AS g
    ) AS raw_s
) AS s
CROSS JOIN LATERAL generate_series(1, s.reach) AS e(n);

ANALYZE;
