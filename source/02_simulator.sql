-- Simulator: makes the source behave like a live system.
--
--   SELECT sim.advance_day();        -- one more business day of activity
--   SELECT sim.inject('bad_amount'); -- break something on purpose (labs)
--   SELECT sim.reset_faults();       -- undo schema faults
--
-- What one day does:
--   * new customers and orders (with items) arrive
--   * yesterday's pending orders settle to completed / cancelled
--   * a few recent completed orders are refunded (days after purchase)
--   * a few customers move country or change segment
--   * a day of clickstream arrives, plus late-arriving events for yesterday
--
-- Everything is a plain UPDATE/INSERT that bumps updated_at, the same thing an
-- application would do. Nothing here is visible to the pipeline except through
-- ordinary queries.
SET timezone TO 'UTC';

CREATE OR REPLACE FUNCTION sim.advance_day(
    p_orders   integer DEFAULT 150,
    p_sessions integer DEFAULT 190
) RETURNS date
LANGUAGE plpgsql AS $$
DECLARE
    d              date;
    v_max_order    integer;
    v_max_item     integer;
    v_max_customer integer;
    v_max_event    bigint;
    v_max_session  bigint;
    v_new_customers integer := 12;
BEGIN
    SELECT (max(order_ts))::date + 1 INTO d FROM app.orders;
    -- Deterministic per day, so reruns of the lab give the same numbers.
    PERFORM setseed(((extract(epoch FROM d::timestamptz)::bigint / 86400) % 1000) / 1000.0);

    SELECT coalesce(max(order_id), 0)       INTO v_max_order    FROM app.orders;
    SELECT coalesce(max(order_item_id), 0)  INTO v_max_item     FROM app.order_items;
    SELECT coalesce(max(customer_id), 0)    INTO v_max_customer FROM app.customers;
    SELECT coalesce(max(event_id), 0)       INTO v_max_event    FROM app.web_events;
    SELECT coalesce(max(session_id), 0)     INTO v_max_session  FROM app.web_events;

    -- New customers
    INSERT INTO app.customers
    SELECT
        v_max_customer + n.g,
        'Customer ' || lpad((v_max_customer + n.g)::text, 5, '0'),
        'customer' || (v_max_customer + n.g) || '@example.com',
        (ARRAY['United States','United Kingdom','Germany','India','Brazil','Canada','Australia'])[1 + floor(n.r1 * 7)::int],
        CASE WHEN n.r2 < 0.62 THEN 'consumer' WHEN n.r2 < 0.85 THEN 'small_business' ELSE 'enterprise' END,
        (ARRAY['organic','paid_search','email','social','affiliate'])[1 + floor(n.r3 * 5)::int],
        d,
        d::timestamptz + interval '8 hours',
        d::timestamptz + interval '8 hours'
    FROM (SELECT g, random() r1, random() r2, random() r3 FROM generate_series(1, v_new_customers) g) n;

    -- Settle yesterday's pending orders
    UPDATE app.orders o
    SET status = CASE WHEN random() < 0.92 THEN 'completed' ELSE 'cancelled' END,
        updated_at = d::timestamptz + interval '1 hour' + random() * interval '10 hours'
    WHERE o.status = 'pending' AND o.order_ts < d::timestamptz;

    -- A trickle of refunds on completed orders from the last 10 days
    UPDATE app.orders o
    SET status = 'refunded',
        updated_at = d::timestamptz + interval '11 hours' + random() * interval '8 hours'
    WHERE o.order_id IN (
        SELECT order_id FROM app.orders
        WHERE status = 'completed'
          AND order_ts >= d::timestamptz - interval '10 days'
          AND order_ts <  d::timestamptz
          AND random() < 0.01
    );

    -- Customers move country / change segment
    UPDATE app.customers c
    SET country = (ARRAY['United States','United Kingdom','Germany','India','Brazil','Canada','Australia'])[1 + floor(random() * 7)::int],
        updated_at = d::timestamptz + interval '12 hours'
    WHERE c.customer_id IN (SELECT customer_id FROM app.customers WHERE random() < 0.006);

    UPDATE app.customers c
    SET segment = CASE c.segment WHEN 'consumer' THEN 'small_business' WHEN 'small_business' THEN 'enterprise' ELSE c.segment END,
        updated_at = d::timestamptz + interval '13 hours'
    WHERE c.customer_id IN (SELECT customer_id FROM app.customers WHERE random() < 0.004)
      AND c.updated_at < d::timestamptz + interval '12 hours';   -- one change per day per customer

    -- New orders (start life as pending; step 2 settles them tomorrow)
    INSERT INTO app.orders
    SELECT
        v_max_order + s.g,
        1 + floor(power(s.r_c, 1.6) * v_max_customer)::int,
        d::timestamptz + s.r_t * interval '23 hours',
        'pending',
        CASE WHEN s.r_ch < 0.30 THEN 'organic' WHEN s.r_ch < 0.55 THEN 'paid_search'
             WHEN s.r_ch < 0.75 THEN 'email'   WHEN s.r_ch < 0.90 THEN 'social' ELSE 'affiliate' END,
        CASE WHEN s.r_d < 0.70 THEN 0 WHEN s.r_d < 0.90 THEN 0.10 WHEN s.r_d < 0.97 THEN 0.20 ELSE 0.30 END,
        d::timestamptz + s.r_t * interval '23 hours',
        d::timestamptz + s.r_t * interval '23 hours'
    FROM (SELECT g, random() r_c, random() r_t, random() r_ch, random() r_d FROM generate_series(1, p_orders) g) s;

    INSERT INTO app.order_items
    SELECT
        v_max_item + row_number() OVER (ORDER BY li.order_id, li.n),
        li.order_id, p.product_id, li.qty,
        round((p.unit_price * li.jitter)::numeric, 2),
        li.created_at, li.created_at
    FROM (
        SELECT o.order_id, n, o.order_ts AS created_at,
               1 + floor(random() * 60)::int AS pid,
               1 + floor(random() * 3)::int  AS qty,
               0.95 + random() * 0.10        AS jitter
        FROM (
            SELECT order_id, order_ts, 1 + floor(random() * 4)::int AS n_items
            FROM app.orders WHERE order_id > v_max_order
        ) o
        CROSS JOIN LATERAL generate_series(1, o.n_items) n
    ) li
    JOIN app.products p ON p.product_id = li.pid;

    -- Clickstream for the day
    INSERT INTO app.web_events
    SELECT
        v_max_event + row_number() OVER (ORDER BY s.session_id, e.n),
        s.session_id, s.customer_id,
        s.start_ts + (e.n - 1) * s.r_gap * interval '4 minutes',
        (ARRAY['page_view','product_view','add_to_cart','begin_checkout','purchase'])[e.n],
        s.device,
        s.start_ts + (e.n - 1) * s.r_gap * interval '4 minutes' + interval '5 minutes'
    FROM (
        SELECT
            v_max_session + g AS session_id,
            CASE WHEN random() < 0.35 THEN 1 + floor(random() * v_max_customer)::int END AS customer_id,
            d::timestamptz + random() * interval '22 hours' AS start_ts,
            CASE WHEN dv < 0.55 THEN 'mobile' WHEN dv < 0.90 THEN 'desktop' ELSE 'tablet' END AS device,
            random() AS r_gap,
            CASE WHEN r1 >= 0.75 THEN 1 WHEN r2 >= 0.45 THEN 2 WHEN r3 >= 0.60 THEN 3
                 WHEN r4 >= 0.70 THEN 4 ELSE 5 END AS reach
        FROM (SELECT g, random() dv, random() r1, random() r2, random() r3, random() r4
              FROM generate_series(1, p_sessions) g) raw_s
    ) s
    CROSS JOIN LATERAL generate_series(1, s.reach) e(n);

    -- Late-arriving events: they belong to yesterday but reach us today
    SELECT coalesce(max(event_id), 0) INTO v_max_event FROM app.web_events;
    SELECT coalesce(max(session_id), 0) INTO v_max_session FROM app.web_events;
    INSERT INTO app.web_events
    SELECT
        v_max_event + g,
        v_max_session + g,
        NULL,
        d::timestamptz - interval '3 hours' - random() * interval '6 hours',
        'page_view',
        'mobile',
        d::timestamptz + interval '2 hours' + random() * interval '4 hours'   -- ingested late
    FROM generate_series(1, greatest(p_sessions / 20, 1)) g;

    RETURN d;
END;
$$;

-- Fault injection, used by the failure labs (docs/LABS.md).
CREATE OR REPLACE FUNCTION sim.inject(p_kind text, p_n integer DEFAULT 20)
RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    v_last  timestamptz;
    v_count integer;
BEGIN
    SELECT max(updated_at) INTO v_last FROM app.orders;
    v_last := v_last + interval '1 hour';

    IF p_kind = 'bad_amount' THEN
        -- A pricing bug flips the sign on some recent line items.
        UPDATE app.order_items SET unit_price = -abs(unit_price), updated_at = v_last
        WHERE order_item_id IN (SELECT order_item_id FROM app.order_items ORDER BY order_item_id DESC LIMIT p_n);
        GET DIAGNOSTICS v_count = ROW_COUNT;
        RETURN format('flipped the sign on %s order items', v_count);

    ELSIF p_kind = 'null_channel' THEN
        UPDATE app.orders SET channel = NULL, updated_at = v_last
        WHERE order_id IN (SELECT order_id FROM app.orders ORDER BY order_id DESC LIMIT p_n);
        GET DIAGNOSTICS v_count = ROW_COUNT;
        RETURN format('nulled the channel on %s orders', v_count);

    ELSIF p_kind = 'orphan_order' THEN
        INSERT INTO app.orders
        SELECT (SELECT max(order_id) FROM app.orders) + g, 99000000 + g, v_last, 'completed', 'organic', 0, v_last, v_last
        FROM generate_series(1, p_n) g;
        RETURN format('inserted %s orders for customers that do not exist', p_n);

    ELSIF p_kind = 'late_events' THEN
        -- p_n = how many days back the events belong to (2 is recovered by the
        -- reprocessing window, 6 is not).
        INSERT INTO app.web_events
        SELECT (SELECT max(event_id) FROM app.web_events) + g,
               (SELECT max(session_id) FROM app.web_events) + g,
               NULL,
               v_last - make_interval(days => p_n) - random() * interval '3 hours',
               'page_view', 'desktop', v_last
        FROM generate_series(1, 50) g;
        RETURN format('inserted 50 events that belong %s days ago', p_n);

    ELSIF p_kind = 'schema_add_column' THEN
        ALTER TABLE app.orders ADD COLUMN IF NOT EXISTS coupon_code text;
        RETURN 'added app.orders.coupon_code (a harmless, additive change)';

    ELSIF p_kind = 'schema_rename_column' THEN
        IF EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_schema = 'app' AND table_name = 'orders' AND column_name = 'channel') THEN
            ALTER TABLE app.orders RENAME COLUMN channel TO sales_channel;
        END IF;
        RETURN 'renamed app.orders.channel to sales_channel (a breaking change)';

    ELSE
        RAISE EXCEPTION 'unknown fault kind: %', p_kind
            USING HINT = 'bad_amount, null_channel, orphan_order, late_events, schema_add_column, schema_rename_column';
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION sim.reset_faults() RETURNS text
LANGUAGE plpgsql AS $$
BEGIN
    IF EXISTS (SELECT 1 FROM information_schema.columns
               WHERE table_schema = 'app' AND table_name = 'orders' AND column_name = 'sales_channel') THEN
        ALTER TABLE app.orders RENAME COLUMN sales_channel TO channel;
    END IF;
    ALTER TABLE app.orders DROP COLUMN IF EXISTS coupon_code;
    RETURN 'schema faults reverted (data faults stay, as they would in real life)';
END;
$$;

