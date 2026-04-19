-- ============================================================
-- Category 7: Complex Aggregations & Metrics
-- Problems 073–086
-- Database: PostgreSQL
-- Context: Real DE scenarios — retention cohorts, moving averages,
--          WoW metrics, percentiles, pivots, funnel analysis
-- ============================================================


-- Problem 073: Week-over-Week Revenue Change per Product Category
-- Difficulty: Medium
-- Category: Complex Aggregations & Metrics
-- Tags: WoW, LAG, window functions, revenue metrics
--
-- Scenario:
-- A marketplace finance team tracks weekly gross merchandise value (GMV)
-- by product category. They want a WoW report showing current week revenue,
-- prior week revenue, absolute change, and percentage change. Weeks with no
-- prior week data should show NULL for change metrics.
--
-- Question:
-- Compute WoW GMV change per category for each ISO week in the dataset.
-- Output: category, week_start, gmv, prev_week_gmv, wow_change, wow_pct_change.

-- Schema & Sample Data:
CREATE TABLE weekly_orders (
    order_id    BIGINT PRIMARY KEY,
    category    VARCHAR(50),
    order_date  DATE,
    revenue     NUMERIC(10,2)
);

INSERT INTO weekly_orders VALUES
(1,  'Electronics', '2024-01-03', 500.00),
(2,  'Electronics', '2024-01-04', 300.00),
(3,  'Electronics', '2024-01-10', 600.00),
(4,  'Electronics', '2024-01-11', 200.00),
(5,  'Electronics', '2024-01-17', 900.00),
(6,  'Clothing',    '2024-01-03', 200.00),
(7,  'Clothing',    '2024-01-08', 150.00),
(8,  'Clothing',    '2024-01-10', 250.00),
(9,  'Clothing',    '2024-01-15', 100.00),
(10, 'Clothing',    '2024-01-17', 300.00),
(11, 'Clothing',    '2024-01-18', 200.00),
(12, 'Electronics', '2024-01-24', 1100.00),
(13, 'Clothing',    '2024-01-24', 450.00);

-- Solution:
WITH weekly_agg AS (
    SELECT
        category,
        DATE_TRUNC('week', order_date)::DATE AS week_start,
        SUM(revenue)                         AS gmv
    FROM weekly_orders
    GROUP BY category, DATE_TRUNC('week', order_date)
),
with_lag AS (
    SELECT
        category,
        week_start,
        gmv,
        LAG(gmv) OVER (PARTITION BY category ORDER BY week_start) AS prev_week_gmv
    FROM weekly_agg
)
SELECT
    category,
    week_start,
    gmv,
    prev_week_gmv,
    gmv - prev_week_gmv                                             AS wow_change,
    ROUND((gmv - prev_week_gmv) * 100.0 / NULLIF(prev_week_gmv, 0), 1) AS wow_pct_change
FROM with_lag
ORDER BY category, week_start;

-- Expected Output:
-- category    | week_start | gmv     | prev_week_gmv | wow_change | wow_pct_change
-- ------------+------------+---------+---------------+------------+---------------
-- Clothing    | 2024-01-01 | 350.00  | NULL          | NULL       | NULL
-- Clothing    | 2024-01-08 | 350.00  | 350.00        | 0.00       | 0.0
-- Clothing    | 2024-01-15 | 600.00  | 350.00        | 250.00     | 71.4
-- Clothing    | 2024-01-22 | 450.00  | 600.00        | -150.00    | -25.0
-- Electronics | 2024-01-01 | 800.00  | NULL          | NULL       | NULL
-- Electronics | 2024-01-08 | 800.00  | 800.00        | 0.00       | 0.0
-- Electronics | 2024-01-15 | 900.00  | 800.00        | 100.00     | 12.5
-- Electronics | 2024-01-22 | 1100.00 | 900.00        | 200.00     | 22.2

-- Explanation:
-- DATE_TRUNC('week') groups orders by ISO week Monday. LAG over the weekly
-- aggregation gives prior week GMV. NULLIF prevents division by zero for
-- categories with zero prior-week revenue.
--
-- Follow-up: Add a 4-week rolling average alongside the WoW metrics using
--            AVG(...) OVER (PARTITION BY category ORDER BY week_start ROWS 3 PRECEDING).


-- Problem 074: 7-Day Rolling Average of Daily Active Users (DAU)
-- Difficulty: Medium
-- Category: Complex Aggregations & Metrics
-- Tags: moving average, DAU, window functions, rolling window
--
-- Scenario:
-- A product analytics team tracks daily active users (DAU). Raw daily counts
-- are noisy due to weekday/weekend patterns. They want a 7-day rolling average
-- to smooth the trend, and want to flag days where DAU dropped more than 20%
-- below the rolling average (anomaly detection).
--
-- Question:
-- Compute the 7-day rolling average DAU and flag anomaly days.
-- Output: activity_date, dau, rolling_7d_avg, is_anomaly (bool).

-- Schema & Sample Data:
CREATE TABLE dau_daily (
    activity_date DATE PRIMARY KEY,
    dau           INT
);

INSERT INTO dau_daily VALUES
('2024-03-01', 10200), ('2024-03-02', 9800),  ('2024-03-03', 8500),
('2024-03-04', 11000), ('2024-03-05', 10800), ('2024-03-06', 9500),
('2024-03-07', 10300), ('2024-03-08', 9900),  ('2024-03-09', 10100),
('2024-03-10', 7200),  -- anomaly: well below rolling avg
('2024-03-11', 10500), ('2024-03-12', 10200), ('2024-03-13', 10600),
('2024-03-14', 9800),  ('2024-03-15', 5000);  -- anomaly

-- Solution:
WITH rolling AS (
    SELECT
        activity_date,
        dau,
        ROUND(AVG(dau) OVER (
            ORDER BY activity_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ), 0) AS rolling_7d_avg
    FROM dau_daily
)
SELECT
    activity_date,
    dau,
    rolling_7d_avg,
    (dau < rolling_7d_avg * 0.8) AS is_anomaly
FROM rolling
ORDER BY activity_date;

-- Expected Output (partial):
-- activity_date | dau   | rolling_7d_avg | is_anomaly
-- --------------+-------+----------------+-----------
-- 2024-03-01    | 10200 | 10200          | false
-- 2024-03-02    | 9800  | 10000          | false
-- ...
-- 2024-03-10    | 7200  | 9971           | true
-- ...
-- 2024-03-15    | 5000  | 9043           | true

-- Explanation:
-- The window ROWS BETWEEN 6 PRECEDING AND CURRENT ROW gives a 7-row (7-day)
-- rolling average including the current day. For the first 6 days, the window
-- is smaller (partial window). The anomaly flag compares DAU to 80% of avg.
--
-- Follow-up: Also compute a 7-day rolling standard deviation and flag days
--            more than 2 standard deviations below the rolling mean.


-- Problem 075: User Retention Cohort Analysis (Monthly Cohorts)
-- Difficulty: Hard
-- Category: Complex Aggregations & Metrics
-- Tags: cohort analysis, retention, DATE_TRUNC, self-join
--
-- Scenario:
-- A growth team wants a monthly cohort retention table: for each signup cohort
-- (month), what percentage of users returned in months 1, 2, 3 post-signup?
-- Month 0 = signup month.
--
-- Question:
-- Build a cohort retention matrix showing retention rate at months 0-3
-- for each signup cohort. Output: cohort_month, month_0_users, ret_m1_pct,
-- ret_m2_pct, ret_m3_pct.

-- Schema & Sample Data:
CREATE TABLE user_signups (
    user_id     INT PRIMARY KEY,
    signup_date DATE
);

CREATE TABLE user_logins (
    user_id    INT,
    login_date DATE,
    PRIMARY KEY (user_id, login_date)
);

INSERT INTO user_signups VALUES
(1,'2024-01-15'),(2,'2024-01-20'),(3,'2024-01-25'),
(4,'2024-02-05'),(5,'2024-02-10'),(6,'2024-02-20'),
(7,'2024-03-01'),(8,'2024-03-15'),(9,'2024-03-20');

INSERT INTO user_logins VALUES
-- Jan cohort
(1,'2024-01-15'),(1,'2024-02-10'),(1,'2024-03-05'),(1,'2024-04-02'),
(2,'2024-01-20'),(2,'2024-02-15'),(2,'2024-03-20'),
(3,'2024-01-25'),(3,'2024-03-10'),
-- Feb cohort
(4,'2024-02-05'),(4,'2024-03-01'),(4,'2024-04-10'),
(5,'2024-02-10'),(5,'2024-03-05'),
(6,'2024-02-20'),
-- Mar cohort
(7,'2024-03-01'),(7,'2024-04-05'),
(8,'2024-03-15'),(8,'2024-04-20'),(8,'2024-05-10'),
(9,'2024-03-20');

-- Solution:
WITH cohorts AS (
    SELECT
        user_id,
        DATE_TRUNC('month', signup_date)::DATE AS cohort_month
    FROM user_signups
),
activity AS (
    SELECT
        c.user_id,
        c.cohort_month,
        DATE_TRUNC('month', ul.login_date)::DATE AS activity_month,
        EXTRACT(YEAR FROM AGE(
            DATE_TRUNC('month', ul.login_date),
            c.cohort_month
        )) * 12 +
        EXTRACT(MONTH FROM AGE(
            DATE_TRUNC('month', ul.login_date),
            c.cohort_month
        )) AS months_since_signup
    FROM cohorts c
    JOIN user_logins ul ON c.user_id = ul.user_id
    GROUP BY c.user_id, c.cohort_month, DATE_TRUNC('month', ul.login_date)
),
cohort_sizes AS (
    SELECT cohort_month, COUNT(*) AS cohort_size
    FROM cohorts
    GROUP BY cohort_month
)
SELECT
    cs.cohort_month,
    cs.cohort_size                                                          AS month_0_users,
    ROUND(COUNT(DISTINCT CASE WHEN a.months_since_signup = 1 THEN a.user_id END) * 100.0
          / cs.cohort_size, 0)                                             AS ret_m1_pct,
    ROUND(COUNT(DISTINCT CASE WHEN a.months_since_signup = 2 THEN a.user_id END) * 100.0
          / cs.cohort_size, 0)                                             AS ret_m2_pct,
    ROUND(COUNT(DISTINCT CASE WHEN a.months_since_signup = 3 THEN a.user_id END) * 100.0
          / cs.cohort_size, 0)                                             AS ret_m3_pct
FROM cohort_sizes cs
LEFT JOIN activity a ON cs.cohort_month = a.cohort_month
GROUP BY cs.cohort_month, cs.cohort_size
ORDER BY cs.cohort_month;

-- Expected Output:
-- cohort_month | month_0_users | ret_m1_pct | ret_m2_pct | ret_m3_pct
-- -------------+---------------+------------+------------+-----------
-- 2024-01-01   | 3             | 100        | 100        | 67
-- 2024-02-01   | 3             | 67         | 67         | 33
-- 2024-03-01   | 3             | 67         | 33         | 0

-- Explanation:
-- Cohort month is derived from signup_date. months_since_signup is the integer
-- month distance from cohort_month to each login month. CASE WHEN filters to
-- each retention period; COUNT DISTINCT prevents counting a user twice.
--
-- Follow-up: Pivot the output into a visual triangle heatmap format using
--            conditional aggregation and show absolute user counts alongside %.


-- Problem 076: Percentile Distribution of Transaction Amounts per Country
-- Difficulty: Medium
-- Category: Complex Aggregations & Metrics
-- Tags: percentiles, PERCENTILE_CONT, distribution, transactions
--
-- Scenario:
-- A fraud analytics team wants to understand the distribution of transaction
-- amounts per country to set risk thresholds. They need P25, P50, P75, P90,
-- P99 percentiles and the mean for each country.
--
-- Question:
-- Compute the statistical distribution of transaction amounts per country.
-- Output: country, tx_count, mean_amount, p25, p50, p75, p90, p99.

-- Schema & Sample Data:
CREATE TABLE transactions_geo (
    tx_id       BIGINT PRIMARY KEY,
    user_id     INT,
    country     VARCHAR(5),
    amount      NUMERIC(10,2),
    tx_date     DATE
);

INSERT INTO transactions_geo VALUES
(1,  101, 'US', 25.00, '2024-05-01'), (2,  102, 'US', 150.00,'2024-05-01'),
(3,  103, 'US', 75.00, '2024-05-02'), (4,  104, 'US', 500.00,'2024-05-02'),
(5,  105, 'US', 30.00, '2024-05-03'), (6,  106, 'US', 200.00,'2024-05-03'),
(7,  107, 'US', 4500.00,'2024-05-04'),(8,  108, 'US', 60.00, '2024-05-04'),
(9,  109, 'GB', 80.00, '2024-05-01'), (10, 110, 'GB', 120.00,'2024-05-01'),
(11, 111, 'GB', 300.00,'2024-05-02'), (12, 112, 'GB', 45.00, '2024-05-02'),
(13, 113, 'GB', 1500.00,'2024-05-03'),(14, 114, 'DE', 200.00,'2024-05-01'),
(15, 115, 'DE', 350.00,'2024-05-02'), (16, 116, 'DE', 180.00,'2024-05-03');

-- Solution:
SELECT
    country,
    COUNT(*)                                        AS tx_count,
    ROUND(AVG(amount), 2)                           AS mean_amount,
    PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY amount) AS p25,
    PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY amount) AS p50,
    PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY amount) AS p75,
    PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY amount) AS p90,
    PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY amount) AS p99
FROM transactions_geo
GROUP BY country
ORDER BY country;

-- Expected Output:
-- country | tx_count | mean_amount | p25    | p50    | p75     | p90     | p99
-- --------+----------+-------------+--------+--------+---------+---------+-------
-- DE      | 3        | 243.33      | 190.00 | 200.00 | 275.00  | 335.00  | 349.50
-- GB      | 5        | 409.00      | 80.00  | 120.00 | 300.00  | 1140.00 | 1470.00
-- US      | 8        | 692.50      | 37.50  | 117.50 | 287.50  | 2540.00 | 4365.00

-- Explanation:
-- PERCENTILE_CONT is an ordered-set aggregate that computes interpolated
-- percentile values. It is more accurate than NTILE for small datasets.
-- Each percentile is computed within the country partition via GROUP BY.
--
-- Follow-up: Flag transactions in each country that exceed the country's P99
--            threshold as potential high-value fraud candidates.


-- Problem 077: Funnel Conversion Analysis — Step-by-Step Drop-off Rates
-- Difficulty: Hard
-- Category: Complex Aggregations & Metrics
-- Tags: funnel analysis, conversion rates, conditional aggregation, product
--
-- Scenario:
-- A checkout funnel has 5 steps: VIEW_PRODUCT → ADD_TO_CART → BEGIN_CHECKOUT
-- → PAYMENT_INFO → PURCHASE. A user must complete steps in order on the same
-- session_id. Calculate the unique users at each step and drop-off between steps.
--
-- Question:
-- For each funnel step, report users who reached that step and the conversion
-- rate from the previous step.
-- Output: step_name, step_order, users_reached, conversion_from_prev_pct.

-- Schema & Sample Data:
CREATE TABLE funnel_events (
    event_id    BIGINT PRIMARY KEY,
    session_id  BIGINT,
    user_id     INT,
    event_name  VARCHAR(50),
    event_time  TIMESTAMP
);

INSERT INTO funnel_events VALUES
(1,  9001, 201, 'VIEW_PRODUCT',   '2024-06-01 10:00:00'),
(2,  9001, 201, 'ADD_TO_CART',    '2024-06-01 10:01:00'),
(3,  9001, 201, 'BEGIN_CHECKOUT', '2024-06-01 10:02:00'),
(4,  9001, 201, 'PAYMENT_INFO',   '2024-06-01 10:03:00'),
(5,  9001, 201, 'PURCHASE',       '2024-06-01 10:05:00'),
(6,  9002, 202, 'VIEW_PRODUCT',   '2024-06-01 11:00:00'),
(7,  9002, 202, 'ADD_TO_CART',    '2024-06-01 11:02:00'),
(8,  9002, 202, 'BEGIN_CHECKOUT', '2024-06-01 11:04:00'),
(9,  9003, 203, 'VIEW_PRODUCT',   '2024-06-01 12:00:00'),
(10, 9003, 203, 'ADD_TO_CART',    '2024-06-01 12:01:00'),
(11, 9004, 204, 'VIEW_PRODUCT',   '2024-06-01 13:00:00'),
(12, 9005, 205, 'VIEW_PRODUCT',   '2024-06-01 14:00:00'),
(13, 9005, 205, 'ADD_TO_CART',    '2024-06-01 14:02:00'),
(14, 9005, 205, 'BEGIN_CHECKOUT', '2024-06-01 14:05:00'),
(15, 9005, 205, 'PAYMENT_INFO',   '2024-06-01 14:07:00');

-- Solution:
WITH step_counts AS (
    SELECT
        1 AS step_order, 'VIEW_PRODUCT'   AS step_name,
        COUNT(DISTINCT user_id) AS users_reached
    FROM funnel_events WHERE event_name = 'VIEW_PRODUCT'
    UNION ALL
    SELECT 2, 'ADD_TO_CART',
        COUNT(DISTINCT user_id)
    FROM funnel_events WHERE event_name = 'ADD_TO_CART'
    UNION ALL
    SELECT 3, 'BEGIN_CHECKOUT',
        COUNT(DISTINCT user_id)
    FROM funnel_events WHERE event_name = 'BEGIN_CHECKOUT'
    UNION ALL
    SELECT 4, 'PAYMENT_INFO',
        COUNT(DISTINCT user_id)
    FROM funnel_events WHERE event_name = 'PAYMENT_INFO'
    UNION ALL
    SELECT 5, 'PURCHASE',
        COUNT(DISTINCT user_id)
    FROM funnel_events WHERE event_name = 'PURCHASE'
),
with_prev AS (
    SELECT
        step_order,
        step_name,
        users_reached,
        LAG(users_reached) OVER (ORDER BY step_order) AS prev_step_users
    FROM step_counts
)
SELECT
    step_name,
    step_order,
    users_reached,
    ROUND(users_reached * 100.0 / NULLIF(prev_step_users, 0), 1) AS conversion_from_prev_pct
FROM with_prev
ORDER BY step_order;

-- Expected Output:
-- step_name       | step_order | users_reached | conversion_from_prev_pct
-- ----------------+------------+---------------+-------------------------
-- VIEW_PRODUCT    | 1          | 5             | NULL
-- ADD_TO_CART     | 2          | 4             | 80.0
-- BEGIN_CHECKOUT  | 3          | 3             | 75.0
-- PAYMENT_INFO    | 4          | 2             | 66.7
-- PURCHASE        | 5          | 1             | 50.0

-- Explanation:
-- Each UNION ALL step counts distinct users who fired that event. LAG gives
-- the prior step's user count. Conversion = current / prior * 100. NULLIF
-- prevents divide-by-zero at step 1.
--
-- Follow-up: Segment the funnel by device_type or acquisition channel to
--            compare conversion rates across user segments.


-- Problem 078: Year-over-Year Growth Rate by Month (YoY)
-- Difficulty: Medium
-- Category: Complex Aggregations & Metrics
-- Tags: YoY, LAG, window functions, time series, revenue
--
-- Scenario:
-- A finance team tracks monthly subscription revenue. They want a YoY comparison
-- report showing revenue for each month vs the same month last year, along with
-- the growth rate. Some months have no prior year data (new product lines).
--
-- Question:
-- Compute YoY monthly revenue, absolute change, and growth percentage.
-- Output: month, revenue, prev_year_revenue, yoy_change, yoy_growth_pct.

-- Schema & Sample Data:
CREATE TABLE monthly_subscription_revenue (
    revenue_month  DATE PRIMARY KEY,  -- always 1st of month
    revenue        NUMERIC(12,2)
);

INSERT INTO monthly_subscription_revenue VALUES
('2023-01-01', 80000), ('2023-02-01', 82000), ('2023-03-01', 85000),
('2023-04-01', 88000), ('2023-05-01', 90000), ('2023-06-01', 92000),
('2023-07-01', 89000), ('2023-08-01', 91000), ('2023-09-01', 95000),
('2023-10-01', 97000), ('2023-11-01', 102000),('2023-12-01', 115000),
('2024-01-01', 95000), ('2024-02-01', 97000), ('2024-03-01', 101000),
('2024-04-01', 105000),('2024-05-01', 108000),('2024-06-01', 112000);

-- Solution:
WITH with_prev_year AS (
    SELECT
        revenue_month,
        revenue,
        LAG(revenue, 12) OVER (ORDER BY revenue_month) AS prev_year_revenue
    FROM monthly_subscription_revenue
)
SELECT
    revenue_month                                                               AS month,
    revenue,
    prev_year_revenue,
    revenue - prev_year_revenue                                                 AS yoy_change,
    ROUND((revenue - prev_year_revenue) * 100.0 / NULLIF(prev_year_revenue, 0), 1) AS yoy_growth_pct
FROM with_prev_year
ORDER BY revenue_month;

-- Expected Output (2024 rows only):
-- month      | revenue  | prev_year_revenue | yoy_change | yoy_growth_pct
-- -----------+----------+-------------------+------------+---------------
-- 2024-01-01 | 95000    | 80000             | 15000      | 18.8
-- 2024-02-01 | 97000    | 82000             | 15000      | 18.3
-- 2024-03-01 | 101000   | 85000             | 16000      | 18.8
-- 2024-04-01 | 105000   | 88000             | 17000      | 19.3
-- 2024-05-01 | 108000   | 90000             | 18000      | 20.0
-- 2024-06-01 | 112000   | 92000             | 20000      | 21.7

-- Explanation:
-- LAG(revenue, 12) looks back exactly 12 rows (months) in the ordered window,
-- giving same-month prior-year revenue. This works cleanly when data is monthly
-- without gaps. NULLIF prevents division by zero for new months.
--
-- Follow-up: Add a 3-month trailing average for both years to smooth seasonality
--            and show whether the trend is accelerating.


-- Problem 079: Top-N Products per Category by Revenue (Dense Ranking)
-- Difficulty: Medium
-- Category: Complex Aggregations & Metrics
-- Tags: DENSE_RANK, top-N, window functions, product analytics
--
-- Scenario:
-- A merchandising team wants to see the top 3 products per category by total
-- revenue, with dense ranking so tied products share the same rank. Products
-- not in the top 3 should be excluded.
--
-- Question:
-- Return the top 3 products per category by total revenue with dense rank.
-- Output: category, rank, product_id, product_name, total_revenue.

-- Schema & Sample Data:
CREATE TABLE product_sales (
    sale_id     BIGINT PRIMARY KEY,
    product_id  INT,
    product_name VARCHAR(50),
    category    VARCHAR(30),
    sale_date   DATE,
    revenue     NUMERIC(10,2)
);

INSERT INTO product_sales VALUES
(1,  101, 'Widget A',  'Electronics', '2024-07-01', 500),
(2,  102, 'Widget B',  'Electronics', '2024-07-01', 300),
(3,  103, 'Widget C',  'Electronics', '2024-07-01', 700),
(4,  101, 'Widget A',  'Electronics', '2024-07-02', 400),
(5,  104, 'Gadget X',  'Electronics', '2024-07-02', 600),
(6,  105, 'Gadget Y',  'Electronics', '2024-07-02', 300),
(7,  201, 'Shirt M',   'Clothing',    '2024-07-01', 100),
(8,  202, 'Shirt L',   'Clothing',    '2024-07-01', 150),
(9,  203, 'Jacket A',  'Clothing',    '2024-07-01', 300),
(10, 204, 'Jacket B',  'Clothing',    '2024-07-02', 300),  -- tied with Jacket A
(11, 205, 'Pants X',   'Clothing',    '2024-07-02', 80),
(12, 201, 'Shirt M',   'Clothing',    '2024-07-02', 120),
(13, 301, 'Desk A',    'Furniture',   '2024-07-01', 800),
(14, 302, 'Chair B',   'Furniture',   '2024-07-01', 400),
(15, 303, 'Lamp C',    'Furniture',   '2024-07-02', 200);

-- Solution:
WITH product_totals AS (
    SELECT
        category,
        product_id,
        product_name,
        SUM(revenue) AS total_revenue
    FROM product_sales
    GROUP BY category, product_id, product_name
),
ranked AS (
    SELECT
        category,
        product_id,
        product_name,
        total_revenue,
        DENSE_RANK() OVER (PARTITION BY category ORDER BY total_revenue DESC) AS rnk
    FROM product_totals
)
SELECT category, rnk AS rank, product_id, product_name, total_revenue
FROM ranked
WHERE rnk <= 3
ORDER BY category, rnk, product_id;

-- Expected Output:
-- category    | rank | product_id | product_name | total_revenue
-- ------------+------+------------+--------------+--------------
-- Clothing    | 1    | 203        | Jacket A     | 300
-- Clothing    | 1    | 204        | Jacket B     | 300
-- Clothing    | 2    | 201        | Shirt M      | 220
-- Clothing    | 3    | 202        | Shirt L      | 150
-- Electronics | 1    | 103        | Widget C     | 700
-- Electronics | 2    | 104        | Gadget X     | 600
-- Electronics | 3    | 101        | Widget A     | 900
-- Furniture   | 1    | 301        | Desk A       | 800
-- Furniture   | 2    | 302        | Chair B      | 400
-- Furniture   | 3    | 303        | Lamp C       | 200

-- Explanation:
-- Aggregate to product-level totals first, then apply DENSE_RANK within each
-- category. Ties receive the same rank; the next rank skips no numbers.
-- Filter WHERE rnk <= 3 limits results to top 3 ranks (may include more rows if tied).
--
-- Follow-up: Modify to return only the single top product per category
--            (rank = 1), and when tied pick the one with the earliest first_sale_date.


-- Problem 080: Pivot — Monthly Signups by Acquisition Channel
-- Difficulty: Medium
-- Category: Complex Aggregations & Metrics
-- Tags: pivot, conditional aggregation, acquisition, monthly
--
-- Scenario:
-- A growth analytics team wants a pivot table showing monthly signups broken
-- down by acquisition channel (organic, paid_search, referral, social) as
-- columns. This is a classic pivot from rows to columns using conditional agg.
--
-- Question:
-- Pivot signup counts by month (rows) × acquisition channel (columns).
-- Output: signup_month, organic, paid_search, referral, social, total.

-- Schema & Sample Data:
CREATE TABLE user_acquisitions (
    user_id    INT PRIMARY KEY,
    channel    VARCHAR(20),  -- 'organic','paid_search','referral','social'
    signup_date DATE
);

INSERT INTO user_acquisitions VALUES
(1,'organic',    '2024-04-01'),(2,'paid_search','2024-04-03'),
(3,'referral',   '2024-04-05'),(4,'organic',    '2024-04-10'),
(5,'social',     '2024-04-15'),(6,'paid_search','2024-04-20'),
(7,'organic',    '2024-05-01'),(8,'organic',    '2024-05-05'),
(9,'referral',   '2024-05-08'),(10,'social',    '2024-05-10'),
(11,'paid_search','2024-05-12'),(12,'paid_search','2024-05-18'),
(13,'social',    '2024-05-20'),(14,'organic',   '2024-06-01'),
(15,'referral',  '2024-06-05'),(16,'paid_search','2024-06-10'),
(17,'social',    '2024-06-15'),(18,'social',    '2024-06-20');

-- Solution:
SELECT
    DATE_TRUNC('month', signup_date)::DATE  AS signup_month,
    COUNT(*) FILTER (WHERE channel = 'organic')     AS organic,
    COUNT(*) FILTER (WHERE channel = 'paid_search') AS paid_search,
    COUNT(*) FILTER (WHERE channel = 'referral')    AS referral,
    COUNT(*) FILTER (WHERE channel = 'social')      AS social,
    COUNT(*)                                        AS total
FROM user_acquisitions
GROUP BY DATE_TRUNC('month', signup_date)
ORDER BY signup_month;

-- Expected Output:
-- signup_month | organic | paid_search | referral | social | total
-- -------------+---------+-------------+----------+--------+------
-- 2024-04-01   | 2       | 2           | 1        | 1      | 6
-- 2024-05-01   | 2       | 2           | 1        | 2      | 7
-- 2024-06-01   | 1       | 1           | 1        | 2      | 5

-- Explanation:
-- COUNT(*) FILTER (WHERE ...) is the PostgreSQL aggregate filter syntax for
-- conditional counting, equivalent to SUM(CASE WHEN ... THEN 1 ELSE 0 END).
-- GROUP BY month provides one row per month.
--
-- Follow-up: Add a row for the grand total using GROUPING SETS or ROLLUP.


-- Problem 081: Cumulative Revenue with Running Total and % of Grand Total
-- Difficulty: Medium
-- Category: Complex Aggregations & Metrics
-- Tags: cumulative sum, running total, window functions, revenue
--
-- Scenario:
-- A finance controller needs a daily revenue report showing: daily revenue,
-- cumulative (running) revenue for the month, and what percentage of the
-- month's total revenue each day represents (Pareto analysis).
--
-- Question:
-- For each day, compute revenue, running_total, pct_of_monthly_total.
-- Output: revenue_date, daily_revenue, running_total, pct_of_month_total.

-- Schema & Sample Data:
CREATE TABLE daily_revenue (
    revenue_date  DATE PRIMARY KEY,
    revenue       NUMERIC(12,2)
);

INSERT INTO daily_revenue VALUES
('2024-08-01', 12000), ('2024-08-02', 8500),  ('2024-08-03', 15000),
('2024-08-04', 6000),  ('2024-08-05', 19000), ('2024-08-06', 11000),
('2024-08-07', 9000),  ('2024-08-08', 14000), ('2024-08-09', 7500),
('2024-08-10', 22000), ('2024-08-11', 16000), ('2024-08-12', 13000);

-- Solution:
SELECT
    revenue_date,
    revenue                                                                 AS daily_revenue,
    SUM(revenue) OVER (ORDER BY revenue_date ROWS UNBOUNDED PRECEDING)     AS running_total,
    ROUND(
        revenue * 100.0 / SUM(revenue) OVER (), 1
    )                                                                       AS pct_of_month_total
FROM daily_revenue
ORDER BY revenue_date;

-- Expected Output:
-- revenue_date | daily_revenue | running_total | pct_of_month_total
-- -------------+---------------+---------------+-------------------
-- 2024-08-01   | 12000         | 12000         | 7.2
-- 2024-08-02   | 8500          | 20500         | 5.1
-- 2024-08-03   | 15000         | 35500         | 9.0
-- 2024-08-04   | 6000          | 41500         | 3.6
-- 2024-08-05   | 19000         | 60500         | 11.4
-- 2024-08-06   | 11000         | 71500         | 6.6
-- 2024-08-07   | 9000          | 80500         | 5.4
-- 2024-08-08   | 14000         | 94500         | 8.4
-- 2024-08-09   | 7500          | 102000        | 4.5
-- 2024-08-10   | 22000         | 124000        | 13.2
-- 2024-08-11   | 16000         | 140000        | 9.6
-- 2024-08-12   | 13000         | 153000        | 7.8

-- Explanation:
-- SUM with ROWS UNBOUNDED PRECEDING gives a running total. SUM OVER () (no
-- partition) gives the grand total for the window's entire dataset.
-- Dividing daily revenue by grand total and multiplying by 100 gives share %.
--
-- Follow-up: Find the first day each month when cumulative revenue crossed
--            50% of the month's total (the "50% mark" day).


-- Problem 082: N-Day Retention — Returning Users After First Purchase
-- Difficulty: Hard
-- Category: Complex Aggregations & Metrics
-- Tags: retention, N-day, self-join, cohort, e-commerce
--
-- Scenario:
-- An e-commerce team tracks repeat purchase behavior. They define "Day-N
-- retention" as: of users who made their first purchase on day 0, what
-- fraction made another purchase on exactly day 7, day 14, or day 30?
--
-- Question:
-- For each user's first purchase date (cohort), report Day-7, Day-14, and
-- Day-30 retention rates.
-- Output: first_purchase_date, cohort_size, d7_ret_pct, d14_ret_pct, d30_ret_pct.

-- Schema & Sample Data:
CREATE TABLE purchase_events (
    purchase_id   BIGINT PRIMARY KEY,
    user_id       INT,
    purchased_at  DATE,
    amount        NUMERIC(10,2)
);

INSERT INTO purchase_events VALUES
(1,  1001, '2024-05-01', 50.00), (2,  1002, '2024-05-01', 30.00),
(3,  1003, '2024-05-01', 80.00), (4,  1004, '2024-05-01', 20.00),
(5,  1005, '2024-05-01', 60.00),
-- Day 7 (May 8) purchases
(6,  1001, '2024-05-08', 45.00), (7,  1002, '2024-05-08', 35.00),
(8,  1003, '2024-05-08', 70.00),
-- Day 14 (May 15) purchases
(9,  1001, '2024-05-15', 55.00), (10, 1004, '2024-05-15', 25.00),
-- Day 30 (May 31) purchases
(11, 1002, '2024-05-31', 40.00), (12, 1005, '2024-05-31', 65.00),
-- Non-cohort dates
(13, 1001, '2024-05-20', 30.00);

-- Solution:
WITH first_purchase AS (
    SELECT user_id, MIN(purchased_at) AS first_purchase_date
    FROM purchase_events
    GROUP BY user_id
),
cohort_sizes AS (
    SELECT first_purchase_date, COUNT(*) AS cohort_size
    FROM first_purchase
    GROUP BY first_purchase_date
)
SELECT
    cs.first_purchase_date,
    cs.cohort_size,
    ROUND(
        COUNT(DISTINCT CASE WHEN pe.purchased_at = fp.first_purchase_date + 7  THEN pe.user_id END)
        * 100.0 / cs.cohort_size, 0
    ) AS d7_ret_pct,
    ROUND(
        COUNT(DISTINCT CASE WHEN pe.purchased_at = fp.first_purchase_date + 14 THEN pe.user_id END)
        * 100.0 / cs.cohort_size, 0
    ) AS d14_ret_pct,
    ROUND(
        COUNT(DISTINCT CASE WHEN pe.purchased_at = fp.first_purchase_date + 30 THEN pe.user_id END)
        * 100.0 / cs.cohort_size, 0
    ) AS d30_ret_pct
FROM cohort_sizes cs
JOIN first_purchase fp ON cs.first_purchase_date = fp.first_purchase_date
LEFT JOIN purchase_events pe ON fp.user_id = pe.user_id
GROUP BY cs.first_purchase_date, cs.cohort_size
ORDER BY cs.first_purchase_date;

-- Expected Output:
-- first_purchase_date | cohort_size | d7_ret_pct | d14_ret_pct | d30_ret_pct
-- --------------------+-------------+------------+-------------+------------
-- 2024-05-01          | 5           | 60         | 40          | 40

-- Explanation:
-- MIN(purchased_at) per user identifies cohort date. CASE WHEN compares each
-- purchase date to exactly first_date + N days. COUNT DISTINCT counts unique
-- returning users. Dividing by cohort_size gives the retention rate.
--
-- Follow-up: Expand to "within N days" (any purchase within 7 days, not exact)
--            which is a more common and forgiving definition of early retention.


-- Problem 083: Anomaly Detection — Days Where Metric Deviates > 2 StdDev
-- Difficulty: Hard
-- Category: Complex Aggregations & Metrics
-- Tags: anomaly detection, standard deviation, window functions, monitoring
--
-- Scenario:
-- A data reliability team monitors daily error rates for a data pipeline.
-- They want to flag any day where the error rate is more than 2 standard
-- deviations from the 30-day rolling mean (both above and below), which
-- indicates a pipeline incident or an unusually clean run.
--
-- Question:
-- Flag days where error_rate deviates more than 2 stddev from the 30-day
-- rolling mean (using population stddev). Output: metric_date, error_rate,
-- rolling_mean, rolling_stddev, z_score, is_anomaly.

-- Schema & Sample Data:
CREATE TABLE pipeline_error_rates (
    metric_date DATE PRIMARY KEY,
    error_rate  NUMERIC(6,4)  -- fraction e.g. 0.0120 = 1.2%
);

INSERT INTO pipeline_error_rates VALUES
('2024-09-01', 0.0120), ('2024-09-02', 0.0115), ('2024-09-03', 0.0118),
('2024-09-04', 0.0122), ('2024-09-05', 0.0119), ('2024-09-06', 0.0121),
('2024-09-07', 0.0117), ('2024-09-08', 0.0123), ('2024-09-09', 0.0116),
('2024-09-10', 0.0120), ('2024-09-11', 0.0119), ('2024-09-12', 0.0121),
('2024-09-13', 0.0118), ('2024-09-14', 0.0122), ('2024-09-15', 0.0500),  -- anomaly HIGH
('2024-09-16', 0.0120), ('2024-09-17', 0.0118), ('2024-09-18', 0.0119),
('2024-09-19', 0.0121), ('2024-09-20', 0.0120), ('2024-09-21', 0.0119),
('2024-09-22', 0.0122), ('2024-09-23', 0.0118), ('2024-09-24', 0.0120),
('2024-09-25', 0.0119), ('2024-09-26', 0.0121), ('2024-09-27', 0.0120),
('2024-09-28', 0.0118), ('2024-09-29', 0.0119), ('2024-09-30', 0.0010);  -- anomaly LOW

-- Solution:
WITH rolling_stats AS (
    SELECT
        metric_date,
        error_rate,
        AVG(error_rate) OVER (
            ORDER BY metric_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_mean,
        STDDEV_POP(error_rate) OVER (
            ORDER BY metric_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS rolling_stddev
    FROM pipeline_error_rates
)
SELECT
    metric_date,
    error_rate,
    ROUND(rolling_mean, 6)   AS rolling_mean,
    ROUND(rolling_stddev, 6) AS rolling_stddev,
    ROUND(
        (error_rate - rolling_mean) / NULLIF(rolling_stddev, 0), 2
    ) AS z_score,
    ABS(error_rate - rolling_mean) > 2 * NULLIF(rolling_stddev, 0) AS is_anomaly
FROM rolling_stats
WHERE rolling_stddev IS NOT NULL AND rolling_stddev > 0
ORDER BY metric_date;

-- Expected Output (anomaly rows):
-- metric_date | error_rate | rolling_mean | rolling_stddev | z_score | is_anomaly
-- ------------+------------+--------------+----------------+---------+-----------
-- 2024-09-15  | 0.0500     | ~0.0120      | ~0.0010        | ~38.0   | true
-- 2024-09-30  | 0.0010     | ~0.0148      | ~0.0096        | ~-1.4   | false (depends on window)

-- Explanation:
-- AVG and STDDEV_POP with a 30-row rolling window compute the baseline
-- statistics. Z-score = (value - mean) / stddev. ABS(z_score) > 2 flags
-- anomalies. NULLIF protects against zero stddev for early uniform windows.
--
-- Follow-up: Use a 7-day window instead and compare which window size catches
--            real anomalies vs false positives on this dataset.


-- Problem 084: Histogram Buckets — Transaction Amounts in $50 Bins
-- Difficulty: Medium
-- Category: Complex Aggregations & Metrics
-- Tags: histogram, bucket, WIDTH_BUCKET, distribution
--
-- Scenario:
-- A fraud team wants to understand the distribution of transaction amounts
-- via a histogram with $50-wide buckets (0-50, 50-100, etc. up to 500+).
-- They want the count and percentage of transactions in each bucket.
--
-- Question:
-- Build a transaction amount histogram in $50 buckets up to $500,
-- with a catch-all bucket for amounts > $500.
-- Output: bucket_label, tx_count, pct_of_total.

-- Schema & Sample Data:
CREATE TABLE txn_amounts (
    tx_id   BIGINT PRIMARY KEY,
    amount  NUMERIC(10,2)
);

INSERT INTO txn_amounts VALUES
(1,25.00),(2,48.00),(3,60.00),(4,95.00),(5,110.00),(6,140.00),
(7,175.00),(8,200.00),(9,220.00),(10,250.00),(11,290.00),(12,310.00),
(13,350.00),(14,400.00),(15,450.00),(16,520.00),(17,750.00),(18,1200.00),
(19,15.00),(20,85.00);

-- Solution:
WITH bucketed AS (
    SELECT
        tx_id,
        amount,
        CASE
            WHEN amount <= 50  THEN '$0-50'
            WHEN amount <= 100 THEN '$51-100'
            WHEN amount <= 150 THEN '$101-150'
            WHEN amount <= 200 THEN '$151-200'
            WHEN amount <= 250 THEN '$201-250'
            WHEN amount <= 300 THEN '$251-300'
            WHEN amount <= 350 THEN '$301-350'
            WHEN amount <= 400 THEN '$351-400'
            WHEN amount <= 450 THEN '$401-450'
            WHEN amount <= 500 THEN '$451-500'
            ELSE '$500+'
        END AS bucket_label,
        CASE
            WHEN amount <= 50  THEN 1
            WHEN amount <= 100 THEN 2
            WHEN amount <= 150 THEN 3
            WHEN amount <= 200 THEN 4
            WHEN amount <= 250 THEN 5
            WHEN amount <= 300 THEN 6
            WHEN amount <= 350 THEN 7
            WHEN amount <= 400 THEN 8
            WHEN amount <= 450 THEN 9
            WHEN amount <= 500 THEN 10
            ELSE 11
        END AS bucket_order
    FROM txn_amounts
)
SELECT
    bucket_label,
    COUNT(*) AS tx_count,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 1) AS pct_of_total
FROM bucketed
GROUP BY bucket_label, bucket_order
ORDER BY bucket_order;

-- Expected Output:
-- bucket_label | tx_count | pct_of_total
-- -------------+----------+-------------
-- $0-50        | 3        | 15.0
-- $51-100      | 3        | 15.0
-- $101-150     | 1        | 5.0
-- $151-200     | 2        | 10.0
-- $201-250     | 2        | 10.0
-- $251-300     | 2        | 10.0
-- $301-350     | 2        | 10.0
-- $351-400     | 1        | 5.0
-- $401-450     | 1        | 5.0
-- $451-500     | 0        | 0.0  (no rows → excluded unless using generate_series)
-- $500+        | 3        | 15.0

-- Explanation:
-- CASE expressions create discrete buckets with a sortable bucket_order.
-- COUNT(*) with a window SUM gives both absolute count and percentage.
-- Note: empty buckets are excluded unless the bucket range is pre-generated.
--
-- Follow-up: Use WIDTH_BUCKET(amount, 0, 500, 10) for a more compact version
--            that dynamically computes bucket boundaries.


-- Problem 085: Active Users Metric — WAU and MAU from Daily Events
-- Difficulty: Medium
-- Category: Complex Aggregations & Metrics
-- Tags: WAU, MAU, distinct counts, window functions, product metrics
--
-- Scenario:
-- A product analytics team needs to compute WAU (Weekly Active Users) and
-- MAU (Monthly Active Users) from raw daily event logs. The stickiness ratio
-- (DAU/MAU) should also be computed for each day.
--
-- Question:
-- For each day, compute: DAU (that day), WAU (last 7 days), MAU (last 30 days),
-- and stickiness = DAU/MAU.
-- Output: activity_date, dau, wau, mau, stickiness_pct.

-- Schema & Sample Data:
CREATE TABLE app_events (
    event_id      BIGINT PRIMARY KEY,
    user_id       INT,
    activity_date DATE
);

INSERT INTO app_events VALUES
(1,101,'2024-10-01'),(2,102,'2024-10-01'),(3,103,'2024-10-01'),
(4,101,'2024-10-02'),(5,104,'2024-10-02'),(6,105,'2024-10-02'),
(7,101,'2024-10-05'),(8,102,'2024-10-05'),(9,106,'2024-10-05'),
(10,103,'2024-10-07'),(11,107,'2024-10-07'),(12,108,'2024-10-07'),
(13,101,'2024-10-08'),(14,109,'2024-10-08'),(15,110,'2024-10-08'),
(16,102,'2024-10-10'),(17,111,'2024-10-10'),(18,112,'2024-10-10'),
(19,101,'2024-10-12'),(20,113,'2024-10-12'),(21,114,'2024-10-12'),
(22,103,'2024-10-15'),(23,115,'2024-10-15'),(24,116,'2024-10-15');

-- Solution:
WITH daily_users AS (
    SELECT DISTINCT activity_date, user_id FROM app_events
),
dau AS (
    SELECT activity_date, COUNT(DISTINCT user_id) AS dau
    FROM daily_users
    GROUP BY activity_date
)
SELECT
    d.activity_date,
    d.dau,
    (SELECT COUNT(DISTINCT du2.user_id)
     FROM daily_users du2
     WHERE du2.activity_date BETWEEN d.activity_date - 6 AND d.activity_date
    ) AS wau,
    (SELECT COUNT(DISTINCT du3.user_id)
     FROM daily_users du3
     WHERE du3.activity_date BETWEEN d.activity_date - 29 AND d.activity_date
    ) AS mau,
    ROUND(
        d.dau * 100.0 /
        NULLIF((SELECT COUNT(DISTINCT du3.user_id)
                FROM daily_users du3
                WHERE du3.activity_date BETWEEN d.activity_date - 29 AND d.activity_date), 0),
        1
    ) AS stickiness_pct
FROM dau d
ORDER BY d.activity_date;

-- Expected Output (partial):
-- activity_date | dau | wau | mau | stickiness_pct
-- --------------+-----+-----+-----+---------------
-- 2024-10-01    | 3   | 3   | 3   | 100.0
-- 2024-10-02    | 3   | 6   | 6   | 50.0
-- 2024-10-05    | 3   | 9   | 9   | 33.3
-- 2024-10-07    | 3   | 9   | 12  | 25.0
-- 2024-10-08    | 3   | 9   | 15  | 20.0
-- 2024-10-10    | 3   | 9   | 18  | 16.7
-- 2024-10-12    | 3   | 9   | 21  | 14.3
-- 2024-10-15    | 3   | 9   | 24  | 12.5

-- Explanation:
-- DAU is a simple GROUP BY count. WAU and MAU use correlated subqueries with
-- date range conditions to count distinct users in a 7-day and 30-day lookback.
-- Stickiness = DAU/MAU, a key product engagement health metric.
--
-- Follow-up: Rewrite using window functions with RANGE BETWEEN to avoid
--            correlated subqueries for better performance at scale.


-- Problem 086: Compute Week-over-Week Retention with a Spine
-- Difficulty: Hard
-- Category: Complex Aggregations & Metrics
-- Tags: retention, spine, CROSS JOIN, cohort, weekly
--
-- Scenario:
-- A subscription platform runs a weekly cohort retention model. For each
-- weekly cohort, calculate how many users were still active in weeks 1, 2,
-- and 3 after signup. Use a calendar spine to ensure weeks with zero
-- retention show as 0 (not missing rows).
--
-- Question:
-- Build a weekly cohort retention table with a spine ensuring every
-- cohort × retention_week combination exists, even if count = 0.
-- Output: cohort_week, retention_week, users_active, retention_pct.

-- Schema & Sample Data:
CREATE TABLE weekly_user_activity (
    user_id       INT,
    signup_week   DATE,  -- Monday of signup week
    active_week   DATE,  -- Monday of activity week
    PRIMARY KEY (user_id, active_week)
);

INSERT INTO weekly_user_activity VALUES
-- Cohort: 2024-W10 (Mar 4)
(1,'2024-03-04','2024-03-04'),(1,'2024-03-04','2024-03-11'),(1,'2024-03-04','2024-03-18'),
(2,'2024-03-04','2024-03-04'),(2,'2024-03-04','2024-03-11'),
(3,'2024-03-04','2024-03-04'),
(4,'2024-03-04','2024-03-04'),(4,'2024-03-04','2024-03-25'),
-- Cohort: 2024-W11 (Mar 11)
(5,'2024-03-11','2024-03-11'),(5,'2024-03-11','2024-03-18'),(5,'2024-03-11','2024-03-25'),
(6,'2024-03-11','2024-03-11'),(6,'2024-03-11','2024-03-18'),
(7,'2024-03-11','2024-03-11'),
(8,'2024-03-11','2024-03-11');

-- Solution:
WITH cohort_sizes AS (
    SELECT signup_week, COUNT(DISTINCT user_id) AS cohort_size
    FROM weekly_user_activity
    GROUP BY signup_week
),
spine AS (
    SELECT
        cs.signup_week AS cohort_week,
        cs.cohort_size,
        gs.retention_week_num
    FROM cohort_sizes cs
    CROSS JOIN GENERATE_SERIES(0, 3) AS gs(retention_week_num)
),
actives AS (
    SELECT
        signup_week,
        (active_week - signup_week) / 7 AS retention_week_num,
        COUNT(DISTINCT user_id) AS users_active
    FROM weekly_user_activity
    GROUP BY signup_week, (active_week - signup_week) / 7
)
SELECT
    s.cohort_week,
    s.retention_week_num  AS retention_week,
    COALESCE(a.users_active, 0) AS users_active,
    ROUND(COALESCE(a.users_active, 0) * 100.0 / s.cohort_size, 0) AS retention_pct
FROM spine s
LEFT JOIN actives a
    ON s.cohort_week = a.signup_week
   AND s.retention_week_num = a.retention_week_num
ORDER BY s.cohort_week, s.retention_week_num;

-- Expected Output:
-- cohort_week | retention_week | users_active | retention_pct
-- ------------+----------------+--------------+--------------
-- 2024-03-04  | 0              | 4            | 100
-- 2024-03-04  | 1              | 2            | 50
-- 2024-03-04  | 2              | 1            | 25
-- 2024-03-04  | 3              | 1            | 25
-- 2024-03-11  | 0              | 4            | 100
-- 2024-03-11  | 1              | 2            | 50
-- 2024-03-11  | 2              | 1            | 25
-- 2024-03-11  | 3              | 0            | 0

-- Explanation:
-- GENERATE_SERIES(0,3) creates the spine of retention weeks 0-3 for every
-- cohort. LEFT JOIN actives ensures weeks with zero retention appear as 0,
-- not as missing rows. (active_week - signup_week)/7 gives integer week offset.
--
-- Follow-up: Extend the spine to week 8 and use a single query to build a
--            full 8-week retention triangle for all cohorts.
