-- ============================================================
-- CATEGORY 2: CTEs & SUBQUERIES
-- Problems 021–032
-- Covers: recursive CTEs, chained CTEs, correlated subqueries,
--         EXISTS, lateral joins, subquery in FROM/WHERE/SELECT,
--         multi-step transformations, self-referential hierarchies
-- ============================================================


-- Problem 021: Chained CTEs — Multi-Step User Activation Funnel
-- Difficulty: Medium
-- Category: CTEs & Subqueries
-- Tags: chained-CTEs, funnel, activation
--
-- Scenario:
-- A growth team defines activation as: user signed up → verified email →
-- completed onboarding → made first purchase — all within 7 days of signup.
-- They want the count and conversion rate at each funnel step.
--
-- Question:
-- Using chained CTEs, compute step counts and conversion rates from signup
-- through first purchase. Return: step_name, users_reached, pct_of_signup.

-- Schema & Sample Data:
CREATE TABLE user_events_activation (
    event_id   BIGINT,
    user_id    INT,
    event_type VARCHAR(50),
    event_time TIMESTAMP
);

INSERT INTO user_events_activation VALUES
(1,  3101, 'signup',             '2024-03-01 09:00:00'),
(2,  3101, 'email_verified',     '2024-03-01 10:00:00'),
(3,  3101, 'onboarding_complete','2024-03-01 11:00:00'),
(4,  3101, 'first_purchase',     '2024-03-02 14:00:00'),
(5,  3102, 'signup',             '2024-03-01 09:30:00'),
(6,  3102, 'email_verified',     '2024-03-01 12:00:00'),
(7,  3102, 'onboarding_complete','2024-03-02 09:00:00'),
(8,  3103, 'signup',             '2024-03-01 10:00:00'),
(9,  3103, 'email_verified',     '2024-03-01 11:00:00'),
(10, 3104, 'signup',             '2024-03-01 11:00:00'),
(11, 3104, 'email_verified',     '2024-03-09 09:00:00'),  -- outside 7d window
(12, 3105, 'signup',             '2024-03-01 12:00:00'),
(13, 3105, 'email_verified',     '2024-03-01 13:00:00'),
(14, 3105, 'onboarding_complete','2024-03-01 14:00:00'),
(15, 3105, 'first_purchase',     '2024-03-03 10:00:00');

-- Solution:
WITH signups AS (
    SELECT user_id, MIN(event_time) AS signup_time
    FROM user_events_activation
    WHERE event_type = 'signup'
    GROUP BY user_id
),
verified AS (
    SELECT s.user_id
    FROM signups s
    JOIN user_events_activation e
      ON e.user_id = s.user_id
     AND e.event_type = 'email_verified'
     AND e.event_time BETWEEN s.signup_time AND s.signup_time + INTERVAL '7 days'
),
onboarded AS (
    SELECT v.user_id
    FROM verified v
    JOIN user_events_activation e
      ON e.user_id = v.user_id
     AND e.event_type = 'onboarding_complete'
),
purchased AS (
    SELECT o.user_id
    FROM onboarded o
    JOIN user_events_activation e
      ON e.user_id = o.user_id
     AND e.event_type = 'first_purchase'
),
total_signups AS (SELECT COUNT(*) AS n FROM signups)
SELECT
    step_name,
    users_reached,
    ROUND(100.0 * users_reached / (SELECT n FROM total_signups), 1) AS pct_of_signup
FROM (
    VALUES
        ('1_signup',              (SELECT COUNT(*) FROM signups)),
        ('2_email_verified',      (SELECT COUNT(*) FROM verified)),
        ('3_onboarding_complete', (SELECT COUNT(*) FROM onboarded)),
        ('4_first_purchase',      (SELECT COUNT(*) FROM purchased))
) AS t(step_name, users_reached)
ORDER BY step_name;

-- Expected Output:
-- step_name               | users_reached | pct_of_signup
-- 1_signup                |       5       |    100.0
-- 2_email_verified        |       4       |     80.0
-- 3_onboarding_complete   |       3       |     60.0
-- 4_first_purchase        |       2       |     40.0

-- Explanation:
-- Each CTE represents one funnel step, joining forward only for users who
-- completed the prior step within the constraint window. The VALUES subquery
-- at the end assembles the summary table cleanly.
--
-- Follow-up: Segment the funnel by signup_date cohort (week of signup) and
-- compare activation rates across cohorts.


-- ============================================================


-- Problem 022: Recursive CTE — Traverse a Content Moderation Appeal Hierarchy
-- Difficulty: Hard
-- Category: CTEs & Subqueries
-- Tags: recursive-CTE, hierarchy, appeals
--
-- Scenario:
-- A policy appeals system allows users to escalate appeals through manager
-- tiers. Given a parent–child table of appeals (each escalation references
-- its parent), traverse the full chain from any root appeal down to leaves.
--
-- Question:
-- For each appeal node, show its full path from root using a recursive CTE.
-- Output: appeal_id, parent_id, level, path (as a string like '1 > 3 > 7').

-- Schema & Sample Data:
CREATE TABLE appeals (
    appeal_id   INT,
    parent_id   INT,   -- NULL = root
    description VARCHAR(100)
);

INSERT INTO appeals VALUES
(1,  NULL, 'Original removal'),
(2,  NULL, 'Original removal 2'),
(3,  1,    'Level-1 appeal'),
(4,  1,    'Level-1 appeal alt'),
(5,  3,    'Level-2 escalation'),
(6,  3,    'Level-2 escalation alt'),
(7,  5,    'Level-3 final review'),
(8,  4,    'Level-2 via alt branch'),
(9,  2,    'Level-1 on second root'),
(10, 9,    'Level-2 on second root');

-- Solution:
WITH RECURSIVE appeal_tree AS (
    -- Base: root nodes
    SELECT
        appeal_id,
        parent_id,
        description,
        1                             AS level,
        appeal_id::TEXT               AS path
    FROM appeals
    WHERE parent_id IS NULL

    UNION ALL

    -- Recursive: children
    SELECT
        a.appeal_id,
        a.parent_id,
        a.description,
        at.level + 1,
        at.path || ' > ' || a.appeal_id::TEXT
    FROM appeals a
    JOIN appeal_tree at ON a.parent_id = at.appeal_id
)
SELECT appeal_id, parent_id, level, path
FROM appeal_tree
ORDER BY path;

-- Expected Output:
-- appeal_id | parent_id | level | path
--     1     |   NULL    |   1   | 1
--     3     |     1     |   2   | 1 > 3
--     5     |     3     |   3   | 1 > 3 > 5
--     7     |     5     |   4   | 1 > 3 > 5 > 7
--     6     |     3     |   3   | 1 > 3 > 6
--     4     |     1     |   2   | 1 > 4
--     8     |     4     |   3   | 1 > 4 > 8
--     2     |   NULL    |   1   | 2
--     9     |     2     |   2   | 2 > 9
--    10     |     9     |   3   | 2 > 9 > 10

-- Explanation:
-- Recursive CTE: base case anchors on root nodes (parent_id IS NULL).
-- Recursive step joins children to their parent rows, accumulating level
-- and path string. Cycle protection is implicit here because the graph is
-- a tree (no cycles), but CYCLE clause can be added in PostgreSQL 14+.
--
-- Follow-up: Find the maximum depth of any appeal chain and list all
-- leaf nodes (appeals that have no children).


-- ============================================================


-- Problem 023: Correlated Subquery — Latest Event per User Without Window Functions
-- Difficulty: Medium
-- Category: CTEs & Subqueries
-- Tags: correlated-subquery, latest-event, optimization
--
-- Scenario:
-- A data engineer is profiling whether a correlated subquery or a window
-- function is cleaner for extracting the most recent event per user.
-- The goal is to understand both approaches. Write the correlated subquery version.
--
-- Question:
-- Return each user's most recent event_type and event_time using a correlated
-- subquery in the WHERE clause (no window functions). Output: user_id,
-- event_type, event_time.

-- Schema & Sample Data:
CREATE TABLE user_event_log (
    event_id   BIGINT,
    user_id    INT,
    event_type VARCHAR(40),
    event_time TIMESTAMP
);

INSERT INTO user_event_log VALUES
(1,  4001, 'login',    '2024-05-01 08:00:00'),
(2,  4001, 'search',   '2024-05-01 08:05:00'),
(3,  4001, 'purchase', '2024-05-01 08:20:00'),
(4,  4002, 'login',    '2024-05-01 09:00:00'),
(5,  4002, 'search',   '2024-05-01 09:10:00'),
(6,  4003, 'login',    '2024-05-01 10:00:00'),
(7,  4003, 'logout',   '2024-05-01 10:45:00'),
(8,  4004, 'login',    '2024-05-01 11:00:00'),
(9,  4004, 'search',   '2024-05-01 11:05:00'),
(10, 4004, 'purchase', '2024-05-01 11:30:00'),
(11, 4004, 'logout',   '2024-05-01 12:00:00'),
(12, 4005, 'login',    '2024-05-01 12:00:00'),
(13, 4005, 'purchase', '2024-05-01 12:30:00');

-- Solution:
SELECT user_id, event_type, event_time
FROM user_event_log e
WHERE event_time = (
    SELECT MAX(event_time)
    FROM user_event_log e2
    WHERE e2.user_id = e.user_id
)
ORDER BY user_id;

-- Expected Output:
-- user_id | event_type | event_time
--  4001   | purchase   | 2024-05-01 08:20:00
--  4002   | search     | 2024-05-01 09:10:00
--  4003   | logout     | 2024-05-01 10:45:00
--  4004   | logout     | 2024-05-01 12:00:00
--  4005   | purchase   | 2024-05-01 12:30:00

-- Explanation:
-- The correlated subquery executes once per outer row, checking if event_time
-- equals the MAX for that user. Works but is O(n*m); window function (ROW_NUMBER)
-- is generally more efficient on indexed tables at scale.
--
-- Follow-up: Rewrite using ROW_NUMBER() and compare EXPLAIN plans on a larger
-- dataset. Which approach uses fewer sequential scans?


-- ============================================================


-- Problem 024: Subquery in FROM — Cohort Size with Conditional Aggregation
-- Difficulty: Medium
-- Category: CTEs & Subqueries
-- Tags: subquery-in-FROM, conditional-aggregation, cohorts
--
-- Scenario:
-- A marketing team cohorts users by their first-ever purchase month and then
-- computes how many users in each cohort ever made a second purchase within
-- 30 days. A derived table (subquery in FROM) builds the cohort base.
--
-- Question:
-- Return: cohort_month, cohort_size, returned_within_30d,
-- return_rate_pct (rounded to 1 decimal).

-- Schema & Sample Data:
CREATE TABLE all_purchases (
    purchase_id   INT,
    user_id       INT,
    purchase_date DATE,
    amount_usd    NUMERIC(10,2)
);

INSERT INTO all_purchases VALUES
(1,  5001, '2024-01-05', 29.99),
(2,  5001, '2024-01-20', 49.99),   -- 15 days later
(3,  5002, '2024-01-10', 59.99),
(4,  5002, '2024-02-20', 19.99),   -- 41 days later
(5,  5003, '2024-01-15', 39.99),
(6,  5003, '2024-01-30', 29.99),   -- 15 days later
(7,  5004, '2024-02-01', 99.99),
(8,  5004, '2024-02-10', 49.99),   -- 9 days later
(9,  5005, '2024-02-05', 14.99),
(10, 5005, '2024-03-10', 24.99),   -- 34 days later
(11, 5006, '2024-02-08', 9.99),
(12, 5007, '2024-01-20', 34.99),
(13, 5007, '2024-01-25', 44.99),   -- 5 days later
(14, 5008, '2024-02-15', 79.99);

-- Solution:
WITH first_purchase AS (
    SELECT
        user_id,
        MIN(purchase_date)                          AS first_date,
        DATE_TRUNC('month', MIN(purchase_date))::DATE AS cohort_month
    FROM all_purchases
    GROUP BY user_id
),
second_purchase AS (
    SELECT
        fp.user_id,
        fp.cohort_month,
        MIN(p.purchase_date) AS second_date
    FROM first_purchase fp
    JOIN all_purchases p
      ON p.user_id = fp.user_id
     AND p.purchase_date > fp.first_date
    GROUP BY fp.user_id, fp.cohort_month
)
SELECT
    fp.cohort_month,
    COUNT(DISTINCT fp.user_id)                                AS cohort_size,
    COUNT(DISTINCT CASE
        WHEN sp.second_date IS NOT NULL
         AND sp.second_date - fp.first_date <= 30
        THEN fp.user_id END)                                  AS returned_within_30d,
    ROUND(
        100.0 * COUNT(DISTINCT CASE
            WHEN sp.second_date IS NOT NULL
             AND sp.second_date - fp.first_date <= 30
            THEN fp.user_id END)
          / COUNT(DISTINCT fp.user_id),
    1)                                                        AS return_rate_pct
FROM first_purchase fp
LEFT JOIN second_purchase sp
       ON sp.user_id = fp.user_id
GROUP BY fp.cohort_month
ORDER BY fp.cohort_month;

-- Expected Output:
-- cohort_month | cohort_size | returned_within_30d | return_rate_pct
-- 2024-01-01   |      4      |          3          |      75.0
-- 2024-02-01   |      4      |          1          |      25.0

-- Explanation:
-- first_purchase CTE builds the cohort base. second_purchase finds the
-- next purchase. The outer query computes cohort size and 30-day return rate.
--
-- Follow-up: Extend to a 90-day window and add a column for users who
-- returned but NOT within 30 days (late returners).


-- ============================================================


-- Problem 025: EXISTS vs IN — Users Who Never Triggered a Safety Alert
-- Difficulty: Medium
-- Category: CTEs & Subqueries
-- Tags: EXISTS, NOT EXISTS, safety-signals, set-operations
--
-- Scenario:
-- A safety dashboard needs to surface users who have logged in at least once
-- but have NEVER appeared in the safety_alerts table — these are "clean" users
-- for the purposes of trust scoring calibration.
--
-- Question:
-- Return all users who have at least one login but no records in safety_alerts.
-- Use NOT EXISTS. Output: user_id, total_logins, first_login_date.

-- Schema & Sample Data:
CREATE TABLE logins (
    login_id    INT,
    user_id     INT,
    login_date  DATE
);

CREATE TABLE safety_alerts (
    alert_id   INT,
    user_id    INT,
    alert_type VARCHAR(40),
    alert_date DATE
);

INSERT INTO logins VALUES
(1, 6001, '2024-01-01'), (2, 6001, '2024-01-05'),
(3, 6002, '2024-01-02'), (4, 6002, '2024-01-10'),
(5, 6003, '2024-01-03'),
(6, 6004, '2024-01-04'), (7, 6004, '2024-01-08'), (8, 6004, '2024-01-12'),
(9, 6005, '2024-01-06'),
(10,6006, '2024-01-07'), (11,6006, '2024-01-09'),
(12,6007, '2024-01-10');

INSERT INTO safety_alerts VALUES
(1, 6001, 'spam',       '2024-01-06'),
(2, 6003, 'harassment', '2024-01-04'),
(3, 6005, 'spam',       '2024-01-07');

-- Solution:
SELECT
    l.user_id,
    COUNT(*)        AS total_logins,
    MIN(login_date) AS first_login_date
FROM logins l
WHERE NOT EXISTS (
    SELECT 1
    FROM safety_alerts sa
    WHERE sa.user_id = l.user_id
)
GROUP BY l.user_id
ORDER BY l.user_id;

-- Expected Output:
-- user_id | total_logins | first_login_date
--  6002   |      2       | 2024-01-02
--  6004   |      3       | 2024-01-04
--  6006   |      2       | 2024-01-07
--  6007   |      1       | 2024-01-10

-- Explanation:
-- NOT EXISTS is generally preferred over NOT IN when the subquery column can
-- contain NULLs — NOT IN with a NULL in the subquery returns no rows, which
-- is a common bug. NOT EXISTS handles NULLs correctly.
--
-- Follow-up: Rewrite using LEFT JOIN … WHERE sa.user_id IS NULL and compare
-- query plans. Which is more readable? Which performs better on large tables?


-- ============================================================


-- Problem 026: LATERAL JOIN — Per-User Last 3 Transactions
-- Difficulty: Hard
-- Category: CTEs & Subqueries
-- Tags: LATERAL, correlated-subquery, time-series
--
-- Scenario:
-- A fraud ops team needs the last 3 transactions for each account to feed
-- into a risk model. A LATERAL join efficiently retrieves this per-account
-- slice without a full window function over the entire table.
--
-- Question:
-- For each account, return its last 3 transactions (by txn_time) using a
-- LATERAL join. Output: account_id, txn_id, txn_time, amount_usd.

-- Schema & Sample Data:
CREATE TABLE account_transactions (
    txn_id     INT,
    account_id INT,
    txn_time   TIMESTAMP,
    amount_usd NUMERIC(10,2)
);

INSERT INTO account_transactions VALUES
(1,  7001, '2024-06-01 09:00:00', 200.00),
(2,  7001, '2024-06-02 10:00:00', 350.00),
(3,  7001, '2024-06-03 11:00:00', 150.00),
(4,  7001, '2024-06-04 12:00:00', 500.00),
(5,  7001, '2024-06-05 13:00:00', 125.00),
(6,  7002, '2024-06-01 08:00:00', 75.00),
(7,  7002, '2024-06-02 09:00:00', 90.00),
(8,  7002, '2024-06-03 10:00:00', 110.00),
(9,  7003, '2024-06-01 07:00:00', 1000.00),
(10, 7003, '2024-06-03 08:00:00', 2500.00),
(11, 7003, '2024-06-05 09:00:00', 750.00),
(12, 7003, '2024-06-06 10:00:00', 3000.00),
(13, 7003, '2024-06-07 11:00:00', 1200.00);

-- Solution:
SELECT
    a.account_id,
    t.txn_id,
    t.txn_time,
    t.amount_usd
FROM (SELECT DISTINCT account_id FROM account_transactions) a
CROSS JOIN LATERAL (
    SELECT txn_id, txn_time, amount_usd
    FROM account_transactions
    WHERE account_id = a.account_id
    ORDER BY txn_time DESC
    LIMIT 3
) t
ORDER BY a.account_id, t.txn_time DESC;

-- Expected Output:
-- account_id | txn_id | txn_time                | amount_usd
--   7001     |   5    | 2024-06-05 13:00:00     |  125.00
--   7001     |   4    | 2024-06-04 12:00:00     |  500.00
--   7001     |   3    | 2024-06-03 11:00:00     |  150.00
--   7002     |   8    | 2024-06-03 10:00:00     |  110.00
--   7002     |   7    | 2024-06-02 09:00:00     |   90.00
--   7002     |   6    | 2024-06-01 08:00:00     |   75.00
--   7003     |  13    | 2024-06-07 11:00:00     | 1200.00
--   7003     |  12    | 2024-06-06 10:00:00     | 3000.00
--   7003     |  11    | 2024-06-05 09:00:00     |  750.00

-- Explanation:
-- CROSS JOIN LATERAL lets the subquery reference the outer table (account_id).
-- ORDER BY + LIMIT inside the lateral subquery is the key pattern — efficient
-- when account_id + txn_time has a composite index.
--
-- Follow-up: Compute the average amount_usd over each account's last 3
-- transactions and flag accounts where the latest transaction is 2x that average.


-- ============================================================


-- Problem 027: CTE with Self-Join — Manager Reporting Chain Depth
-- Difficulty: Medium
-- Category: CTEs & Subqueries
-- Tags: recursive-CTE, org-hierarchy, self-join
--
-- Scenario:
-- An analytics engineering team is modeling an org hierarchy for headcount
-- reporting. Each employee has an optional manager_id. The team needs to know
-- how many levels deep each employee sits in the hierarchy.
--
-- Question:
-- Compute the hierarchy_depth for each employee (CEO = depth 1). Return:
-- employee_id, name, manager_id, hierarchy_depth.

-- Schema & Sample Data:
CREATE TABLE org_chart (
    employee_id INT,
    name        VARCHAR(50),
    manager_id  INT   -- NULL = top of hierarchy
);

INSERT INTO org_chart VALUES
(1, 'Alice (CEO)',   NULL),
(2, 'Bob (VP Eng)',  1),
(3, 'Carol (VP Prod)',1),
(4, 'Dave (Dir Eng)',2),
(5, 'Eve (Eng)',     4),
(6, 'Frank (Eng)',   4),
(7, 'Grace (PM)',    3),
(8, 'Hank (PM)',     3),
(9, 'Ivy (Eng)',     2),
(10,'Jack (Dir Prod)',3);

-- Solution:
WITH RECURSIVE hierarchy AS (
    SELECT employee_id, name, manager_id, 1 AS hierarchy_depth
    FROM org_chart
    WHERE manager_id IS NULL

    UNION ALL

    SELECT o.employee_id, o.name, o.manager_id, h.hierarchy_depth + 1
    FROM org_chart o
    JOIN hierarchy h ON o.manager_id = h.employee_id
)
SELECT employee_id, name, manager_id, hierarchy_depth
FROM hierarchy
ORDER BY hierarchy_depth, employee_id;

-- Expected Output:
-- employee_id | name             | manager_id | hierarchy_depth
--     1       | Alice (CEO)      |    NULL    |       1
--     2       | Bob (VP Eng)     |     1      |       2
--     3       | Carol (VP Prod)  |     1      |       2
--     4       | Dave (Dir Eng)   |     2      |       3
--     9       | Ivy (Eng)        |     2      |       3
--     7       | Grace (PM)       |     3      |       3
--     8       | Hank (PM)        |     3      |       3
--    10       | Jack (Dir Prod)  |     3      |       3
--     5       | Eve (Eng)        |     4      |       4
--     6       | Frank (Eng)      |     4      |       4

-- Explanation:
-- Recursive CTE anchors on the root (NULL manager_id) at depth 1 and
-- increments depth with each recursive step. Terminates when no more
-- children exist.
--
-- Follow-up: Count the number of direct reports per manager and identify
-- managers with more than 3 direct reports.


-- ============================================================


-- Problem 028: Subquery in SELECT — Days Since Last Login Inline
-- Difficulty: Medium
-- Category: CTEs & Subqueries
-- Tags: subquery-in-SELECT, user-inactivity, churn-signals
--
-- Scenario:
-- A customer success team's dashboard needs a user activity summary showing
-- each user's total logins, first login date, and days since their last
-- login — all in a single row per user. The "days since last login" must be
-- computed as of 2024-07-01.
--
-- Question:
-- Use a subquery in the SELECT clause to compute days_since_last_login inline.
-- Return: user_id, total_logins, first_login_date, last_login_date,
-- days_since_last_login (as of 2024-07-01).

-- Schema & Sample Data:
CREATE TABLE user_login_log (
    login_id   INT,
    user_id    INT,
    login_date DATE
);

INSERT INTO user_login_log VALUES
(1, 8001, '2024-01-10'), (2, 8001, '2024-03-15'), (3, 8001, '2024-06-20'),
(4, 8002, '2024-02-01'), (5, 8002, '2024-02-28'),
(6, 8003, '2024-01-05'), (7, 8003, '2024-04-01'), (8, 8003, '2024-04-15'),
(9, 8004, '2024-05-01'),
(10,8005, '2024-01-01'), (11,8005, '2024-06-30'),
(12,8006, '2024-03-10'),
(13,8007, '2024-06-15'), (14,8007, '2024-06-25');

-- Solution:
SELECT
    user_id,
    COUNT(*)             AS total_logins,
    MIN(login_date)      AS first_login_date,
    MAX(login_date)      AS last_login_date,
    DATE '2024-07-01' - MAX(login_date) AS days_since_last_login
FROM user_login_log
GROUP BY user_id
ORDER BY days_since_last_login DESC;

-- (Alternative using subquery in SELECT for pedagogical purposes:)
SELECT
    user_id,
    COUNT(*)        AS total_logins,
    MIN(login_date) AS first_login_date,
    (SELECT MAX(l2.login_date)
     FROM user_login_log l2
     WHERE l2.user_id = l.user_id)  AS last_login_date,
    DATE '2024-07-01' -
    (SELECT MAX(l2.login_date)
     FROM user_login_log l2
     WHERE l2.user_id = l.user_id)  AS days_since_last_login
FROM user_login_log l
GROUP BY user_id
ORDER BY days_since_last_login DESC;

-- Explanation:
-- The subquery-in-SELECT version re-executes per row of the outer query —
-- useful for illustration but MAX() within GROUP BY is equivalent and more
-- efficient. In practice, use the CTE/GROUP BY approach.
--
-- Follow-up: Classify users as 'active' (< 14 days), 'at_risk' (14–30 days),
-- or 'churned' (> 30 days) based on days_since_last_login.


-- ============================================================


-- Problem 029: Multi-Step CTE — Daily Active Users (DAU) and 7-Day MAU Ratio
-- Difficulty: Hard
-- Category: CTEs & Subqueries
-- Tags: chained-CTEs, DAU, MAU, engagement-metrics
--
-- Scenario:
-- A product analytics team needs to compute the DAU/MAU ratio (also called
-- "stickiness") for each day. MAU is defined as unique users active in the
-- rolling 7-day window ending on that date (7-day MAU, sometimes called WAU).
-- DAU/WAU stickiness is a key engagement health metric.
--
-- Question:
-- For each date, compute: dau, wau (unique users in last 7 days), and
-- stickiness = dau / wau (rounded to 3 decimal places).

-- Schema & Sample Data:
CREATE TABLE daily_active_users (
    event_date DATE,
    user_id    INT
);

INSERT INTO daily_active_users VALUES
('2024-01-01',9001),('2024-01-01',9002),('2024-01-01',9003),
('2024-01-02',9001),('2024-01-02',9004),
('2024-01-03',9002),('2024-01-03',9003),('2024-01-03',9005),
('2024-01-04',9001),('2024-01-04',9002),('2024-01-04',9006),
('2024-01-05',9003),('2024-01-05',9004),('2024-01-05',9007),
('2024-01-06',9001),('2024-01-06',9005),
('2024-01-07',9002),('2024-01-07',9003),('2024-01-07',9004),('2024-01-07',9007),
('2024-01-08',9001),('2024-01-08',9006),('2024-01-08',9007);

-- Solution:
WITH dau AS (
    SELECT event_date, COUNT(DISTINCT user_id) AS dau
    FROM daily_active_users
    GROUP BY event_date
),
dates AS (
    SELECT DISTINCT event_date FROM daily_active_users
),
wau AS (
    SELECT
        d.event_date,
        COUNT(DISTINCT a.user_id) AS wau
    FROM dates d
    JOIN daily_active_users a
      ON a.event_date BETWEEN d.event_date - INTERVAL '6 days' AND d.event_date
    GROUP BY d.event_date
)
SELECT
    dau.event_date,
    dau.dau,
    wau.wau,
    ROUND(dau.dau::NUMERIC / wau.wau, 3) AS stickiness
FROM dau
JOIN wau USING (event_date)
ORDER BY event_date;

-- Explanation:
-- DAU is a simple GROUP BY. WAU requires joining each date to the 7-day
-- window — this is a range self-join. The ratio (stickiness) measures what
-- fraction of the weekly audience is active on any given day.
--
-- Follow-up: Compute 28-day MAU instead and plot how stickiness changes
-- over a 30-day period. Alert if stickiness drops below 0.2 for 3 consecutive days.


-- ============================================================


-- Problem 030: Identifying Duplicate Accounts via Subquery
-- Difficulty: Medium
-- Category: CTEs & Subqueries
-- Tags: duplicate-detection, subquery, trust-safety
--
-- Scenario:
-- A fraud team suspects that some users registered multiple accounts using
-- the same phone number or email. They need to identify all user_ids that
-- share an identifier with at least one other user.
--
-- Question:
-- Find all user_ids that share either phone_number OR email with another user.
-- Return: user_id, email, phone_number, duplicate_reason
-- (value 'email', 'phone', or 'both').

-- Schema & Sample Data:
CREATE TABLE user_profiles (
    user_id      INT,
    email        VARCHAR(100),
    phone_number VARCHAR(20),
    created_at   DATE
);

INSERT INTO user_profiles VALUES
(10001, 'alice@ex.com', '+1-555-0101', '2024-01-01'),
(10002, 'bob@ex.com',   '+1-555-0102', '2024-01-02'),
(10003, 'carol@ex.com', '+1-555-0101', '2024-01-03'),  -- shared phone with 10001
(10004, 'dave@ex.com',  '+1-555-0104', '2024-01-04'),
(10005, 'alice@ex.com', '+1-555-0105', '2024-01-05'),  -- shared email with 10001
(10006, 'frank@ex.com', '+1-555-0106', '2024-01-06'),
(10007, 'grace@ex.com', '+1-555-0101', '2024-01-07'),  -- shared phone with 10001&10003
(10008, 'alice@ex.com', '+1-555-0101', '2024-01-08');  -- shared both

-- Solution:
WITH dup_email AS (
    SELECT email
    FROM user_profiles
    GROUP BY email
    HAVING COUNT(*) > 1
),
dup_phone AS (
    SELECT phone_number
    FROM user_profiles
    GROUP BY phone_number
    HAVING COUNT(*) > 1
)
SELECT
    up.user_id,
    up.email,
    up.phone_number,
    CASE
        WHEN up.email IN (SELECT email FROM dup_email)
         AND up.phone_number IN (SELECT phone_number FROM dup_phone)
            THEN 'both'
        WHEN up.email IN (SELECT email FROM dup_email)
            THEN 'email'
        ELSE 'phone'
    END AS duplicate_reason
FROM user_profiles up
WHERE up.email        IN (SELECT email        FROM dup_email)
   OR up.phone_number IN (SELECT phone_number FROM dup_phone)
ORDER BY up.user_id;

-- Explanation:
-- dup_email / dup_phone CTEs find the shared values using HAVING COUNT > 1.
-- The outer query joins back to classify each flagged user. Using IN with
-- CTEs is readable; at scale, a JOIN is faster.
--
-- Follow-up: Build a graph of connected accounts (users sharing an identifier
-- form a group). Use a recursive CTE to assign a cluster_id to each connected component.


-- ============================================================


-- Problem 031: CTE for Time-Bucketed Aggregation — Hourly Alert Volume
-- Difficulty: Medium
-- Category: CTEs & Subqueries
-- Tags: time-bucketing, generate_series, alerting
--
-- Scenario:
-- A safety ops team wants an hourly heatmap of alert volume over a 24-hour
-- period. Some hours may have zero alerts — these must still appear as rows
-- with count = 0. Use generate_series in a CTE to create the full 24-hour spine.
--
-- Question:
-- For each hour of 2024-06-01 (00:00 to 23:00), return the count of alerts
-- in that hour. Hours with no alerts must appear with count = 0.

-- Schema & Sample Data:
CREATE TABLE safety_alerts_log (
    alert_id   INT,
    alert_time TIMESTAMP,
    alert_type VARCHAR(40)
);

INSERT INTO safety_alerts_log VALUES
(1,  '2024-06-01 00:15:00', 'spam'),
(2,  '2024-06-01 00:45:00', 'harassment'),
(3,  '2024-06-01 02:10:00', 'spam'),
(4,  '2024-06-01 08:30:00', 'misinformation'),
(5,  '2024-06-01 08:55:00', 'spam'),
(6,  '2024-06-01 08:59:00', 'spam'),
(7,  '2024-06-01 12:00:00', 'harassment'),
(8,  '2024-06-01 14:20:00', 'spam'),
(9,  '2024-06-01 14:50:00', 'spam'),
(10, '2024-06-01 18:05:00', 'spam'),
(11, '2024-06-01 20:30:00', 'misinformation'),
(12, '2024-06-01 23:15:00', 'harassment');

-- Solution:
WITH hour_spine AS (
    SELECT generate_series(
        '2024-06-01 00:00:00'::TIMESTAMP,
        '2024-06-01 23:00:00'::TIMESTAMP,
        '1 hour'::INTERVAL
    ) AS hour_start
)
SELECT
    hs.hour_start,
    COUNT(a.alert_id) AS alert_count
FROM hour_spine hs
LEFT JOIN safety_alerts_log a
       ON a.alert_time >= hs.hour_start
      AND a.alert_time <  hs.hour_start + INTERVAL '1 hour'
GROUP BY hs.hour_start
ORDER BY hs.hour_start;

-- Explanation:
-- generate_series creates a complete 24-row spine so that hours with zero
-- alerts are preserved via LEFT JOIN. COUNT() on a NULLable column counts
-- only matched rows, naturally returning 0 for empty hours.
--
-- Follow-up: Add a 3-hour rolling total using SUM OVER on the result to
-- smooth the hourly signal and identify sustained alert surges.


-- ============================================================


-- Problem 032: Subquery Optimization — Rewrite Correlated to SET-BASED
-- Difficulty: Hard
-- Category: CTEs & Subqueries
-- Tags: query-optimization, correlated-to-set-based, performance
--
-- Scenario:
-- A data engineer inherits a slow query that uses a correlated subquery to
-- compute each user's spend relative to the average spend of users in their
-- country. The task is to rewrite it as a set-based CTE for better performance.
--
-- Question:
-- For each user, compute spend_vs_country_avg: the difference between their
-- total_spend and their country's average total_spend. Flag users above average
-- as 'above' and below as 'below'. Provide both the slow correlated version
-- (for comparison) and the CTE-based rewrite.

-- Schema & Sample Data:
CREATE TABLE user_spend (
    user_id     INT,
    country     VARCHAR(30),
    total_spend NUMERIC(12,2)
);

INSERT INTO user_spend VALUES
(11001, 'US', 1200.00),
(11002, 'US', 3400.00),
(11003, 'US', 800.00),
(11004, 'US', 5000.00),
(11005, 'UK', 2200.00),
(11006, 'UK', 900.00),
(11007, 'UK', 3100.00),
(11008, 'IN', 400.00),
(11009, 'IN', 600.00),
(11010, 'IN', 1100.00),
(11011, 'DE', 2800.00),
(11012, 'DE', 4200.00),
(11013, 'DE', 1600.00);

-- Solution (SLOW — correlated subquery, for reference):
SELECT
    user_id,
    country,
    total_spend,
    total_spend - (
        SELECT AVG(us2.total_spend)
        FROM user_spend us2
        WHERE us2.country = us.country
    ) AS spend_vs_country_avg,
    CASE
        WHEN total_spend > (
            SELECT AVG(us2.total_spend)
            FROM user_spend us2
            WHERE us2.country = us.country
        ) THEN 'above'
        ELSE 'below'
    END AS vs_avg_flag
FROM user_spend us;

-- Solution (FAST — CTE-based rewrite):
WITH country_avg AS (
    SELECT country, AVG(total_spend) AS avg_spend
    FROM user_spend
    GROUP BY country
)
SELECT
    us.user_id,
    us.country,
    us.total_spend,
    ROUND(us.total_spend - ca.avg_spend, 2) AS spend_vs_country_avg,
    CASE WHEN us.total_spend > ca.avg_spend THEN 'above' ELSE 'below' END AS vs_avg_flag
FROM user_spend us
JOIN country_avg ca USING (country)
ORDER BY us.country, us.total_spend DESC;

-- Explanation:
-- The correlated version runs a subquery once per row (O(n^2) effectively).
-- The CTE version computes AVG per country once (O(n)) and joins — vastly
-- more efficient on large tables. This is a critical pattern for a DE to master.
--
-- Follow-up: Use AVG() OVER (PARTITION BY country) as a window function
-- alternative. Compare: window function vs. CTE join vs. correlated subquery
-- in terms of readability, performance, and when each is appropriate.
