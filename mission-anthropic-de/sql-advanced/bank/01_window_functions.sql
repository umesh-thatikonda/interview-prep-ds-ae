-- ============================================================
-- CATEGORY 1: WINDOW FUNCTIONS
-- Problems 001–020
-- Covers: ROW_NUMBER, RANK, DENSE_RANK, LAG, LEAD,
--         FIRST_VALUE, LAST_VALUE, SUM OVER, AVG OVER,
--         COUNT OVER, NTILE, PERCENT_RANK, CUMULATIVE SUM,
--         MOVING AVERAGE, GAPS & ISLANDS
-- ============================================================


-- Problem 001: Rank Users by Weekly Message Volume
-- Difficulty: Easy
-- Category: Window Functions
-- Tags: RANK, user-activity, safety-signals
--
-- Scenario:
-- A Trust & Safety team tracks messages sent per user per week to detect
-- spam or coordinated inauthentic behavior. They want to rank users by
-- message volume within each week, so analysts can quickly surface the
-- top senders for review.
--
-- Question:
-- For each week, rank users by total messages sent (highest first).
-- Use RANK() so ties share the same rank. Return: week_start, user_id,
-- total_messages, rank_in_week.

-- Schema & Sample Data:
CREATE TABLE user_messages (
    message_id   BIGINT,
    user_id      INT,
    sent_at      TIMESTAMP,
    content_type VARCHAR(30)
);

INSERT INTO user_messages VALUES
(1,  101, '2024-01-01 08:00:00', 'text'),
(2,  102, '2024-01-01 09:00:00', 'text'),
(3,  101, '2024-01-02 10:00:00', 'image'),
(4,  103, '2024-01-02 11:00:00', 'text'),
(5,  101, '2024-01-03 12:00:00', 'text'),
(6,  102, '2024-01-03 13:00:00', 'text'),
(7,  102, '2024-01-04 09:00:00', 'text'),
(8,  103, '2024-01-04 14:00:00', 'image'),
(9,  104, '2024-01-08 08:00:00', 'text'),
(10, 101, '2024-01-08 09:00:00', 'text'),
(11, 101, '2024-01-09 10:00:00', 'text'),
(12, 104, '2024-01-09 11:00:00', 'text'),
(13, 104, '2024-01-10 12:00:00', 'text'),
(14, 102, '2024-01-10 13:00:00', 'image');

-- Solution:
WITH weekly AS (
    SELECT
        DATE_TRUNC('week', sent_at)::DATE AS week_start,
        user_id,
        COUNT(*)                          AS total_messages
    FROM user_messages
    GROUP BY 1, 2
)
SELECT
    week_start,
    user_id,
    total_messages,
    RANK() OVER (PARTITION BY week_start ORDER BY total_messages DESC) AS rank_in_week
FROM weekly
ORDER BY week_start, rank_in_week;

-- Expected Output:
-- week_start  | user_id | total_messages | rank_in_week
-- 2024-01-01  |   101   |       3        |      1
-- 2024-01-01  |   102   |       3        |      1   ← tie
-- 2024-01-01  |   103   |       2        |      3
-- 2024-01-08  |   101   |       2        |      1
-- 2024-01-08  |   104   |       3        |      1   (104 has 3 in wk2)
-- (exact rows depend on GROUP BY output)

-- Explanation:
-- DATE_TRUNC('week') normalizes all timestamps to Monday of that ISO week.
-- RANK() assigns the same rank to ties and skips the next rank(s).
-- Useful alternative: DENSE_RANK() when you don't want gaps in rank numbers.
--
-- Follow-up: Switch to DENSE_RANK and add a filter to show only top-3
-- per week. How would the result change if a user sent exactly the same
-- number of messages as another?


-- ============================================================


-- Problem 002: ROW_NUMBER to Deduplicate Duplicate Events
-- Difficulty: Easy
-- Category: Window Functions
-- Tags: ROW_NUMBER, deduplication, data-quality
--
-- Scenario:
-- An event pipeline occasionally delivers duplicate rows due to at-least-once
-- delivery semantics. Each event has an event_id and ingestion_timestamp.
-- The team wants to keep only the first-received copy of each event_id.
--
-- Question:
-- Using ROW_NUMBER(), return one row per event_id — the one with the
-- earliest ingestion_timestamp. Output: event_id, user_id, event_type,
-- ingestion_timestamp.

-- Schema & Sample Data:
CREATE TABLE raw_events (
    event_id          VARCHAR(20),
    user_id           INT,
    event_type        VARCHAR(40),
    ingestion_timestamp TIMESTAMP
);

INSERT INTO raw_events VALUES
('E001', 201, 'page_view',    '2024-03-01 10:00:00'),
('E001', 201, 'page_view',    '2024-03-01 10:00:05'),  -- duplicate
('E002', 202, 'click',        '2024-03-01 10:01:00'),
('E003', 201, 'purchase',     '2024-03-01 10:02:00'),
('E003', 201, 'purchase',     '2024-03-01 10:02:03'),  -- duplicate
('E004', 203, 'page_view',    '2024-03-01 10:03:00'),
('E005', 204, 'add_to_cart',  '2024-03-01 10:04:00'),
('E005', 204, 'add_to_cart',  '2024-03-01 10:04:01'),  -- duplicate
('E006', 203, 'checkout',     '2024-03-01 10:05:00'),
('E007', 202, 'purchase',     '2024-03-01 10:06:00'),
('E008', 205, 'click',        '2024-03-01 10:07:00'),
('E009', 205, 'page_view',    '2024-03-01 10:08:00'),
('E010', 201, 'logout',       '2024-03-01 10:09:00');

-- Solution:
WITH ranked AS (
    SELECT
        event_id,
        user_id,
        event_type,
        ingestion_timestamp,
        ROW_NUMBER() OVER (
            PARTITION BY event_id
            ORDER BY ingestion_timestamp ASC
        ) AS rn
    FROM raw_events
)
SELECT event_id, user_id, event_type, ingestion_timestamp
FROM ranked
WHERE rn = 1
ORDER BY ingestion_timestamp;

-- Explanation:
-- ROW_NUMBER() assigns 1 to the earliest copy of each event_id.
-- Filtering rn = 1 discards every subsequent duplicate.
-- This pattern is foundational in idempotent pipeline design.
--
-- Follow-up: What if two duplicates share the exact same ingestion_timestamp?
-- Add a secondary sort on a hash of the row to make ROW_NUMBER deterministic.


-- ============================================================


-- Problem 003: LAG to Compute Session Gap Between Logins
-- Difficulty: Easy
-- Category: Window Functions
-- Tags: LAG, session-analysis, user-behavior
--
-- Scenario:
-- A streaming platform wants to understand how frequently users return.
-- Given a table of login events, compute the number of days between each
-- user's consecutive logins to identify churn risk (gap > 14 days).
--
-- Question:
-- For each login event, compute days_since_last_login using LAG().
-- Return: user_id, login_date, days_since_last_login (NULL for first login).

-- Schema & Sample Data:
CREATE TABLE user_logins (
    login_id   SERIAL,
    user_id    INT,
    login_date DATE
);

INSERT INTO user_logins (user_id, login_date) VALUES
(301, '2024-01-01'),
(301, '2024-01-05'),
(301, '2024-01-20'),
(301, '2024-02-10'),
(302, '2024-01-02'),
(302, '2024-01-04'),
(302, '2024-01-06'),
(303, '2024-01-10'),
(303, '2024-01-25'),
(304, '2024-01-01'),
(304, '2024-01-02'),
(304, '2024-01-03'),
(304, '2024-02-01'),
(305, '2024-01-15');

-- Solution:
SELECT
    user_id,
    login_date,
    login_date - LAG(login_date) OVER (
        PARTITION BY user_id
        ORDER BY login_date
    ) AS days_since_last_login
FROM user_logins
ORDER BY user_id, login_date;

-- Explanation:
-- LAG() looks back one row within each user's ordered partition.
-- Subtracting DATE values in PostgreSQL yields an integer (days).
-- NULL for first login is the correct behavior — no prior event exists.
--
-- Follow-up: Flag any gap > 14 days as a "churn risk" event. Count how
-- many users experienced at least one churn-risk gap.


-- ============================================================


-- Problem 004: LEAD to Find Next Safety Violation per User
-- Difficulty: Easy
-- Category: Window Functions
-- Tags: LEAD, safety-signals, event-sequencing
--
-- Scenario:
-- A safety operations team logs policy violations per user. They want to
-- understand repeat offense patterns by computing the time between a
-- user's current violation and their next one.
--
-- Question:
-- For each violation, use LEAD() to show the next_violation_date for the
-- same user. Also compute days_until_next_violation. Return all violations;
-- show NULL when there is no subsequent violation.

-- Schema & Sample Data:
CREATE TABLE policy_violations (
    violation_id   INT,
    user_id        INT,
    violation_date DATE,
    violation_type VARCHAR(40)
);

INSERT INTO policy_violations VALUES
(1,  401, '2024-01-05', 'spam'),
(2,  401, '2024-01-18', 'harassment'),
(3,  401, '2024-02-03', 'spam'),
(4,  402, '2024-01-10', 'misinformation'),
(5,  402, '2024-01-22', 'spam'),
(6,  403, '2024-01-07', 'spam'),
(7,  403, '2024-01-08', 'spam'),
(8,  403, '2024-01-09', 'spam'),
(9,  404, '2024-02-01', 'harassment'),
(10, 404, '2024-03-15', 'spam'),
(11, 405, '2024-01-20', 'misinformation'),
(12, 401, '2024-02-20', 'harassment'),
(13, 402, '2024-02-05', 'misinformation');

-- Solution:
SELECT
    violation_id,
    user_id,
    violation_date,
    violation_type,
    LEAD(violation_date) OVER (
        PARTITION BY user_id
        ORDER BY violation_date
    ) AS next_violation_date,
    LEAD(violation_date) OVER (
        PARTITION BY user_id
        ORDER BY violation_date
    ) - violation_date AS days_until_next_violation
FROM policy_violations
ORDER BY user_id, violation_date;

-- Explanation:
-- LEAD() peeks at the next row in the same user partition ordered by date.
-- Subtracting dates gives days. Single LEAD call can be aliased and reused
-- in a CTE if the expression needs to be referenced multiple times.
--
-- Follow-up: Identify "rapid recidivists" — users who committed a second
-- violation within 7 days of the first. Return user_id and violation pair.


-- ============================================================


-- Problem 005: Running Total of Revenue with SUM OVER
-- Difficulty: Easy
-- Category: Window Functions
-- Tags: SUM OVER, running-total, revenue
--
-- Scenario:
-- An ads monetization team wants a daily running total of ad revenue
-- to monitor if the platform is on track to hit monthly targets.
--
-- Question:
-- Compute the cumulative ad revenue per day (ordered by date) for the
-- month of January 2024. Return: revenue_date, daily_revenue,
-- cumulative_revenue.

-- Schema & Sample Data:
CREATE TABLE ad_revenue (
    revenue_id   INT,
    revenue_date DATE,
    revenue_usd  NUMERIC(12,2)
);

INSERT INTO ad_revenue VALUES
(1,  '2024-01-01', 12500.00),
(2,  '2024-01-02', 13200.00),
(3,  '2024-01-03', 11800.00),
(4,  '2024-01-04', 14500.00),
(5,  '2024-01-05', 9800.00),
(6,  '2024-01-06', 10200.00),
(7,  '2024-01-07', 15000.00),
(8,  '2024-01-08', 16300.00),
(9,  '2024-01-09', 14100.00),
(10, '2024-01-10', 13700.00),
(11, '2024-01-11', 12900.00),
(12, '2024-01-12', 11500.00),
(13, '2024-01-13', 10800.00),
(14, '2024-01-14', 17200.00);

-- Solution:
SELECT
    revenue_date,
    revenue_usd                           AS daily_revenue,
    SUM(revenue_usd) OVER (
        ORDER BY revenue_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    )                                     AS cumulative_revenue
FROM ad_revenue
ORDER BY revenue_date;

-- Explanation:
-- ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW is the standard frame
-- for a running total. Without an explicit frame, the default RANGE frame
-- can produce unexpected results when dates tie. Explicit ROWS is safer.
--
-- Follow-up: Add a 7-day rolling average alongside the running total to
-- smooth daily noise. Use ROWS BETWEEN 6 PRECEDING AND CURRENT ROW.


-- ============================================================


-- Problem 006: 7-Day Moving Average of Content Reports
-- Difficulty: Medium
-- Category: Window Functions
-- Tags: AVG OVER, moving-average, trust-safety
--
-- Scenario:
-- A content moderation team tracks the daily count of abuse reports.
-- A 7-day moving average helps separate signal from noise and detect
-- sustained spikes that require escalation.
--
-- Question:
-- For each day, compute a 7-day trailing moving average of report_count
-- (including the current day). Return: report_date, report_count,
-- avg_7d (rounded to 1 decimal place).

-- Schema & Sample Data:
CREATE TABLE daily_abuse_reports (
    report_date  DATE,
    report_count INT
);

INSERT INTO daily_abuse_reports VALUES
('2024-01-01', 320),
('2024-01-02', 415),
('2024-01-03', 380),
('2024-01-04', 290),
('2024-01-05', 510),
('2024-01-06', 470),
('2024-01-07', 395),
('2024-01-08', 440),
('2024-01-09', 520),
('2024-01-10', 610),
('2024-01-11', 580),
('2024-01-12', 490),
('2024-01-13', 450),
('2024-01-14', 530);

-- Solution:
SELECT
    report_date,
    report_count,
    ROUND(
        AVG(report_count) OVER (
            ORDER BY report_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ),
    1) AS avg_7d
FROM daily_abuse_reports
ORDER BY report_date;

-- Explanation:
-- "ROWS BETWEEN 6 PRECEDING AND CURRENT ROW" creates a window of up to 7 rows.
-- For the first 6 rows the window is smaller (partial window) — this is
-- correct behavior; if you need exactly 7 rows, filter out the first 6 dates.
--
-- Follow-up: Identify days where report_count is more than 1.5x the 7-day
-- moving average, indicating an anomalous spike.


-- ============================================================


-- Problem 007: DENSE_RANK for Percentile Bucketing of Users
-- Difficulty: Easy
-- Category: Window Functions
-- Tags: DENSE_RANK, user-segmentation, engagement
--
-- Scenario:
-- A product analytics team segments users into engagement tiers based on
-- their 30-day session count. DENSE_RANK avoids gaps so tier labels are
-- contiguous even when many users share the same count.
--
-- Question:
-- Rank each user by their 30-day session_count using DENSE_RANK() (highest
-- sessions = rank 1). Return: user_id, session_count, dense_rank.

-- Schema & Sample Data:
CREATE TABLE user_sessions_30d (
    user_id       INT,
    session_count INT
);

INSERT INTO user_sessions_30d VALUES
(501, 95),
(502, 43),
(503, 95),
(504, 12),
(505, 67),
(506, 43),
(507, 120),
(508, 8),
(509, 67),
(510, 43),
(511, 120),
(512, 5),
(513, 95),
(514, 1);

-- Solution:
SELECT
    user_id,
    session_count,
    DENSE_RANK() OVER (ORDER BY session_count DESC) AS dense_rank
FROM user_sessions_30d
ORDER BY dense_rank, user_id;

-- Expected Output (partial):
-- user_id | session_count | dense_rank
--   507   |     120       |     1
--   511   |     120       |     1
--   501   |      95       |     2
--   503   |      95       |     2
--   513   |      95       |     2
--   505   |      67       |     3
--   509   |      67       |     3
--   ...

-- Explanation:
-- DENSE_RANK does not skip rank values after ties, unlike RANK().
-- This is preferred when the rank value is used as a tier label.
--
-- Follow-up: Bucket users into quartiles using NTILE(4) instead, then
-- compare how DENSE_RANK tiers align with quartile assignments.


-- ============================================================


-- Problem 008: NTILE for Load Balancing Review Queue
-- Difficulty: Easy
-- Category: Window Functions
-- Tags: NTILE, queue-assignment, operations
--
-- Scenario:
-- A trust-and-safety team has 4 human reviewers. They need to evenly split
-- a queue of flagged content items across the 4 reviewers. Items should be
-- assigned by priority (highest risk score first).
--
-- Question:
-- Assign each flagged_item to one of 4 reviewer buckets using NTILE(4),
-- ordered by risk_score DESC. Return: item_id, risk_score, reviewer_bucket.

-- Schema & Sample Data:
CREATE TABLE flagged_items (
    item_id    INT,
    content    TEXT,
    risk_score NUMERIC(5,2)
);

INSERT INTO flagged_items VALUES
(1001, 'post_abc', 9.8),
(1002, 'post_def', 7.5),
(1003, 'post_ghi', 8.3),
(1004, 'post_jkl', 6.1),
(1005, 'post_mno', 9.2),
(1006, 'post_pqr', 5.4),
(1007, 'post_stu', 7.8),
(1008, 'post_vwx', 4.9),
(1009, 'post_yza', 8.9),
(1010, 'post_bcd', 6.7),
(1011, 'post_efg', 9.5),
(1012, 'post_hij', 3.2);

-- Solution:
SELECT
    item_id,
    risk_score,
    NTILE(4) OVER (ORDER BY risk_score DESC) AS reviewer_bucket
FROM flagged_items
ORDER BY reviewer_bucket, risk_score DESC;

-- Explanation:
-- NTILE(4) divides the ordered result set into 4 roughly equal groups.
-- If the row count is not divisible by 4, the first buckets get one extra row.
-- Assigning by risk_score DESC ensures reviewers see the highest-risk items first.
--
-- Follow-up: Add reviewer names (reviewer_bucket mapped to a lookup table)
-- and compute the average risk_score each reviewer is assigned.


-- ============================================================


-- Problem 009: FIRST_VALUE — Identify Each User's First Purchase Category
-- Difficulty: Medium
-- Category: Window Functions
-- Tags: FIRST_VALUE, user-behavior, e-commerce
--
-- Scenario:
-- An e-commerce analytics team wants to understand what product category
-- each user bought first, to analyze how first-touch category correlates
-- with lifetime value.
--
-- Question:
-- For every purchase, show the first_category the user ever purchased
-- (by purchase_date) alongside the current purchase details. Use FIRST_VALUE().

-- Schema & Sample Data:
CREATE TABLE purchases (
    purchase_id   INT,
    user_id       INT,
    purchase_date DATE,
    category      VARCHAR(30),
    amount_usd    NUMERIC(10,2)
);

INSERT INTO purchases VALUES
(1,  601, '2024-01-05', 'electronics', 299.99),
(2,  601, '2024-01-20', 'clothing',    59.99),
(3,  601, '2024-02-10', 'books',       14.99),
(4,  602, '2024-01-02', 'clothing',    89.99),
(5,  602, '2024-01-15', 'electronics', 499.00),
(6,  603, '2024-01-08', 'books',       12.99),
(7,  603, '2024-01-09', 'books',       9.99),
(8,  603, '2024-02-01', 'clothing',    44.99),
(9,  604, '2024-01-12', 'electronics', 799.00),
(10, 604, '2024-01-25', 'electronics', 199.00),
(11, 605, '2024-01-30', 'clothing',    34.99),
(12, 606, '2024-02-05', 'books',       19.99),
(13, 601, '2024-02-15', 'electronics', 149.99);

-- Solution:
SELECT
    purchase_id,
    user_id,
    purchase_date,
    category,
    amount_usd,
    FIRST_VALUE(category) OVER (
        PARTITION BY user_id
        ORDER BY purchase_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS first_category
FROM purchases
ORDER BY user_id, purchase_date;

-- Explanation:
-- FIRST_VALUE with UNBOUNDED PRECEDING/FOLLOWING ensures the first category
-- is correctly propagated to every row in the partition.
-- Without the explicit frame, FIRST_VALUE on some engines defaults to
-- RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW which is still correct
-- for this case but ROWS is more explicit and portable.
--
-- Follow-up: Use LAST_VALUE() with the same frame to find each user's most
-- recent category, then compare first vs. last category to detect "category shifters."


-- ============================================================


-- Problem 010: LAST_VALUE — Most Recent Device per Session
-- Difficulty: Medium
-- Category: Window Functions
-- Tags: LAST_VALUE, session-analysis, device-tracking
--
-- Scenario:
-- Users sometimes switch devices mid-session. The analytics team needs the
-- last device seen in each session (by event timestamp) to accurately
-- attribute sessions to a device type in their BI dashboards.
--
-- Question:
-- For each event in a session, show the last_device seen in that session
-- using LAST_VALUE(). Return: session_id, event_ts, device, last_device.

-- Schema & Sample Data:
CREATE TABLE session_events (
    event_id   INT,
    session_id VARCHAR(10),
    event_ts   TIMESTAMP,
    device     VARCHAR(20),
    event_type VARCHAR(30)
);

INSERT INTO session_events VALUES
(1,  'S001', '2024-03-01 10:00:00', 'mobile', 'page_view'),
(2,  'S001', '2024-03-01 10:02:00', 'mobile', 'click'),
(3,  'S001', '2024-03-01 10:05:00', 'desktop','page_view'),
(4,  'S001', '2024-03-01 10:07:00', 'desktop','purchase'),
(5,  'S002', '2024-03-01 11:00:00', 'tablet', 'page_view'),
(6,  'S002', '2024-03-01 11:03:00', 'tablet', 'click'),
(7,  'S002', '2024-03-01 11:06:00', 'mobile', 'checkout'),
(8,  'S003', '2024-03-01 12:00:00', 'desktop','page_view'),
(9,  'S003', '2024-03-01 12:10:00', 'desktop','click'),
(10, 'S003', '2024-03-01 12:20:00', 'desktop','purchase'),
(11, 'S004', '2024-03-01 13:00:00', 'mobile', 'page_view'),
(12, 'S004', '2024-03-01 13:05:00', 'tablet', 'page_view');

-- Solution:
SELECT
    event_id,
    session_id,
    event_ts,
    device,
    LAST_VALUE(device) OVER (
        PARTITION BY session_id
        ORDER BY event_ts
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ) AS last_device
FROM session_events
ORDER BY session_id, event_ts;

-- Explanation:
-- LAST_VALUE requires ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
-- to see all rows in the partition. Without the frame extension the default
-- frame stops at the current row, returning the current device — not the last.
--
-- Follow-up: Count the number of sessions where the first_device != last_device
-- (multi-device sessions). What share of total sessions are these?


-- ============================================================


-- Problem 011: PERCENT_RANK for Revenue Percentile Scoring
-- Difficulty: Medium
-- Category: Window Functions
-- Tags: PERCENT_RANK, percentile, revenue-analysis
--
-- Scenario:
-- An advertiser team wants to know where each campaign's total spend falls
-- in the distribution of all campaigns for the quarter, expressed as a
-- percentile (0.0 = lowest, 1.0 = highest). This informs tiered account
-- management staffing.
--
-- Question:
-- Compute the percent_rank of each campaign by total_spend. Return:
-- campaign_id, advertiser_id, total_spend, percent_rank (rounded to 2 decimals).

-- Schema & Sample Data:
CREATE TABLE campaign_spend (
    campaign_id   INT,
    advertiser_id INT,
    total_spend   NUMERIC(12,2)
);

INSERT INTO campaign_spend VALUES
(2001, 701, 15000.00),
(2002, 702, 82000.00),
(2003, 703, 5000.00),
(2004, 701, 210000.00),
(2005, 704, 47000.00),
(2006, 705, 82000.00),
(2007, 706, 3000.00),
(2008, 703, 125000.00),
(2009, 707, 9500.00),
(2010, 702, 310000.00),
(2011, 708, 67000.00),
(2012, 709, 47000.00);

-- Solution:
SELECT
    campaign_id,
    advertiser_id,
    total_spend,
    ROUND(
        PERCENT_RANK() OVER (ORDER BY total_spend),
    2) AS percent_rank
FROM campaign_spend
ORDER BY total_spend;

-- Expected Output (partial):
-- campaign_id | advertiser_id | total_spend | percent_rank
--    2007     |     706       |   3000.00   |    0.00
--    2003     |     703       |   5000.00   |    0.09
--    2007...  |               |             |
--    2010     |     702       | 310000.00   |    1.00

-- Explanation:
-- PERCENT_RANK = (rank - 1) / (total_rows - 1). It ranges from 0.0 to 1.0.
-- Useful for identifying top-10% spenders: WHERE percent_rank >= 0.9.
--
-- Follow-up: Use CUME_DIST() instead and compare: CUME_DIST includes the
-- current row in the denominator, so values are always > 0.


-- ============================================================


-- Problem 012: COUNT OVER for Concurrent Active Sessions
-- Difficulty: Medium
-- Category: Window Functions
-- Tags: COUNT OVER, concurrent-sessions, streaming
--
-- Scenario:
-- A streaming platform wants to detect peak concurrent viewer load. Given
-- session start/end times, count how many sessions overlap with each session's
-- start time (sessions active at the moment each new session begins).
--
-- Question:
-- For each session, count how many other sessions were already active
-- (started but not yet ended) at the time this session started.
-- Return: session_id, user_id, start_time, concurrent_sessions_at_start.

-- Schema & Sample Data:
CREATE TABLE streaming_sessions (
    session_id  INT,
    user_id     INT,
    start_time  TIMESTAMP,
    end_time    TIMESTAMP
);

INSERT INTO streaming_sessions VALUES
(1, 801, '2024-02-01 19:00:00', '2024-02-01 20:30:00'),
(2, 802, '2024-02-01 19:15:00', '2024-02-01 21:00:00'),
(3, 803, '2024-02-01 19:30:00', '2024-02-01 20:00:00'),
(4, 804, '2024-02-01 20:00:00', '2024-02-01 22:00:00'),
(5, 805, '2024-02-01 20:15:00', '2024-02-01 21:30:00'),
(6, 806, '2024-02-01 20:30:00', '2024-02-01 21:00:00'),
(7, 807, '2024-02-01 21:00:00', '2024-02-01 22:30:00'),
(8, 808, '2024-02-01 21:15:00', '2024-02-01 22:00:00'),
(9, 809, '2024-02-01 19:00:00', '2024-02-01 19:45:00'),
(10,810, '2024-02-01 22:00:00', '2024-02-01 23:00:00');

-- Solution:
SELECT
    s.session_id,
    s.user_id,
    s.start_time,
    COUNT(o.session_id) AS concurrent_sessions_at_start
FROM streaming_sessions s
LEFT JOIN streaming_sessions o
       ON o.session_id <> s.session_id
      AND o.start_time < s.start_time
      AND o.end_time   > s.start_time
GROUP BY s.session_id, s.user_id, s.start_time
ORDER BY s.start_time;

-- Explanation:
-- A self-join captures all sessions that were already running when session s started.
-- For true window function approach, use SUM(1) OVER with RANGE BETWEEN for
-- timestamp-based intervals — but that requires PostgreSQL 13+ range frames.
-- The self-join approach is portable and clear.
--
-- Follow-up: Find the single minute with the maximum concurrent sessions using
-- a generate_series of 1-minute intervals joined against session start/end.


-- ============================================================


-- Problem 013: Detecting Gaps in Daily Activity (Gaps & Islands)
-- Difficulty: Medium
-- Category: Window Functions
-- Tags: ROW_NUMBER, gaps-islands, continuity
--
-- Scenario:
-- A safety team monitors a high-risk user's daily posting activity. They
-- want to identify contiguous "active streaks" — consecutive days without
-- a gap — to understand behavioral patterns around policy violations.
--
-- Question:
-- Group each day of activity into streak_id groups where each consecutive
-- run of days forms one streak. Return: user_id, activity_date, streak_id.

-- Schema & Sample Data:
CREATE TABLE user_activity_log (
    user_id       INT,
    activity_date DATE
);

INSERT INTO user_activity_log VALUES
(901, '2024-01-01'),
(901, '2024-01-02'),
(901, '2024-01-03'),
(901, '2024-01-05'),  -- gap
(901, '2024-01-06'),
(901, '2024-01-07'),
(901, '2024-01-10'),  -- gap
(901, '2024-01-11'),
(902, '2024-01-01'),
(902, '2024-01-02'),
(902, '2024-01-05'),  -- gap
(902, '2024-01-06'),
(902, '2024-01-07'),
(902, '2024-01-08');

-- Solution:
WITH numbered AS (
    SELECT
        user_id,
        activity_date,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY activity_date) AS rn
    FROM user_activity_log
),
island_key AS (
    SELECT
        user_id,
        activity_date,
        -- If consecutive, activity_date - rn is constant within a streak
        activity_date - CAST(rn AS INT) AS grp
    FROM numbered
)
SELECT
    user_id,
    activity_date,
    DENSE_RANK() OVER (PARTITION BY user_id ORDER BY grp) AS streak_id
FROM island_key
ORDER BY user_id, activity_date;

-- Explanation:
-- Classic gaps-and-islands: subtract row_number from the date. When dates
-- are consecutive the difference is constant (a "key" for the island).
-- A gap increments the key, starting a new island. DENSE_RANK converts
-- the key to a readable streak_id.
--
-- Follow-up: Compute streak length (number of days) and the start/end date
-- of each streak per user using MIN/MAX within streak groups.


-- ============================================================


-- Problem 014: TOP-N per Group — Top 3 Earners per Department
-- Difficulty: Medium
-- Category: Window Functions
-- Tags: ROW_NUMBER, top-N, filtering
--
-- Scenario:
-- An HR analytics team needs the top 3 highest-paid employees in each
-- department for a compensation benchmarking report. Ties should not
-- inflate the count — use ROW_NUMBER so exactly 3 rows per department
-- are returned.
--
-- Question:
-- Return the top 3 earners per department using ROW_NUMBER(). Output:
-- department, employee_id, salary, rn.

-- Schema & Sample Data:
CREATE TABLE employees (
    employee_id INT,
    department  VARCHAR(30),
    salary      NUMERIC(10,2)
);

INSERT INTO employees VALUES
(1001, 'Engineering', 180000),
(1002, 'Engineering', 160000),
(1003, 'Engineering', 160000),
(1004, 'Engineering', 140000),
(1005, 'Engineering', 120000),
(1006, 'Product',     175000),
(1007, 'Product',     155000),
(1008, 'Product',     155000),
(1009, 'Product',     130000),
(1010, 'Marketing',   95000),
(1011, 'Marketing',   88000),
(1012, 'Marketing',   82000),
(1013, 'Marketing',   75000),
(1014, 'Data',        165000),
(1015, 'Data',        150000);

-- Solution:
WITH ranked AS (
    SELECT
        department,
        employee_id,
        salary,
        ROW_NUMBER() OVER (
            PARTITION BY department
            ORDER BY salary DESC
        ) AS rn
    FROM employees
)
SELECT department, employee_id, salary, rn
FROM ranked
WHERE rn <= 3
ORDER BY department, rn;

-- Explanation:
-- ROW_NUMBER breaks ties arbitrarily (by internal row order) — exactly 3 rows
-- per department guaranteed. Use RANK() if you want all tied employees at
-- rank 3 to appear (may return more than 3 rows per department).
--
-- Follow-up: Rewrite using RANK() and count the departments where rank <= 3
-- returns more than 3 employees (i.e., departments with salary ties at the boundary).


-- ============================================================


-- Problem 015: Year-over-Year Growth with LAG
-- Difficulty: Medium
-- Category: Window Functions
-- Tags: LAG, YoY-growth, revenue
--
-- Scenario:
-- A finance analytics team tracks annual platform revenue and needs YoY
-- growth percentages for executive reporting. Negative growth should be
-- clearly represented as a negative percentage.
--
-- Question:
-- Compute YoY revenue growth percentage for each year using LAG(). Return:
-- year, revenue_usd, prev_year_revenue, yoy_growth_pct (rounded to 1 decimal).

-- Schema & Sample Data:
CREATE TABLE annual_revenue (
    year         INT,
    revenue_usd  NUMERIC(15,2)
);

INSERT INTO annual_revenue VALUES
(2018, 45000000.00),
(2019, 58000000.00),
(2020, 52000000.00),
(2021, 71000000.00),
(2022, 94000000.00),
(2023, 88000000.00),
(2024, 110000000.00);

-- Solution:
SELECT
    year,
    revenue_usd,
    LAG(revenue_usd) OVER (ORDER BY year)   AS prev_year_revenue,
    ROUND(
        100.0 * (revenue_usd - LAG(revenue_usd) OVER (ORDER BY year))
             / LAG(revenue_usd) OVER (ORDER BY year),
    1)                                       AS yoy_growth_pct
FROM annual_revenue
ORDER BY year;

-- Expected Output:
-- year | revenue_usd   | prev_year_revenue | yoy_growth_pct
-- 2018 | 45000000.00   | NULL              | NULL
-- 2019 | 58000000.00   | 45000000.00       | 28.9
-- 2020 | 52000000.00   | 58000000.00       | -10.3
-- 2021 | 71000000.00   | 52000000.00       | 36.5
-- 2022 | 94000000.00   | 71000000.00       | 32.4
-- 2023 | 88000000.00   | 94000000.00       | -6.4
-- 2024 | 110000000.00  | 88000000.00       | 25.0

-- Explanation:
-- LAG with no offset argument defaults to 1 row back.
-- Dividing by LAG() inline requires repeating the expression; a CTE
-- alternative assigns prev_year_revenue once and references it twice.
--
-- Follow-up: Add a quarter-level breakdown with QoQ growth using LAG()
-- partitioned by year, ordered by quarter.


-- ============================================================


-- Problem 016: Sliding Window Fraud Score Aggregation
-- Difficulty: Medium
-- Category: Window Functions
-- Tags: SUM OVER, fraud-detection, sliding-window
--
-- Scenario:
-- A fraud detection system assigns a risk_score to each transaction. The
-- rule engine flags an account if the sum of risk_scores over the last 5
-- transactions (including current) exceeds a threshold of 40.
--
-- Question:
-- For each transaction, compute the 5-transaction rolling sum of risk_score
-- per account (ordered by txn_time). Flag rows where this sum > 40.
-- Return: txn_id, account_id, txn_time, risk_score, rolling_5_sum, flagged.

-- Schema & Sample Data:
CREATE TABLE transactions (
    txn_id     INT,
    account_id INT,
    txn_time   TIMESTAMP,
    risk_score NUMERIC(5,2)
);

INSERT INTO transactions VALUES
(1,  1101, '2024-04-01 09:00:00', 5.0),
(2,  1101, '2024-04-01 09:30:00', 8.0),
(3,  1101, '2024-04-01 10:00:00', 12.0),
(4,  1101, '2024-04-01 10:30:00', 9.0),
(5,  1101, '2024-04-01 11:00:00', 14.0),   -- rolling sum = 48
(6,  1101, '2024-04-01 11:30:00', 3.0),
(7,  1102, '2024-04-01 09:00:00', 6.0),
(8,  1102, '2024-04-01 09:30:00', 7.0),
(9,  1102, '2024-04-01 10:00:00', 5.0),
(10, 1102, '2024-04-01 10:30:00', 4.0),
(11, 1102, '2024-04-01 11:00:00', 8.0),
(12, 1103, '2024-04-01 09:00:00', 15.0),
(13, 1103, '2024-04-01 09:30:00', 18.0),
(14, 1103, '2024-04-01 10:00:00', 20.0),
(15, 1103, '2024-04-01 10:30:00', 11.0);

-- Solution:
SELECT
    txn_id,
    account_id,
    txn_time,
    risk_score,
    SUM(risk_score) OVER (
        PARTITION BY account_id
        ORDER BY txn_time
        ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
    ) AS rolling_5_sum,
    CASE
        WHEN SUM(risk_score) OVER (
            PARTITION BY account_id
            ORDER BY txn_time
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) > 40 THEN TRUE
        ELSE FALSE
    END AS flagged
FROM transactions
ORDER BY account_id, txn_time;

-- Explanation:
-- ROWS BETWEEN 4 PRECEDING AND CURRENT ROW = 5-row window.
-- A CTE avoids repeating the window expression in the CASE. The flag
-- fires only after enough history accumulates per account.
--
-- Follow-up: Instead of 5 transactions, use a time-based window: flag accounts
-- whose total risk_score in the last 60 minutes exceeds 40. Requires RANGE
-- with INTERVAL (PostgreSQL 13+).


-- ============================================================


-- Problem 017: Nth Event per User — Find the 3rd Purchase
-- Difficulty: Medium
-- Category: Window Functions
-- Tags: ROW_NUMBER, Nth-event, user-journey
--
-- Scenario:
-- Product research shows that users who complete a 3rd purchase are highly
-- likely to become long-term subscribers. The growth team wants to extract
-- the 3rd purchase event per user to analyze what drove them to that milestone.
--
-- Question:
-- Return the 3rd purchase for each user (by purchase_date). If a user has
-- fewer than 3 purchases, exclude them. Output: user_id, purchase_id,
-- purchase_date, amount_usd.

-- Schema & Sample Data:
CREATE TABLE user_purchases (
    purchase_id   INT,
    user_id       INT,
    purchase_date DATE,
    amount_usd    NUMERIC(10,2)
);

INSERT INTO user_purchases VALUES
(1,  1201, '2024-01-10', 29.99),
(2,  1201, '2024-01-25', 49.99),
(3,  1201, '2024-02-05', 19.99),   -- 3rd
(4,  1201, '2024-02-20', 99.99),
(5,  1202, '2024-01-12', 39.99),
(6,  1202, '2024-01-30', 59.99),
(7,  1203, '2024-01-15', 14.99),
(8,  1203, '2024-01-28', 24.99),
(9,  1203, '2024-02-10', 34.99),   -- 3rd
(10, 1203, '2024-02-25', 44.99),
(11, 1204, '2024-01-20', 9.99),
(12, 1205, '2024-01-22', 79.99),
(13, 1205, '2024-01-29', 89.99),
(14, 1205, '2024-02-12', 99.99),   -- 3rd
(15, 1205, '2024-02-28', 109.99);

-- Solution:
WITH ranked AS (
    SELECT
        purchase_id,
        user_id,
        purchase_date,
        amount_usd,
        ROW_NUMBER() OVER (
            PARTITION BY user_id
            ORDER BY purchase_date
        ) AS purchase_rank
    FROM user_purchases
)
SELECT user_id, purchase_id, purchase_date, amount_usd
FROM ranked
WHERE purchase_rank = 3
ORDER BY user_id;

-- Expected Output:
-- user_id | purchase_id | purchase_date | amount_usd
--  1201   |      3      | 2024-02-05    |  19.99
--  1203   |      9      | 2024-02-10    |  34.99
--  1205   |     14      | 2024-02-12    |  99.99

-- Explanation:
-- ROW_NUMBER over purchase_date per user assigns sequential ordinals.
-- Filtering to rank 3 returns exactly the milestone event.
-- Users 1202 and 1204 have < 3 purchases and are naturally excluded.
--
-- Follow-up: Compute the median days_to_3rd_purchase across qualifying users.


-- ============================================================


-- Problem 018: Partition-Level Aggregates — Share of Revenue per Channel
-- Difficulty: Medium
-- Category: Window Functions
-- Tags: SUM OVER PARTITION, revenue-share, channel-analysis
--
-- Scenario:
-- A revenue ops team needs to know each ad channel's share of total monthly
-- revenue. They want both the channel total and the total-across-channels in
-- the same row so finance can compute share without a separate join.
--
-- Question:
-- For each row in the monthly channel revenue table, add total_all_channels
-- and channel_revenue_share_pct. Use SUM OVER() without ORDER BY for a
-- partition-level (non-ordered) aggregate.

-- Schema & Sample Data:
CREATE TABLE channel_revenue (
    month_start  DATE,
    channel      VARCHAR(30),
    revenue_usd  NUMERIC(12,2)
);

INSERT INTO channel_revenue VALUES
('2024-01-01', 'search',       450000.00),
('2024-01-01', 'social',       320000.00),
('2024-01-01', 'display',      180000.00),
('2024-01-01', 'video',        290000.00),
('2024-01-01', 'email',        60000.00),
('2024-02-01', 'search',       480000.00),
('2024-02-01', 'social',       340000.00),
('2024-02-01', 'display',      195000.00),
('2024-02-01', 'video',        310000.00),
('2024-02-01', 'email',        55000.00),
('2024-03-01', 'search',       520000.00),
('2024-03-01', 'social',       370000.00),
('2024-03-01', 'display',      210000.00);

-- Solution:
SELECT
    month_start,
    channel,
    revenue_usd,
    SUM(revenue_usd) OVER (PARTITION BY month_start) AS total_all_channels,
    ROUND(
        100.0 * revenue_usd /
        SUM(revenue_usd) OVER (PARTITION BY month_start),
    1) AS channel_revenue_share_pct
FROM channel_revenue
ORDER BY month_start, revenue_usd DESC;

-- Explanation:
-- SUM OVER (PARTITION BY month_start) with no ORDER BY computes the partition
-- total (not a running total) and repeats it in every row of that partition.
-- This eliminates a self-join or subquery and is cleaner for ratio calculations.
--
-- Follow-up: Add MoM revenue change per channel using LAG(revenue_usd) OVER
-- (PARTITION BY channel ORDER BY month_start).


-- ============================================================


-- Problem 019: Finding Streaks — Consecutive Days with Policy Flags
-- Difficulty: Hard
-- Category: Window Functions
-- Tags: LAG, gaps-islands, consecutive-events, policy-enforcement
--
-- Scenario:
-- A policy team flags accounts that post policy-violating content on 3+ consecutive
-- days. An automated suspension should trigger when such a streak is detected.
-- Given a daily flag log, identify accounts with at least one 3-day consecutive streak.
--
-- Question:
-- Identify all (account_id, streak_start, streak_end) for streaks of 3+ consecutive
-- flagged days. Return one row per streak.

-- Schema & Sample Data:
CREATE TABLE daily_flags (
    account_id  INT,
    flag_date   DATE
);

INSERT INTO daily_flags VALUES
(2001, '2024-01-01'),
(2001, '2024-01-02'),
(2001, '2024-01-03'),   -- streak of 3
(2001, '2024-01-05'),
(2001, '2024-01-06'),
(2002, '2024-01-01'),
(2002, '2024-01-03'),   -- gap, no streak
(2003, '2024-01-10'),
(2003, '2024-01-11'),
(2003, '2024-01-12'),
(2003, '2024-01-13'),   -- streak of 4
(2004, '2024-01-15'),
(2004, '2024-01-16');   -- streak of only 2

-- Solution:
WITH numbered AS (
    SELECT
        account_id,
        flag_date,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY flag_date) AS rn
    FROM daily_flags
),
grouped AS (
    SELECT
        account_id,
        flag_date,
        flag_date - CAST(rn AS INT) AS grp
    FROM numbered
),
streaks AS (
    SELECT
        account_id,
        grp,
        MIN(flag_date) AS streak_start,
        MAX(flag_date) AS streak_end,
        COUNT(*)        AS streak_length
    FROM grouped
    GROUP BY account_id, grp
)
SELECT account_id, streak_start, streak_end, streak_length
FROM streaks
WHERE streak_length >= 3
ORDER BY account_id, streak_start;

-- Expected Output:
-- account_id | streak_start | streak_end | streak_length
--   2001     | 2024-01-01   | 2024-01-03 |      3
--   2003     | 2024-01-10   | 2024-01-13 |      4

-- Explanation:
-- Same gaps-and-islands technique: subtract row_number from date to get a
-- constant group key per island. Then aggregate to get MIN/MAX date and count.
-- Filter to streak_length >= 3 for the policy trigger.
--
-- Follow-up: For accounts with a qualifying streak, join to the violations
-- table and list the violation types that occurred during the streak window.


-- ============================================================


-- Problem 020: Multi-Level Window — User Percentile Within Country and Global
-- Difficulty: Hard
-- Category: Window Functions
-- Tags: PERCENT_RANK, multi-partition, nested-windows
--
-- Scenario:
-- A global content platform needs to compare each creator's monthly earnings
-- against peers within their country AND against all creators globally.
-- This dual ranking drives both local and global "Top Creator" badges.
--
-- Question:
-- For each creator, compute:
--   1. country_percentile: PERCENT_RANK within their country
--   2. global_percentile:  PERCENT_RANK globally
-- Round both to 3 decimal places. Return: creator_id, country, monthly_earnings,
-- country_percentile, global_percentile.

-- Schema & Sample Data:
CREATE TABLE creator_earnings (
    creator_id      INT,
    country         VARCHAR(30),
    monthly_earnings NUMERIC(10,2)
);

INSERT INTO creator_earnings VALUES
(3001, 'US',  8500.00),
(3002, 'US',  12000.00),
(3003, 'US',  5000.00),
(3004, 'US',  22000.00),
(3005, 'UK',  9500.00),
(3006, 'UK',  4500.00),
(3007, 'UK',  15000.00),
(3008, 'IN',  2000.00),
(3009, 'IN',  3500.00),
(3010, 'IN',  7000.00),
(3011, 'IN',  1200.00),
(3012, 'DE',  11000.00),
(3013, 'DE',  8000.00),
(3014, 'DE',  19000.00);

-- Solution:
SELECT
    creator_id,
    country,
    monthly_earnings,
    ROUND(
        PERCENT_RANK() OVER (
            PARTITION BY country
            ORDER BY monthly_earnings
        ),
    3) AS country_percentile,
    ROUND(
        PERCENT_RANK() OVER (
            ORDER BY monthly_earnings
        ),
    3) AS global_percentile
FROM creator_earnings
ORDER BY country, monthly_earnings;

-- Explanation:
-- Two PERCENT_RANK window functions in the same SELECT: one partitioned by
-- country for local context, one unpartitioned for global context. Both
-- reference the same source table with zero extra joins or CTEs.
-- This pattern eliminates the need to pre-aggregate.
--
-- Follow-up: Find creators whose global_percentile >= 0.9 but whose
-- country_percentile < 0.5 — globally elite creators who are merely
-- average within their own country.
