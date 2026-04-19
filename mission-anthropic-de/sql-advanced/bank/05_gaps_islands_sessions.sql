-- ============================================================
-- Category 5: Gaps & Islands / Session Detection
-- Problems 051–062
-- Database: PostgreSQL
-- Context: Real DE scenarios — user events, safety signals,
--          streaming sessions, ad delivery logs
-- ============================================================


-- Problem 051: User Session Detection from Raw Clickstream
-- Difficulty: Medium
-- Category: Gaps & Islands / Session Detection
-- Tags: session detection, gaps, window functions, lag
--
-- Scenario:
-- A trust & safety platform tracks every page request from users.
-- A session is defined as a sequence of events for the same user
-- where no two consecutive events are more than 30 minutes apart.
-- You need to assign a session_id (per user) to every event row.
--
-- Question:
-- Assign a sequential session number (per user) to each event.
-- Output: user_id, event_time, page, session_num

-- Schema & Sample Data:
CREATE TABLE clickstream (
    event_id   BIGINT PRIMARY KEY,
    user_id    INT,
    event_time TIMESTAMP,
    page       VARCHAR(50)
);

INSERT INTO clickstream VALUES
(1,  101, '2024-01-10 09:00:00', 'home'),
(2,  101, '2024-01-10 09:05:00', 'search'),
(3,  101, '2024-01-10 09:12:00', 'product'),
(4,  101, '2024-01-10 09:55:00', 'home'),       -- gap > 30 min → new session
(5,  101, '2024-01-10 10:03:00', 'checkout'),
(6,  102, '2024-01-10 08:00:00', 'home'),
(7,  102, '2024-01-10 08:25:00', 'search'),
(8,  102, '2024-01-10 09:10:00', 'product'),    -- gap > 30 min → new session
(9,  102, '2024-01-10 09:15:00', 'cart'),
(10, 102, '2024-01-10 11:00:00', 'home');       -- gap > 30 min → new session

-- Solution:
WITH lag_events AS (
    SELECT
        event_id,
        user_id,
        event_time,
        page,
        LAG(event_time) OVER (PARTITION BY user_id ORDER BY event_time) AS prev_time
    FROM clickstream
),
session_starts AS (
    SELECT
        event_id,
        user_id,
        event_time,
        page,
        CASE
            WHEN prev_time IS NULL
              OR EXTRACT(EPOCH FROM (event_time - prev_time)) > 1800 THEN 1
            ELSE 0
        END AS is_new_session
    FROM lag_events
)
SELECT
    user_id,
    event_time,
    page,
    SUM(is_new_session) OVER (PARTITION BY user_id ORDER BY event_time) AS session_num
FROM session_starts
ORDER BY user_id, event_time;

-- Expected Output:
-- user_id | event_time          | page     | session_num
-- --------+---------------------+----------+------------
-- 101     | 2024-01-10 09:00:00 | home     | 1
-- 101     | 2024-01-10 09:05:00 | search   | 1
-- 101     | 2024-01-10 09:12:00 | product  | 1
-- 101     | 2024-01-10 09:55:00 | home     | 2
-- 101     | 2024-01-10 10:03:00 | checkout | 2
-- 102     | 2024-01-10 08:00:00 | home     | 1
-- 102     | 2024-01-10 08:25:00 | search   | 1
-- 102     | 2024-01-10 09:10:00 | product  | 2
-- 102     | 2024-01-10 09:15:00 | cart     | 2
-- 102     | 2024-01-10 11:00:00 | home     | 3

-- Explanation:
-- LAG gets the previous event time per user. A new session starts when
-- there is no previous event or the gap exceeds 1800 seconds (30 min).
-- A running SUM of the is_new_session flag produces the session number.
--
-- Follow-up: Also compute session duration (last_event - first_event per session)
--            and number of pages per session.


-- Problem 052: Find Gaps in Sequential Content IDs
-- Difficulty: Medium
-- Category: Gaps & Islands / Session Detection
-- Tags: gaps, lead/lag, missing rows, data quality
--
-- Scenario:
-- A content ingestion pipeline assigns sequential content_ids to articles
-- published on a news platform. After a batch load, you suspect some IDs
-- were skipped due to a pipeline bug and need to identify the missing ranges.
--
-- Question:
-- Find all missing content_id ranges (gap_start, gap_end) where at least
-- one ID is absent. Output: gap_start, gap_end, missing_count.

-- Schema & Sample Data:
CREATE TABLE published_content (
    content_id  INT PRIMARY KEY,
    title       VARCHAR(100),
    published_at TIMESTAMP
);

INSERT INTO published_content VALUES
(1,  'Article A', '2024-03-01 10:00:00'),
(2,  'Article B', '2024-03-01 10:05:00'),
(3,  'Article C', '2024-03-01 10:10:00'),
(7,  'Article D', '2024-03-01 10:15:00'),  -- gap: 4,5,6 missing
(8,  'Article E', '2024-03-01 10:20:00'),
(12, 'Article F', '2024-03-01 10:25:00'),  -- gap: 9,10,11 missing
(13, 'Article G', '2024-03-01 10:30:00'),
(14, 'Article H', '2024-03-01 10:35:00'),
(20, 'Article I', '2024-03-01 10:40:00'); -- gap: 15-19 missing

-- Solution:
WITH next_id AS (
    SELECT
        content_id,
        LEAD(content_id) OVER (ORDER BY content_id) AS next_content_id
    FROM published_content
)
SELECT
    content_id + 1              AS gap_start,
    next_content_id - 1         AS gap_end,
    next_content_id - content_id - 1 AS missing_count
FROM next_id
WHERE next_content_id - content_id > 1;

-- Expected Output:
-- gap_start | gap_end | missing_count
-- ----------+---------+--------------
-- 4         | 6       | 3
-- 9         | 11      | 3
-- 15        | 19      | 5

-- Explanation:
-- LEAD gives the next content_id in sorted order. Any gap greater than 1
-- between current and next id indicates missing values. gap_start = current+1,
-- gap_end = next-1, missing_count = difference - 1.
--
-- Follow-up: Generate the actual list of every missing content_id using
--            generate_series(gap_start, gap_end).


-- Problem 053: Island Detection — Consecutive Active Days Streaks
-- Difficulty: Medium
-- Category: Gaps & Islands / Session Detection
-- Tags: islands, consecutive, date arithmetic, window functions
--
-- Scenario:
-- A user engagement team at a streaming platform wants to find "engagement
-- streaks" — consecutive days a user was active. For each streak, report
-- the user, streak start, streak end, and streak length.
--
-- Question:
-- Find all consecutive active-day streaks per user.
-- Output: user_id, streak_start, streak_end, streak_length_days.

-- Schema & Sample Data:
CREATE TABLE daily_active_users (
    user_id    INT,
    active_date DATE,
    PRIMARY KEY (user_id, active_date)
);

INSERT INTO daily_active_users VALUES
(1, '2024-02-01'), (1, '2024-02-02'), (1, '2024-02-03'),
(1, '2024-02-07'), (1, '2024-02-08'),   -- gap on 4,5,6 → new streak
(2, '2024-02-01'), (2, '2024-02-02'),
(2, '2024-02-05'), (2, '2024-02-06'), (2, '2024-02-07'), (2, '2024-02-08'),
(3, '2024-02-10');                       -- single-day streak

-- Solution:
WITH ranked AS (
    SELECT
        user_id,
        active_date,
        active_date - (ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY active_date) * INTERVAL '1 day') AS grp
    FROM daily_active_users
)
SELECT
    user_id,
    MIN(active_date) AS streak_start,
    MAX(active_date) AS streak_end,
    COUNT(*)         AS streak_length_days
FROM ranked
GROUP BY user_id, grp
ORDER BY user_id, streak_start;

-- Expected Output:
-- user_id | streak_start | streak_end  | streak_length_days
-- --------+--------------+-------------+-------------------
-- 1       | 2024-02-01   | 2024-02-03  | 3
-- 1       | 2024-02-07   | 2024-02-08  | 2
-- 2       | 2024-02-01   | 2024-02-02  | 2
-- 2       | 2024-02-05   | 2024-02-08  | 4
-- 3       | 2024-02-10   | 2024-02-10  | 1

-- Explanation:
-- Subtracting the row_number (as interval) from each date produces a constant
-- "grp" value for consecutive dates. Grouping by user_id + grp isolates each
-- island. MIN/MAX give the streak boundaries.
--
-- Follow-up: Find the longest single streak per user across all time.


-- Problem 054: Ad Impression Deduplication — Overlapping Delivery Windows
-- Difficulty: Hard
-- Category: Gaps & Islands / Session Detection
-- Tags: overlapping intervals, merge intervals, ads, window functions
--
-- Scenario:
-- An ad platform records impression windows (start_time, end_time) for each
-- ad campaign per device. Some windows overlap due to re-delivery bugs.
-- You need to merge overlapping windows and report the total unique impression
-- seconds per campaign.
--
-- Question:
-- Merge all overlapping impression windows per campaign_id and return
-- campaign_id, merged_start, merged_end, and duration_seconds.

-- Schema & Sample Data:
CREATE TABLE ad_impressions (
    impression_id BIGINT PRIMARY KEY,
    campaign_id   INT,
    start_time    TIMESTAMP,
    end_time      TIMESTAMP
);

INSERT INTO ad_impressions VALUES
(1, 10, '2024-04-01 10:00:00', '2024-04-01 10:05:00'),
(2, 10, '2024-04-01 10:03:00', '2024-04-01 10:08:00'),  -- overlaps with 1
(3, 10, '2024-04-01 10:07:00', '2024-04-01 10:12:00'),  -- overlaps with 2
(4, 10, '2024-04-01 11:00:00', '2024-04-01 11:10:00'),  -- separate
(5, 20, '2024-04-01 09:00:00', '2024-04-01 09:30:00'),
(6, 20, '2024-04-01 09:45:00', '2024-04-01 10:00:00'),  -- separate
(7, 20, '2024-04-01 09:50:00', '2024-04-01 10:05:00'),  -- overlaps with 6
(8, 10, '2024-04-01 10:04:00', '2024-04-01 10:06:00');  -- fully inside 1+2+3 merge

-- Solution:
WITH ordered AS (
    SELECT
        campaign_id,
        start_time,
        end_time,
        MAX(end_time) OVER (
            PARTITION BY campaign_id
            ORDER BY start_time
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_max_end
    FROM ad_impressions
),
island_flags AS (
    SELECT
        campaign_id,
        start_time,
        end_time,
        CASE WHEN start_time > prev_max_end OR prev_max_end IS NULL THEN 1 ELSE 0 END AS is_new_island
    FROM ordered
),
island_groups AS (
    SELECT
        campaign_id,
        start_time,
        end_time,
        SUM(is_new_island) OVER (PARTITION BY campaign_id ORDER BY start_time) AS island_id
    FROM island_flags
)
SELECT
    campaign_id,
    MIN(start_time)                                               AS merged_start,
    MAX(end_time)                                                 AS merged_end,
    EXTRACT(EPOCH FROM (MAX(end_time) - MIN(start_time)))::INT   AS duration_seconds
FROM island_groups
GROUP BY campaign_id, island_id
ORDER BY campaign_id, merged_start;

-- Expected Output:
-- campaign_id | merged_start        | merged_end          | duration_seconds
-- ------------+---------------------+---------------------+-----------------
-- 10          | 2024-04-01 10:00:00 | 2024-04-01 10:12:00 | 720
-- 10          | 2024-04-01 11:00:00 | 2024-04-01 11:10:00 | 600
-- 20          | 2024-04-01 09:00:00 | 2024-04-01 09:30:00 | 1800
-- 20          | 2024-04-01 09:45:00 | 2024-04-01 10:05:00 | 1200

-- Explanation:
-- The running MAX of end_time from all previous rows detects whether the
-- current row's start_time falls inside an existing window. If it does not,
-- it starts a new island. Grouping by campaign + island then takes MIN/MAX.
--
-- Follow-up: Calculate the total wasted impression seconds
--            (raw total duration minus merged duration) per campaign.


-- Problem 055: Safety Signal — Find Continuous Violation Periods
-- Difficulty: Hard
-- Category: Gaps & Islands / Session Detection
-- Tags: islands, safety, contiguous, window functions
--
-- Scenario:
-- A content moderation system records daily policy violation scores for accounts.
-- An account is considered "in violation" on days when its score >= 70.
-- Find all continuous violation periods per account (start, end, length).
--
-- Question:
-- Identify every contiguous block of days where an account's score >= 70.
-- Output: account_id, period_start, period_end, days_in_violation.

-- Schema & Sample Data:
CREATE TABLE daily_violation_scores (
    account_id INT,
    score_date DATE,
    score      INT,
    PRIMARY KEY (account_id, score_date)
);

INSERT INTO daily_violation_scores VALUES
(1001, '2024-05-01', 45), (1001, '2024-05-02', 72), (1001, '2024-05-03', 80),
(1001, '2024-05-04', 75), (1001, '2024-05-05', 40), (1001, '2024-05-06', 71),
(1001, '2024-05-07', 69), (1001, '2024-05-08', 90),
(1002, '2024-05-01', 80), (1002, '2024-05-02', 85), (1002, '2024-05-03', 88),
(1002, '2024-05-04', 30), (1002, '2024-05-05', 91), (1002, '2024-05-06', 95);

-- Solution:
WITH violations AS (
    SELECT account_id, score_date
    FROM daily_violation_scores
    WHERE score >= 70
),
ranked AS (
    SELECT
        account_id,
        score_date,
        score_date - (ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY score_date) * INTERVAL '1 day') AS grp
    FROM violations
)
SELECT
    account_id,
    MIN(score_date) AS period_start,
    MAX(score_date) AS period_end,
    COUNT(*)        AS days_in_violation
FROM ranked
GROUP BY account_id, grp
ORDER BY account_id, period_start;

-- Expected Output:
-- account_id | period_start | period_end  | days_in_violation
-- -----------+--------------+-------------+------------------
-- 1001       | 2024-05-02   | 2024-05-04  | 3
-- 1001       | 2024-05-06   | 2024-05-06  | 1
-- 1001       | 2024-05-08   | 2024-05-08  | 1
-- 1002       | 2024-05-01   | 2024-05-03  | 3
-- 1002       | 2024-05-05   | 2024-05-06  | 2

-- Explanation:
-- First filter only days where score >= 70 (the "violation" island).
-- Use the classic date-minus-rownumber trick to group consecutive dates.
-- MIN/MAX within each group give the period boundaries.
--
-- Follow-up: Return only accounts that had at least one violation period
--            lasting 3 or more consecutive days.


-- Problem 056: Streaming Platform — Detect Buffering Events in Playback
-- Difficulty: Medium
-- Category: Gaps & Islands / Session Detection
-- Tags: session detection, video quality, gaps, streaming
--
-- Scenario:
-- A streaming platform logs QoS (quality-of-service) events per playback
-- session. When the player buffers, it emits a BUFFER_START event; when
-- playback resumes it emits BUFFER_END. Detect each buffering episode:
-- start time, end time, and duration in seconds.
--
-- Question:
-- For each playback session, find every buffering episode
-- (BUFFER_START paired with the next BUFFER_END).
-- Output: session_id, buffer_start, buffer_end, buffer_duration_seconds.

-- Schema & Sample Data:
CREATE TABLE playback_events (
    event_id    BIGINT PRIMARY KEY,
    session_id  BIGINT,
    event_type  VARCHAR(20),  -- 'BUFFER_START' or 'BUFFER_END'
    event_time  TIMESTAMP
);

INSERT INTO playback_events VALUES
(1, 9001, 'BUFFER_START', '2024-06-01 20:01:05'),
(2, 9001, 'BUFFER_END',   '2024-06-01 20:01:12'),
(3, 9001, 'BUFFER_START', '2024-06-01 20:15:30'),
(4, 9001, 'BUFFER_END',   '2024-06-01 20:15:45'),
(5, 9002, 'BUFFER_START', '2024-06-01 21:00:00'),
(6, 9002, 'BUFFER_END',   '2024-06-01 21:00:08'),
(7, 9002, 'BUFFER_START', '2024-06-01 21:30:00'),
(8, 9002, 'BUFFER_END',   '2024-06-01 21:30:20'),
(9, 9002, 'BUFFER_START', '2024-06-01 21:45:00'),
(10,9002, 'BUFFER_END',   '2024-06-01 21:45:03');

-- Solution:
WITH numbered AS (
    SELECT
        session_id,
        event_type,
        event_time,
        ROW_NUMBER() OVER (PARTITION BY session_id, event_type ORDER BY event_time) AS rn
    FROM playback_events
)
SELECT
    s.session_id,
    s.event_time                                             AS buffer_start,
    e.event_time                                             AS buffer_end,
    EXTRACT(EPOCH FROM (e.event_time - s.event_time))::INT   AS buffer_duration_seconds
FROM numbered s
JOIN numbered e
    ON s.session_id = e.session_id
   AND s.rn = e.rn
   AND s.event_type = 'BUFFER_START'
   AND e.event_type = 'BUFFER_END'
ORDER BY s.session_id, s.event_time;

-- Expected Output:
-- session_id | buffer_start        | buffer_end          | buffer_duration_seconds
-- -----------+---------------------+---------------------+------------------------
-- 9001       | 2024-06-01 20:01:05 | 2024-06-01 20:01:12 | 7
-- 9001       | 2024-06-01 20:15:30 | 2024-06-01 20:15:45 | 15
-- 9002       | 2024-06-01 21:00:00 | 2024-06-01 21:00:08 | 8
-- 9002       | 2024-06-01 21:30:00 | 2024-06-01 21:30:20 | 20
-- 9002       | 2024-06-01 21:45:00 | 2024-06-01 21:45:03 | 3

-- Explanation:
-- Assign a row number to START events and END events separately within
-- each session. Join on matching row numbers to pair the nth START with
-- the nth END. EXTRACT gives duration in seconds.
--
-- Follow-up: Compute total buffering time per session and flag sessions
--            where total buffering exceeds 30 seconds.


-- Problem 057: E-Commerce — Find Periods of No Order Activity (Dead Zones)
-- Difficulty: Medium
-- Category: Gaps & Islands / Session Detection
-- Tags: gaps, e-commerce, time series, lead
--
-- Scenario:
-- A marketplace ops team wants to identify "dead zones" — time gaps between
-- consecutive orders on the platform — to correlate with outages or holidays.
-- They want gaps longer than 2 hours between any two consecutive orders.
--
-- Question:
-- Find all gaps > 2 hours between consecutive orders (platform-wide, not per user).
-- Output: gap_start (end of prior order), gap_end (start of next order),
--         gap_hours (rounded to 2 decimals).

-- Schema & Sample Data:
CREATE TABLE orders (
    order_id    BIGINT PRIMARY KEY,
    user_id     INT,
    placed_at   TIMESTAMP,
    amount      NUMERIC(10,2)
);

INSERT INTO orders VALUES
(101, 1, '2024-07-04 08:00:00', 99.00),
(102, 2, '2024-07-04 08:45:00', 45.50),
(103, 3, '2024-07-04 09:10:00', 120.00),
(104, 1, '2024-07-04 12:00:00', 60.00),   -- gap ~2h50m from 103
(105, 4, '2024-07-04 12:30:00', 30.00),
(106, 2, '2024-07-04 16:00:00', 75.00),   -- gap ~3h30m from 105
(107, 5, '2024-07-04 16:05:00', 50.00),
(108, 3, '2024-07-04 23:00:00', 200.00),  -- gap ~6h55m from 107
(109, 1, '2024-07-04 23:50:00', 40.00);

-- Solution:
WITH ordered_orders AS (
    SELECT
        placed_at,
        LEAD(placed_at) OVER (ORDER BY placed_at) AS next_placed_at
    FROM orders
)
SELECT
    placed_at                                                          AS gap_start,
    next_placed_at                                                     AS gap_end,
    ROUND(EXTRACT(EPOCH FROM (next_placed_at - placed_at)) / 3600.0, 2) AS gap_hours
FROM ordered_orders
WHERE next_placed_at IS NOT NULL
  AND EXTRACT(EPOCH FROM (next_placed_at - placed_at)) > 7200
ORDER BY gap_start;

-- Expected Output:
-- gap_start            | gap_end             | gap_hours
-- ---------------------+---------------------+----------
-- 2024-07-04 09:10:00  | 2024-07-04 12:00:00 | 2.83
-- 2024-07-04 12:30:00  | 2024-07-04 16:00:00 | 3.50
-- 2024-07-04 16:05:00  | 2024-07-04 23:00:00 | 6.92

-- Explanation:
-- LEAD gives the next order timestamp across all orders sorted by time.
-- Filter on gaps > 7200 seconds (2 hours). ROUND with 2 decimals formats
-- the hours. The final gap (23:50 → NULL) is excluded by the IS NOT NULL check.
--
-- Follow-up: Annotate each gap with whether it falls on a US public holiday
--            using a LEFT JOIN to a holidays reference table.


-- Problem 058: Kafka Consumer Lag — Detect Periods of Lag Spike
-- Difficulty: Hard
-- Category: Gaps & Islands / Session Detection
-- Tags: islands, monitoring, lag detection, time series
--
-- Scenario:
-- A data engineering team monitors Kafka consumer lag. Every 5 minutes a
-- metric is recorded. A "lag spike episode" is a contiguous sequence of
-- readings where lag_messages > 10000. Find every such episode per consumer
-- group, including its duration.
--
-- Question:
-- Find all continuous lag spike episodes (lag_messages > 10000) per consumer group.
-- Output: consumer_group, episode_start, episode_end, peak_lag, duration_minutes.

-- Schema & Sample Data:
CREATE TABLE kafka_consumer_lag (
    recorded_at      TIMESTAMP,
    consumer_group   VARCHAR(50),
    lag_messages     BIGINT,
    PRIMARY KEY (recorded_at, consumer_group)
);

INSERT INTO kafka_consumer_lag VALUES
('2024-08-01 10:00', 'group_a', 500),
('2024-08-01 10:05', 'group_a', 12000),
('2024-08-01 10:10', 'group_a', 15000),
('2024-08-01 10:15', 'group_a', 11000),
('2024-08-01 10:20', 'group_a', 800),
('2024-08-01 10:25', 'group_a', 9000),
('2024-08-01 10:30', 'group_a', 13000),
('2024-08-01 10:35', 'group_a', 14000),
('2024-08-01 10:00', 'group_b', 50),
('2024-08-01 10:05', 'group_b', 200),
('2024-08-01 10:10', 'group_b', 20000),
('2024-08-01 10:15', 'group_b', 22000),
('2024-08-01 10:20', 'group_b', 18000),
('2024-08-01 10:25', 'group_b', 100);

-- Solution:
WITH spikes AS (
    SELECT consumer_group, recorded_at, lag_messages
    FROM kafka_consumer_lag
    WHERE lag_messages > 10000
),
ranked AS (
    SELECT
        consumer_group,
        recorded_at,
        lag_messages,
        recorded_at - (ROW_NUMBER() OVER (PARTITION BY consumer_group ORDER BY recorded_at) * INTERVAL '5 minutes') AS grp
    FROM spikes
)
SELECT
    consumer_group,
    MIN(recorded_at)                                                          AS episode_start,
    MAX(recorded_at)                                                          AS episode_end,
    MAX(lag_messages)                                                         AS peak_lag,
    EXTRACT(EPOCH FROM (MAX(recorded_at) - MIN(recorded_at))) / 60 + 5       AS duration_minutes
FROM ranked
GROUP BY consumer_group, grp
ORDER BY consumer_group, episode_start;

-- Expected Output:
-- consumer_group | episode_start    | episode_end      | peak_lag | duration_minutes
-- ---------------+------------------+------------------+----------+-----------------
-- group_a        | 2024-08-01 10:05 | 2024-08-01 10:15 | 15000    | 15
-- group_a        | 2024-08-01 10:30 | 2024-08-01 10:35 | 14000    | 10
-- group_b        | 2024-08-01 10:10 | 2024-08-01 10:20 | 22000    | 15

-- Explanation:
-- Filter to spike-only rows, then apply the row_number subtraction trick
-- (using 5-minute intervals). Each consecutive 5-min spike block forms one
-- island. Duration adds 5 minutes because the last reading itself covers 5 min.
--
-- Follow-up: Also report the total number of spike episodes per consumer group
--            and the average peak lag across all episodes.


-- Problem 059: Finding the First and Last Active Day in Each Month (Per User)
-- Difficulty: Medium
-- Category: Gaps & Islands / Session Detection
-- Tags: date trunc, first/last, window functions, user activity
--
-- Scenario:
-- A SaaS analytics platform needs to compute, for each user and each calendar
-- month, their first active day and last active day. This feeds a churn model
-- that looks at within-month engagement patterns.
--
-- Question:
-- For each user and each month, return first_active_date, last_active_date,
-- and days_active count.

-- Schema & Sample Data:
CREATE TABLE user_activity_log (
    user_id     INT,
    activity_date DATE,
    PRIMARY KEY (user_id, activity_date)
);

INSERT INTO user_activity_log VALUES
(1, '2024-01-03'), (1, '2024-01-07'), (1, '2024-01-15'), (1, '2024-01-22'),
(1, '2024-02-01'), (1, '2024-02-28'),
(2, '2024-01-05'), (2, '2024-01-06'), (2, '2024-01-07'),
(2, '2024-02-14'), (2, '2024-02-15'), (2, '2024-02-20'),
(3, '2024-01-31');

-- Solution:
SELECT
    user_id,
    DATE_TRUNC('month', activity_date)::DATE  AS activity_month,
    MIN(activity_date)                        AS first_active_date,
    MAX(activity_date)                        AS last_active_date,
    COUNT(*)                                  AS days_active
FROM user_activity_log
GROUP BY user_id, DATE_TRUNC('month', activity_date)
ORDER BY user_id, activity_month;

-- Expected Output:
-- user_id | activity_month | first_active_date | last_active_date | days_active
-- --------+----------------+-------------------+------------------+------------
-- 1       | 2024-01-01     | 2024-01-03        | 2024-01-22       | 4
-- 1       | 2024-02-01     | 2024-02-01        | 2024-02-28       | 2
-- 2       | 2024-01-01     | 2024-01-05        | 2024-01-07       | 3
-- 2       | 2024-02-01     | 2024-02-14        | 2024-02-20       | 3
-- 3       | 2024-01-01     | 2024-01-31        | 2024-01-31       | 1

-- Explanation:
-- DATE_TRUNC to month standardizes the grouping key. MIN/MAX give the
-- boundary dates, COUNT gives days_active within the month.
--
-- Follow-up: Flag users who were only active in the first half of any given
--            month (last_active_date <= 15th) as "early-month dropoff".


-- Problem 060: Detecting State Transitions — Account Status Changes
-- Difficulty: Hard
-- Category: Gaps & Islands / Session Detection
-- Tags: state machine, lag, transitions, account lifecycle
--
-- Scenario:
-- A trust & safety team tracks account status changes over time (active,
-- suspended, banned, reinstated). They need to detect all transitions, the
-- duration spent in each state, and flag accounts that flipped between
-- suspended and active more than twice (potential ban evasion pattern).
--
-- Question:
-- For each account, find every state interval (state, start_date, end_date,
-- days_in_state). Assume the current date is 2024-09-01 for open intervals.

-- Schema & Sample Data:
CREATE TABLE account_status_history (
    account_id   INT,
    status       VARCHAR(20),
    changed_at   DATE,
    PRIMARY KEY (account_id, changed_at)
);

INSERT INTO account_status_history VALUES
(501, 'active',     '2024-01-01'),
(501, 'suspended',  '2024-02-10'),
(501, 'active',     '2024-02-20'),
(501, 'suspended',  '2024-03-15'),
(501, 'banned',     '2024-04-01'),
(502, 'active',     '2024-01-15'),
(502, 'suspended',  '2024-03-01'),
(502, 'active',     '2024-03-10'),
(503, 'active',     '2024-06-01');

-- Solution:
WITH next_status AS (
    SELECT
        account_id,
        status,
        changed_at                                                  AS state_start,
        LEAD(changed_at) OVER (PARTITION BY account_id ORDER BY changed_at) AS state_end
    FROM account_status_history
)
SELECT
    account_id,
    status,
    state_start,
    COALESCE(state_end, DATE '2024-09-01')          AS state_end,
    COALESCE(state_end, DATE '2024-09-01') - state_start AS days_in_state
FROM next_status
ORDER BY account_id, state_start;

-- Expected Output:
-- account_id | status    | state_start | state_end  | days_in_state
-- -----------+-----------+-------------+------------+--------------
-- 501        | active    | 2024-01-01  | 2024-02-10 | 40
-- 501        | suspended | 2024-02-10  | 2024-02-20 | 10
-- 501        | active    | 2024-02-20  | 2024-03-15 | 24
-- 501        | suspended | 2024-03-15  | 2024-04-01 | 17
-- 501        | banned    | 2024-04-01  | 2024-09-01 | 153
-- 502        | active    | 2024-01-15  | 2024-03-01 | 46
-- 502        | suspended | 2024-03-01  | 2024-03-10 | 9
-- 502        | active    | 2024-03-10  | 2024-09-01 | 175
-- 503        | active    | 2024-06-01  | 2024-09-01 | 92

-- Explanation:
-- LEAD gets the next change date for each account. COALESCE fills NULL end
-- dates (still in that state) with the reference current date. Subtraction
-- of DATE values gives days_in_state directly in PostgreSQL.
--
-- Follow-up: Count how many times each account toggled between 'suspended'
--            and 'active', and flag accounts with more than 2 such toggles.


-- Problem 061: Network Downtime — Merge Overlapping Outage Windows Per Region
-- Difficulty: Hard
-- Category: Gaps & Islands / Session Detection
-- Tags: merge intervals, infrastructure, uptime, window functions
--
-- Scenario:
-- An infrastructure monitoring system records outage tickets. Multiple tickets
-- can be open simultaneously for the same region. You need to compute total
-- actual downtime per region by merging overlapping outage windows.
--
-- Question:
-- Compute total downtime minutes per region after merging all overlapping outages.
-- Output: region, total_downtime_minutes, outage_event_count (after merging).

-- Schema & Sample Data:
CREATE TABLE outage_tickets (
    ticket_id  INT PRIMARY KEY,
    region     VARCHAR(30),
    started_at TIMESTAMP,
    resolved_at TIMESTAMP
);

INSERT INTO outage_tickets VALUES
(1, 'us-east-1', '2024-09-10 02:00', '2024-09-10 02:45'),
(2, 'us-east-1', '2024-09-10 02:20', '2024-09-10 03:10'),  -- overlaps 1
(3, 'us-east-1', '2024-09-10 05:00', '2024-09-10 05:30'),  -- separate
(4, 'us-west-2', '2024-09-10 01:00', '2024-09-10 01:30'),
(5, 'us-west-2', '2024-09-10 01:25', '2024-09-10 02:00'),  -- overlaps 4
(6, 'us-west-2', '2024-09-10 02:00', '2024-09-10 02:15'),  -- adjacent to 5
(7, 'eu-west-1', '2024-09-10 06:00', '2024-09-10 07:00'),
(8, 'eu-west-1', '2024-09-10 06:30', '2024-09-10 08:00'),  -- overlaps 7
(9, 'eu-west-1', '2024-09-10 09:00', '2024-09-10 09:20');  -- separate

-- Solution:
WITH ordered AS (
    SELECT
        region,
        started_at,
        resolved_at,
        MAX(resolved_at) OVER (
            PARTITION BY region
            ORDER BY started_at
            ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
        ) AS prev_max_end
    FROM outage_tickets
),
island_flags AS (
    SELECT
        region, started_at, resolved_at,
        CASE WHEN started_at >= prev_max_end OR prev_max_end IS NULL THEN 1 ELSE 0 END AS new_island
    FROM ordered
),
islands AS (
    SELECT
        region, started_at, resolved_at,
        SUM(new_island) OVER (PARTITION BY region ORDER BY started_at) AS island_id
    FROM island_flags
),
merged AS (
    SELECT
        region,
        MIN(started_at)  AS merged_start,
        MAX(resolved_at) AS merged_end
    FROM islands
    GROUP BY region, island_id
)
SELECT
    region,
    SUM(EXTRACT(EPOCH FROM (merged_end - merged_start)) / 60)::INT AS total_downtime_minutes,
    COUNT(*)                                                        AS outage_event_count
FROM merged
GROUP BY region
ORDER BY region;

-- Expected Output:
-- region     | total_downtime_minutes | outage_event_count
-- -----------+------------------------+-------------------
-- eu-west-1  | 110                    | 2
-- us-east-1  | 115                    | 2
-- us-west-2  | 75                     | 1

-- Explanation:
-- Running MAX of resolved_at detects overlapping/adjacent windows. Rows where
-- started_at >= prev_max_end begin a new island. After merging, SUM the
-- duration of each merged window to get total downtime.
--
-- Follow-up: Compute per-region uptime percentage over the full 24-hour day.


-- Problem 062: Detecting Repeated Failed Login Sequences (Brute-Force Detection)
-- Difficulty: Hard
-- Category: Gaps & Islands / Session Detection
-- Tags: security, sequence detection, window functions, islands
--
-- Scenario:
-- A security team wants to detect brute-force login attempts: any sequence of
-- 5 or more consecutive FAILED login attempts from the same IP address,
-- without any SUCCESS in between. Report each such island: ip, start_time,
-- end_time, and attempt_count.
--
-- Question:
-- Identify all islands of 5+ consecutive failed logins per IP address.
-- Output: ip_address, island_start, island_end, consecutive_failures.

-- Schema & Sample Data:
CREATE TABLE login_attempts (
    attempt_id  BIGINT PRIMARY KEY,
    ip_address  VARCHAR(20),
    user_email  VARCHAR(100),
    status      VARCHAR(10),  -- 'SUCCESS' or 'FAILED'
    attempted_at TIMESTAMP
);

INSERT INTO login_attempts VALUES
(1,  '10.0.0.1', 'a@x.com', 'FAILED', '2024-10-01 03:00:00'),
(2,  '10.0.0.1', 'a@x.com', 'FAILED', '2024-10-01 03:00:05'),
(3,  '10.0.0.1', 'a@x.com', 'FAILED', '2024-10-01 03:00:10'),
(4,  '10.0.0.1', 'a@x.com', 'FAILED', '2024-10-01 03:00:15'),
(5,  '10.0.0.1', 'a@x.com', 'FAILED', '2024-10-01 03:00:20'),
(6,  '10.0.0.1', 'a@x.com', 'SUCCESS','2024-10-01 03:00:25'),  -- breaks island
(7,  '10.0.0.1', 'b@x.com', 'FAILED', '2024-10-01 03:01:00'),
(8,  '10.0.0.1', 'b@x.com', 'FAILED', '2024-10-01 03:01:05'),
(9,  '10.0.0.1', 'b@x.com', 'FAILED', '2024-10-01 03:01:10'),
(10, '10.0.0.2', 'c@x.com', 'FAILED', '2024-10-01 04:00:00'),
(11, '10.0.0.2', 'c@x.com', 'FAILED', '2024-10-01 04:00:03'),
(12, '10.0.0.2', 'c@x.com', 'FAILED', '2024-10-01 04:00:06'),
(13, '10.0.0.2', 'c@x.com', 'FAILED', '2024-10-01 04:00:09'),
(14, '10.0.0.2', 'c@x.com', 'FAILED', '2024-10-01 04:00:12'),
(15, '10.0.0.2', 'c@x.com', 'FAILED', '2024-10-01 04:00:15');

-- Solution:
WITH ordered AS (
    SELECT
        attempt_id,
        ip_address,
        status,
        attempted_at,
        CASE
            WHEN status = 'SUCCESS' THEN 1
            WHEN LAG(status) OVER (PARTITION BY ip_address ORDER BY attempted_at) = 'SUCCESS' THEN 1
            WHEN LAG(status) OVER (PARTITION BY ip_address ORDER BY attempted_at) IS NULL THEN 1
            ELSE 0
        END AS new_island
    FROM login_attempts
),
islands AS (
    SELECT
        attempt_id,
        ip_address,
        status,
        attempted_at,
        SUM(new_island) OVER (PARTITION BY ip_address ORDER BY attempted_at) AS island_id
    FROM ordered
),
failed_islands AS (
    SELECT
        ip_address,
        island_id,
        MIN(attempted_at) AS island_start,
        MAX(attempted_at) AS island_end,
        COUNT(*)          AS attempt_count,
        BOOL_AND(status = 'FAILED') AS all_failed
    FROM islands
    GROUP BY ip_address, island_id
)
SELECT
    ip_address,
    island_start,
    island_end,
    attempt_count AS consecutive_failures
FROM failed_islands
WHERE all_failed AND attempt_count >= 5
ORDER BY ip_address, island_start;

-- Expected Output:
-- ip_address | island_start        | island_end          | consecutive_failures
-- -----------+---------------------+---------------------+---------------------
-- 10.0.0.1   | 2024-10-01 03:00:00 | 2024-10-01 03:00:20 | 5
-- 10.0.0.2   | 2024-10-01 04:00:00 | 2024-10-01 04:00:15 | 6

-- Explanation:
-- A new island begins at a SUCCESS row or at the first row per IP. Running SUM
-- of new_island flags assigns each row to an island. After grouping, filter
-- islands where every row is FAILED and count >= 5.
-- The 3-failure island at 10.0.0.1 (rows 7-9) is excluded (count < 5).
--
-- Follow-up: Extend to also capture the target email addresses attempted
--            within each brute-force island as an array.
