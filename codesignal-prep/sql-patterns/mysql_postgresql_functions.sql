-- ══════════════════════════════════════════════════════════════════════════════
-- MySQL & PostgreSQL Functions — Bike Trip Data Context
-- ══════════════════════════════════════════════════════════════════════════════
-- Each section shows MySQL syntax first, then PostgreSQL equivalent.
-- All examples use bike trip columns:
--   started_at, ended_at (DATETIME/TIMESTAMP)
--   duration_sec (INT), member_casual (VARCHAR), ride_id (VARCHAR)
-- ══════════════════════════════════════════════════════════════════════════════


-- ══════════════════════════════════════════════════════════════════════════════
-- 1. TIMESTAMP — CALCULATING DURATION
-- ══════════════════════════════════════════════════════════════════════════════

-- MySQL
SELECT
    ride_id,
    TIMESTAMPDIFF(SECOND, started_at, ended_at)  AS duration_sec,
    TIMESTAMPDIFF(MINUTE, started_at, ended_at)  AS duration_min,
    TIMESTAMPDIFF(HOUR,   started_at, ended_at)  AS duration_hr,
    TIMESTAMPDIFF(DAY,    started_at, ended_at)  AS duration_day
FROM trips;

-- PostgreSQL
SELECT
    ride_id,
    EXTRACT(EPOCH FROM (ended_at - started_at))          AS duration_sec,
    EXTRACT(EPOCH FROM (ended_at - started_at)) / 60     AS duration_min,
    EXTRACT(EPOCH FROM (ended_at - started_at)) / 3600   AS duration_hr,
    ended_at - started_at                                AS interval_value
FROM trips;


-- ══════════════════════════════════════════════════════════════════════════════
-- 2. TIMESTAMP — EXTRACTING PARTS
-- ══════════════════════════════════════════════════════════════════════════════

-- MySQL
SELECT
    started_at,
    YEAR(started_at)        AS yr,
    MONTH(started_at)       AS mon,
    DAY(started_at)         AS dy,
    HOUR(started_at)        AS hr,
    MINUTE(started_at)      AS mn,
    SECOND(started_at)      AS sec,
    DAYOFWEEK(started_at)   AS dow,        -- 1=Sunday, 7=Saturday
    DAYOFYEAR(started_at)   AS doy,
    WEEK(started_at)        AS week_num,
    QUARTER(started_at)     AS qtr,
    DAYNAME(started_at)     AS day_name,   -- "Monday", "Tuesday"...
    MONTHNAME(started_at)   AS month_name  -- "January", "February"...
FROM trips;

-- PostgreSQL
SELECT
    started_at,
    EXTRACT(YEAR    FROM started_at)  AS yr,
    EXTRACT(MONTH   FROM started_at)  AS mon,
    EXTRACT(DAY     FROM started_at)  AS dy,
    EXTRACT(HOUR    FROM started_at)  AS hr,
    EXTRACT(MINUTE  FROM started_at)  AS mn,
    EXTRACT(SECOND  FROM started_at)  AS sec,
    EXTRACT(DOW     FROM started_at)  AS dow,   -- 0=Sunday, 6=Saturday
    EXTRACT(DOY     FROM started_at)  AS doy,
    EXTRACT(WEEK    FROM started_at)  AS week_num,
    EXTRACT(QUARTER FROM started_at)  AS qtr,
    TO_CHAR(started_at, 'Day')        AS day_name,
    TO_CHAR(started_at, 'Month')      AS month_name
FROM trips;


-- ══════════════════════════════════════════════════════════════════════════════
-- 3. TIMESTAMP — FORMATTING & CONVERTING
-- ══════════════════════════════════════════════════════════════════════════════

-- MySQL
SELECT
    DATE_FORMAT(started_at, '%Y-%m-%d')           AS date_only,
    DATE_FORMAT(started_at, '%H:%i:%s')           AS time_only,
    DATE_FORMAT(started_at, '%Y-%m-%d %H:00:00')  AS hour_bucket,
    DATE_FORMAT(started_at, '%W %M %Y')           AS readable,   -- "Friday March 2024"
    DATE(started_at)                              AS date_cast,
    TIME(started_at)                              AS time_cast,
    UNIX_TIMESTAMP(started_at)                    AS unix_ts,
    FROM_UNIXTIME(1709280000)                     AS from_unix
FROM trips;

-- PostgreSQL
SELECT
    TO_CHAR(started_at, 'YYYY-MM-DD')             AS date_only,
    TO_CHAR(started_at, 'HH24:MI:SS')             AS time_only,
    TO_CHAR(started_at, 'YYYY-MM-DD HH24:00:00')  AS hour_bucket,
    TO_CHAR(started_at, 'FMDay FMMonth YYYY')     AS readable,
    started_at::DATE                               AS date_cast,
    started_at::TIME                               AS time_cast,
    EXTRACT(EPOCH FROM started_at)                 AS unix_ts,
    TO_TIMESTAMP(1709280000)                       AS from_unix
FROM trips;


-- ══════════════════════════════════════════════════════════════════════════════
-- 4. TIMESTAMP — TRUNCATING (bucketing by time period)
-- ══════════════════════════════════════════════════════════════════════════════

-- MySQL  (no DATE_TRUNC — use DATE_FORMAT or arithmetic)
SELECT
    DATE(started_at)                                           AS day_bucket,
    DATE_FORMAT(started_at, '%Y-%m-01')                        AS month_bucket,
    DATE_FORMAT(started_at, '%Y-01-01')                        AS year_bucket,
    DATE_FORMAT(started_at, '%Y-%m-%d %H:00:00')               AS hour_bucket,
    -- week bucket (Monday as start)
    DATE_SUB(DATE(started_at), INTERVAL WEEKDAY(started_at) DAY) AS week_bucket
FROM trips;

-- PostgreSQL  (DATE_TRUNC is clean and powerful)
SELECT
    DATE_TRUNC('day',     started_at)   AS day_bucket,
    DATE_TRUNC('month',   started_at)   AS month_bucket,
    DATE_TRUNC('year',    started_at)   AS year_bucket,
    DATE_TRUNC('hour',    started_at)   AS hour_bucket,
    DATE_TRUNC('week',    started_at)   AS week_bucket,    -- Monday start
    DATE_TRUNC('quarter', started_at)   AS quarter_bucket
FROM trips;


-- ══════════════════════════════════════════════════════════════════════════════
-- 5. TIMESTAMP — ARITHMETIC (adding/subtracting time)
-- ══════════════════════════════════════════════════════════════════════════════

-- MySQL
SELECT
    started_at,
    DATE_ADD(started_at,  INTERVAL 7 DAY)     AS plus_7_days,
    DATE_SUB(started_at,  INTERVAL 1 MONTH)   AS minus_1_month,
    DATE_ADD(started_at,  INTERVAL 2 HOUR)    AS plus_2_hours,
    DATE_ADD(started_at,  INTERVAL 30 MINUTE) AS plus_30_min,
    DATEDIFF(ended_at, started_at)            AS days_diff,    -- integer days
    NOW()                                     AS current_ts,
    CURDATE()                                 AS current_date,
    CURTIME()                                 AS current_time
FROM trips;

-- PostgreSQL
SELECT
    started_at,
    started_at + INTERVAL '7 days'            AS plus_7_days,
    started_at - INTERVAL '1 month'           AS minus_1_month,
    started_at + INTERVAL '2 hours'           AS plus_2_hours,
    started_at + INTERVAL '30 minutes'        AS plus_30_min,
    ended_at::DATE - started_at::DATE         AS days_diff,
    NOW()                                     AS current_ts,
    CURRENT_DATE                              AS current_date,
    CURRENT_TIME                              AS current_time
FROM trips;


-- ══════════════════════════════════════════════════════════════════════════════
-- 6. TIMESTAMP — FILTERING BY TIME RANGE
-- ══════════════════════════════════════════════════════════════════════════════

-- MySQL
SELECT * FROM trips
WHERE started_at >= '2024-03-01 00:00:00'
  AND started_at <  '2024-04-01 00:00:00';

-- Filter last 7 days
SELECT * FROM trips
WHERE started_at >= DATE_SUB(NOW(), INTERVAL 7 DAY);

-- Filter by hour of day (morning rush)
SELECT * FROM trips
WHERE HOUR(started_at) BETWEEN 7 AND 9;

-- Filter weekdays only
SELECT * FROM trips
WHERE DAYOFWEEK(started_at) NOT IN (1, 7);  -- 1=Sun, 7=Sat

-- PostgreSQL equivalents
SELECT * FROM trips
WHERE started_at >= '2024-03-01'::TIMESTAMP
  AND started_at <  '2024-04-01'::TIMESTAMP;

SELECT * FROM trips
WHERE started_at >= NOW() - INTERVAL '7 days';

SELECT * FROM trips
WHERE EXTRACT(HOUR FROM started_at) BETWEEN 7 AND 9;

SELECT * FROM trips
WHERE EXTRACT(DOW FROM started_at) NOT IN (0, 6);  -- 0=Sun, 6=Sat


-- ══════════════════════════════════════════════════════════════════════════════
-- 7. AGGREGATIONS
-- ══════════════════════════════════════════════════════════════════════════════

-- Both MySQL and PostgreSQL (same syntax)
SELECT
    COUNT(*)                            AS total_trips,
    COUNT(DISTINCT ride_id)             AS unique_trips,
    COUNT(DISTINCT start_station_id)    AS unique_stations,
    SUM(duration_sec)                   AS total_duration_sec,
    AVG(duration_sec)                   AS avg_duration_sec,
    ROUND(AVG(duration_sec) / 60, 2)    AS avg_duration_min,
    MIN(duration_sec)                   AS min_duration,
    MAX(duration_sec)                   AS max_duration,
    STDDEV(duration_sec)                AS std_duration,    -- MySQL: STDDEV, PG: STDDEV_SAMP
    VARIANCE(duration_sec)              AS var_duration
FROM trips;

-- Conditional aggregation (same in both)
SELECT
    COUNT(*)                                                      AS total,
    SUM(CASE WHEN member_casual = 'member' THEN 1 ELSE 0 END)    AS member_count,
    SUM(CASE WHEN member_casual = 'casual' THEN 1 ELSE 0 END)    AS casual_count,
    ROUND(100.0 * SUM(CASE WHEN member_casual = 'member' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_member
FROM trips;


-- ══════════════════════════════════════════════════════════════════════════════
-- 8. STRING FUNCTIONS
-- ══════════════════════════════════════════════════════════════════════════════

-- MySQL
SELECT
    UPPER(member_casual)                AS upper_val,
    LOWER(member_casual)                AS lower_val,
    LENGTH(ride_id)                     AS str_length,
    TRIM(ride_id)                       AS trimmed,
    CONCAT(start_station_id, '->', end_station_id) AS route,
    SUBSTRING(ride_id, 1, 3)            AS first_3_chars,
    REPLACE(ride_id, 'R', 'RIDE-')      AS replaced,
    COALESCE(start_station_id, 'Unknown') AS with_default,
    NULLIF(start_station_id, '')        AS empty_to_null  -- turns '' into NULL
FROM trips;

-- PostgreSQL (mostly same, minor differences)
SELECT
    UPPER(member_casual)                AS upper_val,
    LOWER(member_casual)                AS lower_val,
    LENGTH(ride_id)                     AS str_length,
    TRIM(ride_id)                       AS trimmed,
    CONCAT(start_station_id, '->', end_station_id) AS route,
    SUBSTRING(ride_id FROM 1 FOR 3)     AS first_3_chars,  -- PG syntax
    LEFT(ride_id, 3)                    AS left_3,         -- also works
    REPLACE(ride_id, 'R', 'RIDE-')      AS replaced,
    COALESCE(start_station_id, 'Unknown') AS with_default,
    NULLIF(start_station_id, '')        AS empty_to_null
FROM trips;


-- ══════════════════════════════════════════════════════════════════════════════
-- 9. WINDOW FUNCTIONS (same in both MySQL 8+ and PostgreSQL)
-- ══════════════════════════════════════════════════════════════════════════════

SELECT
    ride_id,
    start_station_id,
    started_at,
    duration_sec,

    -- Ranking
    ROW_NUMBER()  OVER (ORDER BY duration_sec DESC)                          AS row_num,
    RANK()        OVER (ORDER BY duration_sec DESC)                          AS rnk,
    DENSE_RANK()  OVER (ORDER BY duration_sec DESC)                          AS dense_rnk,
    PERCENT_RANK()OVER (ORDER BY duration_sec)                               AS pct_rank,
    NTILE(4)      OVER (ORDER BY duration_sec)                               AS quartile,

    -- Ranking within group
    ROW_NUMBER()  OVER (PARTITION BY start_station_id ORDER BY duration_sec DESC) AS rank_in_station,

    -- Running totals
    SUM(duration_sec)   OVER (ORDER BY started_at)                           AS running_total_sec,
    COUNT(*)            OVER (ORDER BY started_at)                           AS running_count,
    AVG(duration_sec)   OVER (ORDER BY started_at)                           AS running_avg,

    -- Cumulative within partition
    SUM(duration_sec)   OVER (PARTITION BY start_station_id ORDER BY started_at) AS cumul_per_station,

    -- Rolling window (last 3 rows)
    AVG(duration_sec)   OVER (ORDER BY started_at ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3_avg,
    MAX(duration_sec)   OVER (ORDER BY started_at ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS rolling_3_max,

    -- Lead / Lag
    LAG(duration_sec)   OVER (ORDER BY started_at)                           AS prev_duration,
    LEAD(duration_sec)  OVER (ORDER BY started_at)                           AS next_duration,
    LAG(started_at)     OVER (PARTITION BY start_station_id ORDER BY started_at) AS prev_trip_at_station,

    -- First / Last value in window
    FIRST_VALUE(ride_id)OVER (PARTITION BY start_station_id ORDER BY started_at) AS first_trip_at_station,
    LAST_VALUE(ride_id) OVER (PARTITION BY start_station_id ORDER BY started_at
                              ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS last_trip_at_station

FROM trips;


-- ══════════════════════════════════════════════════════════════════════════════
-- 10. CTEs & SUBQUERIES
-- ══════════════════════════════════════════════════════════════════════════════

-- Both MySQL 8+ and PostgreSQL
WITH valid_trips AS (
    SELECT *
    FROM trips
    WHERE TIMESTAMPDIFF(SECOND, started_at, ended_at) > 0   -- MySQL
    -- WHERE EXTRACT(EPOCH FROM (ended_at - started_at)) > 0  -- PostgreSQL
      AND start_station_id IS NOT NULL
      AND end_station_id   IS NOT NULL
),
station_stats AS (
    SELECT
        start_station_id,
        COUNT(*)                         AS trip_count,
        AVG(TIMESTAMPDIFF(SECOND, started_at, ended_at)) AS avg_duration
    FROM valid_trips
    GROUP BY start_station_id
),
ranked AS (
    SELECT *,
        RANK() OVER (ORDER BY trip_count DESC) AS rnk
    FROM station_stats
)
SELECT * FROM ranked WHERE rnk <= 5;


-- ══════════════════════════════════════════════════════════════════════════════
-- 11. NULL HANDLING
-- ══════════════════════════════════════════════════════════════════════════════

-- Both MySQL and PostgreSQL
SELECT
    COALESCE(start_station_id, 'Unknown')       AS station_or_default,
    NULLIF(duration_sec, 0)                     AS null_if_zero,
    ISNULL(start_station_id)                    AS is_null_mysql,   -- MySQL only
    start_station_id IS NULL                    AS is_null_pg,      -- PostgreSQL / also works in MySQL
    IFNULL(start_station_id, 'Unknown')         AS ifnull_mysql,    -- MySQL only
    COALESCE(start_station_id, 'Unknown')       AS coalesce_both    -- works in both
FROM trips;


-- ══════════════════════════════════════════════════════════════════════════════
-- 12. CASE WHEN
-- ══════════════════════════════════════════════════════════════════════════════

-- Both MySQL and PostgreSQL
SELECT
    ride_id,
    duration_sec,
    CASE
        WHEN duration_sec < 300   THEN 'very short'
        WHEN duration_sec < 600   THEN 'short'
        WHEN duration_sec < 1800  THEN 'medium'
        WHEN duration_sec < 3600  THEN 'long'
        ELSE                           'very long'
    END AS trip_category,

    CASE member_casual
        WHEN 'member' THEN 1
        WHEN 'casual' THEN 0
        ELSE NULL
    END AS is_member,

    -- inline if (MySQL: IF, PostgreSQL: use CASE)
    IF(member_casual = 'member', 'M', 'C')      AS type_code_mysql,  -- MySQL only
    CASE WHEN member_casual = 'member' THEN 'M' ELSE 'C' END AS type_code_pg

FROM trips;


-- ══════════════════════════════════════════════════════════════════════════════
-- 13. JOINS
-- ══════════════════════════════════════════════════════════════════════════════

-- Both MySQL and PostgreSQL (same syntax)

-- Inner join — only trips with matching station
SELECT t.ride_id, s.station_name, s.region
FROM trips t
JOIN stations s ON t.start_station_id = s.station_id;

-- Left join — all trips, null for unmatched stations
SELECT t.ride_id, COALESCE(s.station_name, 'Unknown') AS station_name
FROM trips t
LEFT JOIN stations s ON t.start_station_id = s.station_id;

-- Self join — find consecutive trips from same station
SELECT
    t1.ride_id,
    t1.started_at,
    t2.ride_id   AS next_ride_id,
    t2.started_at AS next_started_at
FROM trips t1
JOIN trips t2
  ON  t1.start_station_id = t2.start_station_id
  AND t2.started_at > t1.started_at;

-- Cross join — every station pair combination
SELECT s1.station_name AS from_station, s2.station_name AS to_station
FROM stations s1
CROSS JOIN stations s2
WHERE s1.station_id != s2.station_id;


-- ══════════════════════════════════════════════════════════════════════════════
-- 14. DEDUPLICATION
-- ══════════════════════════════════════════════════════════════════════════════

-- Keep first occurrence of each ride_id (both MySQL and PostgreSQL)
WITH deduped AS (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY ride_id ORDER BY started_at) AS rn
    FROM trips
)
SELECT * FROM deduped WHERE rn = 1;


-- ══════════════════════════════════════════════════════════════════════════════
-- 15. USEFUL BIKE TRIP QUERY PATTERNS
-- ══════════════════════════════════════════════════════════════════════════════

-- Peak hour analysis
SELECT
    HOUR(started_at) AS hour_of_day,           -- MySQL
    -- EXTRACT(HOUR FROM started_at) AS hour_of_day,  -- PostgreSQL
    COUNT(*) AS trip_count,
    ROUND(AVG(TIMESTAMPDIFF(SECOND, started_at, ended_at)) / 60, 2) AS avg_min
FROM trips
WHERE TIMESTAMPDIFF(SECOND, started_at, ended_at) > 0
GROUP BY hour_of_day
ORDER BY trip_count DESC;

-- Most popular route
SELECT
    start_station_id,
    end_station_id,
    COUNT(*) AS trip_count
FROM trips
GROUP BY start_station_id, end_station_id
ORDER BY trip_count DESC
LIMIT 1;

-- Daily member vs casual split
SELECT
    DATE(started_at) AS date,
    SUM(CASE WHEN member_casual = 'member' THEN 1 ELSE 0 END) AS members,
    SUM(CASE WHEN member_casual = 'casual' THEN 1 ELSE 0 END) AS casuals,
    COUNT(*) AS total
FROM trips
GROUP BY DATE(started_at)
ORDER BY date;

-- Trips longer than station average
SELECT t.ride_id, t.start_station_id, t.duration_sec
FROM trips t
JOIN (
    SELECT start_station_id, AVG(duration_sec) AS avg_dur
    FROM trips
    GROUP BY start_station_id
) station_avg ON t.start_station_id = station_avg.start_station_id
WHERE t.duration_sec > station_avg.avg_dur;

-- Day-over-day trip count change
WITH daily AS (
    SELECT DATE(started_at) AS dt, COUNT(*) AS cnt
    FROM trips
    GROUP BY dt
)
SELECT
    dt,
    cnt,
    LAG(cnt)  OVER (ORDER BY dt)                              AS prev_day,
    cnt - LAG(cnt) OVER (ORDER BY dt)                         AS change,
    ROUND(100.0 * (cnt - LAG(cnt) OVER (ORDER BY dt))
          / LAG(cnt) OVER (ORDER BY dt), 1)                   AS pct_change
FROM daily
ORDER BY dt;
