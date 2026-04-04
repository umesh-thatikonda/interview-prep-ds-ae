-- SQL Speed Reference for CodeSignal — Bike-Sharing Data Context

-- ─── TYPICAL SCHEMA ───────────────────────────────────────────────────────────
-- trips(trip_id, start_time, end_time, duration_sec, start_station_id,
--        end_station_id, bike_id, user_type, birth_year, gender)
-- stations(station_id, station_name, lat, lon, capacity)

-- ─── BASIC AGGREGATIONS ───────────────────────────────────────────────────────
SELECT
    start_station_id,
    COUNT(*)                  AS trip_count,
    AVG(duration_sec)         AS avg_duration,
    SUM(duration_sec)         AS total_duration,
    MIN(start_time)           AS first_trip,
    MAX(start_time)           AS last_trip
FROM trips
GROUP BY start_station_id
ORDER BY trip_count DESC;

-- ─── JOINS ────────────────────────────────────────────────────────────────────
-- Enrich trips with station names
SELECT
    t.trip_id,
    s1.station_name  AS start_station,
    s2.station_name  AS end_station,
    t.duration_sec
FROM trips t
JOIN stations s1 ON t.start_station_id = s1.station_id
JOIN stations s2 ON t.end_station_id   = s2.station_id;

-- Left join (keep all trips even if station missing)
SELECT t.*, s.station_name
FROM trips t
LEFT JOIN stations s ON t.start_station_id = s.station_id;

-- ─── FILTERING ────────────────────────────────────────────────────────────────
WHERE duration_sec > 0
WHERE start_station_id IS NOT NULL
WHERE user_type = 'Subscriber'
WHERE start_time >= '2024-01-01' AND start_time < '2024-02-01'
WHERE start_station_id != end_station_id   -- exclude round trips

-- ─── DATE / TIME ──────────────────────────────────────────────────────────────
-- Extract parts (SQLite style)
strftime('%Y', start_time)         AS year
strftime('%m', start_time)         AS month
strftime('%H', start_time)         AS hour
strftime('%w', start_time)         AS weekday   -- 0=Sunday

-- Standard SQL / PostgreSQL / BigQuery
EXTRACT(HOUR FROM start_time)      AS hour
DATE_TRUNC('month', start_time)    AS month
CAST(start_time AS DATE)           AS date

-- ─── CTEs ─────────────────────────────────────────────────────────────────────
WITH station_counts AS (
    SELECT start_station_id, COUNT(*) AS trips
    FROM trips
    GROUP BY start_station_id
),
ranked AS (
    SELECT *, RANK() OVER (ORDER BY trips DESC) AS rnk
    FROM station_counts
)
SELECT * FROM ranked WHERE rnk <= 10;

-- ─── WINDOW FUNCTIONS ─────────────────────────────────────────────────────────
-- Rank stations by trip count
SELECT
    start_station_id,
    COUNT(*) AS trips,
    RANK()       OVER (ORDER BY COUNT(*) DESC) AS rnk,
    ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS row_num,
    DENSE_RANK() OVER (ORDER BY COUNT(*) DESC) AS dense_rnk
FROM trips
GROUP BY start_station_id;

-- Running total
SELECT
    trip_id,
    start_time,
    duration_sec,
    SUM(duration_sec) OVER (ORDER BY start_time) AS running_total
FROM trips;

-- Lag / Lead (compare to previous row)
SELECT
    trip_id,
    start_time,
    LAG(start_time) OVER (PARTITION BY bike_id ORDER BY start_time) AS prev_trip_start
FROM trips;

-- ─── SUBQUERIES ───────────────────────────────────────────────────────────────
-- Trips longer than average
SELECT * FROM trips
WHERE duration_sec > (SELECT AVG(duration_sec) FROM trips);

-- Top station by trip count
SELECT start_station_id
FROM trips
GROUP BY start_station_id
ORDER BY COUNT(*) DESC
LIMIT 1;

-- ─── CASE WHEN ────────────────────────────────────────────────────────────────
SELECT
    trip_id,
    CASE
        WHEN duration_sec < 600  THEN 'short'
        WHEN duration_sec < 1800 THEN 'medium'
        ELSE 'long'
    END AS trip_category
FROM trips;

-- ─── DEDUPLICATION ────────────────────────────────────────────────────────────
-- Keep first occurrence per trip_id
WITH deduped AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY trip_id ORDER BY start_time) AS rn
    FROM trips
)
SELECT * FROM deduped WHERE rn = 1;

-- ─── HAVING ───────────────────────────────────────────────────────────────────
-- Stations with more than 100 trips
SELECT start_station_id, COUNT(*) AS trips
FROM trips
GROUP BY start_station_id
HAVING COUNT(*) > 100;
