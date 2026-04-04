-- SQL Practice Problems — Bike Share Data
-- All problems use sql_practice_dataset.sql tables: trips, stations, weather
-- Solve each problem, then check against the expected answer below it.

-- ══════════════════════════════════════════════════════════════════════════════
-- WARMUP
-- ══════════════════════════════════════════════════════════════════════════════

-- W1. How many total trips are there?
-- Expected: 30

-- W2. How many distinct start stations were used?
-- Expected: 8

-- W3. How many trips were taken by members vs casual users?
-- Expected: member=18, casual=12



-- ══════════════════════════════════════════════════════════════════════════════
-- LEVEL 1 — BASIC AGGREGATIONS + FILTERS
-- ══════════════════════════════════════════════════════════════════════════════

-- Q1. Calculate trip duration in minutes for each ride.
--     Return: ride_id, started_at, duration_min (rounded to 2 decimal places)
--     Order by duration_min DESC.


-- Q2. Find the top 3 busiest start stations by number of departures.
--     Return: station_name, trip_count
--     Join with stations table to get the name.


-- Q3. What is the average trip duration (in minutes) for members vs casual users?
--     Return: member_casual, avg_duration_min (rounded to 2 dp)


-- Q4. How many trips used each bike type (rideable_type)?
--     Return: rideable_type, trip_count, pct_of_total (rounded to 1 dp)


-- Q5. Find all trips longer than 45 minutes.
--     Return: ride_id, started_at, member_casual, duration_min
--     Order by duration_min DESC.



-- ══════════════════════════════════════════════════════════════════════════════
-- LEVEL 2 — JOINS + DATE/TIME
-- ══════════════════════════════════════════════════════════════════════════════

-- Q6. Enrich each trip with start and end station names and their regions.
--     Return: ride_id, start_station_name, start_region, end_station_name, end_region, member_casual
--     Only include trips where both stations are known.


-- Q7. Count trips per day. Return: date, trip_count, cumulative_trips (running total).
--     Order by date ASC.
--     Hint: use DATE(started_at) and a window function.


-- Q8. For each hour of the day (0–23), how many trips started?
--     Return: hour_of_day, trip_count
--     Order by trip_count DESC to find peak hours.


-- Q9. Join trips with weather data on the trip date.
--     Return: date, condition, trip_count, avg_temp_c
--     How does ridership vary by weather condition?


-- Q10. Find all round trips — where start_station_id = end_station_id.
--      Return: ride_id, station_name, duration_min, member_casual


-- ══════════════════════════════════════════════════════════════════════════════
-- LEVEL 3 — CTEs + SUBQUERIES
-- ══════════════════════════════════════════════════════════════════════════════

-- Q11. Find trips that are longer than the average duration for their user type.
--      (member vs their avg, casual vs their avg — NOT overall avg)
--      Return: ride_id, member_casual, duration_min, user_type_avg_min


-- Q12. For each start station, find the most popular destination.
--      Return: start_station_name, top_end_station_name, trip_count
--      Hint: Use ROW_NUMBER() OVER (PARTITION BY start_station_id ORDER BY count DESC)


-- Q13. Build a daily summary with:
--      date, trip_count, avg_duration_min, pct_member, pct_casual, weather_condition
--      Order by date ASC.


-- Q14. Find stations that are ONLY used as start stations (never as end station).
--      Return: station_id, station_name


-- Q15. What is the busiest 2-hour window of the day?
--      Bucket start times into 2-hour windows: 0-1, 2-3, 4-5, ... 22-23
--      Return: hour_window (e.g. "08-09"), trip_count
--      Order by trip_count DESC.


-- ══════════════════════════════════════════════════════════════════════════════
-- LEVEL 4 — WINDOW FUNCTIONS
-- ══════════════════════════════════════════════════════════════════════════════

-- Q16. Rank stations by total departures using RANK().
--      Return: station_name, trip_count, rank


-- Q17. For each trip, show the previous trip's end station from the SAME start station.
--      Return: ride_id, start_station_id, started_at, prev_end_station_id
--      Hint: LAG(end_station_id) OVER (PARTITION BY start_station_id ORDER BY started_at)


-- Q18. Calculate a 3-day rolling average of daily trip counts.
--      Return: date, trip_count, rolling_3day_avg
--      Hint: Use AVG() OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)


-- Q19. For each day, show:
--      date, trip_count, prev_day_count, day_over_day_change, pct_change
--      Hint: LAG(trip_count) OVER (ORDER BY date)


-- Q20. Identify the top 2 trips (by duration) per start station.
--      Return: station_name, ride_id, duration_min, rank_within_station


-- ══════════════════════════════════════════════════════════════════════════════
-- SOLUTIONS
-- ══════════════════════════════════════════════════════════════════════════════

-- W1.
SELECT COUNT(*) FROM trips;

-- W2.
SELECT COUNT(DISTINCT start_station_id) FROM trips;

-- W3.
SELECT member_casual, COUNT(*) AS trip_count FROM trips GROUP BY member_casual;

-- Q1.
SELECT
    ride_id,
    started_at,
    ROUND((JULIANDAY(ended_at) - JULIANDAY(started_at)) * 24 * 60, 2) AS duration_min
FROM trips
ORDER BY duration_min DESC;

-- Q2.
SELECT s.station_name, COUNT(*) AS trip_count
FROM trips t
JOIN stations s ON t.start_station_id = s.station_id
GROUP BY s.station_name
ORDER BY trip_count DESC
LIMIT 3;

-- Q3.
SELECT
    member_casual,
    ROUND(AVG((JULIANDAY(ended_at) - JULIANDAY(started_at)) * 24 * 60), 2) AS avg_duration_min
FROM trips
GROUP BY member_casual;

-- Q4.
SELECT
    rideable_type,
    COUNT(*) AS trip_count,
    ROUND(100.0 * COUNT(*) / (SELECT COUNT(*) FROM trips), 1) AS pct_of_total
FROM trips
GROUP BY rideable_type;

-- Q5.
SELECT
    ride_id, started_at, member_casual,
    ROUND((JULIANDAY(ended_at) - JULIANDAY(started_at)) * 24 * 60, 2) AS duration_min
FROM trips
WHERE (JULIANDAY(ended_at) - JULIANDAY(started_at)) * 24 * 60 > 45
ORDER BY duration_min DESC;

-- Q6.
SELECT
    t.ride_id,
    s1.station_name AS start_station_name, s1.region AS start_region,
    s2.station_name AS end_station_name,   s2.region AS end_region,
    t.member_casual
FROM trips t
JOIN stations s1 ON t.start_station_id = s1.station_id
JOIN stations s2 ON t.end_station_id   = s2.station_id;

-- Q7.
SELECT
    DATE(started_at) AS date,
    COUNT(*) AS trip_count,
    SUM(COUNT(*)) OVER (ORDER BY DATE(started_at)) AS cumulative_trips
FROM trips
GROUP BY DATE(started_at)
ORDER BY date;

-- Q8.
SELECT
    CAST(strftime('%H', started_at) AS INTEGER) AS hour_of_day,
    COUNT(*) AS trip_count
FROM trips
GROUP BY hour_of_day
ORDER BY trip_count DESC;

-- Q9.
SELECT
    DATE(t.started_at) AS date,
    w.condition,
    COUNT(*) AS trip_count,
    ROUND(w.temp_c, 1) AS avg_temp_c
FROM trips t
JOIN weather w ON DATE(t.started_at) = w.date
GROUP BY DATE(t.started_at), w.condition
ORDER BY date;

-- Q10.
SELECT t.ride_id, s.station_name,
    ROUND((JULIANDAY(t.ended_at) - JULIANDAY(t.started_at)) * 24 * 60, 2) AS duration_min,
    t.member_casual
FROM trips t
JOIN stations s ON t.start_station_id = s.station_id
WHERE t.start_station_id = t.end_station_id;

-- Q11.
WITH user_avgs AS (
    SELECT member_casual,
        AVG((JULIANDAY(ended_at) - JULIANDAY(started_at)) * 24 * 60) AS avg_min
    FROM trips GROUP BY member_casual
)
SELECT
    t.ride_id, t.member_casual,
    ROUND((JULIANDAY(t.ended_at) - JULIANDAY(t.started_at)) * 24 * 60, 2) AS duration_min,
    ROUND(u.avg_min, 2) AS user_type_avg_min
FROM trips t
JOIN user_avgs u ON t.member_casual = u.member_casual
WHERE (JULIANDAY(t.ended_at) - JULIANDAY(t.started_at)) * 24 * 60 > u.avg_min;

-- Q12.
WITH route_counts AS (
    SELECT start_station_id, end_station_id, COUNT(*) AS cnt
    FROM trips GROUP BY start_station_id, end_station_id
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY start_station_id ORDER BY cnt DESC) AS rn
    FROM route_counts
)
SELECT s1.station_name AS start_station_name, s2.station_name AS top_end_station_name, r.cnt AS trip_count
FROM ranked r
JOIN stations s1 ON r.start_station_id = s1.station_id
JOIN stations s2 ON r.end_station_id   = s2.station_id
WHERE r.rn = 1;

-- Q13.
SELECT
    DATE(t.started_at) AS date,
    COUNT(*) AS trip_count,
    ROUND(AVG((JULIANDAY(t.ended_at) - JULIANDAY(t.started_at)) * 24 * 60), 2) AS avg_duration_min,
    ROUND(100.0 * SUM(CASE WHEN t.member_casual = 'member' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_member,
    ROUND(100.0 * SUM(CASE WHEN t.member_casual = 'casual' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_casual,
    w.condition AS weather_condition
FROM trips t
JOIN weather w ON DATE(t.started_at) = w.date
GROUP BY DATE(t.started_at)
ORDER BY date;

-- Q14.
SELECT s.station_id, s.station_name
FROM stations s
WHERE s.station_id IN (SELECT DISTINCT start_station_id FROM trips)
  AND s.station_id NOT IN (SELECT DISTINCT end_station_id FROM trips WHERE end_station_id IS NOT NULL);

-- Q15.
SELECT
    CAST(strftime('%H', started_at) AS INTEGER) / 2 * 2 AS hour_window_start,
    COUNT(*) AS trip_count
FROM trips
GROUP BY hour_window_start
ORDER BY trip_count DESC;

-- Q16.
WITH station_trips AS (
    SELECT start_station_id, COUNT(*) AS trip_count FROM trips GROUP BY start_station_id
)
SELECT s.station_name, st.trip_count,
    RANK() OVER (ORDER BY st.trip_count DESC) AS rank
FROM station_trips st
JOIN stations s ON st.start_station_id = s.station_id;

-- Q17.
SELECT
    ride_id, start_station_id, started_at,
    LAG(end_station_id) OVER (PARTITION BY start_station_id ORDER BY started_at) AS prev_end_station_id
FROM trips;

-- Q18.
WITH daily AS (
    SELECT DATE(started_at) AS date, COUNT(*) AS trip_count FROM trips GROUP BY date
)
SELECT date, trip_count,
    ROUND(AVG(trip_count) OVER (ORDER BY date ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 2) AS rolling_3day_avg
FROM daily;

-- Q19.
WITH daily AS (
    SELECT DATE(started_at) AS date, COUNT(*) AS trip_count FROM trips GROUP BY date
)
SELECT
    date, trip_count,
    LAG(trip_count) OVER (ORDER BY date) AS prev_day_count,
    trip_count - LAG(trip_count) OVER (ORDER BY date) AS day_over_day_change,
    ROUND(100.0 * (trip_count - LAG(trip_count) OVER (ORDER BY date))
          / LAG(trip_count) OVER (ORDER BY date), 1) AS pct_change
FROM daily;

-- Q20.
WITH ranked AS (
    SELECT
        t.ride_id, t.start_station_id,
        ROUND((JULIANDAY(t.ended_at) - JULIANDAY(t.started_at)) * 24 * 60, 2) AS duration_min,
        ROW_NUMBER() OVER (PARTITION BY t.start_station_id ORDER BY (JULIANDAY(t.ended_at) - JULIANDAY(t.started_at)) DESC) AS rn
    FROM trips t
)
SELECT s.station_name, r.ride_id, r.duration_min, r.rn AS rank_within_station
FROM ranked r
JOIN stations s ON r.start_station_id = s.station_id
WHERE r.rn <= 2
ORDER BY s.station_name, r.rn;
