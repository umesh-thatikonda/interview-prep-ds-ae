-- Level 3 Simulation — SQL Queries on Bike Data
-- Practice these query patterns. Each has the question, then solution.

-- Schema:
-- trips(trip_id, start_time, end_time, duration_sec, start_station_id, end_station_id, bike_id, user_type)
-- stations(station_id, station_name, lat, lon, capacity)

-- ─────────────────────────────────────────────────────────────────────────────
-- Q1. Find the top 5 busiest start stations by number of trips.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    s.station_name,
    COUNT(*) AS trip_count
FROM trips t
JOIN stations s ON t.start_station_id = s.station_id
GROUP BY s.station_name
ORDER BY trip_count DESC
LIMIT 5;

-- ─────────────────────────────────────────────────────────────────────────────
-- Q2. Average trip duration (in minutes) by user type.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    user_type,
    ROUND(AVG(duration_sec) / 60.0, 2) AS avg_duration_min
FROM trips
GROUP BY user_type;

-- ─────────────────────────────────────────────────────────────────────────────
-- Q3. Number of trips per hour of day (to find peak hours).
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    CAST(strftime('%H', start_time) AS INTEGER) AS hour_of_day,
    COUNT(*) AS trip_count
FROM trips
GROUP BY hour_of_day
ORDER BY hour_of_day;

-- ─────────────────────────────────────────────────────────────────────────────
-- Q4. Trips that start and end at the same station (round trips).
-- ─────────────────────────────────────────────────────────────────────────────
SELECT trip_id, start_station_id, duration_sec
FROM trips
WHERE start_station_id = end_station_id;

-- ─────────────────────────────────────────────────────────────────────────────
-- Q5. Most popular route (start → end pair) with station names.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    s1.station_name AS start_station,
    s2.station_name AS end_station,
    COUNT(*)        AS trip_count
FROM trips t
JOIN stations s1 ON t.start_station_id = s1.station_id
JOIN stations s2 ON t.end_station_id   = s2.station_id
WHERE t.start_station_id != t.end_station_id
GROUP BY s1.station_name, s2.station_name
ORDER BY trip_count DESC
LIMIT 1;

-- ─────────────────────────────────────────────────────────────────────────────
-- Q6. Trips longer than the overall average duration.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT trip_id, duration_sec
FROM trips
WHERE duration_sec > (SELECT AVG(duration_sec) FROM trips)
ORDER BY duration_sec DESC;

-- ─────────────────────────────────────────────────────────────────────────────
-- Q7. Rank stations by total outbound trips using a window function.
-- ─────────────────────────────────────────────────────────────────────────────
WITH station_trips AS (
    SELECT
        start_station_id,
        COUNT(*) AS trip_count
    FROM trips
    GROUP BY start_station_id
)
SELECT
    start_station_id,
    trip_count,
    RANK() OVER (ORDER BY trip_count DESC) AS rank
FROM station_trips;

-- ─────────────────────────────────────────────────────────────────────────────
-- Q8. For each station, find the % of trips that are Subscriber vs Customer.
-- ─────────────────────────────────────────────────────────────────────────────
SELECT
    start_station_id,
    ROUND(100.0 * SUM(CASE WHEN user_type = 'Subscriber' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_subscriber,
    ROUND(100.0 * SUM(CASE WHEN user_type = 'Customer'   THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_customer
FROM trips
GROUP BY start_station_id;
