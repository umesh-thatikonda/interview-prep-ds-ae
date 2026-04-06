-- Level 3 — SQL (MySQL stored procedure format, mirrors CodeSignal exactly)
-- ══════════════════════════════════════════════════════════════════════════════
-- SCENARIO
-- ══════════════════════════════════════════════════════════════════════════════
-- You work for a bike-share company. You have two tables:
--
-- TABLE: trips
--   ride_id          VARCHAR  — unique trip ID
--   started_at       DATETIME — trip start
--   ended_at         DATETIME — trip end
--   start_station_id VARCHAR
--   end_station_id   VARCHAR
--   member_casual    VARCHAR  — "member" or "casual"
--
-- TABLE: stations
--   station_id       VARCHAR  — unique station ID
--   station_name     VARCHAR
--   region           VARCHAR
--   capacity         INT
--
-- ══════════════════════════════════════════════════════════════════════════════
-- TASK
-- ══════════════════════════════════════════════════════════════════════════════
-- Write a stored procedure solution() that returns a result set with:
--
--   station_name     VARCHAR  — name of start station
--   region           VARCHAR  — region of start station
--   trip_count       INT      — number of valid trips from this station
--   avg_duration_min DECIMAL  — average trip duration in minutes, rounded to 2dp
--   pct_member       DECIMAL  — % of trips by members, rounded to 1dp
--
-- Rules:
--   - Only include trips where TIMESTAMPDIFF(SECOND, started_at, ended_at) > 0
--   - Only include trips where both start_station_id and end_station_id are NOT NULL
--   - Only include stations that have at least 1 valid trip
--   - Sort by trip_count DESC, then station_name ASC
--
-- ══════════════════════════════════════════════════════════════════════════════
-- EXAMPLE INPUT
-- ══════════════════════════════════════════════════════════════════════════════
-- trips:
--   R001 | 2024-03-01 07:00:00 | 2024-03-01 07:20:00 | S1 | S2 | member
--   R002 | 2024-03-01 08:00:00 | 2024-03-01 08:45:00 | S2 | S3 | casual
--   R003 | 2024-03-01 09:00:00 | 2024-03-01 09:30:00 | S1 | S3 | member
--   R004 | 2024-03-01 10:00:00 | 2024-03-01 09:50:00 | S1 | S2 | member  ← bad duration
--
-- stations:
--   S1 | Central Park    | North   | 20
--   S2 | Times Square    | Central | 35
--   S3 | Brooklyn Bridge | South   | 15
--
-- EXPECTED OUTPUT:
--   Central Park    | North   | 2 | 25.00 | 100.0
--   Times Square    | Central | 1 | 45.00 | 0.0
--
-- ══════════════════════════════════════════════════════════════════════════════
-- YOUR SOLUTION
-- ══════════════════════════════════════════════════════════════════════════════

CREATE PROCEDURE solution()
BEGIN
    /* Write your SQL here. Terminate each statement with a semicolon. */

END


-- ══════════════════════════════════════════════════════════════════════════════
-- SOLUTION (reveal only if stuck)
-- ══════════════════════════════════════════════════════════════════════════════
/*
CREATE PROCEDURE solution()
BEGIN
    SELECT
        s.station_name,
        s.region,
        COUNT(*) AS trip_count,
        ROUND(AVG(TIMESTAMPDIFF(SECOND, t.started_at, t.ended_at)) / 60.0, 2) AS avg_duration_min,
        ROUND(100.0 * SUM(CASE WHEN t.member_casual = 'member' THEN 1 ELSE 0 END) / COUNT(*), 1) AS pct_member
    FROM trips t
    JOIN stations s ON t.start_station_id = s.station_id
    WHERE TIMESTAMPDIFF(SECOND, t.started_at, t.ended_at) > 0
      AND t.start_station_id IS NOT NULL
      AND t.end_station_id IS NOT NULL
    GROUP BY s.station_id, s.station_name, s.region
    ORDER BY trip_count DESC, s.station_name ASC;
END
*/
