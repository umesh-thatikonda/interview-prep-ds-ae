-- ============================================================
-- Category 6: Deduplication & Data Quality
-- Problems 063–072
-- Database: PostgreSQL
-- Context: Real DE scenarios — Kafka dedup, CDC records,
--          late-arriving data, composite key dedup
-- ============================================================


-- Problem 063: Deduplicate Kafka Event Stream — Keep Latest by Event Timestamp
-- Difficulty: Medium
-- Category: Deduplication & Data Quality
-- Tags: deduplication, ROW_NUMBER, kafka, event dedup
--
-- Scenario:
-- A Kafka consumer loads raw events into a staging table. Due to at-least-once
-- delivery semantics, some events are duplicated (same event_id, different
-- ingested_at). You need to deduplicate by keeping the record with the latest
-- ingested_at per event_id before loading into the clean layer.
--
-- Question:
-- Return exactly one row per event_id — the one with the latest ingested_at.
-- Output: event_id, user_id, event_type, event_ts, ingested_at.

-- Schema & Sample Data:
CREATE TABLE raw_events_staging (
    row_id       BIGSERIAL PRIMARY KEY,
    event_id     VARCHAR(36),
    user_id      INT,
    event_type   VARCHAR(50),
    event_ts     TIMESTAMP,
    ingested_at  TIMESTAMP
);

INSERT INTO raw_events_staging (event_id, user_id, event_type, event_ts, ingested_at) VALUES
('evt-001', 1001, 'page_view',    '2024-11-01 10:00:00', '2024-11-01 10:00:05'),
('evt-001', 1001, 'page_view',    '2024-11-01 10:00:00', '2024-11-01 10:00:08'),  -- dup
('evt-002', 1002, 'add_to_cart',  '2024-11-01 10:01:00', '2024-11-01 10:01:10'),
('evt-003', 1001, 'purchase',     '2024-11-01 10:05:00', '2024-11-01 10:05:12'),
('evt-003', 1001, 'purchase',     '2024-11-01 10:05:00', '2024-11-01 10:05:15'),  -- dup
('evt-003', 1001, 'purchase',     '2024-11-01 10:05:00', '2024-11-01 10:05:17'),  -- dup
('evt-004', 1003, 'page_view',    '2024-11-01 10:07:00', '2024-11-01 10:07:20'),
('evt-005', 1002, 'checkout',     '2024-11-01 10:09:00', '2024-11-01 10:09:30'),
('evt-005', 1002, 'checkout',     '2024-11-01 10:09:00', '2024-11-01 10:09:33'),  -- dup
('evt-006', 1004, 'page_view',    '2024-11-01 10:11:00', '2024-11-01 10:11:05');

-- Solution:
WITH ranked AS (
    SELECT
        event_id,
        user_id,
        event_type,
        event_ts,
        ingested_at,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY ingested_at DESC) AS rn
    FROM raw_events_staging
)
SELECT event_id, user_id, event_type, event_ts, ingested_at
FROM ranked
WHERE rn = 1
ORDER BY event_ts;

-- Expected Output:
-- event_id | user_id | event_type   | event_ts            | ingested_at
-- ---------+---------+--------------+---------------------+---------------------
-- evt-001  | 1001    | page_view    | 2024-11-01 10:00:00 | 2024-11-01 10:00:08
-- evt-002  | 1002    | add_to_cart  | 2024-11-01 10:01:00 | 2024-11-01 10:01:10
-- evt-003  | 1001    | purchase     | 2024-11-01 10:05:00 | 2024-11-01 10:05:17
-- evt-004  | 1003    | page_view    | 2024-11-01 10:07:00 | 2024-11-01 10:07:20
-- evt-005  | 1002    | checkout     | 2024-11-01 10:09:00 | 2024-11-01 10:09:33
-- evt-006  | 1004    | page_view    | 2024-11-01 10:11:00 | 2024-11-01 10:11:05

-- Explanation:
-- ROW_NUMBER partitioned by event_id and ordered by ingested_at DESC ranks
-- the latest ingestion first. Filtering WHERE rn = 1 keeps only that row.
-- This is the canonical Kafka dedup pattern for staging tables.
--
-- Follow-up: Instead of keeping latest, count how many duplicate events existed
--            per event_id and report a dedup audit summary.


-- Problem 064: CDC Deduplication — Keep Latest Record by Updated_At (SCD Type 1)
-- Difficulty: Medium
-- Category: Deduplication & Data Quality
-- Tags: CDC, SCD, deduplication, ROW_NUMBER, upsert pattern
--
-- Scenario:
-- A CDC pipeline streams user profile updates from MySQL binlog into a
-- Snowflake raw table. Each change produces a new row. You need to produce
-- a "current state" view by keeping only the most recent record per user_id.
--
-- Question:
-- From the raw CDC table, return the current (latest) profile record per user.
-- Output: user_id, email, plan_tier, country, updated_at.

-- Schema & Sample Data:
CREATE TABLE raw_user_profiles_cdc (
    cdc_seq     BIGINT PRIMARY KEY,
    user_id     INT,
    email       VARCHAR(100),
    plan_tier   VARCHAR(20),
    country     VARCHAR(5),
    updated_at  TIMESTAMP,
    op_type     CHAR(1)  -- 'I' insert, 'U' update, 'D' delete
);

INSERT INTO raw_user_profiles_cdc VALUES
(1,  201, 'alice@example.com', 'free',    'US', '2024-01-10 09:00:00', 'I'),
(2,  202, 'bob@example.com',   'free',    'CA', '2024-01-11 10:00:00', 'I'),
(3,  201, 'alice@example.com', 'pro',     'US', '2024-02-01 08:30:00', 'U'),
(4,  203, 'carol@example.com', 'free',    'GB', '2024-02-05 14:00:00', 'I'),
(5,  202, 'bob@example.com',   'pro',     'CA', '2024-03-01 11:00:00', 'U'),
(6,  201, 'alice@new.com',     'pro',     'US', '2024-03-15 09:45:00', 'U'),
(7,  204, 'dave@example.com',  'free',    'AU', '2024-03-20 16:00:00', 'I'),
(8,  203, 'carol@example.com', 'enterprise','GB','2024-04-01 10:00:00','U'),
(9,  204, 'dave@example.com',  'free',    'AU', '2024-04-10 09:00:00', 'D'),
(10, 202, 'bob@example.com',   'enterprise','CA','2024-04-15 12:00:00','U');

-- Solution:
WITH ranked AS (
    SELECT
        cdc_seq,
        user_id,
        email,
        plan_tier,
        country,
        updated_at,
        op_type,
        ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY updated_at DESC, cdc_seq DESC) AS rn
    FROM raw_user_profiles_cdc
)
SELECT user_id, email, plan_tier, country, updated_at
FROM ranked
WHERE rn = 1
  AND op_type != 'D'   -- exclude deleted records
ORDER BY user_id;

-- Expected Output:
-- user_id | email            | plan_tier   | country | updated_at
-- --------+------------------+-------------+---------+---------------------
-- 201     | alice@new.com    | pro         | US      | 2024-03-15 09:45:00
-- 202     | bob@example.com  | enterprise  | CA      | 2024-04-15 12:00:00
-- 203     | carol@example.com| enterprise  | GB      | 2024-04-01 10:00:00
-- (user 204 excluded — last op was 'D')

-- Explanation:
-- ROW_NUMBER with DESC ordering on updated_at picks the latest record.
-- Including cdc_seq as tiebreaker handles same-timestamp updates safely.
-- Filtering op_type != 'D' removes logically deleted users from the result.
--
-- Follow-up: Build a MERGE/UPSERT statement that applies these CDC changes
--            to a target dim_users table using the deduped CTE as source.


-- Problem 065: Composite Key Deduplication — Ad Click Dedup with Priority
-- Difficulty: Medium
-- Category: Deduplication & Data Quality
-- Tags: deduplication, composite key, priority, ad clicks
--
-- Scenario:
-- An ad attribution pipeline receives click events from multiple tracking
-- sources (in-app SDK, web pixel, server-side). When two sources report
-- the same (user_id, ad_id, click_date), keep the server-side record first,
-- then in-app, then web. If still tied, keep the earliest recorded_at.
--
-- Question:
-- Deduplicate ad clicks by (user_id, ad_id, click_date) using source priority.
-- Output: user_id, ad_id, click_date, source, recorded_at.

-- Schema & Sample Data:
CREATE TABLE raw_ad_clicks (
    click_id     BIGINT PRIMARY KEY,
    user_id      INT,
    ad_id        INT,
    click_date   DATE,
    source       VARCHAR(20),  -- 'server_side', 'in_app', 'web_pixel'
    recorded_at  TIMESTAMP
);

INSERT INTO raw_ad_clicks VALUES
(1,  301, 5001, '2024-12-01', 'web_pixel',   '2024-12-01 14:00:05'),
(2,  301, 5001, '2024-12-01', 'in_app',      '2024-12-01 14:00:07'),
(3,  301, 5001, '2024-12-01', 'server_side', '2024-12-01 14:00:10'),  -- winner
(4,  302, 5002, '2024-12-01', 'web_pixel',   '2024-12-01 15:00:00'),
(5,  302, 5002, '2024-12-01', 'web_pixel',   '2024-12-01 15:00:02'),  -- dup web
(6,  303, 5001, '2024-12-01', 'in_app',      '2024-12-01 16:00:00'),  -- only one source
(7,  304, 5003, '2024-12-02', 'server_side', '2024-12-02 09:00:00'),
(8,  304, 5003, '2024-12-02', 'in_app',      '2024-12-02 09:00:05'),
(9,  305, 5004, '2024-12-02', 'web_pixel',   '2024-12-02 10:00:00'),
(10, 302, 5002, '2024-12-01', 'in_app',      '2024-12-01 15:01:00');  -- dup, no server

-- Solution:
WITH prioritized AS (
    SELECT
        click_id,
        user_id,
        ad_id,
        click_date,
        source,
        recorded_at,
        ROW_NUMBER() OVER (
            PARTITION BY user_id, ad_id, click_date
            ORDER BY
                CASE source
                    WHEN 'server_side' THEN 1
                    WHEN 'in_app'      THEN 2
                    WHEN 'web_pixel'   THEN 3
                    ELSE 4
                END,
                recorded_at ASC
        ) AS rn
    FROM raw_ad_clicks
)
SELECT user_id, ad_id, click_date, source, recorded_at
FROM prioritized
WHERE rn = 1
ORDER BY user_id, ad_id, click_date;

-- Expected Output:
-- user_id | ad_id | click_date | source      | recorded_at
-- --------+-------+------------+-------------+---------------------
-- 301     | 5001  | 2024-12-01 | server_side | 2024-12-01 14:00:10
-- 302     | 5002  | 2024-12-01 | in_app      | 2024-12-01 15:01:00
-- 303     | 5001  | 2024-12-01 | in_app      | 2024-12-01 16:00:00
-- 304     | 5003  | 2024-12-02 | server_side | 2024-12-02 09:00:00
-- 305     | 5004  | 2024-12-02 | web_pixel   | 2024-12-02 10:00:00

-- Explanation:
-- ROW_NUMBER with a CASE expression in ORDER BY assigns numeric priority
-- to sources. The earliest recorded_at breaks ties within the same source.
-- Filtering rn = 1 keeps the single canonical click per composite key.
--
-- Follow-up: Report the percentage of clicks that were duplicated
--            (had 2+ sources) broken down by source combination.


-- Problem 066: Data Quality — Find Rows Violating Referential Integrity
-- Difficulty: Medium
-- Category: Deduplication & Data Quality
-- Tags: data quality, referential integrity, anti-join, orphan records
--
-- Scenario:
-- A data warehouse load introduced orphan records: fact_orders rows that
-- reference customer_ids or product_ids not present in their dimension tables.
-- As part of a data quality check, identify all orphan orders and the type
-- of violation (missing customer, missing product, or both).
--
-- Question:
-- Find all orders with referential integrity violations.
-- Output: order_id, customer_id, product_id, violation_type.

-- Schema & Sample Data:
CREATE TABLE dim_customers (
    customer_id INT PRIMARY KEY,
    name        VARCHAR(100)
);

CREATE TABLE dim_products (
    product_id INT PRIMARY KEY,
    name       VARCHAR(100)
);

CREATE TABLE fact_orders (
    order_id    BIGINT PRIMARY KEY,
    customer_id INT,
    product_id  INT,
    amount      NUMERIC(10,2)
);

INSERT INTO dim_customers VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Carol');
INSERT INTO dim_products  VALUES (101, 'Widget'), (102, 'Gadget'), (103, 'Doohickey');

INSERT INTO fact_orders VALUES
(1001, 1,    101,  50.00),   -- valid
(1002, 2,    102,  75.00),   -- valid
(1003, 999,  101,  30.00),   -- missing customer 999
(1004, 1,    888,  20.00),   -- missing product 888
(1005, 777,  888,  10.00),   -- missing both
(1006, 3,    103,  90.00),   -- valid
(1007, 555,  102,  45.00),   -- missing customer 555
(1008, 2,    999,  60.00);   -- missing product 999

-- Solution:
SELECT
    fo.order_id,
    fo.customer_id,
    fo.product_id,
    CASE
        WHEN dc.customer_id IS NULL AND dp.product_id IS NULL THEN 'missing_customer_and_product'
        WHEN dc.customer_id IS NULL                           THEN 'missing_customer'
        WHEN dp.product_id  IS NULL                          THEN 'missing_product'
    END AS violation_type
FROM fact_orders fo
LEFT JOIN dim_customers dc ON fo.customer_id = dc.customer_id
LEFT JOIN dim_products  dp ON fo.product_id  = dp.product_id
WHERE dc.customer_id IS NULL OR dp.product_id IS NULL
ORDER BY fo.order_id;

-- Expected Output:
-- order_id | customer_id | product_id | violation_type
-- ---------+-------------+------------+------------------------------
-- 1003     | 999         | 101        | missing_customer
-- 1004     | 1           | 888        | missing_product
-- 1005     | 777         | 888        | missing_customer_and_product
-- 1007     | 555         | 102        | missing_customer
-- 1008     | 2           | 999        | missing_product

-- Explanation:
-- LEFT JOINs to both dimension tables leave NULL on the dimension side when
-- the foreign key has no match. A WHERE clause filtering for at least one NULL
-- captures all orphan rows. The CASE expression classifies the violation type.
--
-- Follow-up: Write a query that produces a DQ summary report: violation_type,
--            count, and percentage of total fact_orders rows.


-- Problem 067: Late-Arriving Data — Identify Records That Updated a Closed Partition
-- Difficulty: Hard
-- Category: Deduplication & Data Quality
-- Tags: late data, partitioning, data quality, pipeline monitoring
--
-- Scenario:
-- A daily batch pipeline closes partitions (marks them as "finalized") at
-- midnight UTC. Records arriving after partition close with an event_date
-- in an already-finalized partition are "late arrivals" and require a
-- reprocessing ticket. Find all late-arriving records and the hours they
-- were late.
--
-- Question:
-- Identify all late-arriving events: event_date fell in a finalized partition,
-- but the record was ingested after the finalization_time.
-- Output: event_id, event_date, ingested_at, finalized_at, hours_late.

-- Schema & Sample Data:
CREATE TABLE partition_registry (
    partition_date  DATE PRIMARY KEY,
    finalized_at    TIMESTAMP
);

CREATE TABLE event_log (
    event_id     BIGINT PRIMARY KEY,
    event_date   DATE,
    ingested_at  TIMESTAMP,
    user_id      INT
);

INSERT INTO partition_registry VALUES
('2024-11-01', '2024-11-02 00:05:00'),
('2024-11-02', '2024-11-03 00:05:00'),
('2024-11-03', '2024-11-04 00:05:00');

INSERT INTO event_log VALUES
(1, '2024-11-01', '2024-11-01 23:50:00', 101),  -- on time
(2, '2024-11-01', '2024-11-02 00:03:00', 102),  -- on time (before finalization)
(3, '2024-11-01', '2024-11-02 01:30:00', 103),  -- LATE: 1.42h after finalization
(4, '2024-11-02', '2024-11-02 22:00:00', 104),  -- on time
(5, '2024-11-02', '2024-11-03 00:10:00', 105),  -- LATE: 0.08h after finalization
(6, '2024-11-03', '2024-11-03 18:00:00', 106),  -- on time
(7, '2024-11-03', '2024-11-04 00:06:00', 107),  -- LATE: 0.02h after finalization
(8, '2024-11-03', '2024-11-04 02:15:00', 108);  -- LATE: 2.17h after finalization

-- Solution:
SELECT
    el.event_id,
    el.event_date,
    el.ingested_at,
    pr.finalized_at,
    ROUND(
        EXTRACT(EPOCH FROM (el.ingested_at - pr.finalized_at)) / 3600.0,
        2
    ) AS hours_late
FROM event_log el
JOIN partition_registry pr ON el.event_date = pr.partition_date
WHERE el.ingested_at > pr.finalized_at
ORDER BY el.event_id;

-- Expected Output:
-- event_id | event_date | ingested_at         | finalized_at        | hours_late
-- ---------+------------+---------------------+---------------------+-----------
-- 3        | 2024-11-01 | 2024-11-02 01:30:00 | 2024-11-02 00:05:00 | 1.42
-- 5        | 2024-11-02 | 2024-11-03 00:10:00 | 2024-11-03 00:05:00 | 0.08
-- 7        | 2024-11-03 | 2024-11-04 00:06:00 | 2024-11-04 00:05:00 | 0.02
-- 8        | 2024-11-03 | 2024-11-04 02:15:00 | 2024-11-04 00:05:00 | 2.17

-- Explanation:
-- JOIN event_log to partition_registry on event_date = partition_date to get
-- the finalization timestamp. Filter to rows where ingested_at > finalized_at.
-- EXTRACT(EPOCH) / 3600 converts the interval to hours.
--
-- Follow-up: Aggregate by event_date to report total late arrivals and max
--            hours_late per partition, sorted by worst partitions first.


-- Problem 068: Dedup Users with Multiple Email Variants (Fuzzy Composite Key)
-- Difficulty: Hard
-- Category: Deduplication & Data Quality
-- Tags: deduplication, string normalization, email, data quality
--
-- Scenario:
-- A CRM system has users who registered multiple times with slight email
-- variations (uppercase, plus-alias, leading/trailing spaces). You need to
-- canonicalize emails and deduplicate, keeping the oldest created_at record
-- per canonical email. Flag duplicate rows for soft-delete.
--
-- Question:
-- Identify duplicate user rows by canonical email (lowercase, strip +alias,
-- trim spaces). For each canonical email, keep the oldest row (min created_at).
-- Output: user_id, raw_email, canonical_email, is_canonical (true/false).

-- Schema & Sample Data:
CREATE TABLE crm_users (
    user_id    BIGINT PRIMARY KEY,
    raw_email  VARCHAR(200),
    created_at TIMESTAMP
);

INSERT INTO crm_users VALUES
(1, 'Alice@Example.com',         '2024-01-01 09:00:00'),
(2, 'alice@example.com',         '2024-01-05 10:00:00'),  -- dup of 1
(3, 'alice+promo@example.com',   '2024-01-10 11:00:00'),  -- dup of 1 (plus alias)
(4, '  bob@example.com ',        '2024-02-01 08:00:00'),
(5, 'BOB@EXAMPLE.COM',           '2024-02-03 09:00:00'),  -- dup of 4
(6, 'carol@example.com',         '2024-03-01 07:00:00'),  -- unique
(7, 'dave@example.com',          '2024-03-10 10:00:00'),  -- unique
(8, 'dave+work@example.com',     '2024-03-12 11:00:00'),  -- dup of 7
(9, 'Eve@Example.Com',           '2024-04-01 08:00:00'),  -- unique
(10,'eve@example.com',           '2024-04-05 09:00:00');  -- dup of 9

-- Solution:
WITH canonicalized AS (
    SELECT
        user_id,
        raw_email,
        created_at,
        LOWER(
            REGEXP_REPLACE(
                TRIM(raw_email),
                '\+[^@]*',
                ''
            )
        ) AS canonical_email
    FROM crm_users
),
ranked AS (
    SELECT
        user_id,
        raw_email,
        canonical_email,
        created_at,
        ROW_NUMBER() OVER (PARTITION BY canonical_email ORDER BY created_at ASC, user_id ASC) AS rn
    FROM canonicalized
)
SELECT
    user_id,
    raw_email,
    canonical_email,
    (rn = 1) AS is_canonical
FROM ranked
ORDER BY canonical_email, created_at;

-- Expected Output:
-- user_id | raw_email                  | canonical_email     | is_canonical
-- --------+----------------------------+---------------------+-------------
-- 1       | Alice@Example.com          | alice@example.com   | true
-- 2       | alice@example.com          | alice@example.com   | false
-- 3       | alice+promo@example.com    | alice@example.com   | false
-- 4       |   bob@example.com          | bob@example.com     | true
-- 5       | BOB@EXAMPLE.COM            | bob@example.com     | false
-- 6       | carol@example.com          | carol@example.com   | true
-- 7       | dave@example.com           | dave@example.com    | true
-- 8       | dave+work@example.com      | dave@example.com    | false
-- 9       | Eve@Example.Com            | eve@example.com     | true
-- 10      | eve@example.com            | eve@example.com     | false

-- Explanation:
-- TRIM removes whitespace, REGEXP_REPLACE strips +alias portion, LOWER
-- normalizes case. ROW_NUMBER ordered by created_at ASC selects the oldest
-- registration per canonical email as the canonical record.
--
-- Follow-up: Write a DELETE statement that removes all non-canonical rows
--            (is_canonical = false) from the crm_users table.


-- Problem 069: Detecting Duplicate Transactions — Idempotency Key Violations
-- Difficulty: Medium
-- Category: Deduplication & Data Quality
-- Tags: deduplication, transactions, idempotency, financial data
--
-- Scenario:
-- A payment processor requires every transaction to have a unique
-- idempotency_key per merchant. During an incident, some transactions were
-- submitted twice (same idempotency_key, different transaction_ids). Find all
-- idempotency key violations and report the duplicate pairs.
--
-- Question:
-- Find all pairs of transaction_ids that share the same (merchant_id, idempotency_key).
-- Output: merchant_id, idempotency_key, transaction_ids (as array), submitted_ats.

-- Schema & Sample Data:
CREATE TABLE payment_transactions (
    transaction_id   VARCHAR(20) PRIMARY KEY,
    merchant_id      INT,
    idempotency_key  VARCHAR(50),
    amount           NUMERIC(10,2),
    submitted_at     TIMESTAMP
);

INSERT INTO payment_transactions VALUES
('txn-001', 500, 'idem-aaa', 100.00, '2024-12-01 10:00:00'),
('txn-002', 500, 'idem-aaa', 100.00, '2024-12-01 10:00:05'),  -- dup
('txn-003', 500, 'idem-bbb', 200.00, '2024-12-01 10:05:00'),
('txn-004', 501, 'idem-ccc', 150.00, '2024-12-01 11:00:00'),
('txn-005', 501, 'idem-ccc', 150.00, '2024-12-01 11:00:03'),  -- dup
('txn-006', 501, 'idem-ccc', 150.00, '2024-12-01 11:00:07'),  -- triple dup
('txn-007', 502, 'idem-ddd', 300.00, '2024-12-01 12:00:00'),
('txn-008', 502, 'idem-eee', 250.00, '2024-12-01 12:05:00'),
('txn-009', 500, 'idem-fff', 180.00, '2024-12-01 13:00:00'),
('txn-010', 503, 'idem-ggg', 90.00,  '2024-12-01 14:00:00');

-- Solution:
SELECT
    merchant_id,
    idempotency_key,
    ARRAY_AGG(transaction_id ORDER BY submitted_at)    AS transaction_ids,
    ARRAY_AGG(submitted_at   ORDER BY submitted_at)    AS submitted_ats,
    COUNT(*) AS duplicate_count
FROM payment_transactions
GROUP BY merchant_id, idempotency_key
HAVING COUNT(*) > 1
ORDER BY merchant_id, idempotency_key;

-- Expected Output:
-- merchant_id | idempotency_key | transaction_ids                     | duplicate_count
-- ------------+-----------------+-------------------------------------+----------------
-- 500         | idem-aaa        | {txn-001, txn-002}                  | 2
-- 501         | idem-ccc        | {txn-004, txn-005, txn-006}         | 3

-- Explanation:
-- GROUP BY the composite idempotency key (merchant_id, idempotency_key).
-- HAVING COUNT(*) > 1 filters to only violated keys. ARRAY_AGG collects the
-- duplicate transaction IDs and timestamps for investigation.
--
-- Follow-up: Calculate the total overpayment amount per merchant due to
--            duplicate transactions (sum of amount * (count - 1) per group).


-- Problem 070: SCD Type 2 — Identify Current vs Historical Dimension Records
-- Difficulty: Hard
-- Category: Deduplication & Data Quality
-- Tags: SCD Type 2, dimension tables, current flag, data modeling
--
-- Scenario:
-- A data warehouse dim_employees table uses SCD Type 2 (a new row per change,
-- with effective_from and effective_to dates). Due to a pipeline bug, some
-- employees have overlapping effective ranges. Find all employees with
-- overlapping rows and report the overlap details.
--
-- Question:
-- Find employees where any two rows have overlapping effective date ranges.
-- Output: employee_id, row1_effective_from, row1_effective_to,
--         row2_effective_from, row2_effective_to, overlap_days.

-- Schema & Sample Data:
CREATE TABLE dim_employees (
    surrogate_key    BIGINT PRIMARY KEY,
    employee_id      INT,
    department       VARCHAR(50),
    effective_from   DATE,
    effective_to     DATE  -- NULL means current/open
);

INSERT INTO dim_employees VALUES
(1,  101, 'Engineering',  '2023-01-01', '2023-06-30'),
(2,  101, 'Engineering',  '2023-06-01', '2023-12-31'),  -- overlaps with row 1 (Jun 1-30)
(3,  101, 'Sales',        '2024-01-01', NULL),
(4,  102, 'Marketing',    '2022-01-01', '2022-12-31'),
(5,  102, 'Marketing',    '2023-01-01', '2023-12-31'),  -- no overlap
(6,  103, 'HR',           '2022-06-01', '2023-03-31'),
(7,  103, 'Legal',        '2023-01-01', '2023-12-31'),  -- overlaps (Jan-Mar)
(8,  104, 'Finance',      '2023-01-01', NULL),
(9,  104, 'Finance',      '2023-06-01', NULL),          -- overlaps (Jun 1 onward)
(10, 105, 'Engineering',  '2024-01-01', NULL);

-- Solution:
SELECT
    a.employee_id,
    a.effective_from                                    AS row1_effective_from,
    COALESCE(a.effective_to, CURRENT_DATE)              AS row1_effective_to,
    b.effective_from                                    AS row2_effective_from,
    COALESCE(b.effective_to, CURRENT_DATE)              AS row2_effective_to,
    GREATEST(a.effective_from, b.effective_from)        AS overlap_start,
    LEAST(COALESCE(a.effective_to, CURRENT_DATE),
          COALESCE(b.effective_to, CURRENT_DATE))       AS overlap_end,
    LEAST(COALESCE(a.effective_to, CURRENT_DATE),
          COALESCE(b.effective_to, CURRENT_DATE))
    - GREATEST(a.effective_from, b.effective_from)      AS overlap_days
FROM dim_employees a
JOIN dim_employees b
    ON  a.employee_id = b.employee_id
    AND a.surrogate_key < b.surrogate_key
    AND a.effective_from <= COALESCE(b.effective_to, CURRENT_DATE)
    AND b.effective_from <= COALESCE(a.effective_to, CURRENT_DATE)
ORDER BY a.employee_id, row1_effective_from;

-- Expected Output (based on CURRENT_DATE = 2024-09-01):
-- employee_id | row1_from  | row1_to    | row2_from  | row2_to    | overlap_start | overlap_end | overlap_days
-- ------------+------------+------------+------------+------------+---------------+-------------+-------------
-- 101         | 2023-01-01 | 2023-06-30 | 2023-06-01 | 2023-12-31 | 2023-06-01    | 2023-06-30  | 29
-- 103         | 2022-06-01 | 2023-03-31 | 2023-01-01 | 2023-12-31 | 2023-01-01    | 2023-03-31  | 89
-- 104         | 2023-01-01 | 2024-09-01 | 2023-06-01 | 2024-09-01 | 2023-06-01    | 2024-09-01  | 458

-- Explanation:
-- Self-join on employee_id with surrogate_key < to avoid duplicate pairs.
-- Overlap condition: each row's from <= the other's to (treating NULL as today).
-- GREATEST/LEAST compute the overlap window; subtraction gives overlap_days.
--
-- Follow-up: Write the remediation query that sets effective_to on the earlier
--            row to (effective_from of the later row - 1 day) to fix overlaps.


-- Problem 071: NULL Audit — Detect Columns Exceeding NULL Threshold
-- Difficulty: Medium
-- Category: Deduplication & Data Quality
-- Tags: data quality, NULL audit, metadata, dynamic SQL
--
-- Scenario:
-- A data quality framework checks every column in a critical fact table for
-- NULL rates. Any column with more than 10% NULLs should be flagged for
-- investigation. Build a NULL audit report for a user_events fact table.
--
-- Question:
-- Compute the NULL count and NULL percentage for each column in the
-- user_events_fact table. Flag columns where null_pct > 10%.
-- Output: column_name, total_rows, null_count, null_pct, flagged.

-- Schema & Sample Data:
CREATE TABLE user_events_fact (
    event_id    BIGINT,
    user_id     INT,
    session_id  BIGINT,
    event_type  VARCHAR(50),
    device_type VARCHAR(30),
    country     VARCHAR(5),
    revenue     NUMERIC(10,2),
    campaign_id INT
);

INSERT INTO user_events_fact VALUES
(1,  101, 9001, 'purchase',  'mobile',  'US', 29.99, 500),
(2,  102, 9002, 'page_view', 'desktop', 'CA', NULL,  501),
(3,  103, 9003, 'purchase',  NULL,      'GB', 49.99, NULL),
(4,  104, NULL, 'click',     'mobile',  NULL, NULL,  500),
(5,  105, 9005, 'purchase',  'mobile',  'AU', 19.99, 502),
(6,  106, 9006, NULL,        'tablet',  'US', NULL,  NULL),
(7,  107, 9007, 'page_view', NULL,      'US', NULL,  501),
(8,  108, 9008, 'click',     'desktop', NULL, NULL,  NULL),
(9,  109, NULL, 'purchase',  'mobile',  'CA', 39.99, 500),
(10, 110, 9010, 'page_view', 'desktop', 'US', NULL,  502);

-- Solution:
SELECT
    column_name,
    total_rows,
    null_count,
    ROUND(null_count * 100.0 / total_rows, 1) AS null_pct,
    CASE WHEN null_count * 100.0 / total_rows > 10 THEN true ELSE false END AS flagged
FROM (
    SELECT
        10 AS total_rows,
        'event_id'    AS column_name, COUNT(*) - COUNT(event_id)    AS null_count FROM user_events_fact
    UNION ALL SELECT 10, 'user_id',     COUNT(*) - COUNT(user_id)     FROM user_events_fact
    UNION ALL SELECT 10, 'session_id',  COUNT(*) - COUNT(session_id)  FROM user_events_fact
    UNION ALL SELECT 10, 'event_type',  COUNT(*) - COUNT(event_type)  FROM user_events_fact
    UNION ALL SELECT 10, 'device_type', COUNT(*) - COUNT(device_type) FROM user_events_fact
    UNION ALL SELECT 10, 'country',     COUNT(*) - COUNT(country)     FROM user_events_fact
    UNION ALL SELECT 10, 'revenue',     COUNT(*) - COUNT(revenue)     FROM user_events_fact
    UNION ALL SELECT 10, 'campaign_id', COUNT(*) - COUNT(campaign_id) FROM user_events_fact
) t
ORDER BY null_pct DESC;

-- Expected Output:
-- column_name | total_rows | null_count | null_pct | flagged
-- ------------+------------+------------+----------+--------
-- revenue     | 10         | 6          | 60.0     | true
-- campaign_id | 10         | 3          | 30.0     | true
-- device_type | 10         | 2          | 20.0     | true
-- country     | 10         | 2          | 20.0     | true
-- session_id  | 10         | 2          | 20.0     | true
-- event_type  | 10         | 1          | 10.0     | false
-- event_id    | 10         | 0          | 0.0      | false
-- user_id     | 10         | 0          | 0.0      | false

-- Explanation:
-- COUNT(*) - COUNT(col) gives the NULL count because COUNT(col) skips NULLs.
-- UNION ALL stacks one row per column. In production, this is typically
-- generated via dynamic SQL or a dbt macro. ROUND and CASE apply threshold logic.
--
-- Follow-up: Turn this into a stored procedure that accepts a table name and
--            threshold percentage and dynamically generates the audit report.


-- Problem 072: Dedup Dimension Table — Resolve Conflicting Attribute Values
-- Difficulty: Hard
-- Category: Deduplication & Data Quality
-- Tags: deduplication, conflict resolution, dimension tables, data quality
--
-- Scenario:
-- A product dimension table was loaded from two source systems that disagree
-- on some attributes. For each product_id, there may be 2 rows with
-- conflicting category or brand. Resolve conflicts using a priority rule:
-- source='erp' > source='crm', and for same source prefer updated_at DESC.
--
-- Question:
-- Produce one canonical row per product_id resolving source conflicts.
-- Output: product_id, product_name, category, brand, source, updated_at.

-- Schema & Sample Data:
CREATE TABLE raw_dim_products (
    load_id      BIGINT PRIMARY KEY,
    product_id   INT,
    product_name VARCHAR(100),
    category     VARCHAR(50),
    brand        VARCHAR(50),
    source       VARCHAR(10),  -- 'erp' or 'crm'
    updated_at   TIMESTAMP
);

INSERT INTO raw_dim_products VALUES
(1,  2001, 'Widget Pro',    'Electronics', 'BrandA', 'erp', '2024-06-01 10:00:00'),
(2,  2001, 'Widget Pro',    'Gadgets',     'BrandA', 'crm', '2024-06-02 10:00:00'),  -- conflicts category
(3,  2002, 'Gadget Plus',   'Electronics', 'BrandB', 'crm', '2024-05-01 09:00:00'),
(4,  2002, 'Gadget Plus',   'Electronics', 'BrandC', 'erp', '2024-05-15 09:00:00'),  -- conflicts brand
(5,  2003, 'Doohickey Max', 'Accessories', 'BrandD', 'erp', '2024-07-01 08:00:00'),
(6,  2003, 'Doohickey Max', 'Accessories', 'BrandD', 'erp', '2024-07-10 09:00:00'),  -- same source, newer
(7,  2004, 'Thingamajig',   'Hardware',    'BrandE', 'crm', '2024-08-01 10:00:00'),
(8,  2004, 'Thingamajig',   'Hardware',    'BrandE', 'crm', '2024-08-05 10:00:00'),  -- same source, newer
(9,  2005, 'Gizmo Ultra',   'Electronics', 'BrandF', 'crm', '2024-09-01 10:00:00'),
(10, 2005, 'Gizmo Ultra',   'Consumer',    'BrandF', 'erp', '2024-08-01 10:00:00');  -- erp but older

-- Solution:
WITH ranked AS (
    SELECT
        load_id,
        product_id,
        product_name,
        category,
        brand,
        source,
        updated_at,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY
                CASE source WHEN 'erp' THEN 1 WHEN 'crm' THEN 2 ELSE 3 END,
                updated_at DESC
        ) AS rn
    FROM raw_dim_products
)
SELECT product_id, product_name, category, brand, source, updated_at
FROM ranked
WHERE rn = 1
ORDER BY product_id;

-- Expected Output:
-- product_id | product_name   | category    | brand  | source | updated_at
-- -----------+----------------+-------------+--------+--------+---------------------
-- 2001       | Widget Pro     | Electronics | BrandA | erp    | 2024-06-01 10:00:00
-- 2002       | Gadget Plus    | Electronics | BrandC | erp    | 2024-05-15 09:00:00
-- 2003       | Doohickey Max  | Accessories | BrandD | erp    | 2024-07-10 09:00:00
-- 2004       | Thingamajig    | Hardware    | BrandE | crm    | 2024-08-05 10:00:00
-- 2005       | Gizmo Ultra    | Consumer    | BrandF | erp    | 2024-08-01 10:00:00

-- Explanation:
-- ROW_NUMBER ORDER BY first on source priority (erp=1 beats crm=2), then
-- on updated_at DESC within same source. This produces exactly one canonical
-- row per product_id resolving both cross-source and same-source conflicts.
--
-- Follow-up: Build an audit table showing which products had conflicts and
--            what the losing row's values were (for reconciliation).
