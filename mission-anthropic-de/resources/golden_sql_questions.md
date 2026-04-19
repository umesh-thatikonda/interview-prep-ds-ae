# Golden SQL Questions — Meta/Anthropic DE Prep

> [!IMPORTANT]
> **Interviewer Availability:** The Safeguards team will be in NYC for an Offsite (May 5–9) and Hackweek (May 10–15). They are unavailable for interviews during this period.


Compiled from:
- `raw/meta_de_prep_code.md` (sources referred to as FS1–FS6, YouTube/DoorDash/Instagram Stories Artifacts)
- `raw/meta_tech_screen_v14.txt` (sources referred to as Meta Screen Set1, Set2, Set3, Set4)

Questions only (no solutions). Deduplicated across sources; each question lists all sources it appeared in.

---

## Q1: Content Watched > 10 Minutes Cumulative (Streaming)
**Source:** FS1 Streaming — SQL Q1 (meta_de_prep_code.md)
**Pattern:** Group-by + HAVING threshold + aggregation on interval durations
**Difficulty:** Medium
**Time target:** 10 mins

### Schema
- `fct_view_logs (session_id, user_id, content_id, start_ts, end_ts, date)`
- `dim_content (content_id, content_type, title, genre, studio_id, creation_date, date)`

### Problem Statement
For content that has been watched more than 10 minutes cumulative (total across all sessions), list the `content_id`, number of viewers (`num_viewers`), and total view time (`total_view_time`).

### Expected Output
Columns: `content_id`, `num_viewers`, `total_view_time` — filtered to content where total view time > 10 minutes.

### Notes
- "Cumulative" may mean lifetime or today-only — clarify with interviewer.
- `num_viewers` = `COUNT(DISTINCT user_id)`.

---

## Q2: Cumulative Studio Metrics (Daily Incremental Update)
**Source:** FS1 Streaming — SQL Q2 (meta_de_prep_code.md)
**Pattern:** Incremental cumulative / lifetime-metric table update (INSERT/MERGE)
**Difficulty:** Medium
**Time target:** 15 mins

### Schema
- `fct_view_logs (session_id, user_id, content_id, start_ts, end_ts, date)`
- `dim_content (content_id, studio_id, ...)`
- `cumulative_studio_metrics (studio_id, date, total_view_time_1d, total_view_time_lifetime)`

### Problem Statement
Update `cumulative_studio_metrics` daily with `total_view_time_1d` (today only) and `total_view_time_lifetime` (cumulative through today) for each studio.

### Expected Output
One row per studio per day with today's total and lifetime total view time.

### Notes
- Formula: `lifetime(today) = lifetime(yesterday) + today_1d`.
- Can use `MERGE` or an `INSERT … SELECT` with a self-join to yesterday's row.
- Gotcha: studios active today but missing from yesterday's row.

---

## Q3: Video Completion Rate + Avg View Time (Newsfeed)
**Source:** FS2 Newsfeed — SQL Q1 (meta_de_prep_code.md)
**Pattern:** Ratio-of-condition (reached full duration) + average
**Difficulty:** Medium
**Time target:** 15 mins

### Schema
- `fct_newsfeed_action (date, user_id, session_id, content_id, action_id, action_type, view_start, view_end)`
- `dim_content (date, content_id, content_type, creator_id, creation_date, video_length_seconds)`

### Problem Statement
Limited to today, for video content, return:
- % of video views that reached full duration,
- Total time spent watching videos,
- Average view length.

### Expected Output
A single row with columns: `pct_completed`, `total_watch_time`, `avg_view_length`.

### Notes
- "Reached full duration": `(view_end - view_start) >= video_length_seconds`.
- Filter `content_type = 'video'` and `date = CURRENT_DATE`.

---

## Q4: Content with Reaction but No Comment (Newsfeed)
**Source:** FS2 Newsfeed — SQL Q2 (meta_de_prep_code.md)
**Pattern:** Existence anti-join within same fact table (reaction EXISTS, comment NOT EXISTS)
**Difficulty:** Medium
**Time target:** 12 mins

### Schema
`fct_newsfeed_action` (with `action_type` including 'reaction', 'comment').

### Problem Statement
What percentage of content generated today had a reaction but no comments today?

### Expected Output
A single scalar percentage.

### Notes
- Denominator = content generated today (DISTINCT `content_id` with `creation_date = today`).
- Numerator = content_ids that have at least one reaction today AND zero comments today.

---

## Q5: Carpool Ratio to All Trips (Last 30 Days)
**Source:** FS3 Carpool — SQL Q1 (meta_de_prep_code.md)
**Pattern:** Conditional aggregation / ratio with filter on date window
**Difficulty:** Easy
**Time target:** 8 mins

### Schema
- `fct_rides (ride_id, user_id, driver_id, vehicle_id, ride_type, region, start_time, end_time, date)`
- `dim_vehicle (vehicle_id, type, capacity)`

### Problem Statement
Compute the ratio of carpool trips to all trips in the last 30 days.

### Expected Output
A scalar ratio (or percent) for the last 30 days.

### Notes
- Carpool identified by `ride_type = 'pool'` (or similar).
- Date filter: `date >= CURRENT_DATE - INTERVAL '30 days'`.

---

## Q6: Rank Vehicle Types by Time Spent in Pool (Carpool / DoorDash)
**Source:** FS3 Carpool — SQL Q2 + DoorDash Artifact — SQL Q1 (meta_de_prep_code.md)
**Pattern:** Sum interval duration per type + RANK() ORDER BY DESC
**Difficulty:** Medium
**Time target:** 12 mins

### Schema
- `fct_rides` (or `fct_deliveries`) joined with `dim_vehicle` on `vehicle_id`.

### Problem Statement
Rank vehicle types by most used for pooled rides/deliveries, where "most used" is measured by total customer time spent in vehicle (sum of `end_time - start_time` for pool trips).

### Expected Output
Columns: `vehicle_type`, `total_time`, `rank`.

### Notes
- Same core question appears in both Carpool and DoorDash sources.
- Use `SUM(end_time - start_time)` grouped by `dim_vehicle.type`, then `RANK()` or `ORDER BY DESC`.

---

## Q7: Drivers With More Pool Rides Than Regular
**Source:** FS3 Carpool — SQL Q3 (meta_de_prep_code.md)
**Pattern:** Conditional count per driver + HAVING / inequality between sums
**Difficulty:** Medium
**Time target:** 10 mins

### Schema
`fct_rides` with `ride_type` = 'pool' or 'regular' and `driver_id`.

### Problem Statement
Return the number of drivers who made more pool rides than regular rides.

### Expected Output
A single scalar count.

### Notes
- Use `SUM(CASE WHEN ride_type='pool' THEN 1 ELSE 0 END) > SUM(CASE WHEN ride_type='regular' THEN 1 ELSE 0 END)` in HAVING.

---

## Q8: Pooled-to-All Delivery Ratio Dashboard (GROUPING SETS)
**Source:** DoorDash Artifact — SQL Q2 (meta_de_prep_code.md)
**Pattern:** `GROUPING SETS` for multi-dimensional rollup
**Difficulty:** Hard
**Time target:** 20 mins

### Schema
Delivery fact table with `region`, `vehicle_type`, `delivery_date`, `delivery_type` (pool vs regular).

### Problem Statement
Build a dashboard aggregation table showing the ratio of pooled to all deliveries across `region × vehicle_type × delivery_date`, using `GROUPING SETS` to include subtotals and grand totals.

### Expected Output
Columns: `region`, `vehicle_type`, `delivery_date`, `ratio_pool_to_all`, with subtotal rows for partial groupings.

### Notes
- `GROUPING SETS ((region, vehicle_type, delivery_date), (region, vehicle_type), (region), ())`.
- Use `GROUPING()` function to identify subtotal rows.

---

## Q9: Average Order Value per User's City Today (Instamart)
**Source:** FS4 Instamart — SQL Q1 (meta_de_prep_code.md)
**Pattern:** Join fact to user → store/city dimension + group by city
**Difficulty:** Easy
**Time target:** 8 mins

### Schema
- `dim_users` (user_id, city, …)
- `dim_stores`
- `fct_order (order_id, store_id, driver_id, user_id, order_type, item_count, order_placed_ts, order_received_ts, store_accept_ts, order_prep_start_ts, order_ready_ts, driver_pickup_ts, order_delivery_ts, cancelled_by, cancellation_reason, net_order_amount, date)`

### Problem Statement
Compute the average order value grouped by the user's city for orders placed today.

### Expected Output
Columns: `city`, `avg_order_value`.

### Notes
- Clarify whether to filter out cancelled orders.
- `AVG(net_order_amount)` grouped by `dim_users.city`.

---

## Q10: Stores with More Pickups Than Deliveries Today
**Source:** FS4 Instamart — SQL Q2 (meta_de_prep_code.md)
**Pattern:** Conditional aggregation + HAVING inequality
**Difficulty:** Easy
**Time target:** 8 mins

### Schema
`fct_order` with `order_type` in ('pickup', 'delivery').

### Problem Statement
How many stores today had more pickups than deliveries?

### Expected Output
A single scalar count.

### Notes
- Same pattern as Q7 (carpool pool > regular), on store granularity.

---

## Q11: Top 3 Most Popular Cuisines Last Month (by Customer Count)
**Source:** FS4 Instamart — SQL Q3 (meta_de_prep_code.md)
**Pattern:** Top-N group by distinct count, date window
**Difficulty:** Easy
**Time target:** 8 mins

### Schema
`fct_order` joined with `dim_stores` (which has `cuisine`).

### Problem Statement
Return the top 3 most popular cuisines last month, ranked by number of distinct customers.

### Expected Output
Columns: `cuisine`, `num_customers` — top 3 rows.

### Notes
- Date range: previous calendar month (`DATE_TRUNC('month', …)`).
- `COUNT(DISTINCT user_id)` + `ORDER BY … DESC LIMIT 3`.

---

## Q12: Follow Acceptance Rate — Users Who Went Private (Last 7 Days)
**Source:** FS5 Twitter — SQL Q1 (meta_de_prep_code.md)
**Pattern:** Scoped numerator/denominator on event types (follow_request/follow_accept) with recent filter
**Difficulty:** Medium
**Time target:** 15 mins

### Schema
- `Dim_user (User_id, privacy_setting, privacy_setting_last_update_ts, date)`
- `Fct_follows (Source_user, Target_user, action, action_ts)` — action ∈ {follow_request, follow_accept, follow_success, reject, unfollow}

### Problem Statement
For users who went private in the last 7 days, compute the overall follow acceptance rate.

### Expected Output
A scalar percentage (accepted / requested).

### Notes
- "Went private" = `privacy_setting = 'private'` and `privacy_setting_last_update_ts >= now - 7d`.
- Denominator: follow_requests received by these users after going private.
- Numerator: follow_accept actions among those.

---

## Q13: Per-User Follow Acceptance Stats — Users Who Went Private (Last 7 Days)
**Source:** FS5 Twitter — SQL Q2 (meta_de_prep_code.md)
**Pattern:** Per-entity rate + aggregate min/avg/max over those rates
**Difficulty:** Hard
**Time target:** 20 mins

### Schema
Same as Q12.

### Problem Statement
For requesters who went private in the last 7 days, return:
- Count of requesters,
- Average acceptance rate per requester,
- Min acceptance rate,
- Max acceptance rate.

### Expected Output
A single row: `num_requesters`, `avg_acceptance_rate`, `min_acceptance_rate`, `max_acceptance_rate`.

### Notes
- Two-level aggregation: compute per-requester acceptance rate in CTE, then average across requesters.
- Careful: avg(rate) ≠ sum(accepted)/sum(requested).

---

## Q14: Build dim_follows Table (SCD-like)
**Source:** FS5 Twitter — SQL Q3 (meta_de_prep_code.md)
**Pattern:** Turn event log into dimension snapshot / SCD2-lite
**Difficulty:** Medium
**Time target:** 15 mins

### Schema
Source: `Fct_follows` (event log).
Target: `dim_follows (date, source, target, valid_from)`.

### Problem Statement
Create `dim_follows` from `fct_follows`: each row represents an active follow relationship with a `valid_from` timestamp.

### Expected Output
Rows per active follow edge: `(date, source, target, valid_from)`.

### Notes
- Active edge = most recent action is `follow_success` or `follow_accept` (not `unfollow` or `reject`).
- Use window functions to grab latest action per (source, target).

---

## Q15: dim_follows with is_reciprocal Flag (Self Join)
**Source:** FS5 Twitter — SQL Q4 (meta_de_prep_code.md)
**Pattern:** Self-join on follows to detect reciprocal edges
**Difficulty:** Medium
**Time target:** 15 mins

### Schema
Same as Q14.

### Problem Statement
Create `dim_follows` augmented with an `is_reciprocal` boolean indicating whether both directions of the follow edge exist.

### Expected Output
`(date, source, target, valid_from, is_reciprocal)`.

### Notes
- Self-join `dim_follows d1 LEFT JOIN dim_follows d2 ON d1.source = d2.target AND d1.target = d2.source`.
- `is_reciprocal = (d2.source IS NOT NULL)`.

---

## Q16: California Car Rental Metrics by Car Size (Last Year)
**Source:** FS6 Car Rental — SQL Q1 (meta_de_prep_code.md)
**Pattern:** Filter by location/state + group by dimension + ratio of distinct counts
**Difficulty:** Medium
**Time target:** 12 mins

### Schema
- `dim_cars (car_id, make, model, year, size, license_plate_number, location_id)`
- `dim_locations (location_id, address, city, state, zip, country, type)`
- `dim_users (user_id, name, email, license_number, license_state)`
- `fct_rentals (rental_id, user_id, car_id, pickup_location_id, dropoff_location_id, pickup_time, dropoff_time, rate_per_day)`

### Problem Statement
For rentals picked up in California last year, grouped by car size, return:
- Total reservations,
- Distinct users,
- Ratio (average rentals per user).

### Expected Output
Columns: `car_size`, `total_reservations`, `distinct_users`, `avg_rentals_per_user`.

### Notes
- Join `fct_rentals → dim_cars → dim_locations` to find pickup state.
- `pickup_time BETWEEN '<last-year-start>' AND '<last-year-end>'`.

---

## Q17: Car Utilization Rate by Size
**Source:** FS6 Car Rental — SQL Q2 (meta_de_prep_code.md)
**Pattern:** Ratio of active-set to total-inventory per group
**Difficulty:** Medium
**Time target:** 12 mins

### Schema
Same as Q16.

### Problem Statement
Utilization rate = cars rented / cars in location, broken down by car size.

### Expected Output
Columns: `car_size`, `utilization_rate` (optionally by location).

### Notes
- Clarify time window for "rented" (today? last 30 days? point-in-time?).
- Numerator = distinct `car_id` appearing in `fct_rentals` in window.
- Denominator = total `car_id` in `dim_cars` with that size.

---

## Q18: Rolling 30-Day Active Users on Newsfeed
**Source:** Instagram Stories Artifact — SQL Q3 extra (meta_de_prep_code.md)
**Pattern:** Rolling window distinct count
**Difficulty:** Medium
**Time target:** 12 mins

### Schema
Newsfeed activity log (`fct_newsfeed_action` or similar with `user_id`, `date`).

### Problem Statement
Return the count of users active on Newsfeed in the last rolling 30 days.

### Expected Output
A single scalar count (or one row per day if producing a rolling series).

### Notes
- `COUNT(DISTINCT user_id)` where `date BETWEEN CURRENT_DATE - 29 AND CURRENT_DATE`.
- Variant: produce the rolling series by joining a date spine and using `WHERE` with window.

---

## Q19: Users Who Reacted Exclusively to Media Today
**Source:** Instagram Stories Artifact — SQL Q4 extra (meta_de_prep_code.md)
**Pattern:** Group-by user with HAVING conditional count = 0 (exclusivity)
**Difficulty:** Medium
**Time target:** 12 mins

### Schema
Interaction fact with `user_id`, `content_type` (photo/video vs other), `action_type = 'reaction'`, `date`.

### Problem Statement
Count users who reacted today exclusively to media (photo or video) — i.e., they have ≥1 reaction today AND zero reactions today on non-media content.

### Expected Output
A single scalar count of users.

### Notes
- `HAVING SUM(CASE WHEN content_type NOT IN ('photo','video') THEN 1 ELSE 0 END) = 0 AND SUM(CASE WHEN content_type IN ('photo','video') THEN 1 ELSE 0 END) > 0`.

---

## Q20: Sales Volume and Unique Paying Customers by Payment Type
**Source:** Meta Screen Set 2 — SQL Q1
**Pattern:** GROUP BY payment_type with SUM + COUNT DISTINCT
**Difficulty:** Easy
**Time target:** 5 mins

### Schema
`transactions (transaction_id, book_id, customer_id, payment_amount, book_count, tax_rate, discount_rate, transaction_date, payment_type)`

### Problem Statement
What was the total volume of sales and the unique number of paying customers grouped and sorted in descending order by `payment_type`?

### Expected Output
Columns: `payment_type`, `total_sales_volume`, `unique_paying_customers`.

### Notes
- `SUM(payment_amount)` and `COUNT(DISTINCT customer_id)`.

---

## Q21: Top 3 Customers by Sales Value of People They Invited
**Source:** Meta Screen Set 1 — SQL Q2
**Pattern:** Join on referral graph (self-referencing `invited_by_customer_id`) + aggregate + top-N
**Difficulty:** Medium
**Time target:** 12 mins

### Schema
- `customers (customer_id, invited_by_customer_id, …)`
- `transactions (customer_id, payment_amount, …)`

### Problem Statement
Find the top 3 customers ordered by the total sales value of the people they directly invited.

### Expected Output
Columns: `customer_id_who_invited_people`, `sales_value_of_invited_people` — top 3.

### Notes
- Join `customers` (as invitee) → `transactions` on `customer_id`, group by `invited_by_customer_id`.
- Filter out NULL inviters.

---

## Q22: Top 5 Customers by Avg Payment-per-Book of Invitees
**Source:** Meta Screen Set 2 — SQL Q2
**Pattern:** Same structure as Q21 but different aggregation (weighted average)
**Difficulty:** Medium
**Time target:** 15 mins

### Schema
Same as Q21 (plus `transactions.book_count`).

### Problem Statement
Find the IDs of the top 5 customers ordered by the average payment per book of the people they invited.

### Expected Output
Columns: `customer_id_who_invited_people`, `avg_sales_value_of_invited_people` — top 5.

### Notes
- Expected follow-up: "Why not use `AVG(payment_amount)`?"
- Answer: `payment_amount` is per-transaction (covering multiple books); correct metric is `SUM(payment_amount) / SUM(book_count)`, not `SUM / COUNT(transactions)`.

---

## Q23: Authors with http:// + Zero Sales (vs Total Authors)
**Source:** Meta Screen Set 1 — SQL Q3
**Pattern:** Anti-join / NOT IN + conditional counting + comparison to total
**Difficulty:** Medium
**Time target:** 12 mins

### Schema
- `authors (author_id, first_name, last_name, birthday, website_url)`
- `books (book_id, author_id, …)`
- `transactions (book_id, …)`

### Problem Statement
Find the number of authors who have website URLs prefixed with `http://` and have never made a sale. Compare this with the total number of authors.

### Expected Output
Columns: `authors_with_http_and_no_sales`, `authors_in_total`.

### Notes
- Anti-join on authors → books → transactions.
- `LIKE 'http://%'` prefix filter.

---

## Q24: % of Authors with .com + No Sales (Extended)
**Source:** Meta Screen Set 2 — SQL Q3 + Meta Screen Set 2 — SQL Q6
**Pattern:** Q23 extended with percentage math + additional metric (5+ books sold)
**Difficulty:** Medium
**Time target:** 15 mins

### Schema
Same as Q23.

### Problem Statement
Find the total number of authors registered at the bookstore, what percentage of them have a website URL containing `.com` and have never made any sale, and (in Q6 extension) the number of authors with more than 5 books sold.

### Expected Output
Columns: `authors_in_total`, `percentage_com_website`, `authors_with_no_sales`, and optionally `authors_with_5_books`.

### Notes
- Be careful about `INNER JOIN` vs `LEFT JOIN` for the "no sales" count — INNER drops zero-sales authors from the join.
- Common interviewer follow-up: "What if I change INNER JOIN to LEFT JOIN, does the output change?"

---

## Q25: Top 3 Customers by Copies of Books from 3+ Authors in Same Category
**Source:** Meta Screen Set 2 — SQL Q4
**Pattern:** HAVING on distinct-count of authors within group + subquery aggregate
**Difficulty:** Hard
**Time target:** 18 mins

### Schema
- `transactions (customer_id, book_id, book_count, …)`
- `books (book_id, author_id, category, …)`

### Problem Statement
Find customers who have purchased groups of books where each group contains books from at least 3 different authors AND all books in the group belong to the same category. Return the top 3 customers ordered by how many copies of these books they purchased.

### Expected Output
Columns: `customer_id`, `total_copies` — top 3 rows.

### Notes
- Inner query: `GROUP BY customer_id, category`, `HAVING COUNT(DISTINCT author_id) >= 3`.
- Outer query sums `total_books` per customer.

---

## Q26: Top 2 Months with Customers Returning From Previous Month
**Source:** Meta Screen Set 2 — SQL Q5
**Pattern:** LAG window function to detect consecutive-month returning customers
**Difficulty:** Hard
**Time target:** 18 mins

### Schema
`transactions (customer_id, transaction_date, …)`.

### Problem Statement
Find the top 2 months with the highest number of unique customers that purchased books in both that month and the previous month.

### Expected Output
Columns: `curr_month` — top 2 rows.

### Notes
- CTE with `DATE_TRUNC('month', transaction_date)` and `LAG(…) OVER (PARTITION BY customer_id ORDER BY transaction_date)`.
- Keep rows where `curr_month = prev_month + INTERVAL '1 month'`.
- Group by `curr_month`, order by `COUNT(customer_id) DESC`, limit 2.

---

## Q27: Authors Who Published at Least 5 Genres
**Source:** Meta Screen Set 3 — SQL Q1
**Pattern:** GROUP BY + HAVING distinct count
**Difficulty:** Easy
**Time target:** 5 mins

### Schema
`books (id, title, author_id, genre, …)` (Set 3 schema).

### Problem Statement
Find the `author_id`s who published at least 5 different genres. Order by genre count.

### Expected Output
Columns: `author_id`, `genres`.

Example:
```
author_id | genres
41        | 5
81        | 5
21        | 6
```

### Notes
- `SELECT author_id, COUNT(DISTINCT genre) AS genres FROM books GROUP BY author_id HAVING COUNT(DISTINCT genre) >= 5`.

---

## Q28: Percent of Sales on Same Day as Registration
**Source:** Meta Screen Set 3 — SQL Q2
**Pattern:** Conditional rate with date equality
**Difficulty:** Medium
**Time target:** 10 mins

### Schema
- `sales (id, book_id, customer_id, amount, transaction_date, refunded)`
- `customers (id, registration_date, …)`

### Problem Statement
What percent of sales occurred on the same day that the customer registered?

### Expected Output
One column: `pct_sales_on_reg_day` (e.g. `6.14%`).

### Notes
- Use `SUM(CASE WHEN s.transaction_date = c.registration_date THEN 1 ELSE 0 END) * 100.0 / COUNT(s.id)`.
- Prefer LEFT JOIN so sales without a matching customer are still counted in the denominator (per source notes).

---

## Q29: Customers Bought > 3 Books on First + Last Shopping Days Combined
**Source:** Meta Screen Set 3 — SQL Q3
**Pattern:** MIN/MAX date per customer + HAVING on combined count
**Difficulty:** Hard
**Time target:** 18 mins

### Schema
Same as Q28.

### Problem Statement
Find customers who shopped on more than 1 day AND who bought more than 3 books on the first and last days of shopping combined.

### Expected Output
Columns: `customer_id`, `transactions_count`.

Example:
```
customer_id | transactions_count
72          | 6
306         | 5
387         | 4
450         | 4
499         | 4
```

### Notes
- CTE computing per-customer `MIN(transaction_date)` and `MAX(transaction_date)` with `HAVING COUNT(DISTINCT transaction_date) > 1`.
- Main query counts book purchases on those two specific days, filters `HAVING COUNT(book_id) > 3`.
- Follow-up: "Do it without CTEs or subqueries" → window functions.

---

## Q30: % Customers With Unpurchased Genre Interest (UNNEST Array)
**Source:** Meta Screen Set 3 — SQL Q4
**Pattern:** `CROSS JOIN UNNEST()` of array column + LEFT JOIN anti-match
**Difficulty:** Hard
**Time target:** 20 mins

### Schema
- `customers (id, genre_interests VARCHAR[])` (array column)
- `sales (customer_id, book_id, …)`
- `books (id, genre, …)`

### Problem Statement
What percent of customers have a genre interest from which they have not yet purchased a book?

### Expected Output
One column: `percent_not_bought_interested_genre` (e.g. `74.09%`).

### Notes
- Hint: `CROSS JOIN UNNEST(genre_interests)` to explode the array to rows.
- LEFT JOIN to purchased (customer, genre) tuples; rows with NULL match are "not yet purchased".
- Numerator is customers with at least one unpurchased interest; denominator is all customers with interests.

---

## Q31: Copies in 'good' Condition Not Returned + % Renewed > 2x
**Source:** Meta Screen Set 4 — SQL Q1
**Pattern:** Join + filter + conditional percentage
**Difficulty:** Easy
**Time target:** 8 mins

### Schema
- `copies (copy_id, book_id, condition, source, price_at_purchase, reserved_by_member_id)`
- `checkouts (checkout_id, copy_id, member_id, checkout_date, due_date, returned_date, renewal_count, late_fee)`

### Problem Statement
How many book copies are in `'good'` condition AND have not been returned (still checked out)? Of those, what percentage have been renewed more than 2 times in their current checkout?

### Expected Output
Columns: `num_copies_not_returned` (INT), `pct_renewed` (DOUBLE).

### Notes
- Filter: `cp.condition = 'good' AND c.returned_date IS NULL`.
- `pct_renewed = 100.0 * SUM(CASE WHEN renewal_count > 2 THEN 1 ELSE 0 END) / COUNT(*)`.

---

## Q32: Top 3 Books by Lifetime Value with > 10 Copies
**Source:** Meta Screen Set 4 — SQL Q2
**Pattern:** Sum of date-differences + filter on copy count + top-N
**Difficulty:** Medium
**Time target:** 15 mins

### Schema
Same as Q31 + `books (book_id, …)`.

### Problem Statement
The "lifetime value" of a book is the total number of days it was checked out (only counting checkouts that have been returned). Return the top 3 books with the highest lifetime value among books that have more than 10 copies.

### Expected Output
Columns: `book_id` (INT), `lifetime_value` (INT).

### Notes
- Use CTE: copy counts per book (HAVING > 10), book lifetime value (sum of `returned_date - checkout_date` where returned_date IS NOT NULL), then join/limit 3.

---

## Q33: Largest Reserved-Copies Difference vs Inviter
**Source:** Meta Screen Set 4 — SQL Q3
**Pattern:** Self-join on members via `invited_by_member_id` + COALESCE + absolute difference
**Difficulty:** Medium
**Time target:** 15 mins

### Schema
- `members (member_id, invited_by_member_id, …)`
- `copies` (with `reserved_by_member_id`)

### Problem Statement
Find the member with the largest absolute difference in number of reserved book copies between themselves and the member who invited them. Include members with 0 reserved copies.

### Expected Output
Columns: `member_id`, `invited_by_member_id`, `diff_num_reserved_copies`.

### Notes
- LEFT JOIN `copies` so members with 0 reservations still appear.
- Self-join on reserved counts: `rc1.invited_by_member_id = rc2.member_id`.
- `ABS(rc1.reserved_count - COALESCE(rc2.reserved_count, 0))` handles NULL inviter.
