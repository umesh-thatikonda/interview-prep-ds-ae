"""
============================================================
CATEGORY 1: GROUPBY & AGGREGATION  (Problems 001 – 015)
============================================================
All solutions use only Python stdlib:
  dict, list, collections (defaultdict, Counter, namedtuple),
  itertools, heapq — NO pandas, NO numpy.
"""

# ─────────────────────────────────────────────────────────────
"""
Problem 001: Daily Active Users per Platform
Difficulty: Easy
Category: Groupby & Aggregation
Tags: groupby, count-distinct, user-events, daily-metrics

Scenario:
A safety-signals pipeline receives login events from multiple client
platforms (web, iOS, Android). You need to produce a daily DAU count
per platform so the Safeguards team can monitor anomalous traffic spikes.

Input:
List of dicts, each with keys: user_id (str), platform (str), date (str YYYY-MM-DD)

Sample Input:
events = [
    {"user_id": "u1", "platform": "iOS",     "date": "2024-03-01"},
    {"user_id": "u2", "platform": "iOS",     "date": "2024-03-01"},
    {"user_id": "u1", "platform": "iOS",     "date": "2024-03-01"},  # duplicate session
    {"user_id": "u3", "platform": "Android", "date": "2024-03-01"},
    {"user_id": "u4", "platform": "web",     "date": "2024-03-01"},
    {"user_id": "u4", "platform": "web",     "date": "2024-03-02"},
    {"user_id": "u5", "platform": "web",     "date": "2024-03-02"},
    {"user_id": "u6", "platform": "Android", "date": "2024-03-02"},
    {"user_id": "u7", "platform": "Android", "date": "2024-03-02"},
    {"user_id": "u8", "platform": "iOS",     "date": "2024-03-02"},
]

Expected Output:
{
  ("2024-03-01", "iOS"):     2,
  ("2024-03-01", "Android"): 1,
  ("2024-03-01", "web"):     1,
  ("2024-03-02", "web"):     2,
  ("2024-03-02", "Android"): 2,
  ("2024-03-02", "iOS"):     1,
}

Follow-up: Instead of DAU, compute WAU (weekly active users) by bucketing
dates into ISO week numbers.
"""

from collections import defaultdict


def daily_active_users_per_platform(events):
    seen = defaultdict(set)
    for e in events:
        key = (e["date"], e["platform"])
        seen[key].add(e["user_id"])
    return {k: len(v) for k, v in seen.items()}


def test_001():
    events = [
        {"user_id": "u1", "platform": "iOS",     "date": "2024-03-01"},
        {"user_id": "u2", "platform": "iOS",     "date": "2024-03-01"},
        {"user_id": "u1", "platform": "iOS",     "date": "2024-03-01"},
        {"user_id": "u3", "platform": "Android", "date": "2024-03-01"},
        {"user_id": "u4", "platform": "web",     "date": "2024-03-01"},
        {"user_id": "u4", "platform": "web",     "date": "2024-03-02"},
        {"user_id": "u5", "platform": "web",     "date": "2024-03-02"},
        {"user_id": "u6", "platform": "Android", "date": "2024-03-02"},
        {"user_id": "u7", "platform": "Android", "date": "2024-03-02"},
        {"user_id": "u8", "platform": "iOS",     "date": "2024-03-02"},
    ]
    result = daily_active_users_per_platform(events)
    assert result[("2024-03-01", "iOS")] == 2
    assert result[("2024-03-01", "Android")] == 1
    assert result[("2024-03-02", "web")] == 2
    assert result[("2024-03-02", "Android")] == 2
    print("001 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 002: Total Ad Spend per Campaign per Day
Difficulty: Easy
Category: Groupby & Aggregation
Tags: groupby, sum, ad-impressions, financial-aggregation

Scenario:
An ad-delivery pipeline writes impression records to a Kafka topic. Each
record has a spend amount. Finance needs daily spend totals per campaign
to reconcile against advertiser invoices.

Input:
List of dicts: campaign_id (str), date (str), spend_cents (int)

Sample Input:
impressions = [
    {"campaign_id": "C1", "date": "2024-04-10", "spend_cents": 500},
    {"campaign_id": "C1", "date": "2024-04-10", "spend_cents": 300},
    {"campaign_id": "C2", "date": "2024-04-10", "spend_cents": 1200},
    {"campaign_id": "C1", "date": "2024-04-11", "spend_cents": 450},
    {"campaign_id": "C2", "date": "2024-04-11", "spend_cents": 800},
    {"campaign_id": "C3", "date": "2024-04-11", "spend_cents": 2000},
    {"campaign_id": "C3", "date": "2024-04-11", "spend_cents": 150},
    {"campaign_id": "C1", "date": "2024-04-12", "spend_cents": 600},
    {"campaign_id": "C2", "date": "2024-04-12", "spend_cents": 900},
    {"campaign_id": "C3", "date": "2024-04-12", "spend_cents": 400},
]

Expected Output:
{
  ("2024-04-10", "C1"): 800,
  ("2024-04-10", "C2"): 1200,
  ("2024-04-11", "C1"): 450,
  ("2024-04-11", "C2"): 800,
  ("2024-04-11", "C3"): 2150,
  ("2024-04-12", "C1"): 600,
  ("2024-04-12", "C2"): 900,
  ("2024-04-12", "C3"): 400,
}

Follow-up: Return only the top-spending campaign per day.
"""


def total_spend_per_campaign_per_day(impressions):
    totals = defaultdict(int)
    for row in impressions:
        totals[(row["date"], row["campaign_id"])] += row["spend_cents"]
    return dict(totals)


def test_002():
    impressions = [
        {"campaign_id": "C1", "date": "2024-04-10", "spend_cents": 500},
        {"campaign_id": "C1", "date": "2024-04-10", "spend_cents": 300},
        {"campaign_id": "C2", "date": "2024-04-10", "spend_cents": 1200},
        {"campaign_id": "C1", "date": "2024-04-11", "spend_cents": 450},
        {"campaign_id": "C2", "date": "2024-04-11", "spend_cents": 800},
        {"campaign_id": "C3", "date": "2024-04-11", "spend_cents": 2000},
        {"campaign_id": "C3", "date": "2024-04-11", "spend_cents": 150},
        {"campaign_id": "C1", "date": "2024-04-12", "spend_cents": 600},
        {"campaign_id": "C2", "date": "2024-04-12", "spend_cents": 900},
        {"campaign_id": "C3", "date": "2024-04-12", "spend_cents": 400},
    ]
    result = total_spend_per_campaign_per_day(impressions)
    assert result[("2024-04-10", "C1")] == 800
    assert result[("2024-04-11", "C3")] == 2150
    assert result[("2024-04-12", "C2")] == 900
    print("002 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 003: Average Session Duration per Content Category
Difficulty: Easy
Category: Groupby & Aggregation
Tags: groupby, average, streaming-sessions, content-catalog

Scenario:
Disney+ Hotstar records streaming sessions with a content category label.
The product team wants average watch duration (in seconds) per category
to prioritise content investment.

Input:
List of dicts: session_id (str), category (str), duration_secs (int)

Sample Input:
sessions = [
    {"session_id": "s01", "category": "drama",   "duration_secs": 1800},
    {"session_id": "s02", "category": "drama",   "duration_secs": 2400},
    {"session_id": "s03", "category": "comedy",  "duration_secs": 900},
    {"session_id": "s04", "category": "comedy",  "duration_secs": 1200},
    {"session_id": "s05", "category": "comedy",  "duration_secs": 600},
    {"session_id": "s06", "category": "sports",  "duration_secs": 3600},
    {"session_id": "s07", "category": "sports",  "duration_secs": 5400},
    {"session_id": "s08", "category": "drama",   "duration_secs": 1200},
    {"session_id": "s09", "category": "news",    "duration_secs": 300},
    {"session_id": "s10", "category": "news",    "duration_secs": 600},
]

Expected Output:
{
  "drama":  1800.0,
  "comedy": 900.0,
  "sports": 4500.0,
  "news":   450.0,
}

Follow-up: Return only categories where average duration exceeds 1000 seconds.
"""


def avg_session_duration_per_category(sessions):
    totals = defaultdict(int)
    counts = defaultdict(int)
    for s in sessions:
        totals[s["category"]] += s["duration_secs"]
        counts[s["category"]] += 1
    return {cat: totals[cat] / counts[cat] for cat in totals}


def test_003():
    sessions = [
        {"session_id": "s01", "category": "drama",   "duration_secs": 1800},
        {"session_id": "s02", "category": "drama",   "duration_secs": 2400},
        {"session_id": "s03", "category": "comedy",  "duration_secs": 900},
        {"session_id": "s04", "category": "comedy",  "duration_secs": 1200},
        {"session_id": "s05", "category": "comedy",  "duration_secs": 600},
        {"session_id": "s06", "category": "sports",  "duration_secs": 3600},
        {"session_id": "s07", "category": "sports",  "duration_secs": 5400},
        {"session_id": "s08", "category": "drama",   "duration_secs": 1200},
        {"session_id": "s09", "category": "news",    "duration_secs": 300},
        {"session_id": "s10", "category": "news",    "duration_secs": 600},
    ]
    result = avg_session_duration_per_category(sessions)
    assert result["drama"] == 1800.0
    assert result["comedy"] == 900.0
    assert result["sports"] == 4500.0
    assert result["news"] == 450.0
    print("003 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 004: Count Abuse Reports per User per Type
Difficulty: Easy
Category: Groupby & Aggregation
Tags: groupby, count, abuse-logs, trust-and-safety

Scenario:
The Trust & Safety pipeline receives abuse reports. Each report has a
reporter, a target user, and a report type. You need to count how many
reports each user has received per report type to feed into a risk-scoring
model.

Input:
List of dicts: reporter_id (str), target_user_id (str), report_type (str)

Sample Input:
reports = [
    {"reporter_id": "r1", "target_user_id": "u10", "report_type": "spam"},
    {"reporter_id": "r2", "target_user_id": "u10", "report_type": "spam"},
    {"reporter_id": "r3", "target_user_id": "u10", "report_type": "hate"},
    {"reporter_id": "r4", "target_user_id": "u11", "report_type": "spam"},
    {"reporter_id": "r5", "target_user_id": "u11", "report_type": "nudity"},
    {"reporter_id": "r6", "target_user_id": "u12", "report_type": "hate"},
    {"reporter_id": "r7", "target_user_id": "u12", "report_type": "hate"},
    {"reporter_id": "r8", "target_user_id": "u12", "report_type": "hate"},
    {"reporter_id": "r9", "target_user_id": "u13", "report_type": "spam"},
    {"reporter_id": "r1", "target_user_id": "u13", "report_type": "nudity"},
]

Expected Output:
{
  ("u10", "spam"):   2,
  ("u10", "hate"):   1,
  ("u11", "spam"):   1,
  ("u11", "nudity"): 1,
  ("u12", "hate"):   3,
  ("u13", "spam"):   1,
  ("u13", "nudity"): 1,
}

Follow-up: Return only (user, report_type) pairs where count >= 2.
"""


def count_reports_per_user_per_type(reports):
    counts = defaultdict(int)
    for r in reports:
        counts[(r["target_user_id"], r["report_type"])] += 1
    return dict(counts)


def test_004():
    reports = [
        {"reporter_id": "r1", "target_user_id": "u10", "report_type": "spam"},
        {"reporter_id": "r2", "target_user_id": "u10", "report_type": "spam"},
        {"reporter_id": "r3", "target_user_id": "u10", "report_type": "hate"},
        {"reporter_id": "r4", "target_user_id": "u11", "report_type": "spam"},
        {"reporter_id": "r5", "target_user_id": "u11", "report_type": "nudity"},
        {"reporter_id": "r6", "target_user_id": "u12", "report_type": "hate"},
        {"reporter_id": "r7", "target_user_id": "u12", "report_type": "hate"},
        {"reporter_id": "r8", "target_user_id": "u12", "report_type": "hate"},
        {"reporter_id": "r9", "target_user_id": "u13", "report_type": "spam"},
        {"reporter_id": "r1", "target_user_id": "u13", "report_type": "nudity"},
    ]
    result = count_reports_per_user_per_type(reports)
    assert result[("u10", "spam")] == 2
    assert result[("u12", "hate")] == 3
    assert result[("u11", "nudity")] == 1
    print("004 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 005: Top-N Most Active Users by Event Count
Difficulty: Easy
Category: Groupby & Aggregation
Tags: groupby, top-n, heapq, user-events

Scenario:
A data pipeline ingests raw user-activity events. The growth team wants
the top-3 most active users (by total event count) to include in a weekly
VIP-user report.

Input:
List of dicts: user_id (str), event_type (str), ts (str)
Integer N = top-N users to return

Sample Input:
events = [
    {"user_id": "alice", "event_type": "click",   "ts": "2024-05-01T10:00"},
    {"user_id": "bob",   "event_type": "view",    "ts": "2024-05-01T10:01"},
    {"user_id": "alice", "event_type": "purchase","ts": "2024-05-01T10:02"},
    {"user_id": "carol", "event_type": "click",   "ts": "2024-05-01T10:03"},
    {"user_id": "alice", "event_type": "view",    "ts": "2024-05-01T10:04"},
    {"user_id": "bob",   "event_type": "click",   "ts": "2024-05-01T10:05"},
    {"user_id": "dave",  "event_type": "view",    "ts": "2024-05-01T10:06"},
    {"user_id": "carol", "event_type": "view",    "ts": "2024-05-01T10:07"},
    {"user_id": "carol", "event_type": "purchase","ts": "2024-05-01T10:08"},
    {"user_id": "alice", "event_type": "click",   "ts": "2024-05-01T10:09"},
]
N = 3

Expected Output:
[("alice", 4), ("carol", 3), ("bob", 2)]

Follow-up: Break ties alphabetically by user_id.
"""

import heapq
from collections import Counter


def top_n_active_users(events, n):
    counts = Counter(e["user_id"] for e in events)
    return heapq.nlargest(n, counts.items(), key=lambda x: x[1])


def test_005():
    events = [
        {"user_id": "alice", "event_type": "click",    "ts": "2024-05-01T10:00"},
        {"user_id": "bob",   "event_type": "view",     "ts": "2024-05-01T10:01"},
        {"user_id": "alice", "event_type": "purchase", "ts": "2024-05-01T10:02"},
        {"user_id": "carol", "event_type": "click",    "ts": "2024-05-01T10:03"},
        {"user_id": "alice", "event_type": "view",     "ts": "2024-05-01T10:04"},
        {"user_id": "bob",   "event_type": "click",    "ts": "2024-05-01T10:05"},
        {"user_id": "dave",  "event_type": "view",     "ts": "2024-05-01T10:06"},
        {"user_id": "carol", "event_type": "view",     "ts": "2024-05-01T10:07"},
        {"user_id": "carol", "event_type": "purchase", "ts": "2024-05-01T10:08"},
        {"user_id": "alice", "event_type": "click",    "ts": "2024-05-01T10:09"},
    ]
    result = top_n_active_users(events, 3)
    assert result[0] == ("alice", 4)
    assert result[1] == ("carol", 3)
    assert result[2] == ("bob", 2)
    print("005 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 006: Min and Max Fare per Ride-Share Zone
Difficulty: Easy
Category: Groupby & Aggregation
Tags: groupby, min-max, transactions, ride-share

Scenario:
A ride-share data pipeline outputs completed trip records with pickup zones
and fares. The pricing team wants the minimum and maximum fare per zone to
calibrate surge-pricing thresholds.

Input:
List of dicts: trip_id (str), pickup_zone (str), fare_cents (int)

Sample Input:
trips = [
    {"trip_id": "t001", "pickup_zone": "downtown", "fare_cents": 850},
    {"trip_id": "t002", "pickup_zone": "downtown", "fare_cents": 1200},
    {"trip_id": "t003", "pickup_zone": "airport",  "fare_cents": 3500},
    {"trip_id": "t004", "pickup_zone": "airport",  "fare_cents": 4200},
    {"trip_id": "t005", "pickup_zone": "suburbs",  "fare_cents": 600},
    {"trip_id": "t006", "pickup_zone": "downtown", "fare_cents": 950},
    {"trip_id": "t007", "pickup_zone": "suburbs",  "fare_cents": 1100},
    {"trip_id": "t008", "pickup_zone": "airport",  "fare_cents": 2900},
    {"trip_id": "t009", "pickup_zone": "suburbs",  "fare_cents": 750},
    {"trip_id": "t010", "pickup_zone": "downtown", "fare_cents": 700},
]

Expected Output:
{
  "downtown": {"min": 700,  "max": 1200},
  "airport":  {"min": 2900, "max": 4200},
  "suburbs":  {"min": 600,  "max": 1100},
}

Follow-up: Also include the trip_id of the min and max fare for each zone.
"""


def min_max_fare_per_zone(trips):
    result = {}
    for t in trips:
        z = t["pickup_zone"]
        f = t["fare_cents"]
        if z not in result:
            result[z] = {"min": f, "max": f}
        else:
            result[z]["min"] = min(result[z]["min"], f)
            result[z]["max"] = max(result[z]["max"], f)
    return result


def test_006():
    trips = [
        {"trip_id": "t001", "pickup_zone": "downtown", "fare_cents": 850},
        {"trip_id": "t002", "pickup_zone": "downtown", "fare_cents": 1200},
        {"trip_id": "t003", "pickup_zone": "airport",  "fare_cents": 3500},
        {"trip_id": "t004", "pickup_zone": "airport",  "fare_cents": 4200},
        {"trip_id": "t005", "pickup_zone": "suburbs",  "fare_cents": 600},
        {"trip_id": "t006", "pickup_zone": "downtown", "fare_cents": 950},
        {"trip_id": "t007", "pickup_zone": "suburbs",  "fare_cents": 1100},
        {"trip_id": "t008", "pickup_zone": "airport",  "fare_cents": 2900},
        {"trip_id": "t009", "pickup_zone": "suburbs",  "fare_cents": 750},
        {"trip_id": "t010", "pickup_zone": "downtown", "fare_cents": 700},
    ]
    result = min_max_fare_per_zone(trips)
    assert result["downtown"] == {"min": 700, "max": 1200}
    assert result["airport"] == {"min": 2900, "max": 4200}
    assert result["suburbs"] == {"min": 600, "max": 1100}
    print("006 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 007: Distinct Countries per User Account (multi-login detection)
Difficulty: Easy
Category: Groupby & Aggregation
Tags: groupby, count-distinct, fraud-detection, login-events

Scenario:
Account-security logs record the country from which each login originates.
Users logging in from more than 2 distinct countries in one day are flagged
for review by the fraud team.

Input:
List of dicts: user_id (str), country (str), date (str)

Sample Input:
logins = [
    {"user_id": "u1", "country": "US", "date": "2024-06-01"},
    {"user_id": "u1", "country": "CA", "date": "2024-06-01"},
    {"user_id": "u1", "country": "MX", "date": "2024-06-01"},
    {"user_id": "u2", "country": "UK", "date": "2024-06-01"},
    {"user_id": "u2", "country": "UK", "date": "2024-06-01"},  # same country twice
    {"user_id": "u3", "country": "DE", "date": "2024-06-01"},
    {"user_id": "u3", "country": "FR", "date": "2024-06-01"},
    {"user_id": "u3", "country": "IT", "date": "2024-06-01"},
    {"user_id": "u4", "country": "JP", "date": "2024-06-01"},
    {"user_id": "u1", "country": "US", "date": "2024-06-02"},
]

Expected Output (flagged users on 2024-06-01):
["u1", "u3"]

Follow-up: Return a dict mapping each flagged user to the set of countries
they logged in from.
"""


def flag_multi_country_logins(logins, date, threshold=2):
    countries = defaultdict(set)
    for l in logins:
        if l["date"] == date:
            countries[l["user_id"]].add(l["country"])
    return sorted(uid for uid, ctry in countries.items() if len(ctry) > threshold)


def test_007():
    logins = [
        {"user_id": "u1", "country": "US", "date": "2024-06-01"},
        {"user_id": "u1", "country": "CA", "date": "2024-06-01"},
        {"user_id": "u1", "country": "MX", "date": "2024-06-01"},
        {"user_id": "u2", "country": "UK", "date": "2024-06-01"},
        {"user_id": "u2", "country": "UK", "date": "2024-06-01"},
        {"user_id": "u3", "country": "DE", "date": "2024-06-01"},
        {"user_id": "u3", "country": "FR", "date": "2024-06-01"},
        {"user_id": "u3", "country": "IT", "date": "2024-06-01"},
        {"user_id": "u4", "country": "JP", "date": "2024-06-01"},
        {"user_id": "u1", "country": "US", "date": "2024-06-02"},
    ]
    result = flag_multi_country_logins(logins, "2024-06-01", threshold=2)
    assert result == ["u1", "u3"]
    print("007 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 008: Event Funnel Conversion Counts
Difficulty: Medium
Category: Groupby & Aggregation
Tags: groupby, funnel, user-journey, conversion

Scenario:
An e-commerce pipeline emits user funnel events: page_view → add_to_cart
→ checkout → purchase. Count how many distinct users reached each funnel
stage so the product team can compute drop-off rates.

Input:
List of dicts: user_id (str), event (str)
Ordered funnel stages list

Sample Input:
events = [
    {"user_id": "u1", "event": "page_view"},
    {"user_id": "u2", "event": "page_view"},
    {"user_id": "u3", "event": "page_view"},
    {"user_id": "u4", "event": "page_view"},
    {"user_id": "u1", "event": "add_to_cart"},
    {"user_id": "u2", "event": "add_to_cart"},
    {"user_id": "u3", "event": "add_to_cart"},
    {"user_id": "u1", "event": "checkout"},
    {"user_id": "u2", "event": "checkout"},
    {"user_id": "u1", "event": "purchase"},
]
funnel = ["page_view", "add_to_cart", "checkout", "purchase"]

Expected Output:
{"page_view": 4, "add_to_cart": 3, "checkout": 2, "purchase": 1}

Follow-up: Also compute the step-over-step conversion rate as a float.
"""


def funnel_conversion_counts(events, funnel):
    stage_users = defaultdict(set)
    for e in events:
        stage_users[e["event"]].add(e["user_id"])
    return {stage: len(stage_users[stage]) for stage in funnel}


def test_008():
    events = [
        {"user_id": "u1", "event": "page_view"},
        {"user_id": "u2", "event": "page_view"},
        {"user_id": "u3", "event": "page_view"},
        {"user_id": "u4", "event": "page_view"},
        {"user_id": "u1", "event": "add_to_cart"},
        {"user_id": "u2", "event": "add_to_cart"},
        {"user_id": "u3", "event": "add_to_cart"},
        {"user_id": "u1", "event": "checkout"},
        {"user_id": "u2", "event": "checkout"},
        {"user_id": "u1", "event": "purchase"},
    ]
    funnel = ["page_view", "add_to_cart", "checkout", "purchase"]
    result = funnel_conversion_counts(events, funnel)
    assert result == {"page_view": 4, "add_to_cart": 3, "checkout": 2, "purchase": 1}
    print("008 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 009: Median Trip Distance per City
Difficulty: Medium
Category: Groupby & Aggregation
Tags: groupby, median, bike-trips, statistical-aggregation

Scenario:
A bike-share data pipeline aggregates completed trips. The operations team
wants the median trip distance (metres) per city to set rebalancing budgets.
Compute the exact median (average of two middle values when even count).

Input:
List of dicts: trip_id (str), city (str), distance_m (int)

Sample Input:
trips = [
    {"trip_id": "t1",  "city": "Chicago",    "distance_m": 1200},
    {"trip_id": "t2",  "city": "Chicago",    "distance_m": 850},
    {"trip_id": "t3",  "city": "Chicago",    "distance_m": 1500},
    {"trip_id": "t4",  "city": "Chicago",    "distance_m": 950},
    {"trip_id": "t5",  "city": "NYC",        "distance_m": 2100},
    {"trip_id": "t6",  "city": "NYC",        "distance_m": 1800},
    {"trip_id": "t7",  "city": "NYC",        "distance_m": 2400},
    {"trip_id": "t8",  "city": "SanFrancisco","distance_m": 900},
    {"trip_id": "t9",  "city": "SanFrancisco","distance_m": 1100},
    {"trip_id": "t10", "city": "SanFrancisco","distance_m": 1300},
]

Expected Output:
{"Chicago": 1075.0, "NYC": 2100.0, "SanFrancisco": 1100.0}

Follow-up: Instead of median, return the 75th percentile distance per city.
"""


def median_distance_per_city(trips):
    city_distances = defaultdict(list)
    for t in trips:
        city_distances[t["city"]].append(t["distance_m"])
    result = {}
    for city, dists in city_distances.items():
        dists.sort()
        n = len(dists)
        mid = n // 2
        if n % 2 == 1:
            result[city] = float(dists[mid])
        else:
            result[city] = (dists[mid - 1] + dists[mid]) / 2.0
    return result


def test_009():
    trips = [
        {"trip_id": "t1",  "city": "Chicago",      "distance_m": 1200},
        {"trip_id": "t2",  "city": "Chicago",      "distance_m": 850},
        {"trip_id": "t3",  "city": "Chicago",      "distance_m": 1500},
        {"trip_id": "t4",  "city": "Chicago",      "distance_m": 950},
        {"trip_id": "t5",  "city": "NYC",          "distance_m": 2100},
        {"trip_id": "t6",  "city": "NYC",          "distance_m": 1800},
        {"trip_id": "t7",  "city": "NYC",          "distance_m": 2400},
        {"trip_id": "t8",  "city": "SanFrancisco", "distance_m": 900},
        {"trip_id": "t9",  "city": "SanFrancisco", "distance_m": 1100},
        {"trip_id": "t10", "city": "SanFrancisco", "distance_m": 1300},
    ]
    result = median_distance_per_city(trips)
    assert result["Chicago"] == 1075.0
    assert result["NYC"] == 2100.0
    assert result["SanFrancisco"] == 1100.0
    print("009 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 010: Revenue Share per Merchant (percentage of total)
Difficulty: Medium
Category: Groupby & Aggregation
Tags: groupby, percentage, transactions, financial-reporting

Scenario:
A payment-processing pipeline records transactions per merchant. Finance
needs each merchant's share of total revenue (as a percentage rounded to
2 decimal places) for a monthly board report.

Input:
List of dicts: transaction_id (str), merchant_id (str), amount_cents (int)

Sample Input:
transactions = [
    {"transaction_id": "tx01", "merchant_id": "M1", "amount_cents": 5000},
    {"transaction_id": "tx02", "merchant_id": "M1", "amount_cents": 3000},
    {"transaction_id": "tx03", "merchant_id": "M2", "amount_cents": 8000},
    {"transaction_id": "tx04", "merchant_id": "M2", "amount_cents": 2000},
    {"transaction_id": "tx05", "merchant_id": "M3", "amount_cents": 1000},
    {"transaction_id": "tx06", "merchant_id": "M3", "amount_cents": 4000},
    {"transaction_id": "tx07", "merchant_id": "M1", "amount_cents": 2000},
    {"transaction_id": "tx08", "merchant_id": "M4", "amount_cents": 5000},
    {"transaction_id": "tx09", "merchant_id": "M4", "amount_cents": 5000},
    {"transaction_id": "tx10", "merchant_id": "M2", "amount_cents": 5000},
]

Expected Output:
{"M1": 25.0, "M2": 37.5, "M3": 12.5, "M4": 25.0}
# total = 40000; M1=10000, M2=15000, M3=5000, M4=10000

Follow-up: Return merchants sorted by revenue share descending.
"""


def revenue_share_per_merchant(transactions):
    merchant_totals = defaultdict(int)
    grand_total = 0
    for tx in transactions:
        merchant_totals[tx["merchant_id"]] += tx["amount_cents"]
        grand_total += tx["amount_cents"]
    return {m: round(100 * amt / grand_total, 2) for m, amt in merchant_totals.items()}


def test_010():
    transactions = [
        {"transaction_id": "tx01", "merchant_id": "M1", "amount_cents": 5000},
        {"transaction_id": "tx02", "merchant_id": "M1", "amount_cents": 3000},
        {"transaction_id": "tx03", "merchant_id": "M2", "amount_cents": 8000},
        {"transaction_id": "tx04", "merchant_id": "M2", "amount_cents": 2000},
        {"transaction_id": "tx05", "merchant_id": "M3", "amount_cents": 1000},
        {"transaction_id": "tx06", "merchant_id": "M3", "amount_cents": 4000},
        {"transaction_id": "tx07", "merchant_id": "M1", "amount_cents": 2000},
        {"transaction_id": "tx08", "merchant_id": "M4", "amount_cents": 5000},
        {"transaction_id": "tx09", "merchant_id": "M4", "amount_cents": 5000},
        {"transaction_id": "tx10", "merchant_id": "M2", "amount_cents": 5000},
    ]
    result = revenue_share_per_merchant(transactions)
    assert result == {"M1": 25.0, "M2": 37.5, "M3": 12.5, "M4": 25.0}
    print("010 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 011: First and Last Event Timestamp per User
Difficulty: Medium
Category: Groupby & Aggregation
Tags: groupby, first-last, session-analysis, user-lifecycle

Scenario:
A pipeline tracking user lifecycle emits events with ISO timestamps.
The data team wants each user's first-seen and last-seen timestamps to
compute account age and detect recently churned accounts.

Input:
List of dicts: user_id (str), event (str), ts (str ISO-8601)

Sample Input:
events = [
    {"user_id": "u1", "event": "signup",   "ts": "2024-01-05T08:00:00"},
    {"user_id": "u2", "event": "signup",   "ts": "2024-01-06T09:00:00"},
    {"user_id": "u1", "event": "login",    "ts": "2024-01-10T10:00:00"},
    {"user_id": "u3", "event": "signup",   "ts": "2024-01-07T11:00:00"},
    {"user_id": "u2", "event": "purchase", "ts": "2024-01-12T14:00:00"},
    {"user_id": "u3", "event": "login",    "ts": "2024-01-08T12:00:00"},
    {"user_id": "u1", "event": "purchase", "ts": "2024-01-15T16:00:00"},
    {"user_id": "u3", "event": "purchase", "ts": "2024-01-20T09:00:00"},
    {"user_id": "u2", "event": "login",    "ts": "2024-01-18T11:00:00"},
    {"user_id": "u1", "event": "logout",   "ts": "2024-01-22T17:00:00"},
]

Expected Output:
{
  "u1": {"first": "2024-01-05T08:00:00", "last": "2024-01-22T17:00:00"},
  "u2": {"first": "2024-01-06T09:00:00", "last": "2024-01-18T11:00:00"},
  "u3": {"first": "2024-01-07T11:00:00", "last": "2024-01-20T09:00:00"},
}

Follow-up: Return only users whose last event is more than 30 days before
a given reference date (churn detection).
"""


def first_last_event_per_user(events):
    result = {}
    for e in events:
        uid, ts = e["user_id"], e["ts"]
        if uid not in result:
            result[uid] = {"first": ts, "last": ts}
        else:
            result[uid]["first"] = min(result[uid]["first"], ts)
            result[uid]["last"] = max(result[uid]["last"], ts)
    return result


def test_011():
    events = [
        {"user_id": "u1", "event": "signup",   "ts": "2024-01-05T08:00:00"},
        {"user_id": "u2", "event": "signup",   "ts": "2024-01-06T09:00:00"},
        {"user_id": "u1", "event": "login",    "ts": "2024-01-10T10:00:00"},
        {"user_id": "u3", "event": "signup",   "ts": "2024-01-07T11:00:00"},
        {"user_id": "u2", "event": "purchase", "ts": "2024-01-12T14:00:00"},
        {"user_id": "u3", "event": "login",    "ts": "2024-01-08T12:00:00"},
        {"user_id": "u1", "event": "purchase", "ts": "2024-01-15T16:00:00"},
        {"user_id": "u3", "event": "purchase", "ts": "2024-01-20T09:00:00"},
        {"user_id": "u2", "event": "login",    "ts": "2024-01-18T11:00:00"},
        {"user_id": "u1", "event": "logout",   "ts": "2024-01-22T17:00:00"},
    ]
    result = first_last_event_per_user(events)
    assert result["u1"] == {"first": "2024-01-05T08:00:00", "last": "2024-01-22T17:00:00"}
    assert result["u2"]["last"] == "2024-01-18T11:00:00"
    assert result["u3"]["first"] == "2024-01-07T11:00:00"
    print("011 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 012: Aggregate Pipeline Record Counts and Error Rate per Stage
Difficulty: Medium
Category: Groupby & Aggregation
Tags: groupby, error-rate, pipeline-monitoring, data-quality

Scenario:
A data pipeline emits processing records per stage (ingest, transform,
load). Each record has a status (success | error). The SRE team wants
total records and error rate per stage to set alerting thresholds.

Input:
List of dicts: stage (str), record_id (str), status (str: "success"|"error")

Sample Input:
records = [
    {"stage": "ingest",    "record_id": "r01", "status": "success"},
    {"stage": "ingest",    "record_id": "r02", "status": "success"},
    {"stage": "ingest",    "record_id": "r03", "status": "error"},
    {"stage": "transform", "record_id": "r04", "status": "success"},
    {"stage": "transform", "record_id": "r05", "status": "error"},
    {"stage": "transform", "record_id": "r06", "status": "error"},
    {"stage": "transform", "record_id": "r07", "status": "success"},
    {"stage": "load",      "record_id": "r08", "status": "success"},
    {"stage": "load",      "record_id": "r09", "status": "success"},
    {"stage": "load",      "record_id": "r10", "status": "success"},
]

Expected Output:
{
  "ingest":    {"total": 3, "errors": 1, "error_rate": 0.3333},
  "transform": {"total": 4, "errors": 2, "error_rate": 0.5},
  "load":      {"total": 3, "errors": 0, "error_rate": 0.0},
}
# error_rate rounded to 4 decimal places

Follow-up: Alert (return stage name) if error_rate exceeds a given threshold.
"""


def pipeline_error_rates(records):
    totals = defaultdict(int)
    errors = defaultdict(int)
    for r in records:
        totals[r["stage"]] += 1
        if r["status"] == "error":
            errors[r["stage"]] += 1
    result = {}
    for stage, total in totals.items():
        err = errors[stage]
        result[stage] = {
            "total": total,
            "errors": err,
            "error_rate": round(err / total, 4),
        }
    return result


def test_012():
    records = [
        {"stage": "ingest",    "record_id": "r01", "status": "success"},
        {"stage": "ingest",    "record_id": "r02", "status": "success"},
        {"stage": "ingest",    "record_id": "r03", "status": "error"},
        {"stage": "transform", "record_id": "r04", "status": "success"},
        {"stage": "transform", "record_id": "r05", "status": "error"},
        {"stage": "transform", "record_id": "r06", "status": "error"},
        {"stage": "transform", "record_id": "r07", "status": "success"},
        {"stage": "load",      "record_id": "r08", "status": "success"},
        {"stage": "load",      "record_id": "r09", "status": "success"},
        {"stage": "load",      "record_id": "r10", "status": "success"},
    ]
    result = pipeline_error_rates(records)
    assert result["ingest"]["error_rate"] == 0.3333
    assert result["transform"]["error_rate"] == 0.5
    assert result["load"]["errors"] == 0
    print("012 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 013: Hourly Impression Counts per Ad Creative
Difficulty: Medium
Category: Groupby & Aggregation
Tags: groupby, time-bucketing, ad-impressions, hourly-aggregation

Scenario:
An ad-tech pipeline receives impression timestamps (Unix epoch seconds).
The ad-ops team wants impression counts bucketed by hour-of-day per
creative to optimise delivery scheduling.

Input:
List of dicts: impression_id (str), creative_id (str), epoch_ts (int)

Sample Input:
impressions = [
    {"impression_id": "i01", "creative_id": "cr1", "epoch_ts": 1704067200},  # 2024-01-01 00:00
    {"impression_id": "i02", "creative_id": "cr1", "epoch_ts": 1704070800},  # 2024-01-01 01:00
    {"impression_id": "i03", "creative_id": "cr2", "epoch_ts": 1704067200},  # 2024-01-01 00:00
    {"impression_id": "i04", "creative_id": "cr1", "epoch_ts": 1704067500},  # 2024-01-01 00:05
    {"impression_id": "i05", "creative_id": "cr2", "epoch_ts": 1704074400},  # 2024-01-01 02:00
    {"impression_id": "i06", "creative_id": "cr1", "epoch_ts": 1704074700},  # 2024-01-01 02:05
    {"impression_id": "i07", "creative_id": "cr2", "epoch_ts": 1704067800},  # 2024-01-01 00:10
    {"impression_id": "i08", "creative_id": "cr1", "epoch_ts": 1704070900},  # 2024-01-01 01:01
    {"impression_id": "i09", "creative_id": "cr3", "epoch_ts": 1704067200},  # 2024-01-01 00:00
    {"impression_id": "i10", "creative_id": "cr3", "epoch_ts": 1704074400},  # 2024-01-01 02:00
]

Expected Output:
{
  ("cr1", 0): 2,
  ("cr1", 1): 2,
  ("cr1", 2): 1,
  ("cr2", 0): 2,
  ("cr2", 2): 1,
  ("cr3", 0): 1,
  ("cr3", 2): 1,
}

Follow-up: Return only (creative, hour) combos where count >= 2.
"""


def hourly_impressions_per_creative(impressions):
    counts = defaultdict(int)
    for imp in impressions:
        hour = (imp["epoch_ts"] % 86400) // 3600
        counts[(imp["creative_id"], hour)] += 1
    return dict(counts)


def test_013():
    impressions = [
        {"impression_id": "i01", "creative_id": "cr1", "epoch_ts": 1704067200},
        {"impression_id": "i02", "creative_id": "cr1", "epoch_ts": 1704070800},
        {"impression_id": "i03", "creative_id": "cr2", "epoch_ts": 1704067200},
        {"impression_id": "i04", "creative_id": "cr1", "epoch_ts": 1704067500},
        {"impression_id": "i05", "creative_id": "cr2", "epoch_ts": 1704074400},
        {"impression_id": "i06", "creative_id": "cr1", "epoch_ts": 1704074700},
        {"impression_id": "i07", "creative_id": "cr2", "epoch_ts": 1704067800},
        {"impression_id": "i08", "creative_id": "cr1", "epoch_ts": 1704070900},
        {"impression_id": "i09", "creative_id": "cr3", "epoch_ts": 1704067200},
        {"impression_id": "i10", "creative_id": "cr3", "epoch_ts": 1704074400},
    ]
    result = hourly_impressions_per_creative(impressions)
    assert result[("cr1", 0)] == 2
    assert result[("cr1", 1)] == 2
    assert result[("cr2", 0)] == 2
    assert result[("cr3", 2)] == 1
    print("013 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 014: Weighted Average Response Latency per Service
Difficulty: Medium
Category: Groupby & Aggregation
Tags: groupby, weighted-average, latency, service-monitoring

Scenario:
An API-gateway log pipeline records per-request latency and the request
weight (priority tier multiplier). The platform team wants the
weighted-average latency per service for SLA reporting.

Input:
List of dicts: request_id (str), service (str), latency_ms (int), weight (float)

Sample Input:
requests = [
    {"request_id": "q01", "service": "auth",    "latency_ms": 12, "weight": 1.0},
    {"request_id": "q02", "service": "auth",    "latency_ms": 18, "weight": 2.0},
    {"request_id": "q03", "service": "auth",    "latency_ms": 10, "weight": 1.0},
    {"request_id": "q04", "service": "search",  "latency_ms": 45, "weight": 3.0},
    {"request_id": "q05", "service": "search",  "latency_ms": 30, "weight": 1.0},
    {"request_id": "q06", "service": "search",  "latency_ms": 60, "weight": 2.0},
    {"request_id": "q07", "service": "profile", "latency_ms": 8,  "weight": 1.0},
    {"request_id": "q08", "service": "profile", "latency_ms": 15, "weight": 1.0},
    {"request_id": "q09", "service": "auth",    "latency_ms": 22, "weight": 2.0},
    {"request_id": "q10", "service": "profile", "latency_ms": 11, "weight": 2.0},
]

Expected Output:
{
  "auth":    17.0,    # (12*1+18*2+10*1+22*2)/(1+2+1+2)=102/6=17.0
  "search":  46.5,    # (45*3+30*1+60*2)/(3+1+2)=279/6=46.5
  "profile": 11.25,   # (8*1+15*1+11*2)/(1+1+2)=45/4=11.25
}

Follow-up: Return services where weighted-average latency exceeds a
given SLA threshold in ms.
"""


def weighted_avg_latency_per_service(requests):
    numerator = defaultdict(float)
    weight_sum = defaultdict(float)
    for r in requests:
        svc = r["service"]
        numerator[svc] += r["latency_ms"] * r["weight"]
        weight_sum[svc] += r["weight"]
    return {svc: round(numerator[svc] / weight_sum[svc], 4) for svc in numerator}


def test_014():
    requests = [
        {"request_id": "q01", "service": "auth",    "latency_ms": 12, "weight": 1.0},
        {"request_id": "q02", "service": "auth",    "latency_ms": 18, "weight": 2.0},
        {"request_id": "q03", "service": "auth",    "latency_ms": 10, "weight": 1.0},
        {"request_id": "q04", "service": "search",  "latency_ms": 45, "weight": 3.0},
        {"request_id": "q05", "service": "search",  "latency_ms": 30, "weight": 1.0},
        {"request_id": "q06", "service": "search",  "latency_ms": 60, "weight": 2.0},
        {"request_id": "q07", "service": "profile", "latency_ms": 8,  "weight": 1.0},
        {"request_id": "q08", "service": "profile", "latency_ms": 15, "weight": 1.0},
        {"request_id": "q09", "service": "auth",    "latency_ms": 22, "weight": 2.0},
        {"request_id": "q10", "service": "profile", "latency_ms": 11, "weight": 2.0},
    ]
    result = weighted_avg_latency_per_service(requests)
    assert result["auth"] == 17.0
    assert result["search"] == 46.5
    assert result["profile"] == 11.25
    print("014 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 015: Running Total Spend per User (cumulative aggregation)
Difficulty: Medium
Category: Groupby & Aggregation
Tags: groupby, running-total, cumulative-sum, transactions

Scenario:
A fintech pipeline processes transactions in arrival order. A fraud model
needs the running cumulative spend per user so it can trigger an alert
the moment a user crosses a spending threshold within a single processing
batch.

Input:
List of dicts (in arrival order): user_id (str), tx_id (str), amount_cents (int)

Sample Input:
transactions = [
    {"user_id": "u1", "tx_id": "tx1", "amount_cents": 200},
    {"user_id": "u2", "tx_id": "tx2", "amount_cents": 500},
    {"user_id": "u1", "tx_id": "tx3", "amount_cents": 300},
    {"user_id": "u3", "tx_id": "tx4", "amount_cents": 150},
    {"user_id": "u2", "tx_id": "tx5", "amount_cents": 700},
    {"user_id": "u1", "tx_id": "tx6", "amount_cents": 400},
    {"user_id": "u3", "tx_id": "tx7", "amount_cents": 350},
    {"user_id": "u2", "tx_id": "tx8", "amount_cents": 100},
    {"user_id": "u1", "tx_id": "tx9", "amount_cents": 600},
    {"user_id": "u3", "tx_id": "tx10","amount_cents": 900},
]
threshold_cents = 1000

Expected Output (users who crossed threshold, with the tx_id that crossed it):
[
  {"user_id": "u1", "tx_id": "tx6", "cumulative": 900},  # crosses at tx9 (1500) but first cross reported
  {"user_id": "u2", "tx_id": "tx5", "cumulative": 1200},
  {"user_id": "u3", "tx_id": "tx10","cumulative": 1400},
]
# NOTE: report the FIRST transaction that pushes user over threshold.
# u1 crosses at tx9 (200+300+400+600=1500 > 1000); but tx6 puts u1 at 900, tx9 at 1500
# Correct: u1 crosses at tx9 with cumulative=1500

Expected Output (corrected):
[
  {"user_id": "u2", "tx_id": "tx5", "cumulative": 1200},
  {"user_id": "u1", "tx_id": "tx9", "cumulative": 1500},
  {"user_id": "u3", "tx_id": "tx10","cumulative": 1400},
]

Follow-up: Reset the running total for a user after each threshold crossing
(detect repeated offenders within the same batch).
"""


def find_threshold_crossings(transactions, threshold_cents):
    running = defaultdict(int)
    crossed = set()
    result = []
    for tx in transactions:
        uid = tx["user_id"]
        if uid in crossed:
            continue
        running[uid] += tx["amount_cents"]
        if running[uid] > threshold_cents:
            result.append({
                "user_id": uid,
                "tx_id": tx["tx_id"],
                "cumulative": running[uid],
            })
            crossed.add(uid)
    return result


def test_015():
    transactions = [
        {"user_id": "u1", "tx_id": "tx1",  "amount_cents": 200},
        {"user_id": "u2", "tx_id": "tx2",  "amount_cents": 500},
        {"user_id": "u1", "tx_id": "tx3",  "amount_cents": 300},
        {"user_id": "u3", "tx_id": "tx4",  "amount_cents": 150},
        {"user_id": "u2", "tx_id": "tx5",  "amount_cents": 700},
        {"user_id": "u1", "tx_id": "tx6",  "amount_cents": 400},
        {"user_id": "u3", "tx_id": "tx7",  "amount_cents": 350},
        {"user_id": "u2", "tx_id": "tx8",  "amount_cents": 100},
        {"user_id": "u1", "tx_id": "tx9",  "amount_cents": 600},
        {"user_id": "u3", "tx_id": "tx10", "amount_cents": 900},
    ]
    result = find_threshold_crossings(transactions, 1000)
    by_user = {r["user_id"]: r for r in result}
    assert by_user["u2"]["tx_id"] == "tx5"
    assert by_user["u2"]["cumulative"] == 1200
    assert by_user["u1"]["tx_id"] == "tx9"
    assert by_user["u1"]["cumulative"] == 1500
    assert by_user["u3"]["tx_id"] == "tx10"
    assert by_user["u3"]["cumulative"] == 1400
    print("015 PASS")


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    test_001()
    test_002()
    test_003()
    test_004()
    test_005()
    test_006()
    test_007()
    test_008()
    test_009()
    test_010()
    test_011()
    test_012()
    test_013()
    test_014()
    test_015()
    print("\nAll Category 1 tests passed.")
