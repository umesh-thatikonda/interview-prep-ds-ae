"""
============================================================
CATEGORY 3: JOINS & DATASET MERGING  (Problems 028 – 040)
============================================================
All solutions use only Python stdlib:
  dict, list, set, collections (defaultdict),
  itertools — NO pandas, NO numpy.
"""

from collections import defaultdict


# ─────────────────────────────────────────────────────────────
"""
Problem 028: Inner Join — Enrich Events with User Profile Data
Difficulty: Easy
Category: Joins & Dataset Merging
Tags: inner-join, hash-join, user-events, enrichment

Scenario:
An analytics pipeline has a stream of raw click events (user_id, url, ts)
and a separate user-profile table (user_id, country, age_group). Enrich
each event with the user's country and age_group. Drop events for users
not found in the profile table (inner join behaviour).

Input:
events: list of dicts — user_id (str), url (str), ts (str)
profiles: list of dicts — user_id (str), country (str), age_group (str)

Sample Input:
events = [
    {"user_id": "u1", "url": "/home",     "ts": "2024-01-10T10:00"},
    {"user_id": "u2", "url": "/product",  "ts": "2024-01-10T10:01"},
    {"user_id": "u3", "url": "/checkout", "ts": "2024-01-10T10:02"},
    {"user_id": "u4", "url": "/home",     "ts": "2024-01-10T10:03"},
    {"user_id": "u5", "url": "/search",   "ts": "2024-01-10T10:04"},  # no profile
    {"user_id": "u1", "url": "/cart",     "ts": "2024-01-10T10:05"},
    {"user_id": "u6", "url": "/home",     "ts": "2024-01-10T10:06"},  # no profile
    {"user_id": "u2", "url": "/checkout", "ts": "2024-01-10T10:07"},
    {"user_id": "u3", "url": "/confirm",  "ts": "2024-01-10T10:08"},
    {"user_id": "u4", "url": "/logout",   "ts": "2024-01-10T10:09"},
]
profiles = [
    {"user_id": "u1", "country": "US", "age_group": "25-34"},
    {"user_id": "u2", "country": "UK", "age_group": "35-44"},
    {"user_id": "u3", "country": "DE", "age_group": "18-24"},
    {"user_id": "u4", "country": "JP", "age_group": "45-54"},
]

Expected Output (8 enriched events — u5 and u6 dropped):
[
  {"user_id": "u1", "url": "/home",     "ts": "...", "country": "US", "age_group": "25-34"},
  ...
]
len = 8, no u5 or u6 rows

Follow-up: Left join variant — keep all events, fill missing profile fields with None.
"""


def inner_join_events_profiles(events, profiles):
    profile_map = {p["user_id"]: p for p in profiles}
    result = []
    for e in events:
        if e["user_id"] in profile_map:
            enriched = dict(e)
            p = profile_map[e["user_id"]]
            enriched["country"] = p["country"]
            enriched["age_group"] = p["age_group"]
            result.append(enriched)
    return result


def test_028():
    events = [
        {"user_id": "u1", "url": "/home",     "ts": "2024-01-10T10:00"},
        {"user_id": "u2", "url": "/product",  "ts": "2024-01-10T10:01"},
        {"user_id": "u3", "url": "/checkout", "ts": "2024-01-10T10:02"},
        {"user_id": "u4", "url": "/home",     "ts": "2024-01-10T10:03"},
        {"user_id": "u5", "url": "/search",   "ts": "2024-01-10T10:04"},
        {"user_id": "u1", "url": "/cart",     "ts": "2024-01-10T10:05"},
        {"user_id": "u6", "url": "/home",     "ts": "2024-01-10T10:06"},
        {"user_id": "u2", "url": "/checkout", "ts": "2024-01-10T10:07"},
        {"user_id": "u3", "url": "/confirm",  "ts": "2024-01-10T10:08"},
        {"user_id": "u4", "url": "/logout",   "ts": "2024-01-10T10:09"},
    ]
    profiles = [
        {"user_id": "u1", "country": "US", "age_group": "25-34"},
        {"user_id": "u2", "country": "UK", "age_group": "35-44"},
        {"user_id": "u3", "country": "DE", "age_group": "18-24"},
        {"user_id": "u4", "country": "JP", "age_group": "45-54"},
    ]
    result = inner_join_events_profiles(events, profiles)
    assert len(result) == 8
    assert all(r["user_id"] not in ("u5", "u6") for r in result)
    assert result[0]["country"] == "US"
    print("028 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 029: Left Join — Retain All Trips, Flag Missing Station Metadata
Difficulty: Easy
Category: Joins & Dataset Merging
Tags: left-join, hash-join, bike-trips, data-quality

Scenario:
A bike-share pipeline joins completed trip records with a station-metadata
table. Some trip records reference station IDs that were decommissioned
and no longer exist in the metadata table. Keep all trips; where station
metadata is missing, fill station_name with "UNKNOWN" and capacity with -1.

Input:
trips: list of dicts — trip_id (str), start_station_id (str), duration_secs (int)
stations: list of dicts — station_id (str), station_name (str), capacity (int)

Sample Input:
trips = [
    {"trip_id": "t1",  "start_station_id": "s1",  "duration_secs": 600},
    {"trip_id": "t2",  "start_station_id": "s2",  "duration_secs": 900},
    {"trip_id": "t3",  "start_station_id": "s99", "duration_secs": 450},  # missing
    {"trip_id": "t4",  "start_station_id": "s3",  "duration_secs": 1200},
    {"trip_id": "t5",  "start_station_id": "s88", "duration_secs": 300},  # missing
    {"trip_id": "t6",  "start_station_id": "s1",  "duration_secs": 750},
    {"trip_id": "t7",  "start_station_id": "s4",  "duration_secs": 500},
    {"trip_id": "t8",  "start_station_id": "s2",  "duration_secs": 820},
    {"trip_id": "t9",  "start_station_id": "s77", "duration_secs": 670},  # missing
    {"trip_id": "t10", "start_station_id": "s3",  "duration_secs": 910},
]
stations = [
    {"station_id": "s1", "station_name": "Central Park North", "capacity": 25},
    {"station_id": "s2", "station_name": "Times Square",       "capacity": 40},
    {"station_id": "s3", "station_name": "Brooklyn Bridge",    "capacity": 20},
    {"station_id": "s4", "station_name": "Battery Park",       "capacity": 15},
]

Expected Output (10 rows, 3 with UNKNOWN station):
t3, t5, t9 have station_name="UNKNOWN" and capacity=-1

Follow-up: Return a list of distinct missing station_ids for a data-quality report.
"""


def left_join_trips_stations(trips, stations):
    station_map = {s["station_id"]: s for s in stations}
    result = []
    for trip in trips:
        enriched = dict(trip)
        meta = station_map.get(trip["start_station_id"])
        if meta:
            enriched["station_name"] = meta["station_name"]
            enriched["capacity"] = meta["capacity"]
        else:
            enriched["station_name"] = "UNKNOWN"
            enriched["capacity"] = -1
        result.append(enriched)
    return result


def test_029():
    trips = [
        {"trip_id": "t1",  "start_station_id": "s1",  "duration_secs": 600},
        {"trip_id": "t2",  "start_station_id": "s2",  "duration_secs": 900},
        {"trip_id": "t3",  "start_station_id": "s99", "duration_secs": 450},
        {"trip_id": "t4",  "start_station_id": "s3",  "duration_secs": 1200},
        {"trip_id": "t5",  "start_station_id": "s88", "duration_secs": 300},
        {"trip_id": "t6",  "start_station_id": "s1",  "duration_secs": 750},
        {"trip_id": "t7",  "start_station_id": "s4",  "duration_secs": 500},
        {"trip_id": "t8",  "start_station_id": "s2",  "duration_secs": 820},
        {"trip_id": "t9",  "start_station_id": "s77", "duration_secs": 670},
        {"trip_id": "t10", "start_station_id": "s3",  "duration_secs": 910},
    ]
    stations = [
        {"station_id": "s1", "station_name": "Central Park North", "capacity": 25},
        {"station_id": "s2", "station_name": "Times Square",       "capacity": 40},
        {"station_id": "s3", "station_name": "Brooklyn Bridge",    "capacity": 20},
        {"station_id": "s4", "station_name": "Battery Park",       "capacity": 15},
    ]
    result = left_join_trips_stations(trips, stations)
    assert len(result) == 10
    missing = [r for r in result if r["station_name"] == "UNKNOWN"]
    assert len(missing) == 3
    assert {r["trip_id"] for r in missing} == {"t3", "t5", "t9"}
    print("029 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 030: Anti-Join — Users Who Never Made a Purchase
Difficulty: Easy
Category: Joins & Dataset Merging
Tags: anti-join, set-difference, user-events, retention

Scenario:
A growth pipeline wants to identify users who registered but never
completed a purchase, for a re-engagement campaign. Return all registered
users whose user_id does not appear in the purchases table.

Input:
registrations: list of dicts — user_id (str), signup_date (str)
purchases: list of dicts — purchase_id (str), user_id (str), amount_cents (int)

Sample Input:
registrations = [
    {"user_id": "u1", "signup_date": "2024-02-01"},
    {"user_id": "u2", "signup_date": "2024-02-02"},
    {"user_id": "u3", "signup_date": "2024-02-03"},
    {"user_id": "u4", "signup_date": "2024-02-04"},
    {"user_id": "u5", "signup_date": "2024-02-05"},
    {"user_id": "u6", "signup_date": "2024-02-06"},
    {"user_id": "u7", "signup_date": "2024-02-07"},
    {"user_id": "u8", "signup_date": "2024-02-08"},
]
purchases = [
    {"purchase_id": "p1", "user_id": "u1", "amount_cents": 1000},
    {"purchase_id": "p2", "user_id": "u3", "amount_cents": 500},
    {"purchase_id": "p3", "user_id": "u5", "amount_cents": 750},
    {"purchase_id": "p4", "user_id": "u1", "amount_cents": 200},  # u1 again
    {"purchase_id": "p5", "user_id": "u7", "amount_cents": 300},
]

Expected Output (users who never purchased, sorted):
["u2", "u4", "u6", "u8"]

Follow-up: Also return the number of days since signup for each non-purchasing user.
"""


def users_who_never_purchased(registrations, purchases):
    buyers = {p["user_id"] for p in purchases}
    return sorted(r["user_id"] for r in registrations if r["user_id"] not in buyers)


def test_030():
    registrations = [
        {"user_id": "u1", "signup_date": "2024-02-01"},
        {"user_id": "u2", "signup_date": "2024-02-02"},
        {"user_id": "u3", "signup_date": "2024-02-03"},
        {"user_id": "u4", "signup_date": "2024-02-04"},
        {"user_id": "u5", "signup_date": "2024-02-05"},
        {"user_id": "u6", "signup_date": "2024-02-06"},
        {"user_id": "u7", "signup_date": "2024-02-07"},
        {"user_id": "u8", "signup_date": "2024-02-08"},
    ]
    purchases = [
        {"purchase_id": "p1", "user_id": "u1", "amount_cents": 1000},
        {"purchase_id": "p2", "user_id": "u3", "amount_cents": 500},
        {"purchase_id": "p3", "user_id": "u5", "amount_cents": 750},
        {"purchase_id": "p4", "user_id": "u1", "amount_cents": 200},
        {"purchase_id": "p5", "user_id": "u7", "amount_cents": 300},
    ]
    result = users_who_never_purchased(registrations, purchases)
    assert result == ["u2", "u4", "u6", "u8"]
    print("030 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 031: Many-to-Many Join — Users to Roles to Permissions
Difficulty: Medium
Category: Joins & Dataset Merging
Tags: many-to-many, hash-join, RBAC, access-control

Scenario:
An access-control pipeline maintains a user-role mapping and a
role-permission mapping. Build a flat list of all (user_id, permission)
pairs by joining through the role table.

Input:
user_roles: list of dicts — user_id (str), role (str)
role_perms: list of dicts — role (str), permission (str)

Sample Input:
user_roles = [
    {"user_id": "alice", "role": "admin"},
    {"user_id": "alice", "role": "analyst"},
    {"user_id": "bob",   "role": "analyst"},
    {"user_id": "carol", "role": "viewer"},
    {"user_id": "dave",  "role": "admin"},
    {"user_id": "eve",   "role": "viewer"},
    {"user_id": "frank", "role": "analyst"},
]
role_perms = [
    {"role": "admin",   "permission": "read"},
    {"role": "admin",   "permission": "write"},
    {"role": "admin",   "permission": "delete"},
    {"role": "analyst", "permission": "read"},
    {"role": "analyst", "permission": "export"},
    {"role": "viewer",  "permission": "read"},
]

Expected Output (sorted set of unique (user_id, permission) pairs):
alice: {read, write, delete, export}  (admin+analyst)
bob:   {read, export}
carol: {read}
dave:  {read, write, delete}
eve:   {read}
frank: {read, export}

Follow-up: Return users who have the "delete" permission.
"""


def user_permissions(user_roles, role_perms):
    role_to_perms = defaultdict(set)
    for rp in role_perms:
        role_to_perms[rp["role"]].add(rp["permission"])

    user_perms = defaultdict(set)
    for ur in user_roles:
        user_perms[ur["user_id"]] |= role_to_perms[ur["role"]]
    return {uid: sorted(perms) for uid, perms in user_perms.items()}


def test_031():
    user_roles = [
        {"user_id": "alice", "role": "admin"},
        {"user_id": "alice", "role": "analyst"},
        {"user_id": "bob",   "role": "analyst"},
        {"user_id": "carol", "role": "viewer"},
        {"user_id": "dave",  "role": "admin"},
        {"user_id": "eve",   "role": "viewer"},
        {"user_id": "frank", "role": "analyst"},
    ]
    role_perms = [
        {"role": "admin",   "permission": "read"},
        {"role": "admin",   "permission": "write"},
        {"role": "admin",   "permission": "delete"},
        {"role": "analyst", "permission": "read"},
        {"role": "analyst", "permission": "export"},
        {"role": "viewer",  "permission": "read"},
    ]
    result = user_permissions(user_roles, role_perms)
    assert set(result["alice"]) == {"read", "write", "delete", "export"}
    assert set(result["bob"]) == {"read", "export"}
    assert set(result["carol"]) == {"read"}
    assert set(result["dave"]) == {"read", "write", "delete"}
    print("031 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 032: Self-Join — Find Referral Chains (1-level)
Difficulty: Medium
Category: Joins & Dataset Merging
Tags: self-join, referral, graph-traversal, growth

Scenario:
A growth pipeline has a referral table where each user optionally has a
referrer. Build a flat list showing each user alongside their referrer's
name (self-join on the same user table).

Input:
users: list of dicts — user_id (str), name (str), referred_by (str or None)

Sample Input:
users = [
    {"user_id": "u1", "name": "Alice",  "referred_by": None},
    {"user_id": "u2", "name": "Bob",    "referred_by": "u1"},
    {"user_id": "u3", "name": "Carol",  "referred_by": "u1"},
    {"user_id": "u4", "name": "Dave",   "referred_by": "u2"},
    {"user_id": "u5", "name": "Eve",    "referred_by": "u2"},
    {"user_id": "u6", "name": "Frank",  "referred_by": "u3"},
    {"user_id": "u7", "name": "Grace",  "referred_by": None},
    {"user_id": "u8", "name": "Heidi",  "referred_by": "u7"},
    {"user_id": "u9", "name": "Ivan",   "referred_by": "u4"},
    {"user_id": "u10","name": "Judy",   "referred_by": "u1"},
]

Expected Output (users with a referrer, sorted by user_id):
[
  {"user_id": "u2",  "name": "Bob",   "referrer_name": "Alice"},
  {"user_id": "u3",  "name": "Carol", "referrer_name": "Alice"},
  {"user_id": "u4",  "name": "Dave",  "referrer_name": "Bob"},
  {"user_id": "u5",  "name": "Eve",   "referrer_name": "Bob"},
  {"user_id": "u6",  "name": "Frank", "referrer_name": "Carol"},
  {"user_id": "u8",  "name": "Heidi", "referrer_name": "Grace"},
  {"user_id": "u9",  "name": "Ivan",  "referrer_name": "Dave"},
  {"user_id": "u10", "name": "Judy",  "referrer_name": "Alice"},
]

Follow-up: Count how many users each person referred (direct referrals only).
"""


def build_referral_list(users):
    uid_to_name = {u["user_id"]: u["name"] for u in users}
    result = []
    for u in sorted(users, key=lambda x: x["user_id"]):
        if u["referred_by"] is not None:
            result.append({
                "user_id": u["user_id"],
                "name": u["name"],
                "referrer_name": uid_to_name[u["referred_by"]],
            })
    return result


def test_032():
    users = [
        {"user_id": "u1",  "name": "Alice",  "referred_by": None},
        {"user_id": "u2",  "name": "Bob",    "referred_by": "u1"},
        {"user_id": "u3",  "name": "Carol",  "referred_by": "u1"},
        {"user_id": "u4",  "name": "Dave",   "referred_by": "u2"},
        {"user_id": "u5",  "name": "Eve",    "referred_by": "u2"},
        {"user_id": "u6",  "name": "Frank",  "referred_by": "u3"},
        {"user_id": "u7",  "name": "Grace",  "referred_by": None},
        {"user_id": "u8",  "name": "Heidi",  "referred_by": "u7"},
        {"user_id": "u9",  "name": "Ivan",   "referred_by": "u4"},
        {"user_id": "u10", "name": "Judy",   "referred_by": "u1"},
    ]
    result = build_referral_list(users)
    assert len(result) == 8
    assert result[0] == {"user_id": "u10", "name": "Judy", "referrer_name": "Alice"}
    bob_row = next(r for r in result if r["user_id"] == "u4")
    assert bob_row["referrer_name"] == "Bob"
    print("032 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 033: Merge Two Event Streams and Sort by Timestamp
Difficulty: Easy
Category: Joins & Dataset Merging
Tags: merge-sort, event-streams, union, pipeline

Scenario:
Two microservices emit events to separate Kafka topics. An analytics job
must merge both streams into a single chronologically ordered stream for
session reconstruction.

Input:
stream_a: list of dicts — event_id (str), source (str), ts (int epoch_ms)
stream_b: list of dicts — event_id (str), source (str), ts (int epoch_ms)

Sample Input:
stream_a = [
    {"event_id": "a1", "source": "checkout", "ts": 1000},
    {"event_id": "a2", "source": "checkout", "ts": 1050},
    {"event_id": "a3", "source": "checkout", "ts": 1200},
    {"event_id": "a4", "source": "checkout", "ts": 1400},
    {"event_id": "a5", "source": "checkout", "ts": 1600},
]
stream_b = [
    {"event_id": "b1", "source": "inventory", "ts": 980},
    {"event_id": "b2", "source": "inventory", "ts": 1020},
    {"event_id": "b3", "source": "inventory", "ts": 1100},
    {"event_id": "b4", "source": "inventory", "ts": 1350},
    {"event_id": "b5", "source": "inventory", "ts": 1500},
]

Expected Output (10 events sorted by ts):
event_ids in order: b1, a1, b2, a2, b3, a3, b4, a4, b5, a5

Follow-up: Handle tie-breaking by source name alphabetically when ts is equal.
"""

import heapq


def merge_event_streams(stream_a, stream_b):
    combined = stream_a + stream_b
    return sorted(combined, key=lambda e: e["ts"])


def test_033():
    stream_a = [
        {"event_id": "a1", "source": "checkout",  "ts": 1000},
        {"event_id": "a2", "source": "checkout",  "ts": 1050},
        {"event_id": "a3", "source": "checkout",  "ts": 1200},
        {"event_id": "a4", "source": "checkout",  "ts": 1400},
        {"event_id": "a5", "source": "checkout",  "ts": 1600},
    ]
    stream_b = [
        {"event_id": "b1", "source": "inventory", "ts": 980},
        {"event_id": "b2", "source": "inventory", "ts": 1020},
        {"event_id": "b3", "source": "inventory", "ts": 1100},
        {"event_id": "b4", "source": "inventory", "ts": 1350},
        {"event_id": "b5", "source": "inventory", "ts": 1500},
    ]
    result = merge_event_streams(stream_a, stream_b)
    assert len(result) == 10
    assert [r["event_id"] for r in result] == ["b1", "a1", "b2", "a2", "b3", "a3", "b4", "a4", "b5", "a5"]
    print("033 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 034: Join Ad Clicks to Impressions (attribution)
Difficulty: Medium
Category: Joins & Dataset Merging
Tags: join, attribution, ad-tech, click-through-rate

Scenario:
An ad-attribution pipeline must link each click event to the impression
that preceded it. For each click, find the most recent impression for the
same (user_id, ad_id) that occurred before the click's timestamp.

Input:
impressions: list of dicts — imp_id (str), user_id (str), ad_id (str), ts (int)
clicks: list of dicts — click_id (str), user_id (str), ad_id (str), ts (int)
Both sorted by ts ascending.

Sample Input:
impressions = [
    {"imp_id": "i1", "user_id": "u1", "ad_id": "a1", "ts": 100},
    {"imp_id": "i2", "user_id": "u1", "ad_id": "a1", "ts": 200},
    {"imp_id": "i3", "user_id": "u2", "ad_id": "a1", "ts": 150},
    {"imp_id": "i4", "user_id": "u1", "ad_id": "a2", "ts": 180},
    {"imp_id": "i5", "user_id": "u3", "ad_id": "a1", "ts": 300},
    {"imp_id": "i6", "user_id": "u2", "ad_id": "a2", "ts": 400},
]
clicks = [
    {"click_id": "c1", "user_id": "u1", "ad_id": "a1", "ts": 210},  # matches i2
    {"click_id": "c2", "user_id": "u2", "ad_id": "a1", "ts": 160},  # matches i3
    {"click_id": "c3", "user_id": "u1", "ad_id": "a2", "ts": 250},  # matches i4
    {"click_id": "c4", "user_id": "u3", "ad_id": "a1", "ts": 350},  # matches i5
    {"click_id": "c5", "user_id": "u4", "ad_id": "a1", "ts": 500},  # no impression
]

Expected Output:
[
  {"click_id": "c1", "attributed_imp_id": "i2"},
  {"click_id": "c2", "attributed_imp_id": "i3"},
  {"click_id": "c3", "attributed_imp_id": "i4"},
  {"click_id": "c4", "attributed_imp_id": "i5"},
  {"click_id": "c5", "attributed_imp_id": None},
]

Follow-up: Only attribute if the click occurred within 300 seconds of the impression.
"""


def attribute_clicks_to_impressions(impressions, clicks):
    # Group impressions by (user_id, ad_id), sorted by ts
    imp_by_key = defaultdict(list)
    for imp in impressions:
        imp_by_key[(imp["user_id"], imp["ad_id"])].append(imp)
    for v in imp_by_key.values():
        v.sort(key=lambda x: x["ts"])

    def find_last_impression_before(key, click_ts):
        candidates = imp_by_key.get(key, [])
        best = None
        for imp in candidates:
            if imp["ts"] < click_ts:
                best = imp["imp_id"]
            else:
                break
        return best

    result = []
    for click in clicks:
        key = (click["user_id"], click["ad_id"])
        attr = find_last_impression_before(key, click["ts"])
        result.append({"click_id": click["click_id"], "attributed_imp_id": attr})
    return result


def test_034():
    impressions = [
        {"imp_id": "i1", "user_id": "u1", "ad_id": "a1", "ts": 100},
        {"imp_id": "i2", "user_id": "u1", "ad_id": "a1", "ts": 200},
        {"imp_id": "i3", "user_id": "u2", "ad_id": "a1", "ts": 150},
        {"imp_id": "i4", "user_id": "u1", "ad_id": "a2", "ts": 180},
        {"imp_id": "i5", "user_id": "u3", "ad_id": "a1", "ts": 300},
        {"imp_id": "i6", "user_id": "u2", "ad_id": "a2", "ts": 400},
    ]
    clicks = [
        {"click_id": "c1", "user_id": "u1", "ad_id": "a1", "ts": 210},
        {"click_id": "c2", "user_id": "u2", "ad_id": "a1", "ts": 160},
        {"click_id": "c3", "user_id": "u1", "ad_id": "a2", "ts": 250},
        {"click_id": "c4", "user_id": "u3", "ad_id": "a1", "ts": 350},
        {"click_id": "c5", "user_id": "u4", "ad_id": "a1", "ts": 500},
    ]
    result = attribute_clicks_to_impressions(impressions, clicks)
    by_click = {r["click_id"]: r["attributed_imp_id"] for r in result}
    assert by_click["c1"] == "i2"
    assert by_click["c2"] == "i3"
    assert by_click["c3"] == "i4"
    assert by_click["c4"] == "i5"
    assert by_click["c5"] is None
    print("034 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 035: Full Outer Join — Reconcile Two Pipeline Datasets
Difficulty: Medium
Category: Joins & Dataset Merging
Tags: full-outer-join, reconciliation, data-quality, pipeline

Scenario:
Two teams independently produce daily revenue summaries for the same
date range. A reconciliation job must full-outer-join them to find
discrepancies: dates present in only one dataset or with mismatched amounts.

Input:
team_a: list of dicts — date (str), revenue_cents (int)
team_b: list of dicts — date (str), revenue_cents (int)

Sample Input:
team_a = [
    {"date": "2024-03-01", "revenue_cents": 10000},
    {"date": "2024-03-02", "revenue_cents": 12000},
    {"date": "2024-03-03", "revenue_cents": 11500},
    {"date": "2024-03-04", "revenue_cents": 9800},
    {"date": "2024-03-05", "revenue_cents": 14000},
    {"date": "2024-03-07", "revenue_cents": 8000},   # only in A
]
team_b = [
    {"date": "2024-03-01", "revenue_cents": 10000},  # match
    {"date": "2024-03-02", "revenue_cents": 12500},  # mismatch
    {"date": "2024-03-03", "revenue_cents": 11500},  # match
    {"date": "2024-03-04", "revenue_cents": 9800},   # match
    {"date": "2024-03-05", "revenue_cents": 13000},  # mismatch
    {"date": "2024-03-06", "revenue_cents": 7500},   # only in B
]

Expected Output:
[
  {"date": "2024-03-02", "status": "mismatch", "a": 12000, "b": 12500},
  {"date": "2024-03-05", "status": "mismatch", "a": 14000, "b": 13000},
  {"date": "2024-03-06", "status": "only_b",   "a": None,  "b": 7500},
  {"date": "2024-03-07", "status": "only_a",   "a": 8000,  "b": None},
]

Follow-up: Return the total absolute discrepancy across all mismatch rows.
"""


def reconcile_datasets(team_a, team_b):
    a_map = {r["date"]: r["revenue_cents"] for r in team_a}
    b_map = {r["date"]: r["revenue_cents"] for r in team_b}
    all_dates = sorted(set(a_map) | set(b_map))
    result = []
    for d in all_dates:
        a_val = a_map.get(d)
        b_val = b_map.get(d)
        if a_val is not None and b_val is not None:
            if a_val != b_val:
                result.append({"date": d, "status": "mismatch", "a": a_val, "b": b_val})
        elif a_val is None:
            result.append({"date": d, "status": "only_b", "a": None, "b": b_val})
        else:
            result.append({"date": d, "status": "only_a", "a": a_val, "b": None})
    return result


def test_035():
    team_a = [
        {"date": "2024-03-01", "revenue_cents": 10000},
        {"date": "2024-03-02", "revenue_cents": 12000},
        {"date": "2024-03-03", "revenue_cents": 11500},
        {"date": "2024-03-04", "revenue_cents": 9800},
        {"date": "2024-03-05", "revenue_cents": 14000},
        {"date": "2024-03-07", "revenue_cents": 8000},
    ]
    team_b = [
        {"date": "2024-03-01", "revenue_cents": 10000},
        {"date": "2024-03-02", "revenue_cents": 12500},
        {"date": "2024-03-03", "revenue_cents": 11500},
        {"date": "2024-03-04", "revenue_cents": 9800},
        {"date": "2024-03-05", "revenue_cents": 13000},
        {"date": "2024-03-06", "revenue_cents": 7500},
    ]
    result = reconcile_datasets(team_a, team_b)
    statuses = {r["date"]: r["status"] for r in result}
    assert statuses["2024-03-02"] == "mismatch"
    assert statuses["2024-03-05"] == "mismatch"
    assert statuses["2024-03-06"] == "only_b"
    assert statuses["2024-03-07"] == "only_a"
    assert len(result) == 4
    print("035 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 036: Interval Join — Match Safety Alerts to Active Sessions
Difficulty: Medium
Category: Joins & Dataset Merging
Tags: interval-join, range-join, safety-signals, session-matching

Scenario:
A Safeguards pipeline receives safety alerts with timestamps and must
determine which user session was active at the time of each alert. A
session is active between its start_ts and end_ts (inclusive). Return
each alert with the matching session_id (None if no active session found).

Input:
alerts: list of dicts — alert_id (str), user_id (str), ts (int)
sessions: list of dicts — session_id (str), user_id (str),
           start_ts (int), end_ts (int)

Sample Input:
alerts = [
    {"alert_id": "al1", "user_id": "u1", "ts": 150},
    {"alert_id": "al2", "user_id": "u1", "ts": 350},
    {"alert_id": "al3", "user_id": "u2", "ts": 200},
    {"alert_id": "al4", "user_id": "u2", "ts": 700},  # no session
    {"alert_id": "al5", "user_id": "u3", "ts": 500},
    {"alert_id": "al6", "user_id": "u1", "ts": 600},
    {"alert_id": "al7", "user_id": "u4", "ts": 100},  # no session (no u4 sessions)
]
sessions = [
    {"session_id": "ss1", "user_id": "u1", "start_ts": 100, "end_ts": 300},
    {"session_id": "ss2", "user_id": "u1", "start_ts": 400, "end_ts": 700},
    {"session_id": "ss3", "user_id": "u2", "start_ts": 150, "end_ts": 400},
    {"session_id": "ss4", "user_id": "u3", "start_ts": 450, "end_ts": 600},
    {"session_id": "ss5", "user_id": "u1", "start_ts": 800, "end_ts": 900},
]

Expected Output:
[
  {"alert_id": "al1", "matched_session": "ss1"},
  {"alert_id": "al2", "matched_session": "ss1"},
  {"alert_id": "al3", "matched_session": "ss3"},
  {"alert_id": "al4", "matched_session": None},
  {"alert_id": "al5", "matched_session": "ss4"},
  {"alert_id": "al6", "matched_session": "ss2"},
  {"alert_id": "al7", "matched_session": None},
]

Follow-up: Handle overlapping sessions by returning all matching session_ids.
"""


def match_alerts_to_sessions(alerts, sessions):
    user_sessions = defaultdict(list)
    for s in sessions:
        user_sessions[s["user_id"]].append(s)

    result = []
    for alert in alerts:
        matched = None
        for sess in user_sessions.get(alert["user_id"], []):
            if sess["start_ts"] <= alert["ts"] <= sess["end_ts"]:
                matched = sess["session_id"]
                break
        result.append({"alert_id": alert["alert_id"], "matched_session": matched})
    return result


def test_036():
    alerts = [
        {"alert_id": "al1", "user_id": "u1", "ts": 150},
        {"alert_id": "al2", "user_id": "u1", "ts": 350},
        {"alert_id": "al3", "user_id": "u2", "ts": 200},
        {"alert_id": "al4", "user_id": "u2", "ts": 700},
        {"alert_id": "al5", "user_id": "u3", "ts": 500},
        {"alert_id": "al6", "user_id": "u1", "ts": 600},
        {"alert_id": "al7", "user_id": "u4", "ts": 100},
    ]
    sessions = [
        {"session_id": "ss1", "user_id": "u1", "start_ts": 100, "end_ts": 300},
        {"session_id": "ss2", "user_id": "u1", "start_ts": 400, "end_ts": 700},
        {"session_id": "ss3", "user_id": "u2", "start_ts": 150, "end_ts": 400},
        {"session_id": "ss4", "user_id": "u3", "start_ts": 450, "end_ts": 600},
        {"session_id": "ss5", "user_id": "u1", "start_ts": 800, "end_ts": 900},
    ]
    result = match_alerts_to_sessions(alerts, sessions)
    by_alert = {r["alert_id"]: r["matched_session"] for r in result}
    assert by_alert["al1"] == "ss1"
    assert by_alert["al2"] == "ss1"
    assert by_alert["al3"] == "ss3"
    assert by_alert["al4"] is None
    assert by_alert["al5"] == "ss4"
    assert by_alert["al6"] == "ss2"
    assert by_alert["al7"] is None
    print("036 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 037: Enrich Transactions with Tiered Discount (range lookup join)
Difficulty: Medium
Category: Joins & Dataset Merging
Tags: range-join, lookup, transactions, business-rules

Scenario:
A payment pipeline applies tiered discounts based on a user's lifetime
spend bracket. Given a discount_tiers table (each tier defines a spend
range and discount_pct), enrich each user record with the correct
discount percentage by range-joining on lifetime_spend_cents.

Input:
users: list of dicts — user_id (str), lifetime_spend_cents (int)
discount_tiers: list of dicts — tier (str), min_cents (int),
                max_cents (int or None), discount_pct (float)
  (None max means unbounded upper end)

Sample Input:
users = [
    {"user_id": "u1", "lifetime_spend_cents": 500},
    {"user_id": "u2", "lifetime_spend_cents": 5000},
    {"user_id": "u3", "lifetime_spend_cents": 15000},
    {"user_id": "u4", "lifetime_spend_cents": 0},
    {"user_id": "u5", "lifetime_spend_cents": 9999},
    {"user_id": "u6", "lifetime_spend_cents": 50000},
    {"user_id": "u7", "lifetime_spend_cents": 1000},
    {"user_id": "u8", "lifetime_spend_cents": 10000},
]
discount_tiers = [
    {"tier": "bronze", "min_cents": 0,     "max_cents": 999,   "discount_pct": 0.0},
    {"tier": "silver", "min_cents": 1000,  "max_cents": 4999,  "discount_pct": 5.0},
    {"tier": "gold",   "min_cents": 5000,  "max_cents": 9999,  "discount_pct": 10.0},
    {"tier": "plat",   "min_cents": 10000, "max_cents": None,  "discount_pct": 15.0},
]

Expected Output:
{
  "u1": {"tier": "bronze", "discount_pct": 0.0},
  "u2": {"tier": "gold",   "discount_pct": 10.0},
  "u3": {"tier": "plat",   "discount_pct": 15.0},
  "u4": {"tier": "bronze", "discount_pct": 0.0},
  "u5": {"tier": "gold",   "discount_pct": 10.0},
  "u6": {"tier": "plat",   "discount_pct": 15.0},
  "u7": {"tier": "silver", "discount_pct": 5.0},
  "u8": {"tier": "plat",   "discount_pct": 15.0},
}

Follow-up: Return users who just entered a new tier (their previous spend was in the tier below).
"""


def enrich_with_discount_tier(users, discount_tiers):
    def find_tier(spend):
        for t in discount_tiers:
            max_ok = t["max_cents"] is None or spend <= t["max_cents"]
            if t["min_cents"] <= spend and max_ok:
                return t
        return None

    result = {}
    for u in users:
        tier = find_tier(u["lifetime_spend_cents"])
        result[u["user_id"]] = {
            "tier": tier["tier"],
            "discount_pct": tier["discount_pct"],
        } if tier else {"tier": None, "discount_pct": None}
    return result


def test_037():
    users = [
        {"user_id": "u1", "lifetime_spend_cents": 500},
        {"user_id": "u2", "lifetime_spend_cents": 5000},
        {"user_id": "u3", "lifetime_spend_cents": 15000},
        {"user_id": "u4", "lifetime_spend_cents": 0},
        {"user_id": "u5", "lifetime_spend_cents": 9999},
        {"user_id": "u6", "lifetime_spend_cents": 50000},
        {"user_id": "u7", "lifetime_spend_cents": 1000},
        {"user_id": "u8", "lifetime_spend_cents": 10000},
    ]
    discount_tiers = [
        {"tier": "bronze", "min_cents": 0,     "max_cents": 999,  "discount_pct": 0.0},
        {"tier": "silver", "min_cents": 1000,  "max_cents": 4999, "discount_pct": 5.0},
        {"tier": "gold",   "min_cents": 5000,  "max_cents": 9999, "discount_pct": 10.0},
        {"tier": "plat",   "min_cents": 10000, "max_cents": None, "discount_pct": 15.0},
    ]
    result = enrich_with_discount_tier(users, discount_tiers)
    assert result["u1"] == {"tier": "bronze", "discount_pct": 0.0}
    assert result["u2"] == {"tier": "gold",   "discount_pct": 10.0}
    assert result["u3"] == {"tier": "plat",   "discount_pct": 15.0}
    assert result["u7"] == {"tier": "silver", "discount_pct": 5.0}
    assert result["u8"] == {"tier": "plat",   "discount_pct": 15.0}
    print("037 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 038: Semi-Join — Filter Products That Have at Least One Review
Difficulty: Easy
Category: Joins & Dataset Merging
Tags: semi-join, filter, content-catalog, reviews

Scenario:
A content-catalog pipeline wants to return only products that have at
least one customer review (to display on a "reviewed" shelf). Do not
return products without reviews.

Input:
products: list of dicts — product_id (str), name (str), category (str)
reviews: list of dicts — review_id (str), product_id (str), rating (int)

Sample Input:
products = [
    {"product_id": "p1", "name": "Widget A",   "category": "tools"},
    {"product_id": "p2", "name": "Widget B",   "category": "tools"},
    {"product_id": "p3", "name": "Gadget C",   "category": "electronics"},
    {"product_id": "p4", "name": "Gadget D",   "category": "electronics"},
    {"product_id": "p5", "name": "Toy E",      "category": "toys"},
    {"product_id": "p6", "name": "Toy F",      "category": "toys"},
    {"product_id": "p7", "name": "Book G",     "category": "books"},
    {"product_id": "p8", "name": "Book H",     "category": "books"},
]
reviews = [
    {"review_id": "rv1", "product_id": "p1", "rating": 4},
    {"review_id": "rv2", "product_id": "p1", "rating": 5},
    {"review_id": "rv3", "product_id": "p3", "rating": 3},
    {"review_id": "rv4", "product_id": "p5", "rating": 4},
    {"review_id": "rv5", "product_id": "p7", "rating": 2},
    {"review_id": "rv6", "product_id": "p7", "rating": 5},
]

Expected Output (product_ids with at least 1 review, sorted):
["p1", "p3", "p5", "p7"]

Follow-up: Return products with an average rating >= 4.0.
"""


def products_with_reviews(products, reviews):
    reviewed_ids = {r["product_id"] for r in reviews}
    return sorted(p["product_id"] for p in products if p["product_id"] in reviewed_ids)


def test_038():
    products = [
        {"product_id": "p1", "name": "Widget A",  "category": "tools"},
        {"product_id": "p2", "name": "Widget B",  "category": "tools"},
        {"product_id": "p3", "name": "Gadget C",  "category": "electronics"},
        {"product_id": "p4", "name": "Gadget D",  "category": "electronics"},
        {"product_id": "p5", "name": "Toy E",     "category": "toys"},
        {"product_id": "p6", "name": "Toy F",     "category": "toys"},
        {"product_id": "p7", "name": "Book G",    "category": "books"},
        {"product_id": "p8", "name": "Book H",    "category": "books"},
    ]
    reviews = [
        {"review_id": "rv1", "product_id": "p1", "rating": 4},
        {"review_id": "rv2", "product_id": "p1", "rating": 5},
        {"review_id": "rv3", "product_id": "p3", "rating": 3},
        {"review_id": "rv4", "product_id": "p5", "rating": 4},
        {"review_id": "rv5", "product_id": "p7", "rating": 2},
        {"review_id": "rv6", "product_id": "p7", "rating": 5},
    ]
    result = products_with_reviews(products, reviews)
    assert result == ["p1", "p3", "p5", "p7"]
    print("038 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 039: Cross-Dataset Consistency Check — Join Logs to Config
Difficulty: Medium
Category: Joins & Dataset Merging
Tags: join, data-quality, config-validation, pipeline

Scenario:
A pipeline config table defines the expected set of data sources and their
expected record counts per batch. An actual log table records what was
received. Join them to produce a per-source status: OK (within 10%
tolerance), UNDER, OVER, or MISSING (no log entry).

Input:
config: list of dicts — source_id (str), expected_count (int)
logs: list of dicts — source_id (str), actual_count (int), batch_id (str)

Sample Input:
config = [
    {"source_id": "src_a", "expected_count": 1000},
    {"source_id": "src_b", "expected_count": 500},
    {"source_id": "src_c", "expected_count": 2000},
    {"source_id": "src_d", "expected_count": 750},
    {"source_id": "src_e", "expected_count": 300},
]
logs = [
    {"source_id": "src_a", "actual_count": 1020, "batch_id": "b1"},  # within 10% → OK
    {"source_id": "src_b", "actual_count": 400,  "batch_id": "b1"},  # 20% under → UNDER
    {"source_id": "src_c", "actual_count": 2300, "batch_id": "b1"},  # 15% over  → OVER
    {"source_id": "src_d", "actual_count": 748,  "batch_id": "b1"},  # within 10% → OK
    # src_e missing from logs
]

Expected Output:
{
  "src_a": "OK",
  "src_b": "UNDER",
  "src_c": "OVER",
  "src_d": "OK",
  "src_e": "MISSING",
}

Follow-up: Return the deviation percentage for each non-OK source.
"""


def check_pipeline_consistency(config, logs, tolerance=0.10):
    log_map = {l["source_id"]: l["actual_count"] for l in logs}
    result = {}
    for c in config:
        sid = c["source_id"]
        expected = c["expected_count"]
        if sid not in log_map:
            result[sid] = "MISSING"
            continue
        actual = log_map[sid]
        deviation = (actual - expected) / expected
        if abs(deviation) <= tolerance:
            result[sid] = "OK"
        elif deviation > tolerance:
            result[sid] = "OVER"
        else:
            result[sid] = "UNDER"
    return result


def test_039():
    config = [
        {"source_id": "src_a", "expected_count": 1000},
        {"source_id": "src_b", "expected_count": 500},
        {"source_id": "src_c", "expected_count": 2000},
        {"source_id": "src_d", "expected_count": 750},
        {"source_id": "src_e", "expected_count": 300},
    ]
    logs = [
        {"source_id": "src_a", "actual_count": 1020, "batch_id": "b1"},
        {"source_id": "src_b", "actual_count": 400,  "batch_id": "b1"},
        {"source_id": "src_c", "actual_count": 2300, "batch_id": "b1"},
        {"source_id": "src_d", "actual_count": 748,  "batch_id": "b1"},
    ]
    result = check_pipeline_consistency(config, logs)
    assert result == {"src_a": "OK", "src_b": "UNDER", "src_c": "OVER", "src_d": "OK", "src_e": "MISSING"}
    print("039 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 040: Broadcast Join — Enrich a Large Event Stream with a Small Lookup Table
Difficulty: Medium
Category: Joins & Dataset Merging
Tags: broadcast-join, lookup, enrichment, scalability

Scenario:
In a distributed pipeline, a small country-code lookup table (ISO code
→ region, currency) is broadcast to all workers to enrich a large stream
of transaction events. Simulate this pattern: build the lookup once, then
apply it to every event. Return events with region and currency added;
unknown country codes get region="UNKNOWN" and currency="XXX".

Input:
events: list of dicts — tx_id (str), user_id (str), country_code (str),
                         amount_cents (int)
country_lookup: list of dicts — code (str), region (str), currency (str)

Sample Input:
events = [
    {"tx_id": "tx1",  "user_id": "u1", "country_code": "US", "amount_cents": 1000},
    {"tx_id": "tx2",  "user_id": "u2", "country_code": "GB", "amount_cents": 500},
    {"tx_id": "tx3",  "user_id": "u3", "country_code": "DE", "amount_cents": 800},
    {"tx_id": "tx4",  "user_id": "u4", "country_code": "ZZ", "amount_cents": 200},  # unknown
    {"tx_id": "tx5",  "user_id": "u5", "country_code": "JP", "amount_cents": 1500},
    {"tx_id": "tx6",  "user_id": "u1", "country_code": "US", "amount_cents": 300},
    {"tx_id": "tx7",  "user_id": "u6", "country_code": "AU", "amount_cents": 700},
    {"tx_id": "tx8",  "user_id": "u7", "country_code": "XX", "amount_cents": 100},  # unknown
    {"tx_id": "tx9",  "user_id": "u3", "country_code": "DE", "amount_cents": 950},
    {"tx_id": "tx10", "user_id": "u2", "country_code": "GB", "amount_cents": 420},
]
country_lookup = [
    {"code": "US", "region": "North America", "currency": "USD"},
    {"code": "GB", "region": "Europe",        "currency": "GBP"},
    {"code": "DE", "region": "Europe",        "currency": "EUR"},
    {"code": "JP", "region": "Asia",          "currency": "JPY"},
    {"code": "AU", "region": "Oceania",       "currency": "AUD"},
]

Expected Output (10 enriched events):
tx4 and tx8 get region="UNKNOWN", currency="XXX"
all others get correct region and currency

Follow-up: Group total amount_cents by region after enrichment.
"""


def broadcast_enrich_events(events, country_lookup):
    # Broadcast: build lookup once
    lookup = {c["code"]: c for c in country_lookup}
    result = []
    for e in events:
        enriched = dict(e)
        meta = lookup.get(e["country_code"])
        if meta:
            enriched["region"] = meta["region"]
            enriched["currency"] = meta["currency"]
        else:
            enriched["region"] = "UNKNOWN"
            enriched["currency"] = "XXX"
        result.append(enriched)
    return result


def test_040():
    events = [
        {"tx_id": "tx1",  "user_id": "u1", "country_code": "US", "amount_cents": 1000},
        {"tx_id": "tx2",  "user_id": "u2", "country_code": "GB", "amount_cents": 500},
        {"tx_id": "tx3",  "user_id": "u3", "country_code": "DE", "amount_cents": 800},
        {"tx_id": "tx4",  "user_id": "u4", "country_code": "ZZ", "amount_cents": 200},
        {"tx_id": "tx5",  "user_id": "u5", "country_code": "JP", "amount_cents": 1500},
        {"tx_id": "tx6",  "user_id": "u1", "country_code": "US", "amount_cents": 300},
        {"tx_id": "tx7",  "user_id": "u6", "country_code": "AU", "amount_cents": 700},
        {"tx_id": "tx8",  "user_id": "u7", "country_code": "XX", "amount_cents": 100},
        {"tx_id": "tx9",  "user_id": "u3", "country_code": "DE", "amount_cents": 950},
        {"tx_id": "tx10", "user_id": "u2", "country_code": "GB", "amount_cents": 420},
    ]
    country_lookup = [
        {"code": "US", "region": "North America", "currency": "USD"},
        {"code": "GB", "region": "Europe",        "currency": "GBP"},
        {"code": "DE", "region": "Europe",        "currency": "EUR"},
        {"code": "JP", "region": "Asia",          "currency": "JPY"},
        {"code": "AU", "region": "Oceania",       "currency": "AUD"},
    ]
    result = broadcast_enrich_events(events, country_lookup)
    assert len(result) == 10
    unknown_txs = [r for r in result if r["region"] == "UNKNOWN"]
    assert {r["tx_id"] for r in unknown_txs} == {"tx4", "tx8"}
    us_tx = next(r for r in result if r["tx_id"] == "tx1")
    assert us_tx["currency"] == "USD"
    print("040 PASS")


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    test_028()
    test_029()
    test_030()
    test_031()
    test_032()
    test_033()
    test_034()
    test_035()
    test_036()
    test_037()
    test_038()
    test_039()
    test_040()
    print("\nAll Category 3 tests passed.")
