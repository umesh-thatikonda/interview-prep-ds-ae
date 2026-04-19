"""
============================================================
CATEGORY 2: DEDUPLICATION & UNIQUENESS  (Problems 016 – 027)
============================================================
All solutions use only Python stdlib:
  dict, list, set, collections (defaultdict, OrderedDict),
  itertools — NO pandas, NO numpy.
"""

from collections import defaultdict


# ─────────────────────────────────────────────────────────────
"""
Problem 016: Deduplicate Kafka Events by Event ID
Difficulty: Easy
Category: Deduplication & Uniqueness
Tags: deduplication, event-id, kafka, idempotency

Scenario:
A Kafka consumer reads user-activity events. Due to at-least-once delivery,
some event_ids appear multiple times. Deduplicate the stream by keeping
only the first occurrence of each event_id (preserve original order).

Input:
List of dicts: event_id (str), user_id (str), action (str), ts (str)

Sample Input:
events = [
    {"event_id": "e1",  "user_id": "u1", "action": "click",    "ts": "2024-07-01T10:00"},
    {"event_id": "e2",  "user_id": "u2", "action": "view",     "ts": "2024-07-01T10:01"},
    {"event_id": "e1",  "user_id": "u1", "action": "click",    "ts": "2024-07-01T10:00"},  # dup
    {"event_id": "e3",  "user_id": "u3", "action": "purchase", "ts": "2024-07-01T10:02"},
    {"event_id": "e2",  "user_id": "u2", "action": "view",     "ts": "2024-07-01T10:01"},  # dup
    {"event_id": "e4",  "user_id": "u1", "action": "logout",   "ts": "2024-07-01T10:03"},
    {"event_id": "e5",  "user_id": "u4", "action": "login",    "ts": "2024-07-01T10:04"},
    {"event_id": "e3",  "user_id": "u3", "action": "purchase", "ts": "2024-07-01T10:02"},  # dup
    {"event_id": "e6",  "user_id": "u5", "action": "click",    "ts": "2024-07-01T10:05"},
    {"event_id": "e5",  "user_id": "u4", "action": "login",    "ts": "2024-07-01T10:04"},  # dup
]

Expected Output (6 unique events in original order):
[
  {"event_id": "e1", ...},
  {"event_id": "e2", ...},
  {"event_id": "e3", ...},
  {"event_id": "e4", ...},
  {"event_id": "e5", ...},
  {"event_id": "e6", ...},
]

Follow-up: Instead of keeping the first occurrence, keep the LAST occurrence
(useful when later records are corrections).
"""


def deduplicate_by_event_id(events):
    seen = set()
    result = []
    for e in events:
        if e["event_id"] not in seen:
            seen.add(e["event_id"])
            result.append(e)
    return result


def test_016():
    events = [
        {"event_id": "e1", "user_id": "u1", "action": "click",    "ts": "2024-07-01T10:00"},
        {"event_id": "e2", "user_id": "u2", "action": "view",     "ts": "2024-07-01T10:01"},
        {"event_id": "e1", "user_id": "u1", "action": "click",    "ts": "2024-07-01T10:00"},
        {"event_id": "e3", "user_id": "u3", "action": "purchase", "ts": "2024-07-01T10:02"},
        {"event_id": "e2", "user_id": "u2", "action": "view",     "ts": "2024-07-01T10:01"},
        {"event_id": "e4", "user_id": "u1", "action": "logout",   "ts": "2024-07-01T10:03"},
        {"event_id": "e5", "user_id": "u4", "action": "login",    "ts": "2024-07-01T10:04"},
        {"event_id": "e3", "user_id": "u3", "action": "purchase", "ts": "2024-07-01T10:02"},
        {"event_id": "e6", "user_id": "u5", "action": "click",    "ts": "2024-07-01T10:05"},
        {"event_id": "e5", "user_id": "u4", "action": "login",    "ts": "2024-07-01T10:04"},
    ]
    result = deduplicate_by_event_id(events)
    assert len(result) == 6
    assert [e["event_id"] for e in result] == ["e1", "e2", "e3", "e4", "e5", "e6"]
    print("016 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 017: Keep Latest Record per User (CDC deduplication)
Difficulty: Easy
Category: Deduplication & Uniqueness
Tags: deduplication, CDC, latest-record, updated-at

Scenario:
A CDC (change-data-capture) stream delivers multiple versions of user
profile records. Each record has an updated_at timestamp. For each user,
keep only the most recent version to build the current-state user table.

Input:
List of dicts: user_id (str), name (str), email (str), updated_at (str ISO)

Sample Input:
records = [
    {"user_id": "u1", "name": "Alice",    "email": "alice@old.com",  "updated_at": "2024-08-01T09:00"},
    {"user_id": "u2", "name": "Bob",      "email": "bob@co.com",     "updated_at": "2024-08-01T08:00"},
    {"user_id": "u1", "name": "Alice",    "email": "alice@new.com",  "updated_at": "2024-08-02T10:00"},
    {"user_id": "u3", "name": "Carol",    "email": "carol@co.com",   "updated_at": "2024-08-01T07:00"},
    {"user_id": "u2", "name": "Bob",      "email": "bob@new.com",    "updated_at": "2024-08-03T11:00"},
    {"user_id": "u4", "name": "Dave",     "email": "dave@co.com",    "updated_at": "2024-08-01T06:00"},
    {"user_id": "u1", "name": "Alice-V3", "email": "alice@v3.com",   "updated_at": "2024-08-04T12:00"},
    {"user_id": "u3", "name": "Carol",    "email": "carol@new.com",  "updated_at": "2024-08-02T08:00"},
    {"user_id": "u4", "name": "Dave",     "email": "dave@new.com",   "updated_at": "2024-08-05T09:00"},
    {"user_id": "u5", "name": "Eve",      "email": "eve@co.com",     "updated_at": "2024-08-01T05:00"},
]

Expected Output (5 users, latest record each):
{
  "u1": {"name": "Alice-V3", "email": "alice@v3.com",  "updated_at": "2024-08-04T12:00"},
  "u2": {"name": "Bob",      "email": "bob@new.com",   "updated_at": "2024-08-03T11:00"},
  "u3": {"name": "Carol",    "email": "carol@new.com", "updated_at": "2024-08-02T08:00"},
  "u4": {"name": "Dave",     "email": "dave@new.com",  "updated_at": "2024-08-05T09:00"},
  "u5": {"name": "Eve",      "email": "eve@co.com",    "updated_at": "2024-08-01T05:00"},
}

Follow-up: Also track how many versions existed for each user_id.
"""


def keep_latest_per_user(records):
    latest = {}
    for r in records:
        uid = r["user_id"]
        if uid not in latest or r["updated_at"] > latest[uid]["updated_at"]:
            latest[uid] = {k: v for k, v in r.items() if k != "user_id"}
    return latest


def test_017():
    records = [
        {"user_id": "u1", "name": "Alice",    "email": "alice@old.com",  "updated_at": "2024-08-01T09:00"},
        {"user_id": "u2", "name": "Bob",      "email": "bob@co.com",     "updated_at": "2024-08-01T08:00"},
        {"user_id": "u1", "name": "Alice",    "email": "alice@new.com",  "updated_at": "2024-08-02T10:00"},
        {"user_id": "u3", "name": "Carol",    "email": "carol@co.com",   "updated_at": "2024-08-01T07:00"},
        {"user_id": "u2", "name": "Bob",      "email": "bob@new.com",    "updated_at": "2024-08-03T11:00"},
        {"user_id": "u4", "name": "Dave",     "email": "dave@co.com",    "updated_at": "2024-08-01T06:00"},
        {"user_id": "u1", "name": "Alice-V3", "email": "alice@v3.com",   "updated_at": "2024-08-04T12:00"},
        {"user_id": "u3", "name": "Carol",    "email": "carol@new.com",  "updated_at": "2024-08-02T08:00"},
        {"user_id": "u4", "name": "Dave",     "email": "dave@new.com",   "updated_at": "2024-08-05T09:00"},
        {"user_id": "u5", "name": "Eve",      "email": "eve@co.com",     "updated_at": "2024-08-01T05:00"},
    ]
    result = keep_latest_per_user(records)
    assert result["u1"]["name"] == "Alice-V3"
    assert result["u2"]["email"] == "bob@new.com"
    assert result["u3"]["updated_at"] == "2024-08-02T08:00"
    assert len(result) == 5
    print("017 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 018: Deduplicate Impressions — Keep One per User per Ad per Hour
Difficulty: Easy
Category: Deduplication & Uniqueness
Tags: deduplication, composite-key, ad-impressions, frequency-capping

Scenario:
An ad-serving system may fire multiple impression pixels for the same
(user, ad, hour) due to page refreshes. For frequency-capping compliance,
deduplicate to at most one impression per user per ad per hour.

Input:
List of dicts: impression_id (str), user_id (str), ad_id (str), epoch_ts (int)

Sample Input:
impressions = [
    {"impression_id": "i01", "user_id": "u1", "ad_id": "a1", "epoch_ts": 1704067200},
    {"impression_id": "i02", "user_id": "u1", "ad_id": "a1", "epoch_ts": 1704067800},  # same hour, dup
    {"impression_id": "i03", "user_id": "u1", "ad_id": "a2", "epoch_ts": 1704067200},  # diff ad, keep
    {"impression_id": "i04", "user_id": "u2", "ad_id": "a1", "epoch_ts": 1704067200},  # diff user, keep
    {"impression_id": "i05", "user_id": "u1", "ad_id": "a1", "epoch_ts": 1704071000},  # next hour, keep
    {"impression_id": "i06", "user_id": "u2", "ad_id": "a1", "epoch_ts": 1704067500},  # same (u2,a1,hr0), dup
    {"impression_id": "i07", "user_id": "u3", "ad_id": "a1", "epoch_ts": 1704067200},  # new user
    {"impression_id": "i08", "user_id": "u3", "ad_id": "a2", "epoch_ts": 1704067200},  # new user+ad
    {"impression_id": "i09", "user_id": "u2", "ad_id": "a2", "epoch_ts": 1704074400},  # new hour
    {"impression_id": "i10", "user_id": "u1", "ad_id": "a1", "epoch_ts": 1704067300},  # hr0 dup for u1/a1
]

Expected Output (7 unique impressions, first occurrence per composite key):
impression_ids = ["i01", "i03", "i04", "i05", "i07", "i08", "i09"]

Follow-up: Instead of capping at 1 per hour, allow up to N impressions per
user-ad-hour (frequency cap = N).
"""


def dedup_impressions_per_user_ad_hour(impressions):
    seen = set()
    result = []
    for imp in impressions:
        hour = imp["epoch_ts"] // 3600
        key = (imp["user_id"], imp["ad_id"], hour)
        if key not in seen:
            seen.add(key)
            result.append(imp)
    return result


def test_018():
    impressions = [
        {"impression_id": "i01", "user_id": "u1", "ad_id": "a1", "epoch_ts": 1704067200},
        {"impression_id": "i02", "user_id": "u1", "ad_id": "a1", "epoch_ts": 1704067800},
        {"impression_id": "i03", "user_id": "u1", "ad_id": "a2", "epoch_ts": 1704067200},
        {"impression_id": "i04", "user_id": "u2", "ad_id": "a1", "epoch_ts": 1704067200},
        {"impression_id": "i05", "user_id": "u1", "ad_id": "a1", "epoch_ts": 1704071000},
        {"impression_id": "i06", "user_id": "u2", "ad_id": "a1", "epoch_ts": 1704067500},
        {"impression_id": "i07", "user_id": "u3", "ad_id": "a1", "epoch_ts": 1704067200},
        {"impression_id": "i08", "user_id": "u3", "ad_id": "a2", "epoch_ts": 1704067200},
        {"impression_id": "i09", "user_id": "u2", "ad_id": "a2", "epoch_ts": 1704074400},
        {"impression_id": "i10", "user_id": "u1", "ad_id": "a1", "epoch_ts": 1704067300},
    ]
    result = dedup_impressions_per_user_ad_hour(impressions)
    ids = [r["impression_id"] for r in result]
    assert ids == ["i01", "i03", "i04", "i05", "i07", "i08", "i09"]
    print("018 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 019: Find Duplicate Transactions (same amount + user within 60 seconds)
Difficulty: Medium
Category: Deduplication & Uniqueness
Tags: deduplication, time-window, fraud-detection, transactions

Scenario:
A payment processor suspects double-charges when the same user submits
transactions for the same amount within 60 seconds. Return all transaction
groups where a duplicate is detected (keep all, flag the duplicates).

Input:
List of dicts: tx_id (str), user_id (str), amount_cents (int), epoch_ts (int)
Sorted by epoch_ts ascending.

Sample Input:
transactions = [
    {"tx_id": "tx01", "user_id": "u1", "amount_cents": 500,  "epoch_ts": 1000},
    {"tx_id": "tx02", "user_id": "u1", "amount_cents": 500,  "epoch_ts": 1045},  # dup of tx01
    {"tx_id": "tx03", "user_id": "u2", "amount_cents": 1000, "epoch_ts": 1050},
    {"tx_id": "tx04", "user_id": "u1", "amount_cents": 500,  "epoch_ts": 1200},  # >60s from tx01, not dup
    {"tx_id": "tx05", "user_id": "u2", "amount_cents": 1000, "epoch_ts": 1100},  # dup of tx03
    {"tx_id": "tx06", "user_id": "u3", "amount_cents": 250,  "epoch_ts": 2000},
    {"tx_id": "tx07", "user_id": "u3", "amount_cents": 250,  "epoch_ts": 2030},  # dup of tx06
    {"tx_id": "tx08", "user_id": "u3", "amount_cents": 250,  "epoch_ts": 2080},  # dup of tx07 (chain)
    {"tx_id": "tx09", "user_id": "u4", "amount_cents": 800,  "epoch_ts": 3000},
    {"tx_id": "tx10", "user_id": "u4", "amount_cents": 900,  "epoch_ts": 3010},  # diff amount, not dup
]

Expected Output (tx_ids flagged as duplicates, not the originals):
["tx02", "tx05", "tx07", "tx08"]

Follow-up: Instead of marking duplicates, return a list of duplicate groups
where each group is a list of tx_ids.
"""


def find_duplicate_transactions(transactions, window_secs=60):
    # Track last seen tx per (user, amount): (epoch_ts, tx_id)
    last_seen = {}
    duplicates = []
    for tx in transactions:
        key = (tx["user_id"], tx["amount_cents"])
        if key in last_seen:
            prev_ts, _ = last_seen[key]
            if tx["epoch_ts"] - prev_ts <= window_secs:
                duplicates.append(tx["tx_id"])
        last_seen[key] = (tx["epoch_ts"], tx["tx_id"])
    return duplicates


def test_019():
    transactions = [
        {"tx_id": "tx01", "user_id": "u1", "amount_cents": 500,  "epoch_ts": 1000},
        {"tx_id": "tx02", "user_id": "u1", "amount_cents": 500,  "epoch_ts": 1045},
        {"tx_id": "tx03", "user_id": "u2", "amount_cents": 1000, "epoch_ts": 1050},
        {"tx_id": "tx04", "user_id": "u1", "amount_cents": 500,  "epoch_ts": 1200},
        {"tx_id": "tx05", "user_id": "u2", "amount_cents": 1000, "epoch_ts": 1100},
        {"tx_id": "tx06", "user_id": "u3", "amount_cents": 250,  "epoch_ts": 2000},
        {"tx_id": "tx07", "user_id": "u3", "amount_cents": 250,  "epoch_ts": 2030},
        {"tx_id": "tx08", "user_id": "u3", "amount_cents": 250,  "epoch_ts": 2080},
        {"tx_id": "tx09", "user_id": "u4", "amount_cents": 800,  "epoch_ts": 3000},
        {"tx_id": "tx10", "user_id": "u4", "amount_cents": 900,  "epoch_ts": 3010},
    ]
    result = find_duplicate_transactions(transactions)
    assert result == ["tx02", "tx05", "tx07", "tx08"]
    print("019 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 020: Unique Devices per User (device fingerprint dedup)
Difficulty: Easy
Category: Deduplication & Uniqueness
Tags: deduplication, set, device-fingerprinting, security

Scenario:
A security pipeline logs each time a user authenticates from a device
(identified by device_fingerprint). The risk team wants the count of
distinct devices per user to identify account-sharing or hijacking.

Input:
List of dicts: user_id (str), device_fingerprint (str), ts (str)

Sample Input:
auth_events = [
    {"user_id": "u1", "device_fingerprint": "fp_aaa", "ts": "2024-09-01T08:00"},
    {"user_id": "u1", "device_fingerprint": "fp_bbb", "ts": "2024-09-01T09:00"},
    {"user_id": "u1", "device_fingerprint": "fp_aaa", "ts": "2024-09-02T10:00"},  # repeat device
    {"user_id": "u2", "device_fingerprint": "fp_ccc", "ts": "2024-09-01T08:30"},
    {"user_id": "u2", "device_fingerprint": "fp_ddd", "ts": "2024-09-01T09:30"},
    {"user_id": "u2", "device_fingerprint": "fp_eee", "ts": "2024-09-02T10:30"},
    {"user_id": "u3", "device_fingerprint": "fp_fff", "ts": "2024-09-01T07:00"},
    {"user_id": "u3", "device_fingerprint": "fp_fff", "ts": "2024-09-02T08:00"},  # repeat
    {"user_id": "u4", "device_fingerprint": "fp_ggg", "ts": "2024-09-01T11:00"},
    {"user_id": "u1", "device_fingerprint": "fp_hhh", "ts": "2024-09-03T12:00"},  # 3rd device for u1
]

Expected Output:
{"u1": 3, "u2": 3, "u3": 1, "u4": 1}

Follow-up: Flag users with more than 2 distinct devices as high-risk.
"""


def unique_devices_per_user(auth_events):
    devices = defaultdict(set)
    for e in auth_events:
        devices[e["user_id"]].add(e["device_fingerprint"])
    return {uid: len(fps) for uid, fps in devices.items()}


def test_020():
    auth_events = [
        {"user_id": "u1", "device_fingerprint": "fp_aaa", "ts": "2024-09-01T08:00"},
        {"user_id": "u1", "device_fingerprint": "fp_bbb", "ts": "2024-09-01T09:00"},
        {"user_id": "u1", "device_fingerprint": "fp_aaa", "ts": "2024-09-02T10:00"},
        {"user_id": "u2", "device_fingerprint": "fp_ccc", "ts": "2024-09-01T08:30"},
        {"user_id": "u2", "device_fingerprint": "fp_ddd", "ts": "2024-09-01T09:30"},
        {"user_id": "u2", "device_fingerprint": "fp_eee", "ts": "2024-09-02T10:30"},
        {"user_id": "u3", "device_fingerprint": "fp_fff", "ts": "2024-09-01T07:00"},
        {"user_id": "u3", "device_fingerprint": "fp_fff", "ts": "2024-09-02T08:00"},
        {"user_id": "u4", "device_fingerprint": "fp_ggg", "ts": "2024-09-01T11:00"},
        {"user_id": "u1", "device_fingerprint": "fp_hhh", "ts": "2024-09-03T12:00"},
    ]
    result = unique_devices_per_user(auth_events)
    assert result == {"u1": 3, "u2": 3, "u3": 1, "u4": 1}
    print("020 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 021: Remove Redundant Pipeline Records (subset dedup)
Difficulty: Medium
Category: Deduplication & Uniqueness
Tags: deduplication, composite-key, data-quality, pipeline

Scenario:
A data pipeline accidentally emitted records with the same (source_table,
record_id, batch_id). Keep only one record per that composite key; prefer
the record with the highest version number when duplicates exist.

Input:
List of dicts: row_id (str), source_table (str), record_id (str),
               batch_id (str), version (int), payload (str)

Sample Input:
rows = [
    {"row_id": "r1",  "source_table": "orders", "record_id": "o1", "batch_id": "b1", "version": 1, "payload": "v1"},
    {"row_id": "r2",  "source_table": "orders", "record_id": "o1", "batch_id": "b1", "version": 2, "payload": "v2"},
    {"row_id": "r3",  "source_table": "orders", "record_id": "o2", "batch_id": "b1", "version": 1, "payload": "a1"},
    {"row_id": "r4",  "source_table": "users",  "record_id": "u1", "batch_id": "b1", "version": 1, "payload": "x1"},
    {"row_id": "r5",  "source_table": "users",  "record_id": "u1", "batch_id": "b1", "version": 3, "payload": "x3"},
    {"row_id": "r6",  "source_table": "users",  "record_id": "u1", "batch_id": "b1", "version": 2, "payload": "x2"},
    {"row_id": "r7",  "source_table": "orders", "record_id": "o1", "batch_id": "b2", "version": 1, "payload": "b2v1"},
    {"row_id": "r8",  "source_table": "orders", "record_id": "o3", "batch_id": "b1", "version": 1, "payload": "c1"},
    {"row_id": "r9",  "source_table": "users",  "record_id": "u2", "batch_id": "b1", "version": 1, "payload": "y1"},
    {"row_id": "r10", "source_table": "users",  "record_id": "u2", "batch_id": "b1", "version": 1, "payload": "y1"},  # exact dup
]

Expected Output (one record per composite key, highest version wins):
row_ids kept = {"r2", "r3", "r5", "r7", "r8", "r9"}
# r1 < r2 (version); r4,r6 < r5; r10 exact dup of r9 (same version, keep first)

Follow-up: Log which row_ids were dropped and why (lower_version | exact_dup).
"""


def dedup_by_composite_key_highest_version(rows):
    best = {}
    for row in rows:
        key = (row["source_table"], row["record_id"], row["batch_id"])
        if key not in best or row["version"] > best[key]["version"]:
            best[key] = row
    return list(best.values())


def test_021():
    rows = [
        {"row_id": "r1",  "source_table": "orders", "record_id": "o1", "batch_id": "b1", "version": 1, "payload": "v1"},
        {"row_id": "r2",  "source_table": "orders", "record_id": "o1", "batch_id": "b1", "version": 2, "payload": "v2"},
        {"row_id": "r3",  "source_table": "orders", "record_id": "o2", "batch_id": "b1", "version": 1, "payload": "a1"},
        {"row_id": "r4",  "source_table": "users",  "record_id": "u1", "batch_id": "b1", "version": 1, "payload": "x1"},
        {"row_id": "r5",  "source_table": "users",  "record_id": "u1", "batch_id": "b1", "version": 3, "payload": "x3"},
        {"row_id": "r6",  "source_table": "users",  "record_id": "u1", "batch_id": "b1", "version": 2, "payload": "x2"},
        {"row_id": "r7",  "source_table": "orders", "record_id": "o1", "batch_id": "b2", "version": 1, "payload": "b2v1"},
        {"row_id": "r8",  "source_table": "orders", "record_id": "o3", "batch_id": "b1", "version": 1, "payload": "c1"},
        {"row_id": "r9",  "source_table": "users",  "record_id": "u2", "batch_id": "b1", "version": 1, "payload": "y1"},
        {"row_id": "r10", "source_table": "users",  "record_id": "u2", "batch_id": "b1", "version": 1, "payload": "y1"},
    ]
    result = dedup_by_composite_key_highest_version(rows)
    kept_ids = {r["row_id"] for r in result}
    assert kept_ids == {"r2", "r3", "r5", "r7", "r8", "r9"}
    print("021 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 022: Identify Users with Multiple Accounts (email normalisation)
Difficulty: Medium
Category: Deduplication & Uniqueness
Tags: deduplication, normalisation, identity-resolution, fraud

Scenario:
Fraudulent users create multiple accounts using email variations (dots,
plus-addressing in Gmail). Normalise Gmail addresses by removing dots
and stripping plus-suffixes, then group accounts that resolve to the
same canonical email.

Input:
List of dicts: account_id (str), email (str)

Sample Input:
accounts = [
    {"account_id": "a1",  "email": "john.doe@gmail.com"},
    {"account_id": "a2",  "email": "johndoe@gmail.com"},       # same as a1
    {"account_id": "a3",  "email": "john.doe+spam@gmail.com"}, # same as a1
    {"account_id": "a4",  "email": "jane.smith@gmail.com"},
    {"account_id": "a5",  "email": "janesmith@gmail.com"},     # same as a4
    {"account_id": "a6",  "email": "bob@yahoo.com"},           # non-Gmail, no normalisation
    {"account_id": "a7",  "email": "bob+test@yahoo.com"},      # different from a6 (not Gmail)
    {"account_id": "a8",  "email": "alice@gmail.com"},         # unique
    {"account_id": "a9",  "email": "j.o.h.n.doe@gmail.com"},  # same as a1
    {"account_id": "a10", "email": "jane.smith+news@gmail.com"}, # same as a4
]

Expected Output (groups with >1 account):
[
  {"canonical": "johndoe@gmail.com",  "account_ids": ["a1", "a2", "a3", "a9"]},
  {"canonical": "janesmith@gmail.com","account_ids": ["a4", "a5", "a10"]},
]

Follow-up: Extend to also handle Googlemail.com as equivalent to gmail.com.
"""


def find_multi_account_users(accounts):
    def normalise(email):
        local, domain = email.split("@", 1)
        if domain.lower() == "gmail.com":
            local = local.split("+")[0]
            local = local.replace(".", "")
            return f"{local}@{domain.lower()}"
        return email.lower()

    groups = defaultdict(list)
    for acc in accounts:
        canon = normalise(acc["email"])
        groups[canon].append(acc["account_id"])

    result = []
    for canon, ids in sorted(groups.items()):
        if len(ids) > 1:
            result.append({"canonical": canon, "account_ids": sorted(ids)})
    return result


def test_022():
    accounts = [
        {"account_id": "a1",  "email": "john.doe@gmail.com"},
        {"account_id": "a2",  "email": "johndoe@gmail.com"},
        {"account_id": "a3",  "email": "john.doe+spam@gmail.com"},
        {"account_id": "a4",  "email": "jane.smith@gmail.com"},
        {"account_id": "a5",  "email": "janesmith@gmail.com"},
        {"account_id": "a6",  "email": "bob@yahoo.com"},
        {"account_id": "a7",  "email": "bob+test@yahoo.com"},
        {"account_id": "a8",  "email": "alice@gmail.com"},
        {"account_id": "a9",  "email": "j.o.h.n.doe@gmail.com"},
        {"account_id": "a10", "email": "jane.smith+news@gmail.com"},
    ]
    result = find_multi_account_users(accounts)
    assert len(result) == 2
    john_group = next(g for g in result if "a1" in g["account_ids"])
    assert sorted(john_group["account_ids"]) == ["a1", "a2", "a3", "a9"]
    jane_group = next(g for g in result if "a4" in g["account_ids"])
    assert sorted(jane_group["account_ids"]) == ["a10", "a4", "a5"]
    print("022 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 023: Dedup Content Catalog Records by Canonical Title
Difficulty: Medium
Category: Deduplication & Uniqueness
Tags: deduplication, normalisation, content-catalog, string-matching

Scenario:
A streaming platform ingests content metadata from multiple providers.
The same title appears with minor variations (case, extra spaces, trailing
punctuation). Normalise each title (lowercase, strip whitespace and
punctuation) and merge duplicates, keeping the record with the most
complete metadata (highest non-None field count).

Input:
List of dicts: content_id (str), title (str), genre (str or None),
               year (int or None), rating (str or None)

Sample Input:
catalog = [
    {"content_id": "c1",  "title": "The Matrix",    "genre": "sci-fi",  "year": 1999, "rating": "R"},
    {"content_id": "c2",  "title": "the matrix",    "genre": None,      "year": 1999, "rating": "R"},
    {"content_id": "c3",  "title": "The Matrix ",   "genre": "sci-fi",  "year": None, "rating": None},
    {"content_id": "c4",  "title": "Inception",     "genre": "thriller","year": 2010, "rating": "PG-13"},
    {"content_id": "c5",  "title": "inception",     "genre": None,      "year": None, "rating": None},
    {"content_id": "c6",  "title": "Interstellar",  "genre": "sci-fi",  "year": 2014, "rating": "PG-13"},
    {"content_id": "c7",  "title": "interstellar.", "genre": "sci-fi",  "year": 2014, "rating": None},
    {"content_id": "c8",  "title": "Parasite",      "genre": "thriller","year": 2019, "rating": "R"},
    {"content_id": "c9",  "title": "Parasite",      "genre": "thriller","year": 2019, "rating": "R"},  # exact dup
    {"content_id": "c10", "title": "Dune",          "genre": "sci-fi",  "year": 2021, "rating": "PG-13"},
]

Expected Output (canonical titles mapped to kept content_id):
{
  "the matrix":   "c1",   # c1 has 4 non-None fields vs c2(3) vs c3(2)
  "inception":    "c4",   # c4 has 4 vs c5(1)
  "interstellar": "c6",   # c6 has 4 vs c7(3)
  "parasite":     "c8",   # c8 first seen (tie)
  "dune":         "c10",  # unique
}

Follow-up: Merge fields from duplicates to fill in None values in the
kept record (record fusion).
"""

import re


def dedup_catalog_by_title(catalog):
    def normalise_title(title):
        return re.sub(r"[^a-z0-9 ]", "", title.strip().lower()).strip()

    def completeness(record):
        return sum(1 for v in record.values() if v is not None)

    best = {}
    for item in catalog:
        canon = normalise_title(item["title"])
        if canon not in best or completeness(item) > completeness(best[canon]):
            best[canon] = item
    return {canon: rec["content_id"] for canon, rec in best.items()}


def test_023():
    catalog = [
        {"content_id": "c1",  "title": "The Matrix",    "genre": "sci-fi",  "year": 1999, "rating": "R"},
        {"content_id": "c2",  "title": "the matrix",    "genre": None,      "year": 1999, "rating": "R"},
        {"content_id": "c3",  "title": "The Matrix ",   "genre": "sci-fi",  "year": None, "rating": None},
        {"content_id": "c4",  "title": "Inception",     "genre": "thriller","year": 2010, "rating": "PG-13"},
        {"content_id": "c5",  "title": "inception",     "genre": None,      "year": None, "rating": None},
        {"content_id": "c6",  "title": "Interstellar",  "genre": "sci-fi",  "year": 2014, "rating": "PG-13"},
        {"content_id": "c7",  "title": "interstellar.", "genre": "sci-fi",  "year": 2014, "rating": None},
        {"content_id": "c8",  "title": "Parasite",      "genre": "thriller","year": 2019, "rating": "R"},
        {"content_id": "c9",  "title": "Parasite",      "genre": "thriller","year": 2019, "rating": "R"},
        {"content_id": "c10", "title": "Dune",          "genre": "sci-fi",  "year": 2021, "rating": "PG-13"},
    ]
    result = dedup_catalog_by_title(catalog)
    assert result["the matrix"] == "c1"
    assert result["inception"] == "c4"
    assert result["interstellar"] == "c6"
    assert result["parasite"] == "c8"
    assert result["dune"] == "c10"
    print("023 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 024: Stable Dedup of Bike Trip Records with Conflicting Sources
Difficulty: Medium
Category: Deduplication & Uniqueness
Tags: deduplication, source-priority, conflict-resolution, bike-share

Scenario:
Two data sources (operator API and third-party scraper) emit bike trip
records for the same trips. Records from the operator API have higher
priority. When both sources have a record for the same trip_id, keep
the operator version; otherwise keep whichever is available.

Input:
List of dicts: trip_id (str), source (str: "api"|"scraper"),
               duration_secs (int), start_station (str)

Sample Input:
trips = [
    {"trip_id": "t1", "source": "api",     "duration_secs": 900,  "start_station": "A"},
    {"trip_id": "t1", "source": "scraper", "duration_secs": 901,  "start_station": "A"},  # prefer api
    {"trip_id": "t2", "source": "scraper", "duration_secs": 450,  "start_station": "B"},  # no api version
    {"trip_id": "t3", "source": "api",     "duration_secs": 1200, "start_station": "C"},
    {"trip_id": "t4", "source": "scraper", "duration_secs": 300,  "start_station": "D"},
    {"trip_id": "t4", "source": "api",     "duration_secs": 298,  "start_station": "D"},  # prefer api
    {"trip_id": "t5", "source": "api",     "duration_secs": 600,  "start_station": "E"},
    {"trip_id": "t5", "source": "scraper", "duration_secs": 605,  "start_station": "E"},  # prefer api
    {"trip_id": "t6", "source": "scraper", "duration_secs": 750,  "start_station": "F"},
    {"trip_id": "t7", "source": "api",     "duration_secs": 320,  "start_station": "G"},
]

Expected Output (7 unique trips, api preferred):
trip_id -> source kept:
t1: api (900s), t2: scraper (450s), t3: api (1200s),
t4: api (298s), t5: api (600s), t6: scraper (750s), t7: api (320s)

Follow-up: Add a third source "partner" with priority between api and scraper.
"""

SOURCE_PRIORITY = {"api": 0, "scraper": 1}


def dedup_trips_by_source_priority(trips):
    best = {}
    for trip in trips:
        tid = trip["trip_id"]
        if tid not in best:
            best[tid] = trip
        else:
            if SOURCE_PRIORITY[trip["source"]] < SOURCE_PRIORITY[best[tid]["source"]]:
                best[tid] = trip
    return list(best.values())


def test_024():
    trips = [
        {"trip_id": "t1", "source": "api",     "duration_secs": 900,  "start_station": "A"},
        {"trip_id": "t1", "source": "scraper", "duration_secs": 901,  "start_station": "A"},
        {"trip_id": "t2", "source": "scraper", "duration_secs": 450,  "start_station": "B"},
        {"trip_id": "t3", "source": "api",     "duration_secs": 1200, "start_station": "C"},
        {"trip_id": "t4", "source": "scraper", "duration_secs": 300,  "start_station": "D"},
        {"trip_id": "t4", "source": "api",     "duration_secs": 298,  "start_station": "D"},
        {"trip_id": "t5", "source": "api",     "duration_secs": 600,  "start_station": "E"},
        {"trip_id": "t5", "source": "scraper", "duration_secs": 605,  "start_station": "E"},
        {"trip_id": "t6", "source": "scraper", "duration_secs": 750,  "start_station": "F"},
        {"trip_id": "t7", "source": "api",     "duration_secs": 320,  "start_station": "G"},
    ]
    result = dedup_trips_by_source_priority(trips)
    by_id = {r["trip_id"]: r for r in result}
    assert len(by_id) == 7
    assert by_id["t1"]["duration_secs"] == 900
    assert by_id["t4"]["source"] == "api"
    assert by_id["t2"]["source"] == "scraper"
    print("024 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 025: Detect Exact-Duplicate Rows in a Batch Upload
Difficulty: Easy
Category: Deduplication & Uniqueness
Tags: deduplication, hashing, data-quality, batch-ingestion

Scenario:
A batch ingestion job receives CSV rows uploaded by partners. Exact
duplicate rows (all fields identical) must be removed before loading
into the warehouse. Return the deduplicated rows and a count of
duplicates removed.

Input:
List of dicts (all string values): any schema

Sample Input:
rows = [
    {"user_id": "u1", "date": "2024-10-01", "revenue": "100", "country": "US"},
    {"user_id": "u2", "date": "2024-10-01", "revenue": "200", "country": "UK"},
    {"user_id": "u1", "date": "2024-10-01", "revenue": "100", "country": "US"},  # exact dup
    {"user_id": "u3", "date": "2024-10-01", "revenue": "150", "country": "DE"},
    {"user_id": "u2", "date": "2024-10-01", "revenue": "200", "country": "UK"},  # exact dup
    {"user_id": "u4", "date": "2024-10-02", "revenue": "300", "country": "FR"},
    {"user_id": "u1", "date": "2024-10-02", "revenue": "120", "country": "US"},  # diff date, keep
    {"user_id": "u3", "date": "2024-10-01", "revenue": "150", "country": "DE"},  # exact dup
    {"user_id": "u5", "date": "2024-10-02", "revenue": "180", "country": "CA"},
    {"user_id": "u4", "date": "2024-10-02", "revenue": "300", "country": "FR"},  # exact dup
]

Expected Output:
{"deduped_rows": <6 rows>, "duplicates_removed": 4}

Follow-up: Return the duplicate rows themselves so they can be logged.
"""


def remove_exact_duplicates(rows):
    seen = set()
    deduped = []
    dups = 0
    for row in rows:
        key = tuple(sorted(row.items()))
        if key not in seen:
            seen.add(key)
            deduped.append(row)
        else:
            dups += 1
    return {"deduped_rows": deduped, "duplicates_removed": dups}


def test_025():
    rows = [
        {"user_id": "u1", "date": "2024-10-01", "revenue": "100", "country": "US"},
        {"user_id": "u2", "date": "2024-10-01", "revenue": "200", "country": "UK"},
        {"user_id": "u1", "date": "2024-10-01", "revenue": "100", "country": "US"},
        {"user_id": "u3", "date": "2024-10-01", "revenue": "150", "country": "DE"},
        {"user_id": "u2", "date": "2024-10-01", "revenue": "200", "country": "UK"},
        {"user_id": "u4", "date": "2024-10-02", "revenue": "300", "country": "FR"},
        {"user_id": "u1", "date": "2024-10-02", "revenue": "120", "country": "US"},
        {"user_id": "u3", "date": "2024-10-01", "revenue": "150", "country": "DE"},
        {"user_id": "u5", "date": "2024-10-02", "revenue": "180", "country": "CA"},
        {"user_id": "u4", "date": "2024-10-02", "revenue": "300", "country": "FR"},
    ]
    result = remove_exact_duplicates(rows)
    assert result["duplicates_removed"] == 4
    assert len(result["deduped_rows"]) == 6
    print("025 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 026: Deduplicate Safety Signals — Keep Earliest per Signal Type per User per Day
Difficulty: Medium
Category: Deduplication & Uniqueness
Tags: deduplication, earliest-record, safety-signals, trust-and-safety

Scenario:
The Safeguards pipeline receives safety-signal events (hate_speech, csam,
self_harm, etc.) for users. Due to redundant detection models running in
parallel, the same signal type fires multiple times for the same user on
the same day. Keep only the earliest signal per (user, signal_type, date)
for downstream risk-scoring.

Input:
List of dicts: signal_id (str), user_id (str), signal_type (str),
               ts (str ISO YYYY-MM-DDTHH:MM:SS)

Sample Input:
signals = [
    {"signal_id": "s01", "user_id": "u1", "signal_type": "hate_speech", "ts": "2024-11-01T08:00:00"},
    {"signal_id": "s02", "user_id": "u1", "signal_type": "hate_speech", "ts": "2024-11-01T08:05:00"},  # later, drop
    {"signal_id": "s03", "user_id": "u1", "signal_type": "spam",        "ts": "2024-11-01T09:00:00"},
    {"signal_id": "s04", "user_id": "u2", "signal_type": "hate_speech", "ts": "2024-11-01T07:00:00"},
    {"signal_id": "s05", "user_id": "u2", "signal_type": "hate_speech", "ts": "2024-11-01T06:00:00"},  # earlier, keep
    {"signal_id": "s06", "user_id": "u1", "signal_type": "hate_speech", "ts": "2024-11-02T08:00:00"},  # diff day, keep
    {"signal_id": "s07", "user_id": "u3", "signal_type": "csam",        "ts": "2024-11-01T10:00:00"},
    {"signal_id": "s08", "user_id": "u3", "signal_type": "csam",        "ts": "2024-11-01T10:30:00"},  # later, drop
    {"signal_id": "s09", "user_id": "u4", "signal_type": "self_harm",   "ts": "2024-11-01T11:00:00"},
    {"signal_id": "s10", "user_id": "u4", "signal_type": "self_harm",   "ts": "2024-11-01T10:45:00"},  # earlier, keep
]

Expected Output (signal_ids kept):
{"s01", "s03", "s05", "s06", "s07", "s09_replacement", ...}
# Specifically: s01, s03, s05 (not s04), s06, s07, s10 (not s09)
kept_signal_ids = {"s01", "s03", "s05", "s06", "s07", "s10"}

Follow-up: After deduplication, return users who fired >= 2 distinct
signal types in a single day.
"""


def dedup_signals_earliest(signals):
    best = {}
    for sig in signals:
        date = sig["ts"][:10]
        key = (sig["user_id"], sig["signal_type"], date)
        if key not in best or sig["ts"] < best[key]["ts"]:
            best[key] = sig
    return list(best.values())


def test_026():
    signals = [
        {"signal_id": "s01", "user_id": "u1", "signal_type": "hate_speech", "ts": "2024-11-01T08:00:00"},
        {"signal_id": "s02", "user_id": "u1", "signal_type": "hate_speech", "ts": "2024-11-01T08:05:00"},
        {"signal_id": "s03", "user_id": "u1", "signal_type": "spam",        "ts": "2024-11-01T09:00:00"},
        {"signal_id": "s04", "user_id": "u2", "signal_type": "hate_speech", "ts": "2024-11-01T07:00:00"},
        {"signal_id": "s05", "user_id": "u2", "signal_type": "hate_speech", "ts": "2024-11-01T06:00:00"},
        {"signal_id": "s06", "user_id": "u1", "signal_type": "hate_speech", "ts": "2024-11-02T08:00:00"},
        {"signal_id": "s07", "user_id": "u3", "signal_type": "csam",        "ts": "2024-11-01T10:00:00"},
        {"signal_id": "s08", "user_id": "u3", "signal_type": "csam",        "ts": "2024-11-01T10:30:00"},
        {"signal_id": "s09", "user_id": "u4", "signal_type": "self_harm",   "ts": "2024-11-01T11:00:00"},
        {"signal_id": "s10", "user_id": "u4", "signal_type": "self_harm",   "ts": "2024-11-01T10:45:00"},
    ]
    result = dedup_signals_earliest(signals)
    kept_ids = {r["signal_id"] for r in result}
    assert kept_ids == {"s01", "s03", "s05", "s06", "s07", "s10"}
    print("026 PASS")


# ─────────────────────────────────────────────────────────────
"""
Problem 027: Idempotent Upsert — Apply Updates Without Creating Duplicates
Difficulty: Medium
Category: Deduplication & Uniqueness
Tags: deduplication, upsert, idempotency, CDC

Scenario:
A pipeline applies a stream of UPDATE and INSERT operations to a user
table. Operations arrive in order. Implement an idempotent upsert:
for each operation, insert the record if the user doesn't exist,
or update it if the new updated_at is strictly later than the existing one.
Ignore stale updates (older or equal timestamp).

Input:
existing_table: list of dicts (current state): user_id, name, status, updated_at
operations: list of dicts: op (INSERT|UPDATE), user_id, name, status, updated_at

Sample Input:
existing_table = [
    {"user_id": "u1", "name": "Alice", "status": "active",   "updated_at": "2024-12-01T10:00"},
    {"user_id": "u2", "name": "Bob",   "status": "inactive", "updated_at": "2024-12-01T11:00"},
    {"user_id": "u3", "name": "Carol", "status": "active",   "updated_at": "2024-12-01T09:00"},
]
operations = [
    {"op": "UPDATE", "user_id": "u1", "name": "Alice", "status": "banned",  "updated_at": "2024-12-02T08:00"},  # apply
    {"op": "UPDATE", "user_id": "u1", "name": "Alice", "status": "active",  "updated_at": "2024-12-01T10:00"},  # stale, ignore
    {"op": "INSERT", "user_id": "u4", "name": "Dave",  "status": "active",  "updated_at": "2024-12-02T09:00"},  # insert
    {"op": "UPDATE", "user_id": "u2", "name": "Bob",   "status": "active",  "updated_at": "2024-12-02T10:00"},  # apply
    {"op": "INSERT", "user_id": "u1", "name": "Alice", "status": "active",  "updated_at": "2024-12-01T10:00"},  # already exists + stale
    {"op": "UPDATE", "user_id": "u5", "name": "Eve",   "status": "active",  "updated_at": "2024-12-02T11:00"},  # upsert new
    {"op": "UPDATE", "user_id": "u3", "name": "Carol", "status": "deleted", "updated_at": "2024-12-01T08:00"},  # stale, ignore
    {"op": "INSERT", "user_id": "u6", "name": "Frank", "status": "active",  "updated_at": "2024-12-02T12:00"},  # insert
]

Expected Output (final table state, 6 users):
{
  "u1": {"name": "Alice", "status": "banned",   "updated_at": "2024-12-02T08:00"},
  "u2": {"name": "Bob",   "status": "active",   "updated_at": "2024-12-02T10:00"},
  "u3": {"name": "Carol", "status": "active",   "updated_at": "2024-12-01T09:00"},
  "u4": {"name": "Dave",  "status": "active",   "updated_at": "2024-12-02T09:00"},
  "u5": {"name": "Eve",   "status": "active",   "updated_at": "2024-12-02T11:00"},
  "u6": {"name": "Frank", "status": "active",   "updated_at": "2024-12-02T12:00"},
}

Follow-up: Track and return the count of applied vs skipped operations.
"""


def idempotent_upsert(existing_table, operations):
    table = {r["user_id"]: {k: v for k, v in r.items() if k != "user_id"}
             for r in existing_table}
    for op in operations:
        uid = op["user_id"]
        record = {k: v for k, v in op.items() if k not in ("op", "user_id")}
        if uid not in table:
            table[uid] = record
        elif record["updated_at"] > table[uid]["updated_at"]:
            table[uid] = record
        # else: stale — ignore
    return table


def test_027():
    existing_table = [
        {"user_id": "u1", "name": "Alice", "status": "active",   "updated_at": "2024-12-01T10:00"},
        {"user_id": "u2", "name": "Bob",   "status": "inactive", "updated_at": "2024-12-01T11:00"},
        {"user_id": "u3", "name": "Carol", "status": "active",   "updated_at": "2024-12-01T09:00"},
    ]
    operations = [
        {"op": "UPDATE", "user_id": "u1", "name": "Alice", "status": "banned",  "updated_at": "2024-12-02T08:00"},
        {"op": "UPDATE", "user_id": "u1", "name": "Alice", "status": "active",  "updated_at": "2024-12-01T10:00"},
        {"op": "INSERT", "user_id": "u4", "name": "Dave",  "status": "active",  "updated_at": "2024-12-02T09:00"},
        {"op": "UPDATE", "user_id": "u2", "name": "Bob",   "status": "active",  "updated_at": "2024-12-02T10:00"},
        {"op": "INSERT", "user_id": "u1", "name": "Alice", "status": "active",  "updated_at": "2024-12-01T10:00"},
        {"op": "UPDATE", "user_id": "u5", "name": "Eve",   "status": "active",  "updated_at": "2024-12-02T11:00"},
        {"op": "UPDATE", "user_id": "u3", "name": "Carol", "status": "deleted", "updated_at": "2024-12-01T08:00"},
        {"op": "INSERT", "user_id": "u6", "name": "Frank", "status": "active",  "updated_at": "2024-12-02T12:00"},
    ]
    result = idempotent_upsert(existing_table, operations)
    assert len(result) == 6
    assert result["u1"]["status"] == "banned"
    assert result["u2"]["status"] == "active"
    assert result["u3"]["status"] == "active"
    assert result["u4"]["name"] == "Dave"
    assert result["u5"]["name"] == "Eve"
    print("027 PASS")


# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    test_016()
    test_017()
    test_018()
    test_019()
    test_020()
    test_021()
    test_022()
    test_023()
    test_024()
    test_025()
    test_026()
    test_027()
    print("\nAll Category 2 tests passed.")
