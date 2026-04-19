"""
=============================================================
CATEGORY 6: ETL PIPELINE LOGIC  (Problems 064-073)
=============================================================
Covers: incremental loads, watermarking, record routing,
dead-letter queues, idempotency, backfill detection, late
arrivals, and partition-aware writes — all in realistic DE
pipeline contexts.
"""

# ─────────────────────────────────────────────────────────────
# Problem 064: Identify Records That Need Incremental Re-Load
# ─────────────────────────────────────────────────────────────
"""
Problem 064: Identify Records That Need Incremental Re-Load
Difficulty: Easy
Category: ETL Pipeline Logic
Tags: incremental-load, watermark, change-detection, upsert

Scenario:
Your pipeline does incremental loads from a source table into the
warehouse. You maintain a "last loaded" snapshot keyed on record_id
with its checksum (hash of content). For each source record, decide:
"insert" (new record), "update" (checksum changed), or "skip" (no
change). Return only the records that need action.

Input:
snapshot: dict mapping record_id -> checksum (str)
source:   list of dicts — record_id (str), checksum (str), payload (str)

Sample Input:
snapshot = {
    "r1": "abc123",
    "r2": "def456",
    "r3": "ghi789",
    "r5": "jkl012",
    "r6": "mno345",
}
source = [
    {"record_id": "r1", "checksum": "abc123", "payload": "unchanged"},
    {"record_id": "r2", "checksum": "def999", "payload": "updated content"},
    {"record_id": "r3", "checksum": "ghi789", "payload": "unchanged"},
    {"record_id": "r4", "checksum": "pqr678", "payload": "brand new record"},
    {"record_id": "r5", "checksum": "jkl000", "payload": "also updated"},
    {"record_id": "r6", "checksum": "mno345", "payload": "unchanged"},
    {"record_id": "r7", "checksum": "stu901", "payload": "another new record"},
    {"record_id": "r8", "checksum": "vwx234", "payload": "yet another new"},
]

Expected Output (sorted by record_id):
[
    {"record_id": "r2", "action": "update", "payload": "updated content"},
    {"record_id": "r4", "action": "insert", "payload": "brand new record"},
    {"record_id": "r5", "action": "update", "payload": "also updated"},
    {"record_id": "r7", "action": "insert", "payload": "another new record"},
    {"record_id": "r8", "action": "insert", "payload": "yet another new"},
]

Follow-up: Add a "delete" action for records in snapshot that are
absent from source (detect soft deletes).
"""

def incremental_load(snapshot, source):
    result = []
    for rec in source:
        rid = rec["record_id"]
        if rid not in snapshot:
            result.append({"record_id": rid, "action": "insert", "payload": rec["payload"]})
        elif snapshot[rid] != rec["checksum"]:
            result.append({"record_id": rid, "action": "update", "payload": rec["payload"]})
        # else: skip
    return sorted(result, key=lambda x: x["record_id"])


def test_064():
    snapshot = {
        "r1": "abc123", "r2": "def456", "r3": "ghi789",
        "r5": "jkl012", "r6": "mno345",
    }
    source = [
        {"record_id": "r1", "checksum": "abc123", "payload": "unchanged"},
        {"record_id": "r2", "checksum": "def999", "payload": "updated content"},
        {"record_id": "r3", "checksum": "ghi789", "payload": "unchanged"},
        {"record_id": "r4", "checksum": "pqr678", "payload": "brand new record"},
        {"record_id": "r5", "checksum": "jkl000", "payload": "also updated"},
        {"record_id": "r6", "checksum": "mno345", "payload": "unchanged"},
        {"record_id": "r7", "checksum": "stu901", "payload": "another new record"},
        {"record_id": "r8", "checksum": "vwx234", "payload": "yet another new"},
    ]
    out = incremental_load(snapshot, source)
    assert len(out) == 5
    assert out[0] == {"record_id": "r2", "action": "update", "payload": "updated content"}
    assert out[1]["action"] == "insert" and out[1]["record_id"] == "r4"
    print("064 PASS")

if __name__ == "__main__":
    test_064()


# ─────────────────────────────────────────────────────────────
# Problem 065: Route Records to Target Tables by Content Type
# ─────────────────────────────────────────────────────────────
"""
Problem 065: Route Records to Target Tables by Content Type
Difficulty: Easy
Category: ETL Pipeline Logic
Tags: routing, content-classification, dispatch, pipeline

Scenario:
A mixed-event stream from a mobile app contains events of different
types that must be routed to different warehouse tables. You receive
a routing config mapping event_type -> table_name. Events whose type
is not in the config go to a dead-letter queue (table = "dlq").
Return a dict mapping table_name -> list of records.

Input:
routing_config: dict mapping event_type (str) -> table_name (str)
events: list of dicts — event_id (str), event_type (str), user_id (str)

Sample Input:
routing_config = {
    "page_view": "events_pageview",
    "click":     "events_click",
    "purchase":  "events_purchase",
    "signup":    "events_user",
    "logout":    "events_user",
}
events = [
    {"event_id": "e01", "event_type": "page_view", "user_id": "u1"},
    {"event_id": "e02", "event_type": "click",     "user_id": "u2"},
    {"event_id": "e03", "event_type": "purchase",  "user_id": "u1"},
    {"event_id": "e04", "event_type": "signup",    "user_id": "u3"},
    {"event_id": "e05", "event_type": "unknown",   "user_id": "u4"},
    {"event_id": "e06", "event_type": "page_view", "user_id": "u2"},
    {"event_id": "e07", "event_type": "logout",    "user_id": "u3"},
    {"event_id": "e08", "event_type": "ad_view",   "user_id": "u5"},
    {"event_id": "e09", "event_type": "click",     "user_id": "u1"},
    {"event_id": "e10", "event_type": "error",     "user_id": "u6"},
]

Expected Output:
{
    "events_pageview":  [e01, e06],
    "events_click":     [e02, e09],
    "events_purchase":  [e03],
    "events_user":      [e04, e07],
    "dlq":              [e05, e08, e10],
}

Follow-up: Add a retry mechanism — records routed to "dlq" get a
"dlq_reason" field set to "unknown_event_type:X".
"""

from collections import defaultdict

def route_events(routing_config, events):
    buckets = defaultdict(list)
    for ev in events:
        table = routing_config.get(ev["event_type"], "dlq")
        buckets[table].append(ev)
    return dict(buckets)


def test_065():
    routing_config = {
        "page_view": "events_pageview",
        "click":     "events_click",
        "purchase":  "events_purchase",
        "signup":    "events_user",
        "logout":    "events_user",
    }
    events = [
        {"event_id": "e01", "event_type": "page_view", "user_id": "u1"},
        {"event_id": "e02", "event_type": "click",     "user_id": "u2"},
        {"event_id": "e03", "event_type": "purchase",  "user_id": "u1"},
        {"event_id": "e04", "event_type": "signup",    "user_id": "u3"},
        {"event_id": "e05", "event_type": "unknown",   "user_id": "u4"},
        {"event_id": "e06", "event_type": "page_view", "user_id": "u2"},
        {"event_id": "e07", "event_type": "logout",    "user_id": "u3"},
        {"event_id": "e08", "event_type": "ad_view",   "user_id": "u5"},
        {"event_id": "e09", "event_type": "click",     "user_id": "u1"},
        {"event_id": "e10", "event_type": "error",     "user_id": "u6"},
    ]
    out = route_events(routing_config, events)
    assert len(out["events_pageview"]) == 2
    assert len(out["events_click"]) == 2
    assert len(out["events_user"]) == 2
    assert len(out["dlq"]) == 3
    print("065 PASS")

if __name__ == "__main__":
    test_065()


# ─────────────────────────────────────────────────────────────
# Problem 066: Detect Late-Arriving Events Past Watermark
# ─────────────────────────────────────────────────────────────
"""
Problem 066: Detect Late-Arriving Events Past Watermark
Difficulty: Medium
Category: ETL Pipeline Logic
Tags: watermark, late-data, streaming, event-time

Scenario:
A streaming pipeline tracks a per-partition watermark — the highest
event_time seen so far per partition. Any new event whose event_time
is more than `allowed_lateness` seconds behind the current watermark
for its partition is flagged as "late" and diverted to a corrections
table. Return two lists: on_time and late.

Input:
watermarks: dict mapping partition_id (str) -> current_watermark (int unix)
allowed_lateness: int (seconds)
incoming: list of dicts — event_id (str), partition_id (str), event_time (int)

Sample Input:
watermarks = {
    "p1": 1700001000,
    "p2": 1700002000,
    "p3": 1700003000,
}
allowed_lateness = 300  # 5 minutes
incoming = [
    {"event_id": "e01", "partition_id": "p1", "event_time": 1700001100},
    {"event_id": "e02", "partition_id": "p1", "event_time": 1700000600},
    {"event_id": "e03", "partition_id": "p2", "event_time": 1700001800},
    {"event_id": "e04", "partition_id": "p2", "event_time": 1700001600},
    {"event_id": "e05", "partition_id": "p3", "event_time": 1700003500},
    {"event_id": "e06", "partition_id": "p3", "event_time": 1700002600},
    {"event_id": "e07", "partition_id": "p1", "event_time": 1700000500},
    {"event_id": "e08", "partition_id": "p2", "event_time": 1700002100},
    {"event_id": "e09", "partition_id": "p4", "event_time": 1700005000},
]

Expected Output:
on_time: [e01, e03, e04, e05, e06, e08, e09]
late:    [e02, e07]
(p4 has no watermark — treat as on_time)

Follow-up: After processing, update the watermarks dict to reflect
the maximum event_time seen per partition in the incoming batch.
"""

def detect_late_events(watermarks, allowed_lateness, incoming):
    on_time, late = [], []
    for ev in incoming:
        pid = ev["partition_id"]
        if pid not in watermarks:
            on_time.append(ev)
        elif ev["event_time"] < watermarks[pid] - allowed_lateness:
            late.append(ev)
        else:
            on_time.append(ev)
    return on_time, late


def test_066():
    watermarks = {"p1": 1700001000, "p2": 1700002000, "p3": 1700003000}
    allowed_lateness = 300
    incoming = [
        {"event_id": "e01", "partition_id": "p1", "event_time": 1700001100},
        {"event_id": "e02", "partition_id": "p1", "event_time": 1700000600},
        {"event_id": "e03", "partition_id": "p2", "event_time": 1700001800},
        {"event_id": "e04", "partition_id": "p2", "event_time": 1700001600},
        {"event_id": "e05", "partition_id": "p3", "event_time": 1700003500},
        {"event_id": "e06", "partition_id": "p3", "event_time": 1700002600},
        {"event_id": "e07", "partition_id": "p1", "event_time": 1700000500},
        {"event_id": "e08", "partition_id": "p2", "event_time": 1700002100},
        {"event_id": "e09", "partition_id": "p4", "event_time": 1700005000},
    ]
    on_time, late = detect_late_events(watermarks, allowed_lateness, incoming)
    late_ids = {e["event_id"] for e in late}
    assert late_ids == {"e02", "e07"}
    assert len(on_time) == 7
    print("066 PASS")

if __name__ == "__main__":
    test_066()


# ─────────────────────────────────────────────────────────────
# Problem 067: Partition Records into Hive-Style Date Partitions
# ─────────────────────────────────────────────────────────────
"""
Problem 067: Partition Records into Hive-Style Date Partitions
Difficulty: Easy
Category: ETL Pipeline Logic
Tags: partitioning, hive, date-partition, data-lake

Scenario:
Before writing to a data lake, you need to assign each event record
its Hive-style partition path based on its event_date. The partition
pattern is "year=YYYY/month=MM/day=DD". Return a dict mapping
partition_path -> list of records, which the write step will use to
fan out into the correct S3/GCS prefixes.

Input:
List of dicts: event_id (str), event_date (str YYYY-MM-DD),
               user_id (str), event_type (str).

Sample Input:
events = [
    {"event_id": "e01", "event_date": "2024-01-15", "user_id": "u1", "event_type": "click"},
    {"event_id": "e02", "event_date": "2024-01-15", "user_id": "u2", "event_type": "view"},
    {"event_id": "e03", "event_date": "2024-01-16", "user_id": "u3", "event_type": "click"},
    {"event_id": "e04", "event_date": "2024-02-01", "user_id": "u4", "event_type": "purchase"},
    {"event_id": "e05", "event_date": "2024-01-15", "user_id": "u5", "event_type": "click"},
    {"event_id": "e06", "event_date": "2024-02-01", "user_id": "u6", "event_type": "view"},
    {"event_id": "e07", "event_date": "2024-03-10", "user_id": "u7", "event_type": "click"},
    {"event_id": "e08", "event_date": "2024-01-16", "user_id": "u8", "event_type": "view"},
    {"event_id": "e09", "event_date": "2024-03-10", "user_id": "u9", "event_type": "view"},
    {"event_id": "e10", "event_date": "2024-01-15", "user_id": "u10","event_type": "signup"},
]

Expected Output:
{
    "year=2024/month=01/day=15": [e01, e02, e05, e10],
    "year=2024/month=01/day=16": [e03, e08],
    "year=2024/month=02/day=01": [e04, e06],
    "year=2024/month=03/day=10": [e07, e09],
}

Follow-up: Add an "hour" sub-partition by extracting hour from an
event_timestamp field, producing paths like "year=.../hour=HH".
"""

def partition_by_date(events):
    result = defaultdict(list)
    for ev in events:
        parts = ev["event_date"].split("-")
        path = f"year={parts[0]}/month={parts[1]}/day={parts[2]}"
        result[path].append(ev)
    return dict(result)


def test_067():
    events = [
        {"event_id": "e01", "event_date": "2024-01-15", "user_id": "u1",  "event_type": "click"},
        {"event_id": "e02", "event_date": "2024-01-15", "user_id": "u2",  "event_type": "view"},
        {"event_id": "e03", "event_date": "2024-01-16", "user_id": "u3",  "event_type": "click"},
        {"event_id": "e04", "event_date": "2024-02-01", "user_id": "u4",  "event_type": "purchase"},
        {"event_id": "e05", "event_date": "2024-01-15", "user_id": "u5",  "event_type": "click"},
        {"event_id": "e06", "event_date": "2024-02-01", "user_id": "u6",  "event_type": "view"},
        {"event_id": "e07", "event_date": "2024-03-10", "user_id": "u7",  "event_type": "click"},
        {"event_id": "e08", "event_date": "2024-01-16", "user_id": "u8",  "event_type": "view"},
        {"event_id": "e09", "event_date": "2024-03-10", "user_id": "u9",  "event_type": "view"},
        {"event_id": "e10", "event_date": "2024-01-15", "user_id": "u10", "event_type": "signup"},
    ]
    out = partition_by_date(events)
    assert len(out["year=2024/month=01/day=15"]) == 4
    assert len(out["year=2024/month=01/day=16"]) == 2
    assert len(out["year=2024/month=03/day=10"]) == 2
    print("067 PASS")

if __name__ == "__main__":
    test_067()


# ─────────────────────────────────────────────────────────────
# Problem 068: Idempotency Check — Detect Duplicate Pipeline Runs
# ─────────────────────────────────────────────────────────────
"""
Problem 068: Idempotency Check — Detect Duplicate Pipeline Runs
Difficulty: Medium
Category: ETL Pipeline Logic
Tags: idempotency, pipeline-safety, deduplication, batch

Scenario:
A daily batch pipeline writes job execution records to a run log.
Each successful run is keyed by (job_name, partition_date). If the
same (job_name, partition_date) combination appears more than once
in the run log it indicates a duplicate run (retry or bug). Return
a list of all duplicate run pairs (keeping both records), plus a
summary of which partitions have been double-loaded.

Input:
List of dicts: run_id (str), job_name (str), partition_date (str),
               status (str), started_at (int unix timestamp).

Sample Input:
run_log = [
    {"run_id": "r01", "job_name": "events_daily", "partition_date": "2024-01-10", "status": "success", "started_at": 1704844800},
    {"run_id": "r02", "job_name": "events_daily", "partition_date": "2024-01-11", "status": "success", "started_at": 1704931200},
    {"run_id": "r03", "job_name": "events_daily", "partition_date": "2024-01-10", "status": "success", "started_at": 1704848400},
    {"run_id": "r04", "job_name": "user_dim",     "partition_date": "2024-01-10", "status": "success", "started_at": 1704844900},
    {"run_id": "r05", "job_name": "user_dim",     "partition_date": "2024-01-10", "status": "failed",  "started_at": 1704845000},
    {"run_id": "r06", "job_name": "events_daily", "partition_date": "2024-01-12", "status": "success", "started_at": 1705017600},
    {"run_id": "r07", "job_name": "events_daily", "partition_date": "2024-01-11", "status": "success", "started_at": 1704934800},
    {"run_id": "r08", "job_name": "revenue_agg",  "partition_date": "2024-01-10", "status": "success", "started_at": 1704845100},
    {"run_id": "r09", "job_name": "events_daily", "partition_date": "2024-01-12", "status": "success", "started_at": 1705021200},
    {"run_id": "r10", "job_name": "revenue_agg",  "partition_date": "2024-01-10", "status": "success", "started_at": 1704845200},
]

Expected Output:
duplicates (job_name, partition_date keys that appear >1 time among successful runs):
[
    ("events_daily", "2024-01-10"),   # r01, r03
    ("events_daily", "2024-01-11"),   # r02, r07
    ("events_daily", "2024-01-12"),   # r06, r09
    ("revenue_agg",  "2024-01-10"),   # r08, r10
]
(user_dim 2024-01-10 not a duplicate because one run failed)

Follow-up: For each duplicate key, identify which run_id to keep
(latest started_at) and which to flag for rollback.
"""

def find_duplicate_runs(run_log):
    from collections import defaultdict
    buckets = defaultdict(list)
    for r in run_log:
        if r["status"] == "success":
            key = (r["job_name"], r["partition_date"])
            buckets[key].append(r["run_id"])
    dups = sorted(k for k, v in buckets.items() if len(v) > 1)
    return dups


def test_068():
    run_log = [
        {"run_id": "r01", "job_name": "events_daily", "partition_date": "2024-01-10", "status": "success", "started_at": 1704844800},
        {"run_id": "r02", "job_name": "events_daily", "partition_date": "2024-01-11", "status": "success", "started_at": 1704931200},
        {"run_id": "r03", "job_name": "events_daily", "partition_date": "2024-01-10", "status": "success", "started_at": 1704848400},
        {"run_id": "r04", "job_name": "user_dim",     "partition_date": "2024-01-10", "status": "success", "started_at": 1704844900},
        {"run_id": "r05", "job_name": "user_dim",     "partition_date": "2024-01-10", "status": "failed",  "started_at": 1704845000},
        {"run_id": "r06", "job_name": "events_daily", "partition_date": "2024-01-12", "status": "success", "started_at": 1705017600},
        {"run_id": "r07", "job_name": "events_daily", "partition_date": "2024-01-11", "status": "success", "started_at": 1704934800},
        {"run_id": "r08", "job_name": "revenue_agg",  "partition_date": "2024-01-10", "status": "success", "started_at": 1704845100},
        {"run_id": "r09", "job_name": "events_daily", "partition_date": "2024-01-12", "status": "success", "started_at": 1705021200},
        {"run_id": "r10", "job_name": "revenue_agg",  "partition_date": "2024-01-10", "status": "success", "started_at": 1704845200},
    ]
    out = find_duplicate_runs(run_log)
    assert ("events_daily", "2024-01-10") in out
    assert ("events_daily", "2024-01-11") in out
    assert ("revenue_agg",  "2024-01-10") in out
    assert ("user_dim",     "2024-01-10") not in out
    assert len(out) == 4
    print("068 PASS")

if __name__ == "__main__":
    test_068()


# ─────────────────────────────────────────────────────────────
# Problem 069: Compute Backfill Date Ranges for Missing Partitions
# ─────────────────────────────────────────────────────────────
"""
Problem 069: Compute Backfill Date Ranges for Missing Partitions
Difficulty: Medium
Category: ETL Pipeline Logic
Tags: backfill, date-gaps, partition-inventory, pipeline

Scenario:
A data lake audit shows which date partitions have been successfully
loaded for a given table. Given the expected continuous date range and
the set of loaded partitions, identify contiguous blocks of missing
dates that need to be backfilled. Return each gap as a
(start_date, end_date, gap_size_days) tuple.

Input:
expected_start: str YYYY-MM-DD
expected_end:   str YYYY-MM-DD
loaded_dates:   list of str YYYY-MM-DD (may be unsorted, may have duplicates)

Sample Input:
expected_start = "2024-01-01"
expected_end   = "2024-01-20"
loaded_dates = [
    "2024-01-01", "2024-01-02", "2024-01-03",
    "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10",
    "2024-01-12",
    "2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18",
    "2024-01-19", "2024-01-20",
]

Expected Output:
[
    {"start_date": "2024-01-04", "end_date": "2024-01-06", "gap_days": 3},
    {"start_date": "2024-01-11", "end_date": "2024-01-11", "gap_days": 1},
    {"start_date": "2024-01-13", "end_date": "2024-01-14", "gap_days": 2},
]

Follow-up: Prioritize backfill order by gap_days descending so the
largest gaps are filled first.
"""

from datetime import date, timedelta

def find_backfill_gaps(expected_start, expected_end, loaded_dates):
    loaded_set = set(loaded_dates)
    start = date.fromisoformat(expected_start)
    end   = date.fromisoformat(expected_end)

    gaps = []
    gap_start = None
    current = start
    while current <= end:
        ds = str(current)
        if ds not in loaded_set:
            if gap_start is None:
                gap_start = current
        else:
            if gap_start is not None:
                gap_end = current - timedelta(days=1)
                gaps.append({
                    "start_date": str(gap_start),
                    "end_date":   str(gap_end),
                    "gap_days":   (gap_end - gap_start).days + 1,
                })
                gap_start = None
        current += timedelta(days=1)

    if gap_start is not None:
        gap_end = end
        gaps.append({
            "start_date": str(gap_start),
            "end_date":   str(gap_end),
            "gap_days":   (gap_end - gap_start).days + 1,
        })
    return gaps


def test_069():
    loaded = [
        "2024-01-01", "2024-01-02", "2024-01-03",
        "2024-01-07", "2024-01-08", "2024-01-09", "2024-01-10",
        "2024-01-12",
        "2024-01-15", "2024-01-16", "2024-01-17", "2024-01-18",
        "2024-01-19", "2024-01-20",
    ]
    out = find_backfill_gaps("2024-01-01", "2024-01-20", loaded)
    assert len(out) == 3
    assert out[0] == {"start_date": "2024-01-04", "end_date": "2024-01-06", "gap_days": 3}
    assert out[1] == {"start_date": "2024-01-11", "end_date": "2024-01-11", "gap_days": 1}
    assert out[2] == {"start_date": "2024-01-13", "end_date": "2024-01-14", "gap_days": 2}
    print("069 PASS")

if __name__ == "__main__":
    test_069()


# ─────────────────────────────────────────────────────────────
# Problem 070: Build a Dependency-Ordered Job Execution Plan
# ─────────────────────────────────────────────────────────────
"""
Problem 070: Build a Dependency-Ordered Job Execution Plan
Difficulty: Hard
Category: ETL Pipeline Logic
Tags: topological-sort, DAG, dependencies, orchestration

Scenario:
An Airflow-like orchestrator needs to determine the correct execution
order for a set of pipeline jobs given their upstream dependencies.
Implement a topological sort. If there is a cycle in the dependency
graph, raise a ValueError. Jobs with no dependencies run first.
Return the execution order as a list of job names.

Input:
jobs: list of dicts — job_name (str), depends_on (list of str)

Sample Input:
jobs = [
    {"job_name": "raw_events",       "depends_on": []},
    {"job_name": "user_dim",         "depends_on": []},
    {"job_name": "session_model",    "depends_on": ["raw_events"]},
    {"job_name": "enriched_events",  "depends_on": ["raw_events", "user_dim"]},
    {"job_name": "daily_metrics",    "depends_on": ["session_model", "enriched_events"]},
    {"job_name": "revenue_model",    "depends_on": ["enriched_events"]},
    {"job_name": "exec_dashboard",   "depends_on": ["daily_metrics", "revenue_model"]},
    {"job_name": "data_quality",     "depends_on": ["enriched_events"]},
]

Expected Output (one valid topological order):
["raw_events", "user_dim", "session_model", "enriched_events",
 "daily_metrics", "revenue_model", "exec_dashboard", "data_quality"]
(multiple valid orders exist; verify by checking that each job
appears after all its dependencies)

Follow-up: Return a list of lists representing execution waves —
jobs within the same wave can run in parallel.
"""

from collections import deque

def topological_sort(jobs):
    graph = {j["job_name"]: j["depends_on"] for j in jobs}
    in_degree = {j["job_name"]: 0 for j in jobs}
    adj = {j["job_name"]: [] for j in jobs}
    for job in jobs:
        for dep in job["depends_on"]:
            adj[dep].append(job["job_name"])
            in_degree[job["job_name"]] += 1

    queue = deque(j for j in in_degree if in_degree[j] == 0)
    order = []
    while queue:
        node = queue.popleft()
        order.append(node)
        for neighbor in adj[node]:
            in_degree[neighbor] -= 1
            if in_degree[neighbor] == 0:
                queue.append(neighbor)

    if len(order) != len(jobs):
        raise ValueError("Cycle detected in job dependency graph")
    return order


def test_070():
    jobs = [
        {"job_name": "raw_events",      "depends_on": []},
        {"job_name": "user_dim",        "depends_on": []},
        {"job_name": "session_model",   "depends_on": ["raw_events"]},
        {"job_name": "enriched_events", "depends_on": ["raw_events", "user_dim"]},
        {"job_name": "daily_metrics",   "depends_on": ["session_model", "enriched_events"]},
        {"job_name": "revenue_model",   "depends_on": ["enriched_events"]},
        {"job_name": "exec_dashboard",  "depends_on": ["daily_metrics", "revenue_model"]},
        {"job_name": "data_quality",    "depends_on": ["enriched_events"]},
    ]
    order = topological_sort(jobs)
    assert len(order) == 8
    pos = {name: i for i, name in enumerate(order)}
    # Verify dependency ordering
    assert pos["raw_events"] < pos["session_model"]
    assert pos["enriched_events"] < pos["daily_metrics"]
    assert pos["daily_metrics"] < pos["exec_dashboard"]
    assert pos["revenue_model"] < pos["exec_dashboard"]

    # Verify cycle detection
    cyclic_jobs = [
        {"job_name": "a", "depends_on": ["b"]},
        {"job_name": "b", "depends_on": ["c"]},
        {"job_name": "c", "depends_on": ["a"]},
    ]
    try:
        topological_sort(cyclic_jobs)
        assert False, "Should have raised ValueError"
    except ValueError:
        pass
    print("070 PASS")

if __name__ == "__main__":
    test_070()


# ─────────────────────────────────────────────────────────────
# Problem 071: Merge Two Streams with Priority (Union-Find-Free)
# ─────────────────────────────────────────────────────────────
"""
Problem 071: Merge Two Sorted Event Streams by Timestamp
Difficulty: Medium
Category: ETL Pipeline Logic
Tags: merge, sorted-merge, two-pointer, streaming

Scenario:
A real-time pipeline receives two independently-sorted event streams
(by timestamp) from two data centers. You need to merge them into a
single sorted stream efficiently using a two-pointer approach —
as if processing records that arrive in time order for a unified
event log.

Input:
stream_a: list of dicts — event_id (str), timestamp (int), source (str="dc_a")
stream_b: list of dicts — event_id (str), timestamp (int), source (str="dc_b")
(both already sorted by timestamp ascending)

Sample Input:
stream_a = [
    {"event_id": "a1", "timestamp": 1000, "source": "dc_a"},
    {"event_id": "a2", "timestamp": 1020, "source": "dc_a"},
    {"event_id": "a3", "timestamp": 1050, "source": "dc_a"},
    {"event_id": "a4", "timestamp": 1080, "source": "dc_a"},
    {"event_id": "a5", "timestamp": 1120, "source": "dc_a"},
]
stream_b = [
    {"event_id": "b1", "timestamp": 1010, "source": "dc_b"},
    {"event_id": "b2", "timestamp": 1015, "source": "dc_b"},
    {"event_id": "b3", "timestamp": 1040, "source": "dc_b"},
    {"event_id": "b4", "timestamp": 1090, "source": "dc_b"},
    {"event_id": "b5", "timestamp": 1100, "source": "dc_b"},
    {"event_id": "b6", "timestamp": 1110, "source": "dc_b"},
]

Expected Output (merged, sorted by timestamp):
[a1, b1, b2, a2, b3, a3, a4, b4, b5, b6, a5]
timestamps: [1000,1010,1015,1020,1040,1050,1080,1090,1100,1110,1120]
When timestamps tie, dc_a comes first.

Follow-up: Extend to K streams using a min-heap so the merge stays
O(N log K) rather than O(N log N).
"""

def merge_streams(stream_a, stream_b):
    merged = []
    i, j = 0, 0
    while i < len(stream_a) and j < len(stream_b):
        if stream_a[i]["timestamp"] <= stream_b[j]["timestamp"]:
            merged.append(stream_a[i]); i += 1
        else:
            merged.append(stream_b[j]); j += 1
    merged.extend(stream_a[i:])
    merged.extend(stream_b[j:])
    return merged


def test_071():
    stream_a = [
        {"event_id": "a1", "timestamp": 1000, "source": "dc_a"},
        {"event_id": "a2", "timestamp": 1020, "source": "dc_a"},
        {"event_id": "a3", "timestamp": 1050, "source": "dc_a"},
        {"event_id": "a4", "timestamp": 1080, "source": "dc_a"},
        {"event_id": "a5", "timestamp": 1120, "source": "dc_a"},
    ]
    stream_b = [
        {"event_id": "b1", "timestamp": 1010, "source": "dc_b"},
        {"event_id": "b2", "timestamp": 1015, "source": "dc_b"},
        {"event_id": "b3", "timestamp": 1040, "source": "dc_b"},
        {"event_id": "b4", "timestamp": 1090, "source": "dc_b"},
        {"event_id": "b5", "timestamp": 1100, "source": "dc_b"},
        {"event_id": "b6", "timestamp": 1110, "source": "dc_b"},
    ]
    out = merge_streams(stream_a, stream_b)
    timestamps = [e["timestamp"] for e in out]
    assert timestamps == sorted(timestamps), "Must be sorted"
    assert len(out) == 11
    assert out[0]["event_id"] == "a1"
    print("071 PASS")

if __name__ == "__main__":
    test_071()


# ─────────────────────────────────────────────────────────────
# Problem 072: Validate Pipeline Record Counts Against Manifest
# ─────────────────────────────────────────────────────────────
"""
Problem 072: Validate Pipeline Record Counts Against Manifest
Difficulty: Medium
Category: ETL Pipeline Logic
Tags: data-quality, reconciliation, manifest, count-check

Scenario:
Before marking a daily batch as "complete," your pipeline compares
actual loaded record counts per table per partition against a manifest
file produced by the source system. Flag any partition where:
- actual < expected: "under_loaded" (possible data loss)
- actual > expected: "over_loaded" (possible duplicates)
- actual == expected: "ok"
Return a quality report with status and delta.

Input:
manifest: list of dicts — table (str), partition (str), expected_count (int)
actuals:  list of dicts — table (str), partition (str), actual_count (int)

Sample Input:
manifest = [
    {"table": "events",   "partition": "2024-01-10", "expected_count": 50000},
    {"table": "events",   "partition": "2024-01-11", "expected_count": 48000},
    {"table": "sessions", "partition": "2024-01-10", "expected_count": 12000},
    {"table": "sessions", "partition": "2024-01-11", "expected_count": 11500},
    {"table": "users",    "partition": "2024-01-10", "expected_count": 3000},
    {"table": "users",    "partition": "2024-01-11", "expected_count": 3050},
]
actuals = [
    {"table": "events",   "partition": "2024-01-10", "actual_count": 50000},
    {"table": "events",   "partition": "2024-01-11", "actual_count": 47500},
    {"table": "sessions", "partition": "2024-01-10", "actual_count": 12100},
    {"table": "sessions", "partition": "2024-01-11", "actual_count": 11500},
    {"table": "users",    "partition": "2024-01-10", "actual_count": 2999},
    # users 2024-01-11 missing from actuals
]

Expected Output:
[
    {"table": "events",   "partition": "2024-01-10", "status": "ok",           "delta": 0},
    {"table": "events",   "partition": "2024-01-11", "status": "under_loaded", "delta": -500},
    {"table": "sessions", "partition": "2024-01-10", "status": "over_loaded",  "delta": 100},
    {"table": "sessions", "partition": "2024-01-11", "status": "ok",           "delta": 0},
    {"table": "users",    "partition": "2024-01-10", "status": "under_loaded", "delta": -1},
    {"table": "users",    "partition": "2024-01-11", "status": "under_loaded", "delta": -3050},
]

Follow-up: Add a tolerance parameter (e.g. 0.001 = 0.1%) so tiny
rounding differences don't trigger alerts.
"""

def validate_counts(manifest, actuals):
    actual_map = {(r["table"], r["partition"]): r["actual_count"] for r in actuals}
    result = []
    for m in manifest:
        key = (m["table"], m["partition"])
        actual = actual_map.get(key, 0)
        delta = actual - m["expected_count"]
        if delta == 0:
            status = "ok"
        elif delta < 0:
            status = "under_loaded"
        else:
            status = "over_loaded"
        result.append({"table": m["table"], "partition": m["partition"],
                        "status": status, "delta": delta})
    return result


def test_072():
    manifest = [
        {"table": "events",   "partition": "2024-01-10", "expected_count": 50000},
        {"table": "events",   "partition": "2024-01-11", "expected_count": 48000},
        {"table": "sessions", "partition": "2024-01-10", "expected_count": 12000},
        {"table": "sessions", "partition": "2024-01-11", "expected_count": 11500},
        {"table": "users",    "partition": "2024-01-10", "expected_count": 3000},
        {"table": "users",    "partition": "2024-01-11", "expected_count": 3050},
    ]
    actuals = [
        {"table": "events",   "partition": "2024-01-10", "actual_count": 50000},
        {"table": "events",   "partition": "2024-01-11", "actual_count": 47500},
        {"table": "sessions", "partition": "2024-01-10", "actual_count": 12100},
        {"table": "sessions", "partition": "2024-01-11", "actual_count": 11500},
        {"table": "users",    "partition": "2024-01-10", "actual_count": 2999},
    ]
    out = validate_counts(manifest, actuals)
    by_key = {(r["table"], r["partition"]): r for r in out}
    assert by_key[("events", "2024-01-10")]["status"] == "ok"
    assert by_key[("events", "2024-01-11")]["status"] == "under_loaded"
    assert by_key[("sessions", "2024-01-10")]["status"] == "over_loaded"
    assert by_key[("users", "2024-01-11")]["delta"] == -3050
    print("072 PASS")

if __name__ == "__main__":
    test_072()


# ─────────────────────────────────────────────────────────────
# Problem 073: Compute Per-Source Ingestion Lag Statistics
# ─────────────────────────────────────────────────────────────
"""
Problem 073: Compute Per-Source Ingestion Lag Statistics
Difficulty: Medium
Category: ETL Pipeline Logic
Tags: lag-analysis, pipeline-monitoring, statistics, SLA

Scenario:
A pipeline monitoring system tracks when events were created
(event_time) vs when they were ingested into the warehouse
(ingest_time) for each data source. Compute per-source lag statistics:
min_lag, max_lag, avg_lag, and p95_lag (all in seconds). This is used
to detect SLA breaches and slow sources.

Input:
List of dicts: source (str), event_id (str),
               event_time (int unix), ingest_time (int unix).

Sample Input:
records = [
    {"source": "mobile_app", "event_id": "e01", "event_time": 1700000000, "ingest_time": 1700000030},
    {"source": "mobile_app", "event_id": "e02", "event_time": 1700000010, "ingest_time": 1700000045},
    {"source": "mobile_app", "event_id": "e03", "event_time": 1700000020, "ingest_time": 1700000025},
    {"source": "mobile_app", "event_id": "e04", "event_time": 1700000030, "ingest_time": 1700000390},
    {"source": "mobile_app", "event_id": "e05", "event_time": 1700000040, "ingest_time": 1700000060},
    {"source": "web_app",    "event_id": "e06", "event_time": 1700000000, "ingest_time": 1700000005},
    {"source": "web_app",    "event_id": "e07", "event_time": 1700000010, "ingest_time": 1700000012},
    {"source": "web_app",    "event_id": "e08", "event_time": 1700000020, "ingest_time": 1700000028},
    {"source": "web_app",    "event_id": "e09", "event_time": 1700000030, "ingest_time": 1700000038},
    {"source": "web_app",    "event_id": "e10", "event_time": 1700000040, "ingest_time": 1700000050},
]

Expected Output (sorted by source):
[
    {"source": "mobile_app", "min_lag": 5,  "max_lag": 360, "avg_lag": 86.0, "p95_lag": 360},
    {"source": "web_app",    "min_lag": 5,  "max_lag": 10,  "avg_lag": 7.6,  "p95_lag": 10},
]
(p95 is the value at index ceil(0.95 * n) - 1 in the sorted lag list)

Follow-up: Add a "sla_breach_count" field counting events where
lag > 60 seconds (configurable threshold).
"""

import math
from collections import defaultdict

def lag_statistics(records):
    lags_by_source = defaultdict(list)
    for r in records:
        lag = r["ingest_time"] - r["event_time"]
        lags_by_source[r["source"]].append(lag)

    result = []
    for source in sorted(lags_by_source):
        lags = sorted(lags_by_source[source])
        n = len(lags)
        avg = round(sum(lags) / n, 1)
        p95_idx = min(math.ceil(0.95 * n) - 1, n - 1)
        result.append({
            "source":   source,
            "min_lag":  lags[0],
            "max_lag":  lags[-1],
            "avg_lag":  avg,
            "p95_lag":  lags[p95_idx],
        })
    return result


def test_073():
    records = [
        {"source": "mobile_app", "event_id": "e01", "event_time": 1700000000, "ingest_time": 1700000030},
        {"source": "mobile_app", "event_id": "e02", "event_time": 1700000010, "ingest_time": 1700000045},
        {"source": "mobile_app", "event_id": "e03", "event_time": 1700000020, "ingest_time": 1700000025},
        {"source": "mobile_app", "event_id": "e04", "event_time": 1700000030, "ingest_time": 1700000390},
        {"source": "mobile_app", "event_id": "e05", "event_time": 1700000040, "ingest_time": 1700000060},
        {"source": "web_app",    "event_id": "e06", "event_time": 1700000000, "ingest_time": 1700000005},
        {"source": "web_app",    "event_id": "e07", "event_time": 1700000010, "ingest_time": 1700000012},
        {"source": "web_app",    "event_id": "e08", "event_time": 1700000020, "ingest_time": 1700000028},
        {"source": "web_app",    "event_id": "e09", "event_time": 1700000030, "ingest_time": 1700000038},
        {"source": "web_app",    "event_id": "e10", "event_time": 1700000040, "ingest_time": 1700000050},
    ]
    out = lag_statistics(records)
    mobile = out[0]
    web = out[1]
    assert mobile["source"] == "mobile_app"
    assert mobile["max_lag"] == 360
    assert mobile["min_lag"] == 5
    assert web["avg_lag"] == 7.6
    print("073 PASS")

if __name__ == "__main__":
    test_073()
