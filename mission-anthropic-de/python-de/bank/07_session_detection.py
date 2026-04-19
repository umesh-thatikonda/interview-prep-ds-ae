"""
=============================================================
CATEGORY 7: SESSION DETECTION & GAP ANALYSIS  (Problems 074-083)
=============================================================
Covers: session boundary detection, inactivity-gap splitting,
session metrics (duration, depth, bounce), inter-session gap
analysis, concurrent session detection, and multi-channel
session stitching — all grounded in realistic clickstream,
streaming, and telemetry data.
"""

# ─────────────────────────────────────────────────────────────
# Problem 074: Sessionize Clickstream with 30-Minute Inactivity Gap
# ─────────────────────────────────────────────────────────────
"""
Problem 074: Sessionize Clickstream with 30-Minute Inactivity Gap
Difficulty: Medium
Category: Session Detection & Gap Analysis
Tags: sessionization, inactivity-gap, clickstream, user-events

Scenario:
A web analytics pipeline needs to group user page-view events into
sessions. A new session starts when the gap between consecutive events
for the same user exceeds 30 minutes (1800 seconds). Assign each
event a session_id of the form "user_id-N" where N is 1-based session
count per user. Events are pre-sorted by (user_id, timestamp).

Input:
List of dicts: user_id (str), event_id (str), timestamp (int unix),
               page (str).

Sample Input:
events = [
    {"user_id": "u1", "event_id": "e01", "timestamp": 1700000000, "page": "home"},
    {"user_id": "u1", "event_id": "e02", "timestamp": 1700000500, "page": "search"},
    {"user_id": "u1", "event_id": "e03", "timestamp": 1700001100, "page": "product"},
    {"user_id": "u1", "event_id": "e04", "timestamp": 1700003000, "page": "home"},
    {"user_id": "u2", "event_id": "e05", "timestamp": 1700000100, "page": "home"},
    {"user_id": "u2", "event_id": "e06", "timestamp": 1700001900, "page": "feed"},
    {"user_id": "u2", "event_id": "e07", "timestamp": 1700003800, "page": "home"},
    {"user_id": "u3", "event_id": "e08", "timestamp": 1700000200, "page": "landing"},
    {"user_id": "u3", "event_id": "e09", "timestamp": 1700002100, "page": "home"},
    {"user_id": "u3", "event_id": "e10", "timestamp": 1700002200, "page": "about"},
]
Gap threshold: 1800 seconds

Expected Output (event_id -> session_id):
e01 -> "u1-1", e02 -> "u1-1", e03 -> "u1-1",
e04 -> "u1-2"  (gap from e03: 1900s > 1800)
e05 -> "u2-1",
e06 -> "u2-2"  (gap from e05: 1800s — NOT > threshold, equals threshold is still same session)
e07 -> "u2-3"  (gap from e06: 1900s > 1800)
e08 -> "u3-1",
e09 -> "u3-2"  (gap from e08: 1900s > 1800)
e10 -> "u3-2"

Follow-up: Also compute session-level summary: session_id,
start_time, end_time, page_count per session.
"""

def sessionize(events, gap_seconds=1800):
    result = []
    last_ts = {}
    session_num = {}
    for ev in events:
        uid = ev["user_id"]
        ts = ev["timestamp"]
        if uid not in last_ts:
            session_num[uid] = 1
        else:
            if ts - last_ts[uid] > gap_seconds:
                session_num[uid] += 1
        last_ts[uid] = ts
        session_id = f"{uid}-{session_num[uid]}"
        result.append({**ev, "session_id": session_id})
    return result


def test_074():
    events = [
        {"user_id": "u1", "event_id": "e01", "timestamp": 1700000000, "page": "home"},
        {"user_id": "u1", "event_id": "e02", "timestamp": 1700000500, "page": "search"},
        {"user_id": "u1", "event_id": "e03", "timestamp": 1700001100, "page": "product"},
        {"user_id": "u1", "event_id": "e04", "timestamp": 1700003000, "page": "home"},
        {"user_id": "u2", "event_id": "e05", "timestamp": 1700000100, "page": "home"},
        {"user_id": "u2", "event_id": "e06", "timestamp": 1700001900, "page": "feed"},
        {"user_id": "u2", "event_id": "e07", "timestamp": 1700003800, "page": "home"},
        {"user_id": "u3", "event_id": "e08", "timestamp": 1700000200, "page": "landing"},
        {"user_id": "u3", "event_id": "e09", "timestamp": 1700002100, "page": "home"},
        {"user_id": "u3", "event_id": "e10", "timestamp": 1700002200, "page": "about"},
    ]
    out = sessionize(events)
    sid = {r["event_id"]: r["session_id"] for r in out}
    assert sid["e01"] == sid["e02"] == sid["e03"] == "u1-1"
    assert sid["e04"] == "u1-2"
    assert sid["e05"] == "u2-1"
    assert sid["e06"] == "u2-2"  # gap = 1800, NOT strictly greater
    assert sid["e07"] == "u2-3"
    assert sid["e08"] == "u3-1"
    assert sid["e09"] == sid["e10"] == "u3-2"
    print("074 PASS")

if __name__ == "__main__":
    test_074()


# ─────────────────────────────────────────────────────────────
# Problem 075: Compute Session Duration and Bounce Rate
# ─────────────────────────────────────────────────────────────
"""
Problem 075: Compute Session Duration and Bounce Rate
Difficulty: Medium
Category: Session Detection & Gap Analysis
Tags: session-metrics, bounce-rate, duration, aggregation

Scenario:
After sessionizing a clickstream, compute metrics per session:
duration_seconds (last event ts - first event ts), page_depth
(number of distinct pages visited), and is_bounce (True if the
session has exactly one page view). Also compute the overall
bounce_rate across all sessions.

Input:
List of dicts: session_id (str), event_id (str), timestamp (int),
               page (str).
(A session_id groups events that belong together.)

Sample Input:
session_events = [
    {"session_id": "s1", "event_id": "e01", "timestamp": 1000, "page": "home"},
    {"session_id": "s1", "event_id": "e02", "timestamp": 1120, "page": "search"},
    {"session_id": "s1", "event_id": "e03", "timestamp": 1300, "page": "product"},
    {"session_id": "s2", "event_id": "e04", "timestamp": 2000, "page": "home"},
    {"session_id": "s3", "event_id": "e05", "timestamp": 3000, "page": "landing"},
    {"session_id": "s3", "event_id": "e06", "timestamp": 3400, "page": "home"},
    {"session_id": "s4", "event_id": "e07", "timestamp": 4000, "page": "home"},
    {"session_id": "s5", "event_id": "e08", "timestamp": 5000, "page": "product"},
    {"session_id": "s5", "event_id": "e09", "timestamp": 5200, "page": "cart"},
    {"session_id": "s5", "event_id": "e10", "timestamp": 5500, "page": "checkout"},
]

Expected Output:
sessions = [
    {"session_id": "s1", "duration_seconds": 300, "page_depth": 3, "is_bounce": False},
    {"session_id": "s2", "duration_seconds": 0,   "page_depth": 1, "is_bounce": True},
    {"session_id": "s3", "duration_seconds": 400, "page_depth": 2, "is_bounce": False},
    {"session_id": "s4", "duration_seconds": 0,   "page_depth": 1, "is_bounce": True},
    {"session_id": "s5", "duration_seconds": 500, "page_depth": 3, "is_bounce": False},
]
bounce_rate = 0.4  (2 out of 5 sessions)

Follow-up: Segment bounce rate by the landing page (first page of each
session) to identify high-bounce entry points.
"""

from collections import defaultdict

def session_metrics(session_events):
    groups = defaultdict(list)
    for ev in session_events:
        groups[ev["session_id"]].append(ev)

    sessions = []
    for sid in sorted(groups):
        evs = sorted(groups[sid], key=lambda x: x["timestamp"])
        duration = evs[-1]["timestamp"] - evs[0]["timestamp"]
        pages = {e["page"] for e in evs}
        depth = len(pages)
        bounce = len(evs) == 1
        sessions.append({
            "session_id": sid,
            "duration_seconds": duration,
            "page_depth": depth,
            "is_bounce": bounce,
        })
    bounce_rate = round(sum(1 for s in sessions if s["is_bounce"]) / len(sessions), 4)
    return sessions, bounce_rate


def test_075():
    session_events = [
        {"session_id": "s1", "event_id": "e01", "timestamp": 1000, "page": "home"},
        {"session_id": "s1", "event_id": "e02", "timestamp": 1120, "page": "search"},
        {"session_id": "s1", "event_id": "e03", "timestamp": 1300, "page": "product"},
        {"session_id": "s2", "event_id": "e04", "timestamp": 2000, "page": "home"},
        {"session_id": "s3", "event_id": "e05", "timestamp": 3000, "page": "landing"},
        {"session_id": "s3", "event_id": "e06", "timestamp": 3400, "page": "home"},
        {"session_id": "s4", "event_id": "e07", "timestamp": 4000, "page": "home"},
        {"session_id": "s5", "event_id": "e08", "timestamp": 5000, "page": "product"},
        {"session_id": "s5", "event_id": "e09", "timestamp": 5200, "page": "cart"},
        {"session_id": "s5", "event_id": "e10", "timestamp": 5500, "page": "checkout"},
    ]
    sessions, bounce_rate = session_metrics(session_events)
    by_sid = {s["session_id"]: s for s in sessions}
    assert by_sid["s1"]["duration_seconds"] == 300
    assert by_sid["s2"]["is_bounce"] == True
    assert by_sid["s4"]["is_bounce"] == True
    assert by_sid["s5"]["page_depth"] == 3
    assert bounce_rate == 0.4
    print("075 PASS")

if __name__ == "__main__":
    test_075()


# ─────────────────────────────────────────────────────────────
# Problem 076: Detect Overlapping Sessions (Concurrent Logins)
# ─────────────────────────────────────────────────────────────
"""
Problem 076: Detect Overlapping Sessions (Concurrent Logins)
Difficulty: Medium
Category: Session Detection & Gap Analysis
Tags: overlap-detection, concurrent-sessions, abuse-signals, intervals

Scenario:
A Trust & Safety system monitors concurrent login sessions per user.
If a user has two or more sessions active at the same time (overlapping
time intervals), flag them as potentially compromised (account sharing
or credential theft). Return a list of user_ids with overlapping
sessions and the overlapping session pairs.

Input:
List of dicts: user_id (str), session_id (str),
               login_time (int unix), logout_time (int unix).

Sample Input:
sessions = [
    {"user_id": "u1", "session_id": "s01", "login_time": 1000, "logout_time": 2000},
    {"user_id": "u1", "session_id": "s02", "login_time": 1500, "logout_time": 2500},
    {"user_id": "u2", "session_id": "s03", "login_time": 1000, "logout_time": 1800},
    {"user_id": "u2", "session_id": "s04", "login_time": 1900, "logout_time": 2500},
    {"user_id": "u3", "session_id": "s05", "login_time": 1000, "logout_time": 1500},
    {"user_id": "u3", "session_id": "s06", "login_time": 1400, "logout_time": 2000},
    {"user_id": "u3", "session_id": "s07", "login_time": 2100, "logout_time": 2800},
    {"user_id": "u4", "session_id": "s08", "login_time": 1000, "logout_time": 1200},
    {"user_id": "u4", "session_id": "s09", "login_time": 1200, "logout_time": 1500},
]

Expected Output:
overlaps = [
    {"user_id": "u1", "session_a": "s01", "session_b": "s02", "overlap_start": 1500, "overlap_end": 2000},
    {"user_id": "u3", "session_a": "s05", "session_b": "s06", "overlap_start": 1400, "overlap_end": 1500},
]
(u2: sessions don't overlap; u4: sessions touch at 1200 but don't overlap — use strict inequality)

Follow-up: Count the total concurrent-seconds of overlap per user
as a suspicion score.
"""

def detect_concurrent_sessions(sessions):
    from collections import defaultdict
    user_sessions = defaultdict(list)
    for s in sessions:
        user_sessions[s["user_id"]].append(s)

    overlaps = []
    for uid, slist in sorted(user_sessions.items()):
        slist = sorted(slist, key=lambda x: x["login_time"])
        for i in range(len(slist)):
            for j in range(i + 1, len(slist)):
                a, b = slist[i], slist[j]
                # Strict overlap: a starts before b ends AND b starts before a ends
                overlap_start = max(a["login_time"], b["login_time"])
                overlap_end   = min(a["logout_time"], b["logout_time"])
                if overlap_start < overlap_end:
                    overlaps.append({
                        "user_id": uid,
                        "session_a": a["session_id"],
                        "session_b": b["session_id"],
                        "overlap_start": overlap_start,
                        "overlap_end":   overlap_end,
                    })
    return overlaps


def test_076():
    sessions = [
        {"user_id": "u1", "session_id": "s01", "login_time": 1000, "logout_time": 2000},
        {"user_id": "u1", "session_id": "s02", "login_time": 1500, "logout_time": 2500},
        {"user_id": "u2", "session_id": "s03", "login_time": 1000, "logout_time": 1800},
        {"user_id": "u2", "session_id": "s04", "login_time": 1900, "logout_time": 2500},
        {"user_id": "u3", "session_id": "s05", "login_time": 1000, "logout_time": 1500},
        {"user_id": "u3", "session_id": "s06", "login_time": 1400, "logout_time": 2000},
        {"user_id": "u3", "session_id": "s07", "login_time": 2100, "logout_time": 2800},
        {"user_id": "u4", "session_id": "s08", "login_time": 1000, "logout_time": 1200},
        {"user_id": "u4", "session_id": "s09", "login_time": 1200, "logout_time": 1500},
    ]
    out = detect_concurrent_sessions(sessions)
    assert len(out) == 2
    user_ids = {r["user_id"] for r in out}
    assert user_ids == {"u1", "u3"}
    u1_overlap = next(r for r in out if r["user_id"] == "u1")
    assert u1_overlap["overlap_start"] == 1500
    assert u1_overlap["overlap_end"] == 2000
    print("076 PASS")

if __name__ == "__main__":
    test_076()


# ─────────────────────────────────────────────────────────────
# Problem 077: Identify Active Streaming Watch Windows
# ─────────────────────────────────────────────────────────────
"""
Problem 077: Identify Active Streaming Watch Windows
Difficulty: Medium
Category: Session Detection & Gap Analysis
Tags: watch-sessions, streaming, merge-intervals, video

Scenario:
A Disney+ streaming pipeline receives heartbeat events every 30
seconds during playback. If a user's heartbeats for the same
content_id are more than 60 seconds apart, treat it as a break
(buffer/pause/drop). Merge contiguous watch intervals per
(user_id, content_id) and report total watch time.

Input:
List of dicts: user_id (str), content_id (str), heartbeat_ts (int unix).
(Pre-sorted by user_id, content_id, heartbeat_ts.)

Sample Input:
heartbeats = [
    {"user_id": "u1", "content_id": "c1", "heartbeat_ts": 1000},
    {"user_id": "u1", "content_id": "c1", "heartbeat_ts": 1030},
    {"user_id": "u1", "content_id": "c1", "heartbeat_ts": 1060},
    {"user_id": "u1", "content_id": "c1", "heartbeat_ts": 1200},
    {"user_id": "u1", "content_id": "c1", "heartbeat_ts": 1230},
    {"user_id": "u2", "content_id": "c1", "heartbeat_ts": 2000},
    {"user_id": "u2", "content_id": "c1", "heartbeat_ts": 2030},
    {"user_id": "u2", "content_id": "c2", "heartbeat_ts": 3000},
    {"user_id": "u2", "content_id": "c2", "heartbeat_ts": 3030},
    {"user_id": "u2", "content_id": "c2", "heartbeat_ts": 3060},
]
Gap threshold: 60 seconds

Expected Output:
[
    {"user_id": "u1", "content_id": "c1", "window_start": 1000, "window_end": 1060, "watch_seconds": 60},
    {"user_id": "u1", "content_id": "c1", "window_start": 1200, "window_end": 1230, "watch_seconds": 30},
    {"user_id": "u2", "content_id": "c1", "window_start": 2000, "window_end": 2030, "watch_seconds": 30},
    {"user_id": "u2", "content_id": "c2", "window_start": 3000, "window_end": 3060, "watch_seconds": 60},
]

Follow-up: Add total_watch_seconds per (user_id, content_id) summing
across all their windows for a unified "time-watched" metric.
"""

def watch_windows(heartbeats, gap_seconds=60):
    result = []
    current_key = None
    window_start = None
    window_end = None

    for hb in heartbeats:
        key = (hb["user_id"], hb["content_id"])
        ts = hb["heartbeat_ts"]
        if key != current_key:
            if current_key is not None:
                result.append({
                    "user_id": current_key[0], "content_id": current_key[1],
                    "window_start": window_start, "window_end": window_end,
                    "watch_seconds": window_end - window_start,
                })
            current_key = key
            window_start = ts
            window_end = ts
        else:
            if ts - window_end > gap_seconds:
                result.append({
                    "user_id": current_key[0], "content_id": current_key[1],
                    "window_start": window_start, "window_end": window_end,
                    "watch_seconds": window_end - window_start,
                })
                window_start = ts
                window_end = ts
            else:
                window_end = ts

    if current_key is not None:
        result.append({
            "user_id": current_key[0], "content_id": current_key[1],
            "window_start": window_start, "window_end": window_end,
            "watch_seconds": window_end - window_start,
        })
    return result


def test_077():
    heartbeats = [
        {"user_id": "u1", "content_id": "c1", "heartbeat_ts": 1000},
        {"user_id": "u1", "content_id": "c1", "heartbeat_ts": 1030},
        {"user_id": "u1", "content_id": "c1", "heartbeat_ts": 1060},
        {"user_id": "u1", "content_id": "c1", "heartbeat_ts": 1200},
        {"user_id": "u1", "content_id": "c1", "heartbeat_ts": 1230},
        {"user_id": "u2", "content_id": "c1", "heartbeat_ts": 2000},
        {"user_id": "u2", "content_id": "c1", "heartbeat_ts": 2030},
        {"user_id": "u2", "content_id": "c2", "heartbeat_ts": 3000},
        {"user_id": "u2", "content_id": "c2", "heartbeat_ts": 3030},
        {"user_id": "u2", "content_id": "c2", "heartbeat_ts": 3060},
    ]
    out = watch_windows(heartbeats)
    assert len(out) == 4
    assert out[0]["watch_seconds"] == 60
    assert out[1]["window_start"] == 1200
    assert out[3]["watch_seconds"] == 60
    print("077 PASS")

if __name__ == "__main__":
    test_077()


# ─────────────────────────────────────────────────────────────
# Problem 078: Compute Inter-Session Gap Distribution
# ─────────────────────────────────────────────────────────────
"""
Problem 078: Compute Inter-Session Gap Distribution
Difficulty: Medium
Category: Session Detection & Gap Analysis
Tags: inter-session-gap, engagement, distribution, user-behavior

Scenario:
To understand user re-engagement patterns, compute the gap between
consecutive sessions for each user (session end to next session start).
Then bucket gaps into categories: "< 1h", "1h-24h", "1d-7d", "> 7d".
Return a frequency count per bucket.

Input:
List of dicts: user_id (str), session_id (str),
               start_ts (int unix), end_ts (int unix).
(Multiple sessions per user, may be unsorted.)

Sample Input:
sessions = [
    {"user_id": "u1", "session_id": "s01", "start_ts": 1700000000, "end_ts": 1700001800},
    {"user_id": "u1", "session_id": "s02", "start_ts": 1700005000, "end_ts": 1700006000},
    {"user_id": "u1", "session_id": "s03", "start_ts": 1700100000, "end_ts": 1700101000},
    {"user_id": "u2", "session_id": "s04", "start_ts": 1700000000, "end_ts": 1700000600},
    {"user_id": "u2", "session_id": "s05", "start_ts": 1700100000, "end_ts": 1700100500},
    {"user_id": "u3", "session_id": "s06", "start_ts": 1700000000, "end_ts": 1700000300},
    {"user_id": "u3", "session_id": "s07", "start_ts": 1700003200, "end_ts": 1700004000},
    {"user_id": "u3", "session_id": "s08", "start_ts": 1700700000, "end_ts": 1700701000},
    {"user_id": "u4", "session_id": "s09", "start_ts": 1700000000, "end_ts": 1700001000},
]

Expected Output:
{
    "< 1h":    1,   # u1 s01->s02: 3200s gap = 0.89h
    "1h-24h":  2,   # u1 s02->s03: 94000s ~ 26h? No. Let me recount:
    # u1: s01 ends 1700001800, s02 starts 1700005000 -> gap=3200s -> "< 1h"
    # u1: s02 ends 1700006000, s03 starts 1700100000 -> gap=94000s -> "1d-7d"? 94000/3600=26h -> "1h-24h"? No 26h > 24h -> "1d-7d"
    # u2: s04 ends 1700000600, s05 starts 1700100000 -> gap=99400s -> "1d-7d"
    # u3: s06 ends 1700000300, s07 starts 1700003200 -> gap=2900s -> "< 1h"
    # u3: s07 ends 1700004000, s08 starts 1700700000 -> gap=696000s = 8.05 days -> "> 7d"
    # u4: only 1 session, no gap
}
Corrected Expected Output:
{
    "< 1h":  2,
    "1h-24h": 0,
    "1d-7d": 2,
    "> 7d":  1,
}

Follow-up: Compute median inter-session gap per user and identify
users with gaps > 30 days as churned.
"""

def inter_session_gaps(sessions):
    from collections import defaultdict
    user_sessions = defaultdict(list)
    for s in sessions:
        user_sessions[s["user_id"]].append(s)

    gaps = []
    for uid, slist in user_sessions.items():
        slist = sorted(slist, key=lambda x: x["start_ts"])
        for i in range(len(slist) - 1):
            gap_secs = slist[i + 1]["start_ts"] - slist[i]["end_ts"]
            gaps.append(gap_secs)

    buckets = {"< 1h": 0, "1h-24h": 0, "1d-7d": 0, "> 7d": 0}
    for g in gaps:
        if g < 3600:
            buckets["< 1h"] += 1
        elif g < 86400:
            buckets["1h-24h"] += 1
        elif g < 604800:
            buckets["1d-7d"] += 1
        else:
            buckets["> 7d"] += 1
    return buckets


def test_078():
    sessions = [
        {"user_id": "u1", "session_id": "s01", "start_ts": 1700000000, "end_ts": 1700001800},
        {"user_id": "u1", "session_id": "s02", "start_ts": 1700005000, "end_ts": 1700006000},
        {"user_id": "u1", "session_id": "s03", "start_ts": 1700100000, "end_ts": 1700101000},
        {"user_id": "u2", "session_id": "s04", "start_ts": 1700000000, "end_ts": 1700000600},
        {"user_id": "u2", "session_id": "s05", "start_ts": 1700100000, "end_ts": 1700100500},
        {"user_id": "u3", "session_id": "s06", "start_ts": 1700000000, "end_ts": 1700000300},
        {"user_id": "u3", "session_id": "s07", "start_ts": 1700003200, "end_ts": 1700004000},
        {"user_id": "u3", "session_id": "s08", "start_ts": 1700700000, "end_ts": 1700701000},
        {"user_id": "u4", "session_id": "s09", "start_ts": 1700000000, "end_ts": 1700001000},
    ]
    out = inter_session_gaps(sessions)
    assert out["< 1h"]  == 2   # u1:s01->s02 (3200s), u3:s06->s07 (2900s)
    assert out["1h-24h"] == 0
    assert out["1d-7d"] == 2   # u1:s02->s03, u2:s04->s05
    assert out["> 7d"]  == 1   # u3:s07->s08
    print("078 PASS")

if __name__ == "__main__":
    test_078()


# ─────────────────────────────────────────────────────────────
# Problem 079: Find Users with Unusually Short Sessions (Bot Signal)
# ─────────────────────────────────────────────────────────────
"""
Problem 079: Find Users with Unusually Short Sessions (Bot Signal)
Difficulty: Medium
Category: Session Detection & Gap Analysis
Tags: anomaly-detection, bot-detection, session-analysis, safety

Scenario:
A Trust & Safety bot detection pipeline flags users who have a high
proportion of extremely short sessions (< 5 seconds duration). A user
is suspicious if more than 60% of their sessions are sub-5-second.
Return a list of suspicious users sorted by their short_session_rate
descending.

Input:
List of dicts: user_id (str), session_id (str),
               start_ts (int), end_ts (int).

Sample Input:
sessions = [
    {"user_id": "u1", "session_id": "s01", "start_ts": 1000, "end_ts": 1002},
    {"user_id": "u1", "session_id": "s02", "start_ts": 2000, "end_ts": 2001},
    {"user_id": "u1", "session_id": "s03", "start_ts": 3000, "end_ts": 3004},
    {"user_id": "u1", "session_id": "s04", "start_ts": 4000, "end_ts": 4003},
    {"user_id": "u1", "session_id": "s05", "start_ts": 5000, "end_ts": 5300},
    {"user_id": "u2", "session_id": "s06", "start_ts": 1000, "end_ts": 1200},
    {"user_id": "u2", "session_id": "s07", "start_ts": 2000, "end_ts": 2150},
    {"user_id": "u2", "session_id": "s08", "start_ts": 3000, "end_ts": 3003},
    {"user_id": "u3", "session_id": "s09", "start_ts": 1000, "end_ts": 1001},
    {"user_id": "u3", "session_id": "s10", "start_ts": 2000, "end_ts": 2002},
    {"user_id": "u3", "session_id": "s11", "start_ts": 3000, "end_ts": 3001},
    {"user_id": "u4", "session_id": "s12", "start_ts": 1000, "end_ts": 1500},
]

Expected Output:
[
    {"user_id": "u3", "total_sessions": 3, "short_sessions": 3, "short_session_rate": 1.0},
    {"user_id": "u1", "total_sessions": 5, "short_sessions": 4, "short_session_rate": 0.8},
]
(u2: 1/3 = 0.33 rate, not suspicious; u4: 0/1 rate, not suspicious)

Follow-up: Weight suspicion score by total session count so
high-volume bots rank higher than low-volume ones.
"""

def detect_bot_sessions(sessions, short_threshold=5, rate_threshold=0.6):
    from collections import defaultdict
    user_data = defaultdict(lambda: {"total": 0, "short": 0})
    for s in sessions:
        uid = s["user_id"]
        duration = s["end_ts"] - s["start_ts"]
        user_data[uid]["total"] += 1
        if duration < short_threshold:
            user_data[uid]["short"] += 1

    suspicious = []
    for uid, d in user_data.items():
        rate = d["short"] / d["total"]
        if rate > rate_threshold:
            suspicious.append({
                "user_id": uid,
                "total_sessions": d["total"],
                "short_sessions": d["short"],
                "short_session_rate": round(rate, 4),
            })
    return sorted(suspicious, key=lambda x: -x["short_session_rate"])


def test_079():
    sessions = [
        {"user_id": "u1", "session_id": "s01", "start_ts": 1000, "end_ts": 1002},
        {"user_id": "u1", "session_id": "s02", "start_ts": 2000, "end_ts": 2001},
        {"user_id": "u1", "session_id": "s03", "start_ts": 3000, "end_ts": 3004},
        {"user_id": "u1", "session_id": "s04", "start_ts": 4000, "end_ts": 4003},
        {"user_id": "u1", "session_id": "s05", "start_ts": 5000, "end_ts": 5300},
        {"user_id": "u2", "session_id": "s06", "start_ts": 1000, "end_ts": 1200},
        {"user_id": "u2", "session_id": "s07", "start_ts": 2000, "end_ts": 2150},
        {"user_id": "u2", "session_id": "s08", "start_ts": 3000, "end_ts": 3003},
        {"user_id": "u3", "session_id": "s09", "start_ts": 1000, "end_ts": 1001},
        {"user_id": "u3", "session_id": "s10", "start_ts": 2000, "end_ts": 2002},
        {"user_id": "u3", "session_id": "s11", "start_ts": 3000, "end_ts": 3001},
        {"user_id": "u4", "session_id": "s12", "start_ts": 1000, "end_ts": 1500},
    ]
    out = detect_bot_sessions(sessions)
    assert len(out) == 2
    assert out[0]["user_id"] == "u3"
    assert out[0]["short_session_rate"] == 1.0
    assert out[1]["user_id"] == "u1"
    assert out[1]["short_session_rate"] == 0.8
    print("079 PASS")

if __name__ == "__main__":
    test_079()


# ─────────────────────────────────────────────────────────────
# Problem 080: Find Maximum Concurrent Active Sessions
# ─────────────────────────────────────────────────────────────
"""
Problem 080: Find Maximum Concurrent Active Sessions
Difficulty: Hard
Category: Session Detection & Gap Analysis
Tags: sweep-line, concurrent, peak-load, capacity-planning

Scenario:
A capacity planning team needs to know the peak number of
simultaneously active sessions on the platform at any point in time
(used to size infrastructure). Use a sweep-line algorithm on login/
logout events. Also return the timestamp at which the peak occurred.

Input:
List of dicts: session_id (str), start_ts (int), end_ts (int).

Sample Input:
sessions = [
    {"session_id": "s01", "start_ts": 1000, "end_ts": 1500},
    {"session_id": "s02", "start_ts": 1100, "end_ts": 1800},
    {"session_id": "s03", "start_ts": 1200, "end_ts": 1600},
    {"session_id": "s04", "start_ts": 1400, "end_ts": 2000},
    {"session_id": "s05", "start_ts": 1600, "end_ts": 2200},
    {"session_id": "s06", "start_ts": 1900, "end_ts": 2500},
    {"session_id": "s07", "start_ts": 2100, "end_ts": 2400},
    {"session_id": "s08", "start_ts": 2300, "end_ts": 2800},
    {"session_id": "s09", "start_ts": 2600, "end_ts": 3000},
    {"session_id": "s10", "start_ts": 1300, "end_ts": 1700},
]

Expected Output:
{"peak_concurrent": 5, "peak_at_timestamp": 1400}
(At ts=1400: s01, s02, s03, s04, s10 are all active)

Follow-up: Return the full time series of concurrent session counts
at each start/end event boundary.
"""

def max_concurrent_sessions(sessions):
    events = []
    for s in sessions:
        events.append((s["start_ts"], 1))   # +1 at start
        events.append((s["end_ts"],   -1))  # -1 at end
    # Sort: by time, ties broken by -1 before +1 (ends before starts at same ts)
    events.sort(key=lambda x: (x[0], x[1]))

    peak = 0
    current = 0
    peak_ts = None
    for ts, delta in events:
        current += delta
        if current > peak:
            peak = current
            peak_ts = ts
    return {"peak_concurrent": peak, "peak_at_timestamp": peak_ts}


def test_080():
    sessions = [
        {"session_id": "s01", "start_ts": 1000, "end_ts": 1500},
        {"session_id": "s02", "start_ts": 1100, "end_ts": 1800},
        {"session_id": "s03", "start_ts": 1200, "end_ts": 1600},
        {"session_id": "s04", "start_ts": 1400, "end_ts": 2000},
        {"session_id": "s05", "start_ts": 1600, "end_ts": 2200},
        {"session_id": "s06", "start_ts": 1900, "end_ts": 2500},
        {"session_id": "s07", "start_ts": 2100, "end_ts": 2400},
        {"session_id": "s08", "start_ts": 2300, "end_ts": 2800},
        {"session_id": "s09", "start_ts": 2600, "end_ts": 3000},
        {"session_id": "s10", "start_ts": 1300, "end_ts": 1700},
    ]
    out = max_concurrent_sessions(sessions)
    assert out["peak_concurrent"] == 5
    assert out["peak_at_timestamp"] == 1400
    print("080 PASS")

if __name__ == "__main__":
    test_080()


# ─────────────────────────────────────────────────────────────
# Problem 081: Stitch Multi-Device Sessions by User Identity Graph
# ─────────────────────────────────────────────────────────────
"""
Problem 081: Stitch Multi-Device Sessions by User Identity Graph
Difficulty: Hard
Category: Session Detection & Gap Analysis
Tags: identity-resolution, union-find, multi-device, stitching

Scenario:
An identity resolution system receives device-level sessions and a
set of known device-to-user links. Two devices can be stitched to the
same canonical user if they share a user_id (e.g. same logged-in
account seen on both). Use a Union-Find to merge devices into identity
clusters, then re-label all sessions with the canonical user_id
(smallest user_id in the cluster).

Input:
device_sessions: list of dicts — device_id (str), session_id (str),
                 start_ts (int), end_ts (int)
identity_links:  list of (device_id_a, device_id_b) tuples where both
                 devices are known to belong to the same person

Sample Input:
device_sessions = [
    {"device_id": "d1", "session_id": "s01", "start_ts": 1000, "end_ts": 1500},
    {"device_id": "d2", "session_id": "s02", "start_ts": 1100, "end_ts": 1600},
    {"device_id": "d3", "session_id": "s03", "start_ts": 1200, "end_ts": 1700},
    {"device_id": "d4", "session_id": "s04", "start_ts": 1300, "end_ts": 1800},
    {"device_id": "d5", "session_id": "s05", "start_ts": 1400, "end_ts": 1900},
    {"device_id": "d6", "session_id": "s06", "start_ts": 1500, "end_ts": 2000},
    {"device_id": "d7", "session_id": "s07", "start_ts": 1600, "end_ts": 2100},
    {"device_id": "d8", "session_id": "s08", "start_ts": 1700, "end_ts": 2200},
]
identity_links = [
    ("d1", "d2"),
    ("d2", "d3"),
    ("d5", "d6"),
    ("d7", "d8"),
]

Expected Output (canonical_id = smallest device_id in cluster):
[
    {"session_id": "s01", "device_id": "d1", "canonical_id": "d1"},
    {"session_id": "s02", "device_id": "d2", "canonical_id": "d1"},
    {"session_id": "s03", "device_id": "d3", "canonical_id": "d1"},
    {"session_id": "s04", "device_id": "d4", "canonical_id": "d4"},
    {"session_id": "s05", "device_id": "d5", "canonical_id": "d5"},
    {"session_id": "s06", "device_id": "d6", "canonical_id": "d5"},
    {"session_id": "s07", "device_id": "d7", "canonical_id": "d7"},
    {"session_id": "s08", "device_id": "d8", "canonical_id": "d7"},
]

Follow-up: Count total_sessions and total_watch_time per canonical_id.
"""

def stitch_sessions(device_sessions, identity_links):
    parent = {}

    def find(x):
        parent.setdefault(x, x)
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            # Keep lexicographically smaller as root
            if ra < rb:
                parent[rb] = ra
            else:
                parent[ra] = rb

    for dev in device_sessions:
        find(dev["device_id"])

    for a, b in identity_links:
        union(a, b)

    result = []
    for s in device_sessions:
        canonical = find(s["device_id"])
        result.append({
            "session_id": s["session_id"],
            "device_id": s["device_id"],
            "canonical_id": canonical,
        })
    return result


def test_081():
    device_sessions = [
        {"device_id": "d1", "session_id": "s01", "start_ts": 1000, "end_ts": 1500},
        {"device_id": "d2", "session_id": "s02", "start_ts": 1100, "end_ts": 1600},
        {"device_id": "d3", "session_id": "s03", "start_ts": 1200, "end_ts": 1700},
        {"device_id": "d4", "session_id": "s04", "start_ts": 1300, "end_ts": 1800},
        {"device_id": "d5", "session_id": "s05", "start_ts": 1400, "end_ts": 1900},
        {"device_id": "d6", "session_id": "s06", "start_ts": 1500, "end_ts": 2000},
        {"device_id": "d7", "session_id": "s07", "start_ts": 1600, "end_ts": 2100},
        {"device_id": "d8", "session_id": "s08", "start_ts": 1700, "end_ts": 2200},
    ]
    identity_links = [("d1", "d2"), ("d2", "d3"), ("d5", "d6"), ("d7", "d8")]
    out = stitch_sessions(device_sessions, identity_links)
    by_sid = {r["session_id"]: r for r in out}
    assert by_sid["s01"]["canonical_id"] == "d1"
    assert by_sid["s02"]["canonical_id"] == "d1"
    assert by_sid["s03"]["canonical_id"] == "d1"
    assert by_sid["s04"]["canonical_id"] == "d4"
    assert by_sid["s05"]["canonical_id"] == "d5"
    assert by_sid["s06"]["canonical_id"] == "d5"
    assert by_sid["s07"]["canonical_id"] == "d7"
    print("081 PASS")

if __name__ == "__main__":
    test_081()


# ─────────────────────────────────────────────────────────────
# Problem 082: Segment Users by Engagement Tier from Session Data
# ─────────────────────────────────────────────────────────────
"""
Problem 082: Segment Users by Engagement Tier from Session Data
Difficulty: Medium
Category: Session Detection & Gap Analysis
Tags: user-segmentation, engagement, session-aggregation, product

Scenario:
A product analytics team segments users into engagement tiers based
on their session behavior in the last 30 days:
  - "power": avg_daily_sessions >= 3 OR total_watch_minutes >= 600
  - "casual": avg_daily_sessions >= 1 OR total_watch_minutes >= 60
  - "dormant": everything else
Compute per-user aggregates and assign tier. Users not in the session
data are "dormant".

Input:
sessions: list of dicts — user_id (str), date (str YYYY-MM-DD),
          duration_seconds (int)
all_users: list of str (full user roster)

Sample Input:
sessions = [
    {"user_id": "u1", "date": "2024-01-01", "duration_seconds": 1800},
    {"user_id": "u1", "date": "2024-01-01", "duration_seconds": 1200},
    {"user_id": "u1", "date": "2024-01-02", "duration_seconds": 900},
    {"user_id": "u1", "date": "2024-01-03", "duration_seconds": 600},
    {"user_id": "u2", "date": "2024-01-01", "duration_seconds": 300},
    {"user_id": "u2", "date": "2024-01-05", "duration_seconds": 200},
    {"user_id": "u3", "date": "2024-01-01", "duration_seconds": 7200},
    {"user_id": "u3", "date": "2024-01-01", "duration_seconds": 3600},
    {"user_id": "u3", "date": "2024-01-01", "duration_seconds": 3600},
    {"user_id": "u4", "date": "2024-01-10", "duration_seconds": 30},
]
all_users = ["u1", "u2", "u3", "u4", "u5"]
observation_days = 30

Expected Output (sorted by user_id):
[
    {"user_id": "u1", "total_sessions": 4, "active_days": 3, "total_watch_minutes": 75.0, "avg_daily_sessions": 0.133, "tier": "casual"},
    {"user_id": "u2", "total_sessions": 2, "active_days": 2, "total_watch_minutes": 8.33, "avg_daily_sessions": 0.067, "tier": "dormant"},
    {"user_id": "u3", "total_sessions": 3, "active_days": 1, "total_watch_minutes": 240.0, "avg_daily_sessions": 0.1, "tier": "power"},
    {"user_id": "u4", "total_sessions": 1, "active_days": 1, "total_watch_minutes": 0.5, "avg_daily_sessions": 0.033, "tier": "dormant"},
    {"user_id": "u5", "total_sessions": 0, "active_days": 0, "total_watch_minutes": 0.0, "avg_daily_sessions": 0.0, "tier": "dormant"},
]

Follow-up: Add a "trend" field by comparing this period's metrics to
the prior 30-day window: "up", "down", or "stable".
"""

def segment_users(sessions, all_users, observation_days=30):
    from collections import defaultdict
    data = defaultdict(lambda: {"sessions": 0, "days": set(), "seconds": 0})
    for s in sessions:
        uid = s["user_id"]
        data[uid]["sessions"] += 1
        data[uid]["days"].add(s["date"])
        data[uid]["seconds"] += s["duration_seconds"]

    result = []
    for uid in sorted(all_users):
        d = data[uid]
        total_sessions = d["sessions"]
        active_days = len(d["days"])
        total_watch_minutes = round(d["seconds"] / 60, 2)
        avg_daily = round(total_sessions / observation_days, 3)

        if avg_daily >= 3 or total_watch_minutes >= 600:
            tier = "power"
        elif avg_daily >= 1 or total_watch_minutes >= 60:
            tier = "casual"
        else:
            tier = "dormant"

        result.append({
            "user_id": uid,
            "total_sessions": total_sessions,
            "active_days": active_days,
            "total_watch_minutes": total_watch_minutes,
            "avg_daily_sessions": avg_daily,
            "tier": tier,
        })
    return result


def test_082():
    sessions = [
        {"user_id": "u1", "date": "2024-01-01", "duration_seconds": 1800},
        {"user_id": "u1", "date": "2024-01-01", "duration_seconds": 1200},
        {"user_id": "u1", "date": "2024-01-02", "duration_seconds": 900},
        {"user_id": "u1", "date": "2024-01-03", "duration_seconds": 600},
        {"user_id": "u2", "date": "2024-01-01", "duration_seconds": 300},
        {"user_id": "u2", "date": "2024-01-05", "duration_seconds": 200},
        {"user_id": "u3", "date": "2024-01-01", "duration_seconds": 7200},
        {"user_id": "u3", "date": "2024-01-01", "duration_seconds": 3600},
        {"user_id": "u3", "date": "2024-01-01", "duration_seconds": 3600},
        {"user_id": "u4", "date": "2024-01-10", "duration_seconds": 30},
    ]
    all_users = ["u1", "u2", "u3", "u4", "u5"]
    out = segment_users(sessions, all_users)
    by_user = {r["user_id"]: r for r in out}
    assert by_user["u1"]["tier"] == "casual"
    assert by_user["u3"]["tier"] == "power"
    assert by_user["u5"]["tier"] == "dormant"
    assert by_user["u5"]["total_sessions"] == 0
    print("082 PASS")

if __name__ == "__main__":
    test_082()


# ─────────────────────────────────────────────────────────────
# Problem 083: Detect Rapid Repeated Actions (Rate-Limit Signals)
# ─────────────────────────────────────────────────────────────
"""
Problem 083: Detect Rapid Repeated Actions (Rate-Limit Signals)
Difficulty: Hard
Category: Session Detection & Gap Analysis
Tags: rate-limiting, sliding-window, abuse-detection, safety

Scenario:
An abuse detection system monitors user actions. A user is flagged
for a specific action if they perform that action more than
`max_count` times within any rolling `window_seconds` window. For
each user-action combination that triggers a flag, return the earliest
timestamp at which the threshold was first exceeded and the count
within that window.

Input:
events: list of dicts — user_id (str), action (str), timestamp (int)
max_count: int (allowed actions per window)
window_seconds: int

Sample Input:
events = [
    {"user_id": "u1", "action": "post",    "timestamp": 1000},
    {"user_id": "u1", "action": "post",    "timestamp": 1010},
    {"user_id": "u1", "action": "post",    "timestamp": 1020},
    {"user_id": "u1", "action": "post",    "timestamp": 1025},
    {"user_id": "u1", "action": "like",    "timestamp": 1000},
    {"user_id": "u1", "action": "like",    "timestamp": 1005},
    {"user_id": "u2", "action": "post",    "timestamp": 2000},
    {"user_id": "u2", "action": "post",    "timestamp": 2100},
    {"user_id": "u2", "action": "post",    "timestamp": 2200},
    {"user_id": "u2", "action": "post",    "timestamp": 2050},
    {"user_id": "u3", "action": "comment", "timestamp": 3000},
    {"user_id": "u3", "action": "comment", "timestamp": 3005},
    {"user_id": "u3", "action": "comment", "timestamp": 3010},
    {"user_id": "u3", "action": "comment", "timestamp": 3015},
]
max_count = 3
window_seconds = 30

Expected Output (sorted by user_id, action):
[
    {"user_id": "u1", "action": "post",    "flag_ts": 1025, "window_count": 4},
    {"user_id": "u3", "action": "comment", "flag_ts": 3010, "window_count": 3},
]
(u1 like: only 2 in any 30s window; u2 post: max 3 in any 30s window, but let's verify:
 sort u2 posts: 2000,2050,2100,2200. Window [2000,2050,2100] -> 3 in 100s > 30s window.
 [2050,2100]: 2 in 50s window. So u2 never hits 3 within 30s.)

Follow-up: Return all flag windows (not just the first), so the system
can count how many times a user exceeded the rate limit.
"""

from collections import defaultdict
from collections import deque

def detect_rate_violations(events, max_count, window_seconds):
    # Group events by (user_id, action), sort by timestamp
    groups = defaultdict(list)
    for ev in events:
        groups[(ev["user_id"], ev["action"])].append(ev["timestamp"])

    violations = []
    for (uid, action) in sorted(groups.keys()):
        timestamps = sorted(groups[(uid, action)])
        window = deque()
        flagged = False
        for ts in timestamps:
            window.append(ts)
            # Remove timestamps outside the window
            while window and ts - window[0] > window_seconds:
                window.popleft()
            if len(window) > max_count and not flagged:
                violations.append({
                    "user_id": uid,
                    "action": action,
                    "flag_ts": ts,
                    "window_count": len(window),
                })
                flagged = True
    return violations


def test_083():
    events = [
        {"user_id": "u1", "action": "post",    "timestamp": 1000},
        {"user_id": "u1", "action": "post",    "timestamp": 1010},
        {"user_id": "u1", "action": "post",    "timestamp": 1020},
        {"user_id": "u1", "action": "post",    "timestamp": 1025},
        {"user_id": "u1", "action": "like",    "timestamp": 1000},
        {"user_id": "u1", "action": "like",    "timestamp": 1005},
        {"user_id": "u2", "action": "post",    "timestamp": 2000},
        {"user_id": "u2", "action": "post",    "timestamp": 2100},
        {"user_id": "u2", "action": "post",    "timestamp": 2200},
        {"user_id": "u2", "action": "post",    "timestamp": 2050},
        {"user_id": "u3", "action": "comment", "timestamp": 3000},
        {"user_id": "u3", "action": "comment", "timestamp": 3005},
        {"user_id": "u3", "action": "comment", "timestamp": 3010},
        {"user_id": "u3", "action": "comment", "timestamp": 3015},
    ]
    out = detect_rate_violations(events, max_count=3, window_seconds=30)
    keys = [(r["user_id"], r["action"]) for r in out]
    assert ("u1", "post") in keys
    assert ("u3", "comment") in keys
    assert ("u2", "post") not in keys
    assert ("u1", "like") not in keys
    print("083 PASS")

if __name__ == "__main__":
    test_083()
