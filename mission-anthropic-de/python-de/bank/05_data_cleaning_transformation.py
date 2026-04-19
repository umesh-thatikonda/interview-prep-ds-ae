"""
=============================================================
CATEGORY 5: DATA CLEANING & TRANSFORMATION  (Problems 051-063)
=============================================================
Covers: deduplication, null handling, type coercion, schema
normalization, field standardization, outlier flagging, and
structural reshaping — all in realistic DE contexts.
"""

# ─────────────────────────────────────────────────────────────
# Problem 051: Deduplicate User Events by Latest Timestamp
# ─────────────────────────────────────────────────────────────
"""
Problem 051: Deduplicate User Events by Latest Timestamp
Difficulty: Easy
Category: Data Cleaning & Transformation
Tags: deduplication, events, dict, max

Scenario:
A clickstream pipeline receives duplicate event records for the same
(user_id, event_type) pair because an upstream retry mechanism fires
events more than once. You need to keep only the most recent record
per (user_id, event_type) key before loading into the warehouse.

Input:
List of dicts with keys: user_id (str), event_type (str),
timestamp (int — unix epoch), metadata (str).

Sample Input:
events = [
    {"user_id": "u1", "event_type": "page_view",  "timestamp": 1700000100, "metadata": "home"},
    {"user_id": "u1", "event_type": "page_view",  "timestamp": 1700000200, "metadata": "home_v2"},
    {"user_id": "u2", "event_type": "click",       "timestamp": 1700000150, "metadata": "btn_buy"},
    {"user_id": "u1", "event_type": "click",       "timestamp": 1700000300, "metadata": "btn_cart"},
    {"user_id": "u3", "event_type": "page_view",  "timestamp": 1700000050, "metadata": "search"},
    {"user_id": "u2", "event_type": "click",       "timestamp": 1700000180, "metadata": "btn_buy_v2"},
    {"user_id": "u3", "event_type": "purchase",   "timestamp": 1700000400, "metadata": "sku_99"},
    {"user_id": "u1", "event_type": "page_view",  "timestamp": 1700000180, "metadata": "home_v1"},
]

Expected Output (sorted by user_id, event_type for determinism):
[
    {"user_id": "u1", "event_type": "click",      "timestamp": 1700000300, "metadata": "btn_cart"},
    {"user_id": "u1", "event_type": "page_view",  "timestamp": 1700000200, "metadata": "home_v2"},
    {"user_id": "u2", "event_type": "click",      "timestamp": 1700000180, "metadata": "btn_buy_v2"},
    {"user_id": "u3", "event_type": "page_view",  "timestamp": 1700000050, "metadata": "search"},
    {"user_id": "u3", "event_type": "purchase",   "timestamp": 1700000400, "metadata": "sku_99"},
]

Follow-up: Instead of keeping the latest, keep ALL records but add a
boolean field `is_latest` so downstream consumers can filter.
"""

def deduplicate_latest(events):
    best = {}
    for e in events:
        key = (e["user_id"], e["event_type"])
        if key not in best or e["timestamp"] > best[key]["timestamp"]:
            best[key] = e
    return sorted(best.values(), key=lambda x: (x["user_id"], x["event_type"]))


def test_051():
    events = [
        {"user_id": "u1", "event_type": "page_view",  "timestamp": 1700000100, "metadata": "home"},
        {"user_id": "u1", "event_type": "page_view",  "timestamp": 1700000200, "metadata": "home_v2"},
        {"user_id": "u2", "event_type": "click",       "timestamp": 1700000150, "metadata": "btn_buy"},
        {"user_id": "u1", "event_type": "click",       "timestamp": 1700000300, "metadata": "btn_cart"},
        {"user_id": "u3", "event_type": "page_view",  "timestamp": 1700000050, "metadata": "search"},
        {"user_id": "u2", "event_type": "click",       "timestamp": 1700000180, "metadata": "btn_buy_v2"},
        {"user_id": "u3", "event_type": "purchase",   "timestamp": 1700000400, "metadata": "sku_99"},
        {"user_id": "u1", "event_type": "page_view",  "timestamp": 1700000180, "metadata": "home_v1"},
    ]
    result = deduplicate_latest(events)
    assert len(result) == 5
    assert result[0] == {"user_id": "u1", "event_type": "click", "timestamp": 1700000300, "metadata": "btn_cart"}
    assert result[1]["metadata"] == "home_v2"
    assert result[2]["metadata"] == "btn_buy_v2"
    print("051 PASS")

if __name__ == "__main__":
    test_051()


# ─────────────────────────────────────────────────────────────
# Problem 052: Normalize Phone Numbers to E.164 Format
# ─────────────────────────────────────────────────────────────
"""
Problem 052: Normalize Phone Numbers to E.164 Format
Difficulty: Easy
Category: Data Cleaning & Transformation
Tags: string-cleaning, normalization, regex-free, type-coercion

Scenario:
A fraud-detection pipeline ingests user records from three different
CRM systems, each of which stores phone numbers differently (dashes,
dots, spaces, parentheses, country-code prefixes). You must normalize
all US phone numbers to the E.164 format "+1XXXXXXXXXX" and mark
non-normalizable entries as None.

Input:
List of dicts with keys: user_id (str), phone (str).

Sample Input:
records = [
    {"user_id": "u01", "phone": "(415) 555-2671"},
    {"user_id": "u02", "phone": "415.555.0198"},
    {"user_id": "u03", "phone": "+1-800-555-0199"},
    {"user_id": "u04", "phone": "18005550188"},
    {"user_id": "u05", "phone": "555-0123"},
    {"user_id": "u06", "phone": "not-a-phone"},
    {"user_id": "u07", "phone": "1 (650) 253-0000"},
    {"user_id": "u08", "phone": "6502530001"},
    {"user_id": "u09", "phone": "  (800) 555.0111  "},
    {"user_id": "u10", "phone": "800-555-01AB"},
]

Expected Output:
[
    {"user_id": "u01", "phone": "+14155552671"},
    {"user_id": "u02", "phone": "+14155550198"},
    {"user_id": "u03", "phone": "+18005550199"},
    {"user_id": "u04", "phone": "+18005550188"},
    {"user_id": "u05", "phone": None},
    {"user_id": "u06", "phone": None},
    {"user_id": "u07", "phone": "+16502530000"},
    {"user_id": "u08", "phone": "+16502530001"},
    {"user_id": "u09", "phone": "+18005550111"},
    {"user_id": "u10", "phone": None},
]

Follow-up: Add a "source_format" field that records which original
format was detected: "e164", "10digit", "11digit", "7digit", "invalid".
"""

def normalize_phones(records):
    result = []
    for r in records:
        raw = r["phone"].strip()
        digits = "".join(c for c in raw if c.isdigit())
        if len(digits) == 10:
            normalized = "+1" + digits
        elif len(digits) == 11 and digits[0] == "1":
            normalized = "+" + digits
        else:
            normalized = None
        result.append({"user_id": r["user_id"], "phone": normalized})
    return result


def test_052():
    records = [
        {"user_id": "u01", "phone": "(415) 555-2671"},
        {"user_id": "u02", "phone": "415.555.0198"},
        {"user_id": "u03", "phone": "+1-800-555-0199"},
        {"user_id": "u04", "phone": "18005550188"},
        {"user_id": "u05", "phone": "555-0123"},
        {"user_id": "u06", "phone": "not-a-phone"},
        {"user_id": "u07", "phone": "1 (650) 253-0000"},
        {"user_id": "u08", "phone": "6502530001"},
        {"user_id": "u09", "phone": "  (800) 555.0111  "},
        {"user_id": "u10", "phone": "800-555-01AB"},
    ]
    out = normalize_phones(records)
    assert out[0]["phone"] == "+14155552671"
    assert out[4]["phone"] is None
    assert out[5]["phone"] is None
    assert out[6]["phone"] == "+16502530000"
    assert out[9]["phone"] is None
    print("052 PASS")

if __name__ == "__main__":
    test_052()


# ─────────────────────────────────────────────────────────────
# Problem 053: Fill Forward Null Metric Values in Time Series
# ─────────────────────────────────────────────────────────────
"""
Problem 053: Fill Forward Null Metric Values in Time Series
Difficulty: Easy
Category: Data Cleaning & Transformation
Tags: null-handling, forward-fill, time-series, pipeline

Scenario:
A metrics pipeline collects hourly DAU (daily active users) counts
per product surface. Occasionally a collection job fails, leaving
None for that hour. Downstream aggregations treat None as 0 which
skews weekly averages. Fill forward each None with the last known
non-null value; if the series starts with None, leave it as None.

Input:
List of dicts with keys: hour (str "YYYY-MM-DD HH"), surface (str),
dau (int or None) — pre-sorted by surface then hour.

Sample Input:
rows = [
    {"hour": "2024-01-01 00", "surface": "feed",   "dau": 1200},
    {"hour": "2024-01-01 01", "surface": "feed",   "dau": None},
    {"hour": "2024-01-01 02", "surface": "feed",   "dau": None},
    {"hour": "2024-01-01 03", "surface": "feed",   "dau": 1350},
    {"hour": "2024-01-01 00", "surface": "search", "dau": None},
    {"hour": "2024-01-01 01", "surface": "search", "dau": 800},
    {"hour": "2024-01-01 02", "surface": "search", "dau": None},
    {"hour": "2024-01-01 03", "surface": "search", "dau": 820},
    {"hour": "2024-01-01 04", "surface": "search", "dau": None},
]

Expected Output:
[
    {"hour": "2024-01-01 00", "surface": "feed",   "dau": 1200},
    {"hour": "2024-01-01 01", "surface": "feed",   "dau": 1200},
    {"hour": "2024-01-01 02", "surface": "feed",   "dau": 1200},
    {"hour": "2024-01-01 03", "surface": "feed",   "dau": 1350},
    {"hour": "2024-01-01 00", "surface": "search", "dau": None},
    {"hour": "2024-01-01 01", "surface": "search", "dau": 800},
    {"hour": "2024-01-01 02", "surface": "search", "dau": 800},
    {"hour": "2024-01-01 03", "surface": "search", "dau": 820},
    {"hour": "2024-01-01 04", "surface": "search", "dau": 820},
]

Follow-up: Implement backward-fill for leading nulls (fill with the
first non-null value in the same surface's series).
"""

def forward_fill(rows):
    last_val = {}
    result = []
    for r in rows:
        key = r["surface"]
        if r["dau"] is not None:
            last_val[key] = r["dau"]
            result.append(dict(r))
        else:
            filled = last_val.get(key, None)
            result.append({**r, "dau": filled})
    return result


def test_053():
    rows = [
        {"hour": "2024-01-01 00", "surface": "feed",   "dau": 1200},
        {"hour": "2024-01-01 01", "surface": "feed",   "dau": None},
        {"hour": "2024-01-01 02", "surface": "feed",   "dau": None},
        {"hour": "2024-01-01 03", "surface": "feed",   "dau": 1350},
        {"hour": "2024-01-01 00", "surface": "search", "dau": None},
        {"hour": "2024-01-01 01", "surface": "search", "dau": 800},
        {"hour": "2024-01-01 02", "surface": "search", "dau": None},
        {"hour": "2024-01-01 03", "surface": "search", "dau": 820},
        {"hour": "2024-01-01 04", "surface": "search", "dau": None},
    ]
    out = forward_fill(rows)
    assert out[1]["dau"] == 1200
    assert out[2]["dau"] == 1200
    assert out[4]["dau"] is None   # leading null — no prior value
    assert out[6]["dau"] == 800
    assert out[8]["dau"] == 820
    print("053 PASS")

if __name__ == "__main__":
    test_053()


# ─────────────────────────────────────────────────────────────
# Problem 054: Detect and Flag Schema Drift in Pipeline Records
# ─────────────────────────────────────────────────────────────
"""
Problem 054: Detect and Flag Schema Drift in Pipeline Records
Difficulty: Medium
Category: Data Cleaning & Transformation
Tags: schema-validation, schema-drift, pipeline-quality, dict

Scenario:
Your ingestion pipeline loads JSON records from a partner API. A
known baseline schema defines required fields and their expected
Python types. As the API evolves, new records may be missing fields,
add unexpected fields, or send wrong types. Flag each record with
a list of drift issues; records with no issues get an empty list.

Input:
baseline_schema: dict mapping field_name -> expected_type (Python type)
records: list of dicts (raw records from the API)

Sample Input:
baseline_schema = {
    "event_id": str,
    "user_id": str,
    "timestamp": int,
    "amount": float,
    "status": str,
}
records = [
    {"event_id": "e1", "user_id": "u1", "timestamp": 1700000001, "amount": 9.99,  "status": "ok"},
    {"event_id": "e2", "user_id": "u2", "timestamp": 1700000002, "amount": "bad", "status": "ok"},
    {"event_id": "e3", "user_id": "u3", "timestamp": 1700000003,                  "status": "ok"},
    {"event_id": "e4", "user_id": "u4", "timestamp": 1700000004, "amount": 5.00,  "status": "ok", "extra_field": True},
    {"event_id": "e5",                  "timestamp": 1700000005, "amount": 12.0,  "status": "ok"},
    {"event_id": "e6", "user_id": "u6", "timestamp": "not-int",  "amount": 3.50,  "status": "ok"},
    {"event_id": "e7", "user_id": "u7", "timestamp": 1700000007, "amount": 7.25,  "status": 404},
    {"event_id": "e8", "user_id": "u8", "timestamp": 1700000008, "amount": 0.0,   "status": "ok"},
]

Expected Output:
[
    {"event_id": "e1", "drift_issues": []},
    {"event_id": "e2", "drift_issues": ["wrong_type:amount"]},
    {"event_id": "e3", "drift_issues": ["missing_field:amount"]},
    {"event_id": "e4", "drift_issues": ["unexpected_field:extra_field"]},
    {"event_id": "e5", "drift_issues": ["missing_field:user_id"]},
    {"event_id": "e6", "drift_issues": ["wrong_type:timestamp"]},
    {"event_id": "e7", "drift_issues": ["wrong_type:status"]},
    {"event_id": "e8", "drift_issues": []},
]

Follow-up: Instead of stopping at first issue per field, collect ALL
issues per record and add a severity level ("error" vs "warning")
where unexpected_field is a warning and all others are errors.
"""

def detect_schema_drift(baseline_schema, records):
    result = []
    for r in records:
        issues = []
        for field, expected_type in baseline_schema.items():
            if field not in r:
                issues.append(f"missing_field:{field}")
            elif not isinstance(r[field], expected_type):
                issues.append(f"wrong_type:{field}")
        for field in r:
            if field not in baseline_schema:
                issues.append(f"unexpected_field:{field}")
        result.append({"event_id": r.get("event_id", "unknown"), "drift_issues": issues})
    return result


def test_054():
    baseline_schema = {
        "event_id": str,
        "user_id": str,
        "timestamp": int,
        "amount": float,
        "status": str,
    }
    records = [
        {"event_id": "e1", "user_id": "u1", "timestamp": 1700000001, "amount": 9.99,  "status": "ok"},
        {"event_id": "e2", "user_id": "u2", "timestamp": 1700000002, "amount": "bad", "status": "ok"},
        {"event_id": "e3", "user_id": "u3", "timestamp": 1700000003,                  "status": "ok"},
        {"event_id": "e4", "user_id": "u4", "timestamp": 1700000004, "amount": 5.00,  "status": "ok", "extra_field": True},
        {"event_id": "e5",                  "timestamp": 1700000005, "amount": 12.0,  "status": "ok"},
        {"event_id": "e6", "user_id": "u6", "timestamp": "not-int",  "amount": 3.50,  "status": "ok"},
        {"event_id": "e7", "user_id": "u7", "timestamp": 1700000007, "amount": 7.25,  "status": 404},
        {"event_id": "e8", "user_id": "u8", "timestamp": 1700000008, "amount": 0.0,   "status": "ok"},
    ]
    out = detect_schema_drift(baseline_schema, records)
    assert out[0]["drift_issues"] == []
    assert "wrong_type:amount" in out[1]["drift_issues"]
    assert "missing_field:amount" in out[2]["drift_issues"]
    assert "unexpected_field:extra_field" in out[3]["drift_issues"]
    assert "missing_field:user_id" in out[4]["drift_issues"]
    assert out[7]["drift_issues"] == []
    print("054 PASS")

if __name__ == "__main__":
    test_054()


# ─────────────────────────────────────────────────────────────
# Problem 055: Standardize Country Codes to ISO 3166-1 Alpha-2
# ─────────────────────────────────────────────────────────────
"""
Problem 055: Standardize Country Codes to ISO 3166-1 Alpha-2
Difficulty: Easy
Category: Data Cleaning & Transformation
Tags: normalization, lookup-table, string-cleaning, geo

Scenario:
An ad-serving log joins user records from three regional databases.
Each database uses a different convention for country: full name,
alpha-3 code, or alpha-2 code. You need to normalize everything to
ISO 3166-1 alpha-2. Records with unrecognizable country values should
have country set to "XX".

Input:
List of dicts with keys: impression_id (str), country (str).

Sample Input:
impressions = [
    {"impression_id": "i01", "country": "United States"},
    {"impression_id": "i02", "country": "USA"},
    {"impression_id": "i03", "country": "US"},
    {"impression_id": "i04", "country": "germany"},
    {"impression_id": "i05", "country": "DEU"},
    {"impression_id": "i06", "country": "DE"},
    {"impression_id": "i07", "country": "Japan"},
    {"impression_id": "i08", "country": "JPN"},
    {"impression_id": "i09", "country": "unknown_country"},
    {"impression_id": "i10", "country": "  GBR  "},
    {"impression_id": "i11", "country": "United Kingdom"},
    {"impression_id": "i12", "country": ""},
]

Expected Output:
[
    {"impression_id": "i01", "country": "US"},
    {"impression_id": "i02", "country": "US"},
    {"impression_id": "i03", "country": "US"},
    {"impression_id": "i04", "country": "DE"},
    {"impression_id": "i05", "country": "DE"},
    {"impression_id": "i06", "country": "DE"},
    {"impression_id": "i07", "country": "JP"},
    {"impression_id": "i08", "country": "JP"},
    {"impression_id": "i09", "country": "XX"},
    {"impression_id": "i10", "country": "GB"},
    {"impression_id": "i11", "country": "GB"},
    {"impression_id": "i12", "country": "XX"},
]

Follow-up: Add a "normalization_source" field: "passthrough", "alpha3",
"full_name", or "unknown" to track which mapping path was used.
"""

COUNTRY_MAP = {
    # alpha-2 passthrough
    "US": "US", "DE": "DE", "JP": "JP", "GB": "GB",
    # alpha-3
    "USA": "US", "DEU": "DE", "JPN": "JP", "GBR": "GB",
    # full names (lower-cased for matching)
    "united states": "US", "germany": "DE", "japan": "JP", "united kingdom": "GB",
}

def standardize_countries(impressions):
    result = []
    for imp in impressions:
        raw = imp["country"].strip()
        normalized = (
            COUNTRY_MAP.get(raw)
            or COUNTRY_MAP.get(raw.upper())
            or COUNTRY_MAP.get(raw.lower())
            or "XX"
        )
        result.append({"impression_id": imp["impression_id"], "country": normalized})
    return result


def test_055():
    impressions = [
        {"impression_id": "i01", "country": "United States"},
        {"impression_id": "i02", "country": "USA"},
        {"impression_id": "i03", "country": "US"},
        {"impression_id": "i04", "country": "germany"},
        {"impression_id": "i05", "country": "DEU"},
        {"impression_id": "i06", "country": "DE"},
        {"impression_id": "i07", "country": "Japan"},
        {"impression_id": "i08", "country": "JPN"},
        {"impression_id": "i09", "country": "unknown_country"},
        {"impression_id": "i10", "country": "  GBR  "},
        {"impression_id": "i11", "country": "United Kingdom"},
        {"impression_id": "i12", "country": ""},
    ]
    out = standardize_countries(impressions)
    assert all(r["country"] == "US" for r in out[:3])
    assert all(r["country"] == "DE" for r in out[3:6])
    assert out[8]["country"] == "XX"
    assert out[9]["country"] == "GB"
    assert out[11]["country"] == "XX"
    print("055 PASS")

if __name__ == "__main__":
    test_055()


# ─────────────────────────────────────────────────────────────
# Problem 056: Cap Outlier Values Using IQR Winsorization
# ─────────────────────────────────────────────────────────────
"""
Problem 056: Cap Outlier Values Using IQR Winsorization
Difficulty: Medium
Category: Data Cleaning & Transformation
Tags: outliers, statistics, winsorization, numeric-cleaning

Scenario:
A revenue analytics pipeline processes ad spend records. Extreme
values (bot traffic, misconfigured campaigns) inflate averages. You
must winsorize each campaign's spend values: cap any value below
Q1 - 1.5*IQR to that lower fence, and any value above Q3 + 1.5*IQR
to that upper fence, returning the capped records.

Input:
List of dicts with keys: campaign_id (str), record_id (str),
spend (float).  All records for one campaign are processed together.

Sample Input:
records = [
    {"campaign_id": "c1", "record_id": "r01", "spend": 10.0},
    {"campaign_id": "c1", "record_id": "r02", "spend": 12.0},
    {"campaign_id": "c1", "record_id": "r03", "spend": 11.0},
    {"campaign_id": "c1", "record_id": "r04", "spend": 13.0},
    {"campaign_id": "c1", "record_id": "r05", "spend": 9.0},
    {"campaign_id": "c1", "record_id": "r06", "spend": 500.0},
    {"campaign_id": "c1", "record_id": "r07", "spend": 14.0},
    {"campaign_id": "c1", "record_id": "r08", "spend": 0.1},
    {"campaign_id": "c1", "record_id": "r09", "spend": 11.5},
    {"campaign_id": "c1", "record_id": "r10", "spend": 12.5},
]

Expected Output (r06 and r08 capped):
Spend values for r01-r05, r07, r09, r10 unchanged.
r06 capped to upper_fence, r08 capped to lower_fence.
(verify by checking r06 spend < 500 and r08 spend > 0.1)

Follow-up: Return a summary dict per campaign: {"n_capped_low": int,
"n_capped_high": int, "lower_fence": float, "upper_fence": float}.
"""

def winsorize(records):
    from collections import defaultdict

    by_campaign = defaultdict(list)
    for r in records:
        by_campaign[r["campaign_id"]].append(r)

    result = []
    for campaign_id, recs in by_campaign.items():
        values = sorted(r["spend"] for r in recs)
        n = len(values)
        q1 = values[n // 4]
        q3 = values[(3 * n) // 4]
        iqr = q3 - q1
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        for r in recs:
            capped = max(lower, min(upper, r["spend"]))
            result.append({**r, "spend": capped})
    return result


def test_056():
    records = [
        {"campaign_id": "c1", "record_id": "r01", "spend": 10.0},
        {"campaign_id": "c1", "record_id": "r02", "spend": 12.0},
        {"campaign_id": "c1", "record_id": "r03", "spend": 11.0},
        {"campaign_id": "c1", "record_id": "r04", "spend": 13.0},
        {"campaign_id": "c1", "record_id": "r05", "spend": 9.0},
        {"campaign_id": "c1", "record_id": "r06", "spend": 500.0},
        {"campaign_id": "c1", "record_id": "r07", "spend": 14.0},
        {"campaign_id": "c1", "record_id": "r08", "spend": 0.1},
        {"campaign_id": "c1", "record_id": "r09", "spend": 11.5},
        {"campaign_id": "c1", "record_id": "r10", "spend": 12.5},
    ]
    out = winsorize(records)
    by_id = {r["record_id"]: r["spend"] for r in out}
    assert by_id["r06"] < 500.0, "r06 should be capped"
    assert by_id["r08"] > 0.1,   "r08 should be capped"
    assert by_id["r01"] == 10.0, "r01 unchanged"
    print("056 PASS")

if __name__ == "__main__":
    test_056()


# ─────────────────────────────────────────────────────────────
# Problem 057: Pivot Event Counts into a Wide Table
# ─────────────────────────────────────────────────────────────
"""
Problem 057: Pivot Event Counts into a Wide Table
Difficulty: Medium
Category: Data Cleaning & Transformation
Tags: pivot, reshape, aggregation, dict

Scenario:
A content-safety metrics job produces a long-format table of daily
event counts per user per violation type. An analyst dashboard needs
this in wide format: one row per (user_id, date) with each violation
type as a column. Missing combinations should default to 0.

Input:
List of dicts: user_id (str), date (str), violation_type (str),
count (int).

Sample Input:
rows = [
    {"user_id": "u1", "date": "2024-01-01", "violation_type": "hate_speech",    "count": 3},
    {"user_id": "u1", "date": "2024-01-01", "violation_type": "spam",           "count": 7},
    {"user_id": "u1", "date": "2024-01-02", "violation_type": "hate_speech",    "count": 1},
    {"user_id": "u2", "date": "2024-01-01", "violation_type": "spam",           "count": 2},
    {"user_id": "u2", "date": "2024-01-01", "violation_type": "misinformation", "count": 4},
    {"user_id": "u2", "date": "2024-01-02", "violation_type": "spam",           "count": 5},
    {"user_id": "u3", "date": "2024-01-01", "violation_type": "hate_speech",    "count": 0},
    {"user_id": "u3", "date": "2024-01-02", "violation_type": "misinformation", "count": 2},
]

Expected Output (sorted by user_id, date):
[
    {"user_id": "u1", "date": "2024-01-01", "hate_speech": 3, "misinformation": 0, "spam": 7},
    {"user_id": "u1", "date": "2024-01-02", "hate_speech": 1, "misinformation": 0, "spam": 0},
    {"user_id": "u2", "date": "2024-01-01", "hate_speech": 0, "misinformation": 4, "spam": 2},
    {"user_id": "u2", "date": "2024-01-02", "hate_speech": 0, "misinformation": 0, "spam": 5},
    {"user_id": "u3", "date": "2024-01-01", "hate_speech": 0, "misinformation": 0, "spam": 0},
    {"user_id": "u3", "date": "2024-01-02", "hate_speech": 0, "misinformation": 2, "spam": 0},
]

Follow-up: Add a "total_violations" column that sums all violation
type counts per (user_id, date) row.
"""

def pivot_event_counts(rows):
    from collections import defaultdict
    all_types = sorted({r["violation_type"] for r in rows})
    pivot = defaultdict(lambda: defaultdict(int))
    for r in rows:
        pivot[(r["user_id"], r["date"])][r["violation_type"]] += r["count"]
    result = []
    for (user_id, date), counts in sorted(pivot.items()):
        row = {"user_id": user_id, "date": date}
        for vtype in all_types:
            row[vtype] = counts.get(vtype, 0)
        result.append(row)
    return result


def test_057():
    rows = [
        {"user_id": "u1", "date": "2024-01-01", "violation_type": "hate_speech",    "count": 3},
        {"user_id": "u1", "date": "2024-01-01", "violation_type": "spam",           "count": 7},
        {"user_id": "u1", "date": "2024-01-02", "violation_type": "hate_speech",    "count": 1},
        {"user_id": "u2", "date": "2024-01-01", "violation_type": "spam",           "count": 2},
        {"user_id": "u2", "date": "2024-01-01", "violation_type": "misinformation", "count": 4},
        {"user_id": "u2", "date": "2024-01-02", "violation_type": "spam",           "count": 5},
        {"user_id": "u3", "date": "2024-01-01", "violation_type": "hate_speech",    "count": 0},
        {"user_id": "u3", "date": "2024-01-02", "violation_type": "misinformation", "count": 2},
    ]
    out = pivot_event_counts(rows)
    assert len(out) == 6
    u1_d1 = out[0]
    assert u1_d1["hate_speech"] == 3 and u1_d1["spam"] == 7 and u1_d1["misinformation"] == 0
    u3_d1 = out[4]
    assert u3_d1["hate_speech"] == 0 and u3_d1["spam"] == 0
    print("057 PASS")

if __name__ == "__main__":
    test_057()


# ─────────────────────────────────────────────────────────────
# Problem 058: Parse and Validate ISO 8601 Timestamps
# ─────────────────────────────────────────────────────────────
"""
Problem 058: Parse and Validate ISO 8601 Timestamps
Difficulty: Medium
Category: Data Cleaning & Transformation
Tags: datetime-parsing, validation, type-coercion, pipeline

Scenario:
Event records ingested from mobile apps sometimes contain malformed
or timezone-ambiguous timestamps. Parse each timestamp string into a
(date_str, hour, is_utc) tuple. Strings that cannot be parsed should
produce (None, None, None). Accept formats: "YYYY-MM-DDTHH:MM:SSZ",
"YYYY-MM-DDTHH:MM:SS+HH:MM", "YYYY-MM-DD HH:MM:SS" (assume UTC),
and "YYYY-MM-DDTHH:MM:SS" (assume UTC).

Input:
List of dicts: event_id (str), ts (str raw timestamp string).

Sample Input:
events = [
    {"event_id": "e1",  "ts": "2024-03-15T08:30:00Z"},
    {"event_id": "e2",  "ts": "2024-03-15T08:30:00+05:30"},
    {"event_id": "e3",  "ts": "2024-03-15 14:00:00"},
    {"event_id": "e4",  "ts": "2024-03-15T22:00:00"},
    {"event_id": "e5",  "ts": "not-a-timestamp"},
    {"event_id": "e6",  "ts": "2024-13-01T00:00:00Z"},
    {"event_id": "e7",  "ts": "2024-03-15T25:00:00Z"},
    {"event_id": "e8",  "ts": "2024-03-15T00:00:00+00:00"},
    {"event_id": "e9",  "ts": "2024/03/15 08:00:00"},
    {"event_id": "e10", "ts": "2024-03-15T08:30:00-07:00"},
]

Expected Output:
[
    {"event_id": "e1",  "date": "2024-03-15", "hour": 8,  "is_utc": True},
    {"event_id": "e2",  "date": "2024-03-15", "hour": 8,  "is_utc": False},
    {"event_id": "e3",  "date": "2024-03-15", "hour": 14, "is_utc": True},
    {"event_id": "e4",  "date": "2024-03-15", "hour": 22, "is_utc": True},
    {"event_id": "e5",  "date": None,          "hour": None, "is_utc": None},
    {"event_id": "e6",  "date": None,          "hour": None, "is_utc": None},
    {"event_id": "e7",  "date": None,          "hour": None, "is_utc": None},
    {"event_id": "e8",  "date": "2024-03-15", "hour": 0,  "is_utc": False},
    {"event_id": "e9",  "date": None,          "hour": None, "is_utc": None},
    {"event_id": "e10", "date": "2024-03-15", "hour": 8,  "is_utc": False},
]

Follow-up: Convert all non-UTC timestamps to UTC by applying the
offset, so every output row carries a unified UTC date and hour.
"""

from datetime import datetime

def parse_timestamps(events):
    result = []
    formats = [
        ("%Y-%m-%dT%H:%M:%SZ", True),
        ("%Y-%m-%dT%H:%M:%S", True),   # no tz — assume UTC
        ("%Y-%m-%d %H:%M:%S", True),   # space separator — assume UTC
    ]
    for ev in events:
        ts = ev["ts"]
        parsed_date, parsed_hour, is_utc = None, None, None
        # Check for offset format: ends with +HH:MM or -HH:MM
        if len(ts) > 6 and ts[-6] in ("+", "-") and ts[-3] == ":":
            body = ts[:-6]
            try:
                dt = datetime.strptime(body, "%Y-%m-%dT%H:%M:%S")
                parsed_date = dt.strftime("%Y-%m-%d")
                parsed_hour = dt.hour
                is_utc = False
            except ValueError:
                pass
        else:
            for fmt, utc in formats:
                try:
                    dt = datetime.strptime(ts, fmt)
                    parsed_date = dt.strftime("%Y-%m-%d")
                    parsed_hour = dt.hour
                    is_utc = utc
                    break
                except ValueError:
                    continue
        result.append({
            "event_id": ev["event_id"],
            "date": parsed_date,
            "hour": parsed_hour,
            "is_utc": is_utc,
        })
    return result


def test_058():
    events = [
        {"event_id": "e1",  "ts": "2024-03-15T08:30:00Z"},
        {"event_id": "e2",  "ts": "2024-03-15T08:30:00+05:30"},
        {"event_id": "e3",  "ts": "2024-03-15 14:00:00"},
        {"event_id": "e4",  "ts": "2024-03-15T22:00:00"},
        {"event_id": "e5",  "ts": "not-a-timestamp"},
        {"event_id": "e6",  "ts": "2024-13-01T00:00:00Z"},
        {"event_id": "e7",  "ts": "2024-03-15T25:00:00Z"},
        {"event_id": "e8",  "ts": "2024-03-15T00:00:00+00:00"},
        {"event_id": "e9",  "ts": "2024/03/15 08:00:00"},
        {"event_id": "e10", "ts": "2024-03-15T08:30:00-07:00"},
    ]
    out = parse_timestamps(events)
    assert out[0] == {"event_id": "e1", "date": "2024-03-15", "hour": 8, "is_utc": True}
    assert out[4]["date"] is None
    assert out[5]["date"] is None
    assert out[8]["date"] is None
    assert out[7]["is_utc"] == False
    print("058 PASS")

if __name__ == "__main__":
    test_058()


# ─────────────────────────────────────────────────────────────
# Problem 059: Merge Slow-Changing Dimension Records (SCD2-like)
# ─────────────────────────────────────────────────────────────
"""
Problem 059: Merge Slow-Changing Dimension Records (SCD2-like)
Difficulty: Medium
Category: Data Cleaning & Transformation
Tags: SCD, merge, effective-dates, data-modeling

Scenario:
A user dimension table uses SCD Type 2 to track attribute changes.
You receive a list of "current" dim records (with valid_from and
valid_to=None for active records) and a list of "incoming" updates.
For each incoming update where the attribute has changed, close the
existing active record (set valid_to = update date) and insert a new
active record. Unchanged users pass through unmodified.

Input:
current: list of dicts — user_id, email, plan, valid_from (str YYYY-MM-DD), valid_to (str or None)
updates: list of dicts — user_id, email, plan, effective_date (str YYYY-MM-DD)

Sample Input:
current = [
    {"user_id": "u1", "email": "a@x.com", "plan": "free",  "valid_from": "2023-01-01", "valid_to": None},
    {"user_id": "u2", "email": "b@x.com", "plan": "pro",   "valid_from": "2023-06-01", "valid_to": None},
    {"user_id": "u3", "email": "c@x.com", "plan": "free",  "valid_from": "2023-03-01", "valid_to": None},
    {"user_id": "u4", "email": "d@x.com", "plan": "enterprise", "valid_from": "2022-01-01", "valid_to": None},
]
updates = [
    {"user_id": "u1", "email": "a@x.com", "plan": "pro",   "effective_date": "2024-01-15"},
    {"user_id": "u2", "email": "b@x.com", "plan": "pro",   "effective_date": "2024-01-15"},  # no change
    {"user_id": "u3", "email": "c2@x.com","plan": "free",  "effective_date": "2024-01-15"},  # email change
    {"user_id": "u5", "email": "e@x.com", "plan": "free",  "effective_date": "2024-01-15"},  # new user
]

Expected Output (5 records total, sorted by user_id then valid_from):
u1: old record closed (valid_to="2024-01-15"), new record open (plan="pro", valid_from="2024-01-15")
u2: unchanged (no new record)
u3: old record closed, new record with c2@x.com
u4: unchanged
u5: new record inserted

Follow-up: Support "delete" updates where a user is marked inactive;
set valid_to on the active record without inserting a new one.
"""

def scd2_merge(current, updates):
    current_active = {r["user_id"]: dict(r) for r in current if r["valid_to"] is None}
    result = [dict(r) for r in current if r["valid_to"] is not None]

    for upd in updates:
        uid = upd["user_id"]
        eff = upd["effective_date"]
        if uid in current_active:
            active = current_active[uid]
            changed = (active["email"] != upd["email"] or active["plan"] != upd["plan"])
            if changed:
                closed = dict(active)
                closed["valid_to"] = eff
                result.append(closed)
                new_rec = {"user_id": uid, "email": upd["email"], "plan": upd["plan"],
                           "valid_from": eff, "valid_to": None}
                result.append(new_rec)
                current_active.pop(uid)
            # else: no change — will be added from current_active below
        else:
            # brand new user
            result.append({"user_id": uid, "email": upd["email"], "plan": upd["plan"],
                           "valid_from": eff, "valid_to": None})

    # Add unchanged active records
    for uid, rec in current_active.items():
        result.append(rec)

    return sorted(result, key=lambda x: (x["user_id"], x["valid_from"]))


def test_059():
    current = [
        {"user_id": "u1", "email": "a@x.com", "plan": "free",       "valid_from": "2023-01-01", "valid_to": None},
        {"user_id": "u2", "email": "b@x.com", "plan": "pro",        "valid_from": "2023-06-01", "valid_to": None},
        {"user_id": "u3", "email": "c@x.com", "plan": "free",       "valid_from": "2023-03-01", "valid_to": None},
        {"user_id": "u4", "email": "d@x.com", "plan": "enterprise", "valid_from": "2022-01-01", "valid_to": None},
    ]
    updates = [
        {"user_id": "u1", "email": "a@x.com",  "plan": "pro",  "effective_date": "2024-01-15"},
        {"user_id": "u2", "email": "b@x.com",  "plan": "pro",  "effective_date": "2024-01-15"},
        {"user_id": "u3", "email": "c2@x.com", "plan": "free", "effective_date": "2024-01-15"},
        {"user_id": "u5", "email": "e@x.com",  "plan": "free", "effective_date": "2024-01-15"},
    ]
    out = scd2_merge(current, updates)
    by_user = {}
    for r in out:
        by_user.setdefault(r["user_id"], []).append(r)

    # u1: 2 records (old closed, new open)
    assert len(by_user["u1"]) == 2
    assert by_user["u1"][0]["valid_to"] == "2024-01-15"
    assert by_user["u1"][1]["valid_to"] is None and by_user["u1"][1]["plan"] == "pro"

    # u2: 1 record (no change)
    assert len(by_user["u2"]) == 1
    assert by_user["u2"][0]["valid_to"] is None

    # u3: 2 records
    assert len(by_user["u3"]) == 2

    # u4: untouched
    assert len(by_user["u4"]) == 1

    # u5: new
    assert len(by_user["u5"]) == 1 and by_user["u5"][0]["valid_from"] == "2024-01-15"

    print("059 PASS")

if __name__ == "__main__":
    test_059()


# ─────────────────────────────────────────────────────────────
# Problem 060: Tokenize and Frequency-Count Abuse Report Text
# ─────────────────────────────────────────────────────────────
"""
Problem 060: Tokenize and Frequency-Count Abuse Report Text
Difficulty: Easy
Category: Data Cleaning & Transformation
Tags: text-processing, tokenization, counter, safety

Scenario:
A Trust & Safety team ingests free-text abuse reports and wants to
find the most frequently mentioned terms (excluding stopwords) across
a batch of reports. Tokenize by splitting on whitespace and
punctuation, lowercase all tokens, remove stopwords, and return the
top-N tokens with their counts.

Input:
reports: list of str (raw report text)
stopwords: set of str
top_n: int

Sample Input:
reports = [
    "This account is spamming me with unsolicited messages every day",
    "Spam account keeps sending me advertising messages and spam links",
    "Reported for hate speech and harassing behavior targeting my account",
    "Account sends spam and phishing links to steal credentials",
    "Harassing messages with hate speech content directed at users",
    "Phishing attempt via direct messages trying to steal my password",
    "Account was reported for spam and multiple policy violations",
    "Spam spam spam this account floods my inbox with ads",
]
stopwords = {"this", "is", "me", "with", "and", "my", "for", "to", "at",
             "was", "via", "every", "trying", "keeps", "that", "i", "a", "the"}
top_n = 5

Expected Output (top 5 by count):
[
    ("spam", 9),
    ("account", 6),
    ("messages", 4),
    ("hate", 2),
    ("speech", 2),
]
(ties broken alphabetically)

Follow-up: Group term frequencies by report_id so you can see which
individual reports are most lexically similar using term overlap.
"""

import string
from collections import Counter

def top_terms(reports, stopwords, top_n):
    counts = Counter()
    for text in reports:
        tokens = text.lower().translate(str.maketrans("", "", string.punctuation)).split()
        for t in tokens:
            if t not in stopwords:
                counts[t] += 1
    # Sort by count desc, then alphabetically for ties
    ranked = sorted(counts.items(), key=lambda x: (-x[1], x[0]))
    return ranked[:top_n]


def test_060():
    reports = [
        "This account is spamming me with unsolicited messages every day",
        "Spam account keeps sending me advertising messages and spam links",
        "Reported for hate speech and harassing behavior targeting my account",
        "Account sends spam and phishing links to steal credentials",
        "Harassing messages with hate speech content directed at users",
        "Phishing attempt via direct messages trying to steal my password",
        "Account was reported for spam and multiple policy violations",
        "Spam spam spam this account floods my inbox with ads",
    ]
    stopwords = {"this", "is", "me", "with", "and", "my", "for", "to", "at",
                 "was", "via", "every", "trying", "keeps", "that", "i", "a", "the"}
    out = top_terms(reports, stopwords, 5)
    assert out[0][0] == "spam"
    assert out[1][0] == "account"
    assert len(out) == 5
    print("060 PASS")

if __name__ == "__main__":
    test_060()


# ─────────────────────────────────────────────────────────────
# Problem 061: Compute Rolling 3-Day Retention Rate
# ─────────────────────────────────────────────────────────────
"""
Problem 061: Compute Rolling 3-Day Retention Rate
Difficulty: Medium
Category: Data Cleaning & Transformation
Tags: retention, cohort, rolling-window, date-arithmetic

Scenario:
A product analytics pipeline needs D3 retention: for each cohort
(signup date), what fraction of users who signed up on that date also
had an active event within 3 days (days 1, 2, or 3 after signup)?
Output one row per cohort date with cohort_size and retention_rate.

Input:
signups: list of dicts — user_id (str), signup_date (str YYYY-MM-DD)
activity: list of dicts — user_id (str), active_date (str YYYY-MM-DD)

Sample Input:
signups = [
    {"user_id": "u01", "signup_date": "2024-01-01"},
    {"user_id": "u02", "signup_date": "2024-01-01"},
    {"user_id": "u03", "signup_date": "2024-01-01"},
    {"user_id": "u04", "signup_date": "2024-01-02"},
    {"user_id": "u05", "signup_date": "2024-01-02"},
    {"user_id": "u06", "signup_date": "2024-01-02"},
    {"user_id": "u07", "signup_date": "2024-01-03"},
    {"user_id": "u08", "signup_date": "2024-01-03"},
]
activity = [
    {"user_id": "u01", "active_date": "2024-01-02"},
    {"user_id": "u01", "active_date": "2024-01-04"},
    {"user_id": "u02", "active_date": "2024-01-05"},
    {"user_id": "u03", "active_date": "2024-01-03"},
    {"user_id": "u04", "active_date": "2024-01-03"},
    {"user_id": "u05", "active_date": "2024-01-06"},
    {"user_id": "u07", "active_date": "2024-01-04"},
    {"user_id": "u08", "active_date": "2024-01-07"},
]

Expected Output (sorted by cohort_date):
[
    {"cohort_date": "2024-01-01", "cohort_size": 3, "retained": 2, "retention_rate": 0.6667},
    {"cohort_date": "2024-01-02", "cohort_size": 3, "retained": 1, "retention_rate": 0.3333},
    {"cohort_date": "2024-01-03", "cohort_size": 2, "retained": 1, "retention_rate": 0.5},
]
(rates rounded to 4 decimal places)

Follow-up: Generalize to DN retention (pass N as a parameter) and
support both "within N days" and "on exactly day N" semantics.
"""

from datetime import date, timedelta
from collections import defaultdict

def d3_retention(signups, activity):
    # Build a set of (user_id, active_date) for fast lookup
    active_set = {(r["user_id"], r["active_date"]) for r in activity}

    cohorts = defaultdict(list)
    for s in signups:
        cohorts[s["signup_date"]].append(s["user_id"])

    result = []
    for cohort_date in sorted(cohorts):
        users = cohorts[cohort_date]
        base = date.fromisoformat(cohort_date)
        retained = 0
        for uid in users:
            for d_offset in range(1, 4):
                check = str(base + timedelta(days=d_offset))
                if (uid, check) in active_set:
                    retained += 1
                    break
        n = len(users)
        result.append({
            "cohort_date": cohort_date,
            "cohort_size": n,
            "retained": retained,
            "retention_rate": round(retained / n, 4),
        })
    return result


def test_061():
    signups = [
        {"user_id": "u01", "signup_date": "2024-01-01"},
        {"user_id": "u02", "signup_date": "2024-01-01"},
        {"user_id": "u03", "signup_date": "2024-01-01"},
        {"user_id": "u04", "signup_date": "2024-01-02"},
        {"user_id": "u05", "signup_date": "2024-01-02"},
        {"user_id": "u06", "signup_date": "2024-01-02"},
        {"user_id": "u07", "signup_date": "2024-01-03"},
        {"user_id": "u08", "signup_date": "2024-01-03"},
    ]
    activity = [
        {"user_id": "u01", "active_date": "2024-01-02"},
        {"user_id": "u01", "active_date": "2024-01-04"},
        {"user_id": "u02", "active_date": "2024-01-05"},
        {"user_id": "u03", "active_date": "2024-01-03"},
        {"user_id": "u04", "active_date": "2024-01-03"},
        {"user_id": "u05", "active_date": "2024-01-06"},
        {"user_id": "u07", "active_date": "2024-01-04"},
        {"user_id": "u08", "active_date": "2024-01-07"},
    ]
    out = d3_retention(signups, activity)
    assert out[0] == {"cohort_date": "2024-01-01", "cohort_size": 3, "retained": 2, "retention_rate": 0.6667}
    assert out[1]["retained"] == 1
    assert out[2]["retention_rate"] == 0.5
    print("061 PASS")

if __name__ == "__main__":
    test_061()


# ─────────────────────────────────────────────────────────────
# Problem 062: Reconstruct Funnel Conversion from Step Events
# ─────────────────────────────────────────────────────────────
"""
Problem 062: Reconstruct Funnel Conversion from Step Events
Difficulty: Medium
Category: Data Cleaning & Transformation
Tags: funnel, conversion, ordering, product-analytics

Scenario:
An e-commerce checkout funnel has steps: "view_cart" → "enter_shipping"
→ "enter_payment" → "confirm_order". Given a flat list of user step
events, determine for each user the furthest step they reached (in
order). Users who skipped steps or who only have events outside the
funnel are treated as having reached the step just before the gap.

Input:
List of dicts: user_id (str), step (str), timestamp (int).

Sample Input:
events = [
    {"user_id": "u1", "step": "view_cart",      "timestamp": 1000},
    {"user_id": "u1", "step": "enter_shipping", "timestamp": 1010},
    {"user_id": "u1", "step": "enter_payment",  "timestamp": 1020},
    {"user_id": "u1", "step": "confirm_order",  "timestamp": 1030},
    {"user_id": "u2", "step": "view_cart",      "timestamp": 2000},
    {"user_id": "u2", "step": "enter_shipping", "timestamp": 2010},
    {"user_id": "u3", "step": "view_cart",      "timestamp": 3000},
    {"user_id": "u3", "step": "enter_payment",  "timestamp": 3020},
    {"user_id": "u4", "step": "confirm_order",  "timestamp": 4000},
    {"user_id": "u5", "step": "view_cart",      "timestamp": 5000},
    {"user_id": "u5", "step": "enter_shipping", "timestamp": 5010},
    {"user_id": "u5", "step": "enter_payment",  "timestamp": 5020},
]

Funnel order: ["view_cart", "enter_shipping", "enter_payment", "confirm_order"]

Expected Output (sorted by user_id):
[
    {"user_id": "u1", "max_step": "confirm_order",  "step_index": 3},
    {"user_id": "u2", "max_step": "enter_shipping", "step_index": 1},
    {"user_id": "u3", "max_step": "view_cart",      "step_index": 0},
    {"user_id": "u4", "max_step": None,             "step_index": -1},
    {"user_id": "u5", "max_step": "enter_payment",  "step_index": 2},
]

Note: u3 skipped step 1 so their consecutive progress stops at view_cart.
Note: u4 has no funnel entry step so max_step=None.

Follow-up: Add a "converted" boolean field (True if step_index == 3).
"""

def funnel_max_step(events, funnel):
    step_rank = {s: i for i, s in enumerate(funnel)}
    from collections import defaultdict
    user_steps = defaultdict(set)
    for e in events:
        if e["step"] in step_rank:
            user_steps[e["user_id"]].add(e["step"])

    all_users = sorted({e["user_id"] for e in events})
    result = []
    for uid in all_users:
        steps_hit = user_steps.get(uid, set())
        # Find highest consecutive step from index 0
        max_idx = -1
        for i, step in enumerate(funnel):
            if step in steps_hit and (i == 0 or funnel[i-1] in steps_hit):
                max_idx = i
            else:
                break
        max_step = funnel[max_idx] if max_idx >= 0 else None
        result.append({"user_id": uid, "max_step": max_step, "step_index": max_idx})
    return result


def test_062():
    events = [
        {"user_id": "u1", "step": "view_cart",      "timestamp": 1000},
        {"user_id": "u1", "step": "enter_shipping", "timestamp": 1010},
        {"user_id": "u1", "step": "enter_payment",  "timestamp": 1020},
        {"user_id": "u1", "step": "confirm_order",  "timestamp": 1030},
        {"user_id": "u2", "step": "view_cart",      "timestamp": 2000},
        {"user_id": "u2", "step": "enter_shipping", "timestamp": 2010},
        {"user_id": "u3", "step": "view_cart",      "timestamp": 3000},
        {"user_id": "u3", "step": "enter_payment",  "timestamp": 3020},
        {"user_id": "u4", "step": "confirm_order",  "timestamp": 4000},
        {"user_id": "u5", "step": "view_cart",      "timestamp": 5000},
        {"user_id": "u5", "step": "enter_shipping", "timestamp": 5010},
        {"user_id": "u5", "step": "enter_payment",  "timestamp": 5020},
    ]
    funnel = ["view_cart", "enter_shipping", "enter_payment", "confirm_order"]
    out = funnel_max_step(events, funnel)
    by_user = {r["user_id"]: r for r in out}
    assert by_user["u1"]["step_index"] == 3
    assert by_user["u2"]["step_index"] == 1
    assert by_user["u3"]["step_index"] == 0
    assert by_user["u4"]["step_index"] == -1
    assert by_user["u5"]["step_index"] == 2
    print("062 PASS")

if __name__ == "__main__":
    test_062()


# ─────────────────────────────────────────────────────────────
# Problem 063: Flatten Multi-Value Tags into Normalized Rows
# ─────────────────────────────────────────────────────────────
"""
Problem 063: Flatten Multi-Value Tags into Normalized Rows
Difficulty: Easy
Category: Data Cleaning & Transformation
Tags: explode, normalization, many-to-one, content-catalog

Scenario:
A content catalog stores comma-separated genre tags in a single field.
Before loading into the warehouse fact table, each tag must become its
own row (similar to SQL UNNEST / pandas explode). Tags should be
stripped of whitespace and lowercased. Records with empty or null
tags should produce one row with tag = None.

Input:
List of dicts: content_id (str), title (str), tags (str or None).

Sample Input:
catalog = [
    {"content_id": "c01", "title": "Nature Doc",   "tags": "Nature, Wildlife, Education"},
    {"content_id": "c02", "title": "Comedy Night", "tags": "Comedy,  Stand-up"},
    {"content_id": "c03", "title": "News Hour",    "tags": "News"},
    {"content_id": "c04", "title": "Drama Series", "tags": None},
    {"content_id": "c05", "title": "Kids Show",    "tags": "Animation, Kids, Family, Comedy"},
    {"content_id": "c06", "title": "Sports Wrap",  "tags": "Sports ,  Football , Cricket"},
    {"content_id": "c07", "title": "Empty Tags",   "tags": ""},
    {"content_id": "c08", "title": "Thriller",     "tags": "Thriller, Crime, Mystery"},
]

Expected Output (13 rows total):
[
    {"content_id": "c01", "title": "Nature Doc",   "tag": "nature"},
    {"content_id": "c01", "title": "Nature Doc",   "tag": "wildlife"},
    {"content_id": "c01", "title": "Nature Doc",   "tag": "education"},
    {"content_id": "c02", "title": "Comedy Night", "tag": "comedy"},
    {"content_id": "c02", "title": "Comedy Night", "tag": "stand-up"},
    {"content_id": "c03", "title": "News Hour",    "tag": "news"},
    {"content_id": "c04", "title": "Drama Series", "tag": None},
    {"content_id": "c05", "title": "Kids Show",    "tag": "animation"},
    {"content_id": "c05", "title": "Kids Show",    "tag": "kids"},
    {"content_id": "c05", "title": "Kids Show",    "tag": "family"},
    {"content_id": "c05", "title": "Kids Show",    "tag": "comedy"},
    {"content_id": "c06", "title": "Sports Wrap",  "tag": "sports"},
    {"content_id": "c06", "title": "Sports Wrap",  "tag": "football"},
    {"content_id": "c06", "title": "Sports Wrap",  "tag": "cricket"},
    {"content_id": "c07", "title": "Empty Tags",   "tag": None},
    {"content_id": "c08", "title": "Thriller",     "tag": "thriller"},
    {"content_id": "c08", "title": "Thriller",     "tag": "crime"},
    {"content_id": "c08", "title": "Thriller",     "tag": "mystery"},
]

Follow-up: After exploding, deduplicate so that a content_id never
has the same tag twice (some upstream systems send duplicate tags).
"""

def explode_tags(catalog):
    result = []
    for item in catalog:
        raw = item["tags"]
        if not raw:
            result.append({"content_id": item["content_id"], "title": item["title"], "tag": None})
        else:
            tags = [t.strip().lower() for t in raw.split(",") if t.strip()]
            if not tags:
                result.append({"content_id": item["content_id"], "title": item["title"], "tag": None})
            else:
                for tag in tags:
                    result.append({"content_id": item["content_id"], "title": item["title"], "tag": tag})
    return result


def test_063():
    catalog = [
        {"content_id": "c01", "title": "Nature Doc",   "tags": "Nature, Wildlife, Education"},
        {"content_id": "c02", "title": "Comedy Night", "tags": "Comedy,  Stand-up"},
        {"content_id": "c03", "title": "News Hour",    "tags": "News"},
        {"content_id": "c04", "title": "Drama Series", "tags": None},
        {"content_id": "c05", "title": "Kids Show",    "tags": "Animation, Kids, Family, Comedy"},
        {"content_id": "c06", "title": "Sports Wrap",  "tags": "Sports ,  Football , Cricket"},
        {"content_id": "c07", "title": "Empty Tags",   "tags": ""},
        {"content_id": "c08", "title": "Thriller",     "tags": "Thriller, Crime, Mystery"},
    ]
    out = explode_tags(catalog)
    c01_tags = [r["tag"] for r in out if r["content_id"] == "c01"]
    assert c01_tags == ["nature", "wildlife", "education"]
    none_rows = [r for r in out if r["tag"] is None]
    assert len(none_rows) == 2  # c04 and c07
    c05_tags = [r["tag"] for r in out if r["content_id"] == "c05"]
    assert "animation" in c05_tags and "family" in c05_tags
    print("063 PASS")

if __name__ == "__main__":
    test_063()
