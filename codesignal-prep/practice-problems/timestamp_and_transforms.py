"""
Practice Problems — Timestamps, cut/qcut, melt, rolling, cumsum, expanding, pipe
All problems use bike trip data. Solve each problem, then run assertions.
Uncomment the SOLUTION block only if you are stuck.
"""

import pandas as pd
import numpy as np

# ══════════════════════════════════════════════════════════════════════════════
# SHARED DATASET — used across all problems
# ══════════════════════════════════════════════════════════════════════════════

raw = pd.DataFrame({
    "trip_id": range(1, 13),
    "start_time": pd.to_datetime([
        "2024-01-01 07:05", "2024-01-01 08:45", "2024-01-01 12:10",
        "2024-01-01 17:30", "2024-01-01 22:15", "2024-01-02 07:50",
        "2024-01-02 09:00", "2024-01-02 13:20", "2024-01-02 18:05",
        "2024-01-03 08:10", "2024-01-03 08:55", "2024-01-03 19:40",
    ]),
    "end_time": pd.to_datetime([
        "2024-01-01 07:25", "2024-01-01 09:30", "2024-01-01 12:55",
        "2024-01-01 17:55", "2024-01-01 22:50", "2024-01-02 08:30",
        "2024-01-02 09:45", "2024-01-02 14:10", "2024-01-02 18:35",
        "2024-01-03 08:40", "2024-01-03 10:15", "2024-01-03 20:00",
    ]),
    "start_station": ["A","B","A","C","B","A","C","B","A","C","A","B"],
    "end_station":   ["B","C","C","A","A","C","A","A","B","B","C","A"],
    "user_type":     ["Subscriber","Customer","Subscriber","Customer",
                      "Subscriber","Subscriber","Customer","Subscriber",
                      "Customer","Subscriber","Subscriber","Customer"],
    "bike_id":       [101,102,101,103,102,101,103,102,101,103,101,102],
})


# ══════════════════════════════════════════════════════════════════════════════
# PROBLEM 1 — TIMESTAMP BASICS
# ══════════════════════════════════════════════════════════════════════════════
"""
From the raw dataframe:
1. Add column `duration_sec` (int) — seconds between start and end
2. Add column `duration_min` (float, 2 decimal places)
3. Add column `hour_of_day` — hour the trip started (0–23)
4. Add column `day_of_week` — name of the day (e.g. "Monday")
5. Add column `is_weekend` — True if Saturday or Sunday
6. Add column `date` — just the date portion of start_time (no time)
"""

df = raw.copy()

# YOUR CODE HERE


# --- ASSERTIONS ---
def test_p1(df):
    assert "duration_sec" in df.columns
    assert df.loc[0, "duration_sec"] == 1200
    assert df.loc[0, "duration_min"] == 20.0
    assert df.loc[0, "hour_of_day"] == 7
    assert df.loc[0, "day_of_week"] == "Monday"
    assert df["is_weekend"].dtype == bool
    assert df.loc[0, "is_weekend"] == False
    assert str(df.loc[0, "date"]) == "2024-01-01"
    print("P1 PASSED")

# test_p1(df)  # uncomment to test

"""
SOLUTION:
df["duration_sec"] = (df["end_time"] - df["start_time"]).dt.total_seconds().astype(int)
df["duration_min"] = round(df["duration_sec"] / 60, 2)
df["hour_of_day"]  = df["start_time"].dt.hour
df["day_of_week"]  = df["start_time"].dt.day_name()
df["is_weekend"]   = df["start_time"].dt.dayofweek >= 5
df["date"]         = df["start_time"].dt.date
"""


# ══════════════════════════════════════════════════════════════════════════════
# PROBLEM 2 — TIMESTAMP BUCKETING (time-of-day bins)
# ══════════════════════════════════════════════════════════════════════════════
"""
Using df from P1 (with hour_of_day):
Add column `time_of_day` with these buckets based on hour_of_day:
    00–05  → "Night"
    06–09  → "Morning Rush"
    10–15  → "Midday"
    16–19  → "Evening Rush"
    20–23  → "Night"

Use pd.cut() with explicit bins and labels.
"""

# YOUR CODE HERE


# --- ASSERTIONS ---
def test_p2(df):
    assert "time_of_day" in df.columns
    assert df.loc[0, "time_of_day"] == "Morning Rush"   # 07:05
    assert df.loc[2, "time_of_day"] == "Midday"          # 12:10
    assert df.loc[3, "time_of_day"] == "Evening Rush"    # 17:30
    assert df.loc[4, "time_of_day"] == "Night"           # 22:15
    print("P2 PASSED")

# test_p2(df)

"""
SOLUTION:
bins   = [-1, 5, 9, 15, 19, 23]
labels = ["Night", "Morning Rush", "Midday", "Evening Rush", "Night"]
# Note: labels must be unique — combine Night into one category differently:
bins   = [0, 6, 10, 16, 20, 24]
labels = ["Night", "Morning Rush", "Midday", "Evening Rush", "Night"]
# Cleanest approach — use np.select for non-uniform bins like this:
conditions = [
    df["hour_of_day"].between(0, 5),
    df["hour_of_day"].between(6, 9),
    df["hour_of_day"].between(10, 15),
    df["hour_of_day"].between(16, 19),
    df["hour_of_day"].between(20, 23),
]
choices = ["Night", "Morning Rush", "Midday", "Evening Rush", "Night"]
df["time_of_day"] = np.select(conditions, choices)
"""


# ══════════════════════════════════════════════════════════════════════════════
# PROBLEM 3 — pd.cut() ON DURATION
# ══════════════════════════════════════════════════════════════════════════════
"""
Using df from P1 (with duration_sec):
1. Add column `trip_length` using pd.cut() with these bins (in seconds):
       0–600    → "Short"      (≤10 min)
       601–1800 → "Medium"     (10–30 min)
       1801+    → "Long"       (>30 min)

2. Add column `duration_quartile` using pd.qcut() into 4 equal groups
   labeled Q1, Q2, Q3, Q4.

3. Print the value_counts of trip_length and duration_quartile.
"""

# YOUR CODE HERE


# --- ASSERTIONS ---
def test_p3(df):
    assert "trip_length" in df.columns
    assert "duration_quartile" in df.columns
    assert df.loc[0, "trip_length"] == "Short"     # 1200 sec = 20 min → Medium wait...
    # trip 1 = 1200 sec = 20 min → Medium
    assert str(df.loc[0, "trip_length"]) == "Medium"
    assert df["duration_quartile"].nunique() == 4
    print("P3 PASSED")

# test_p3(df)

"""
SOLUTION:
df["trip_length"] = pd.cut(
    df["duration_sec"],
    bins=[0, 600, 1800, float("inf")],
    labels=["Short", "Medium", "Long"]
)
df["duration_quartile"] = pd.qcut(df["duration_sec"], q=4, labels=["Q1","Q2","Q3","Q4"])
print(df["trip_length"].value_counts())
print(df["duration_quartile"].value_counts())
"""


# ══════════════════════════════════════════════════════════════════════════════
# PROBLEM 4 — FLOOR / RESAMPLE (group trips by hour)
# ══════════════════════════════════════════════════════════════════════════════
"""
Using df from P1:
1. Add column `start_hour` — start_time floored to the nearest hour
   e.g. 07:05 → 07:00,  08:45 → 08:00

2. Count the number of trips per start_hour. Store as `hourly_counts` DataFrame
   with columns: start_hour, trip_count. Sort by start_hour ascending.
"""

# YOUR CODE HERE


# --- ASSERTIONS ---
def test_p4(df):
    assert "start_hour" in df.columns
    assert df.loc[0, "start_hour"] == pd.Timestamp("2024-01-01 07:00")
    assert df.loc[1, "start_hour"] == pd.Timestamp("2024-01-01 08:00")
    print("P4 PASSED")

# test_p4(df)

"""
SOLUTION:
df["start_hour"] = df["start_time"].dt.floor("h")
hourly_counts = (
    df.groupby("start_hour")
    .size()
    .reset_index(name="trip_count")
    .sort_values("start_hour")
)
"""


# ══════════════════════════════════════════════════════════════════════════════
# PROBLEM 5 — CUMSUM & EXPANDING
# ══════════════════════════════════════════════════════════════════════════════
"""
Using df from P1, sorted by start_time:
1. Add column `cumulative_trips` — running count of trips (1, 2, 3, ...)
2. Add column `cumulative_duration_sec` — running total of duration_sec
3. Add column `running_avg_duration` — expanding mean of duration_sec
   (average of all trips seen so far)
4. Add column `running_max_duration` — expanding max of duration_sec

All columns should be computed in start_time order.
"""

df = df.sort_values("start_time").reset_index(drop=True)

# YOUR CODE HERE


# --- ASSERTIONS ---
def test_p5(df):
    assert df.loc[0, "cumulative_trips"] == 1
    assert df.loc[1, "cumulative_trips"] == 2
    assert df.loc[0, "cumulative_duration_sec"] == df.loc[0, "duration_sec"]
    assert df.loc[1, "cumulative_duration_sec"] == df.loc[0, "duration_sec"] + df.loc[1, "duration_sec"]
    assert df.loc[0, "running_avg_duration"] == df.loc[0, "duration_sec"]
    assert df.loc[0, "running_max_duration"] == df.loc[0, "duration_sec"]
    print("P5 PASSED")

# test_p5(df)

"""
SOLUTION:
df = df.sort_values("start_time").reset_index(drop=True)
df["cumulative_trips"]         = range(1, len(df) + 1)
df["cumulative_duration_sec"]  = df["duration_sec"].cumsum()
df["running_avg_duration"]     = df["duration_sec"].expanding().mean()
df["running_max_duration"]     = df["duration_sec"].expanding().max()
"""


# ══════════════════════════════════════════════════════════════════════════════
# PROBLEM 6 — ROLLING WINDOW
# ══════════════════════════════════════════════════════════════════════════════
"""
Using df sorted by start_time:
1. Add column `rolling_3_avg` — 3-trip rolling average of duration_sec
2. Add column `rolling_3_max` — 3-trip rolling max of duration_sec
3. Add column `rolling_3_std` — 3-trip rolling standard deviation

Then answer: Which trip (trip_id) first had a rolling_3_avg above 2000 seconds?
Store the answer in variable `first_high_avg_trip`.
"""

# YOUR CODE HERE


# --- ASSERTIONS ---
def test_p6(df):
    assert "rolling_3_avg" in df.columns
    assert pd.isna(df.loc[0, "rolling_3_avg"])   # first 2 rows are NaN (window not full)
    assert pd.isna(df.loc[1, "rolling_3_avg"])
    assert not pd.isna(df.loc[2, "rolling_3_avg"])
    print("P6 PASSED")

# test_p6(df)

"""
SOLUTION:
df["rolling_3_avg"] = df["duration_sec"].rolling(window=3).mean()
df["rolling_3_max"] = df["duration_sec"].rolling(window=3).max()
df["rolling_3_std"] = df["duration_sec"].rolling(window=3).std()

above = df[df["rolling_3_avg"] > 2000]
first_high_avg_trip = above.iloc[0]["trip_id"] if not above.empty else None
"""


# ══════════════════════════════════════════════════════════════════════════════
# PROBLEM 7 — ROLLING PER GROUP (per bike_id)
# ══════════════════════════════════════════════════════════════════════════════
"""
Each bike takes multiple trips. For each bike, compute a rolling 2-trip
average duration — but only within that bike's trips (not across bikes).

Add column `bike_rolling_2_avg` — rolling(2).mean() of duration_sec,
grouped by bike_id, sorted by start_time within each group.

This is a groupby + rolling combo — tricky but common.
"""

# YOUR CODE HERE


# --- ASSERTIONS ---
def test_p7(df):
    assert "bike_rolling_2_avg" in df.columns
    # bike 101 trips sorted by time: trip 1,3,6,9,11 ...
    # first trip of each bike should be NaN
    bike101 = df[df["bike_id"] == 101].sort_values("start_time").reset_index(drop=True)
    assert pd.isna(bike101.loc[0, "bike_rolling_2_avg"])
    assert not pd.isna(bike101.loc[1, "bike_rolling_2_avg"])
    print("P7 PASSED")

# test_p7(df)

"""
SOLUTION:
df = df.sort_values(["bike_id", "start_time"])
df["bike_rolling_2_avg"] = (
    df.groupby("bike_id")["duration_sec"]
    .transform(lambda x: x.rolling(2).mean())
)
"""


# ══════════════════════════════════════════════════════════════════════════════
# PROBLEM 8 — MELT (wide → long)
# ══════════════════════════════════════════════════════════════════════════════
"""
You have a summary table of avg duration per station per user type:

    station | Subscriber_avg | Customer_avg
    A       | 1800           | 900
    B       | 1200           | 2100
    C       | 600            | 1500

Melt this into long format:
    station | user_type       | avg_duration
    A       | Subscriber_avg  | 1800
    A       | Customer_avg    | 900
    ...

Then clean up user_type to just "Subscriber" or "Customer" (remove "_avg").
Store result in `long_df`.
"""

wide_df = pd.DataFrame({
    "station":        ["A", "B", "C"],
    "Subscriber_avg": [1800, 1200, 600],
    "Customer_avg":   [900,  2100, 1500],
})

# YOUR CODE HERE


# --- ASSERTIONS ---
def test_p8(long_df):
    assert len(long_df) == 6
    assert set(long_df.columns) == {"station", "user_type", "avg_duration"}
    assert set(long_df["user_type"].unique()) == {"Subscriber", "Customer"}
    assert long_df[long_df["station"] == "A"][long_df["user_type"] == "Subscriber"]["avg_duration"].values[0] == 1800
    print("P8 PASSED")

# test_p8(long_df)

"""
SOLUTION:
long_df = pd.melt(
    wide_df,
    id_vars=["station"],
    value_vars=["Subscriber_avg", "Customer_avg"],
    var_name="user_type",
    value_name="avg_duration"
)
long_df["user_type"] = long_df["user_type"].str.replace("_avg", "")
"""


# ══════════════════════════════════════════════════════════════════════════════
# PROBLEM 9 — PIPE (chain operations cleanly)
# ══════════════════════════════════════════════════════════════════════════════
"""
Build a pipeline using .pipe() that takes raw and:
  Step 1 — add_duration: adds duration_sec column
  Step 2 — remove_invalid: drops rows where duration_sec <= 0
  Step 3 — add_time_features: adds hour_of_day, day_of_week, is_weekend
  Step 4 — add_trip_length: adds trip_length using pd.cut (Short/Medium/Long)

Each step should be a standalone function that takes df and returns df.
Chain them all using .pipe().
Store final result in `result`.
"""

def add_duration(df):
    # YOUR CODE HERE
    pass

def remove_invalid(df):
    # YOUR CODE HERE
    pass

def add_time_features(df):
    # YOUR CODE HERE
    pass

def add_trip_length(df):
    # YOUR CODE HERE
    pass

# result = raw.pipe(add_duration).pipe(remove_invalid).pipe(add_time_features).pipe(add_trip_length)


# --- ASSERTIONS ---
def test_p9(result):
    required = {"duration_sec", "hour_of_day", "day_of_week", "is_weekend", "trip_length"}
    assert required.issubset(result.columns)
    assert (result["duration_sec"] > 0).all()
    assert result["trip_length"].isin(["Short", "Medium", "Long"]).all()
    print("P9 PASSED")

# test_p9(result)

"""
SOLUTION:
def add_duration(df):
    df = df.copy()
    df["duration_sec"] = (df["end_time"] - df["start_time"]).dt.total_seconds().astype(int)
    return df

def remove_invalid(df):
    return df[df["duration_sec"] > 0].reset_index(drop=True)

def add_time_features(df):
    df = df.copy()
    df["hour_of_day"] = df["start_time"].dt.hour
    df["day_of_week"] = df["start_time"].dt.day_name()
    df["is_weekend"]  = df["start_time"].dt.dayofweek >= 5
    return df

def add_trip_length(df):
    df = df.copy()
    df["trip_length"] = pd.cut(
        df["duration_sec"],
        bins=[0, 600, 1800, float("inf")],
        labels=["Short", "Medium", "Long"]
    )
    return df

result = raw.pipe(add_duration).pipe(remove_invalid).pipe(add_time_features).pipe(add_trip_length)
"""


# ══════════════════════════════════════════════════════════════════════════════
# PROBLEM 10 — FULL TIMESTAMP ANALYSIS (tying it all together)
# ══════════════════════════════════════════════════════════════════════════════
"""
Using raw data, build a station-level daily summary.

For each (date, start_station) pair compute:
  - trip_count
  - total_duration_sec
  - avg_duration_sec (rounded to 1 decimal)
  - busiest_hour    — the hour with the most departures from that station that day

Then add a column `cumulative_trips_by_station` — for each station,
the running cumulative trip count over dates (sorted by date ascending).

Store result in `daily_summary`.
"""

# YOUR CODE HERE


# --- ASSERTIONS ---
def test_p10(daily_summary):
    required = {"date", "start_station", "trip_count", "total_duration_sec",
                "avg_duration_sec", "busiest_hour", "cumulative_trips_by_station"}
    assert required.issubset(daily_summary.columns)
    assert (daily_summary["trip_count"] > 0).all()
    # Station A on day 1: trips at 07:05, 12:10 → 2 trips
    a_day1 = daily_summary[
        (daily_summary["start_station"] == "A") &
        (daily_summary["date"] == pd.Timestamp("2024-01-01").date())
    ]
    assert a_day1["trip_count"].values[0] == 2
    print("P10 PASSED")

# test_p10(daily_summary)

"""
SOLUTION:
df = raw.copy()
df["duration_sec"] = (df["end_time"] - df["start_time"]).dt.total_seconds().astype(int)
df["date"]         = df["start_time"].dt.date
df["hour_of_day"]  = df["start_time"].dt.hour

# Busiest hour per (date, station)
busiest = (
    df.groupby(["date", "start_station", "hour_of_day"])
    .size()
    .reset_index(name="cnt")
    .sort_values("cnt", ascending=False)
    .drop_duplicates(subset=["date", "start_station"])
    .rename(columns={"hour_of_day": "busiest_hour"})
    [["date", "start_station", "busiest_hour"]]
)

# Daily summary
daily_summary = (
    df.groupby(["date", "start_station"])
    .agg(
        trip_count        =("trip_id",      "count"),
        total_duration_sec=("duration_sec", "sum"),
        avg_duration_sec  =("duration_sec", lambda x: round(x.mean(), 1)),
    )
    .reset_index()
)

daily_summary = pd.merge(daily_summary, busiest, on=["date", "start_station"])
daily_summary = daily_summary.sort_values(["start_station", "date"])

# Cumulative trips per station
daily_summary["cumulative_trips_by_station"] = (
    daily_summary.groupby("start_station")["trip_count"].cumsum()
)
"""
