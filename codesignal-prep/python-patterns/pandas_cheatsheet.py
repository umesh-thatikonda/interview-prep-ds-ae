# Pandas — Speed Reference for CodeSignal
import pandas as pd

# ─── LOADING ──────────────────────────────────────────────────────────────────
df = pd.read_csv("trips.csv")
df = pd.read_csv("trips.csv", parse_dates=["start_time", "end_time"])
df = pd.DataFrame(list_of_dicts)

# ─── INSPECTION ───────────────────────────────────────────────────────────────
df.shape
df.dtypes
df.head()
df.isnull().sum()

# ─── CLEANING ─────────────────────────────────────────────────────────────────
df.dropna()                                 # drop any row with nulls
df.dropna(subset=["start_station"])         # drop only if specific col is null
df.fillna(0)
df.fillna({"duration": 0, "station": ""})

# Remove invalid rows
df = df[df["duration"] > 0]
df = df[df["start_station"] != ""]
df = df[df["start_station"].notna()]

# ─── TYPE CASTING ─────────────────────────────────────────────────────────────
df["duration"] = pd.to_numeric(df["duration"], errors="coerce")
df["start_time"] = pd.to_datetime(df["start_time"], errors="coerce")
df["trip_id"] = df["trip_id"].astype(str)

# ─── COLUMN OPS ───────────────────────────────────────────────────────────────
df["duration_min"] = df["duration_sec"] / 60
df["route"] = df["start_station"] + " -> " + df["end_station"]
df["hour"] = df["start_time"].dt.hour
df["day_of_week"] = df["start_time"].dt.dayofweek  # 0=Mon
df["date"] = df["start_time"].dt.date

# ─── FILTERING ────────────────────────────────────────────────────────────────
df[df["user_type"] == "Subscriber"]
df[df["duration"] > 300]
df[(df["user_type"] == "Subscriber") & (df["duration"] > 300)]
df[df["start_station"].isin(["Station A", "Station B"])]

# ─── GROUPBY ──────────────────────────────────────────────────────────────────
df.groupby("start_station")["duration"].mean()
df.groupby("start_station").agg(
    trip_count=("trip_id", "count"),
    avg_duration=("duration", "mean"),
    total_duration=("duration", "sum"),
)
df.groupby(["start_station", "end_station"]).size().reset_index(name="count")

# ─── SORTING ──────────────────────────────────────────────────────────────────
df.sort_values("duration", ascending=False)
df.sort_values(["start_station", "start_time"])
df.nlargest(10, "duration")
df.nsmallest(5, "duration")

# ─── MERGING / JOINING ────────────────────────────────────────────────────────
# Inner join
merged = pd.merge(trips, stations, left_on="start_station_id", right_on="station_id")

# Left join
merged = pd.merge(trips, stations, on="station_id", how="left")

# Multiple keys
merged = pd.merge(df1, df2, on=["date", "station_id"], how="inner")

# ─── RENAMING / SELECTING ─────────────────────────────────────────────────────
df.rename(columns={"old_name": "new_name"})
df[["trip_id", "duration", "start_station"]]   # select columns
df.drop(columns=["unwanted_col"])

# ─── OUTPUT ───────────────────────────────────────────────────────────────────
df.to_csv("out.csv", index=False)
df.to_dict("records")                           # list of dicts
df.values.tolist()                              # list of lists

# ─── USEFUL ONE-LINERS ────────────────────────────────────────────────────────
df["start_station"].value_counts().head(10)     # top 10 stations
df.duplicated(subset=["trip_id"]).sum()         # count duplicates
df.drop_duplicates(subset=["trip_id"])          # remove duplicates
df["duration"].describe()                       # stats summary
