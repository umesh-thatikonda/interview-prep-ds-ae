"""
Pandas Full Reference — Common + Uncommon Useful Functions
Organized by category. Run sections independently to see output.
"""
import pandas as pd
import numpy as np

# ══════════════════════════════════════════════════════════════════════════════
# 1. CREATING DATAFRAMES
# ══════════════════════════════════════════════════════════════════════════════

df = pd.DataFrame({
    "trip_id":          [1, 2, 3, 4, 5],
    "start_station":    ["A", "B", "A", None, "C"],
    "end_station":      ["B", "C", "B", "A",  "A"],
    "duration_sec":     [300, 600, 300, 900, 1200],
    "user_type":        ["Subscriber", "Customer", "Subscriber", "Customer", "Subscriber"],
    "start_time":       pd.to_datetime(["2024-01-01 08:00", "2024-01-01 09:30",
                                        "2024-01-01 08:00", "2024-01-01 17:00",
                                        "2024-01-02 08:00"]),
})

# From list of dicts
df2 = pd.DataFrame([{"a": 1, "b": 2}, {"a": 3, "b": 4}])

# From dict of lists
df3 = pd.read_csv("file.csv")                        # from CSV
df4 = pd.read_json("file.json")                      # from JSON
df5 = pd.read_csv("file.csv", parse_dates=["col"])   # auto-parse dates

# ══════════════════════════════════════════════════════════════════════════════
# 2. INSPECTION
# ══════════════════════════════════════════════════════════════════════════════

df.shape                        # (rows, cols)
df.dtypes                       # column data types
df.info()                       # summary with nulls + dtypes
df.describe()                   # stats for numeric cols
df.describe(include="all")      # stats for ALL cols including strings
df.head(3)                      # first 3 rows
df.tail(3)                      # last 3 rows
df.sample(3)                    # 3 random rows
df.columns.tolist()             # list of column names
df.index                        # row index
df.nunique()                    # unique count per column
df["user_type"].value_counts()  # frequency count
df["user_type"].value_counts(normalize=True)  # as percentages  ← useful

# ══════════════════════════════════════════════════════════════════════════════
# 3. MISSING VALUES
# ══════════════════════════════════════════════════════════════════════════════

df.isnull().sum()               # null count per column
df.isnull().any()               # True/False per column
df.notnull()                    # opposite of isnull

df.dropna()                     # drop rows with ANY null
df.dropna(subset=["start_station"])         # drop only if specific col null
df.dropna(thresh=3)             # keep rows with at least 3 non-null values ← useful

df.fillna(0)                    # fill all nulls with 0
df.fillna({"duration_sec": 0, "start_station": "Unknown"})  # per column
df.fillna(method="ffill")       # forward fill (carry last valid value) ← useful
df.fillna(method="bfill")       # backward fill ← useful

df["start_station"].isna()      # boolean mask
df[df["start_station"].notna()] # filter out nulls

# ══════════════════════════════════════════════════════════════════════════════
# 4. TYPE CASTING
# ══════════════════════════════════════════════════════════════════════════════

df["duration_sec"].astype(float)
df["trip_id"].astype(str)
pd.to_numeric(df["duration_sec"], errors="coerce")   # bad values → NaN
pd.to_datetime(df["start_time"], errors="coerce")    # bad values → NaT
df["duration_sec"].astype("Int64")   # nullable integer (capital I) ← useful

# ══════════════════════════════════════════════════════════════════════════════
# 5. SELECTING & FILTERING
# ══════════════════════════════════════════════════════════════════════════════

df["duration_sec"]                          # single column (Series)
df[["trip_id", "duration_sec"]]             # multiple columns (DataFrame)

df[df["duration_sec"] > 300]               # filter rows
df[df["user_type"] == "Subscriber"]
df[(df["user_type"] == "Subscriber") & (df["duration_sec"] > 300)]
df[(df["user_type"] == "Subscriber") | (df["duration_sec"] > 600)]
df[~(df["user_type"] == "Subscriber")]     # NOT

df[df["start_station"].isin(["A", "B"])]   # isin filter
df[~df["start_station"].isin(["A", "B"])]  # NOT isin

# .query() — readable alternative to boolean indexing  ← useful
df.query("duration_sec > 300 and user_type == 'Subscriber'")
df.query("start_station in ['A', 'B']")

# .loc and .iloc
df.loc[0]                        # row by label
df.loc[0:2, "duration_sec"]      # rows 0-2, one column
df.iloc[0:3, 1:4]               # rows/cols by position

# ══════════════════════════════════════════════════════════════════════════════
# 6. COLUMN OPERATIONS
# ══════════════════════════════════════════════════════════════════════════════

df["duration_min"] = df["duration_sec"] / 60                  # new column
df["route"] = df["start_station"] + " → " + df["end_station"] # string concat
df["is_round_trip"] = df["start_station"] == df["end_station"] # boolean col

# .assign() — chain-friendly, doesn't modify original  ← useful
df = df.assign(
    duration_min=lambda x: x["duration_sec"] / 60,
    is_long=lambda x: x["duration_sec"] > 600,
)

df.rename(columns={"duration_sec": "duration", "trip_id": "id"})
df.drop(columns=["unwanted_col"])
df.drop(columns=["a", "b"])

# Reorder columns
df[["trip_id", "start_station", "end_station", "duration_sec"]]

# ══════════════════════════════════════════════════════════════════════════════
# 7. STRING OPERATIONS  (.str accessor)
# ══════════════════════════════════════════════════════════════════════════════

df["start_station"].str.lower()
df["start_station"].str.upper()
df["start_station"].str.strip()
df["start_station"].str.replace(" ", "_")
df["start_station"].str.contains("A")          # boolean mask
df["start_station"].str.startswith("St")
df["start_station"].str.len()                  # length of each string
df["start_station"].str.split(" ", expand=True) # split into columns ← useful
df["start_station"].str[0]                     # first character
df["start_station"].str.extract(r"(\d+)")      # regex extract ← useful

# ══════════════════════════════════════════════════════════════════════════════
# 8. DATETIME OPERATIONS  (.dt accessor)
# ══════════════════════════════════════════════════════════════════════════════

df["start_time"].dt.year
df["start_time"].dt.month
df["start_time"].dt.day
df["start_time"].dt.hour
df["start_time"].dt.minute
df["start_time"].dt.dayofweek      # 0=Monday, 6=Sunday
df["start_time"].dt.day_name()     # "Monday", "Tuesday" etc  ← useful
df["start_time"].dt.date           # date only (no time)
df["start_time"].dt.time           # time only (no date)
df["start_time"].dt.floor("H")     # round down to hour  ← useful
df["start_time"].dt.ceil("H")      # round up to hour
df["start_time"].dt.is_month_end   # boolean  ← useful

# Duration between two datetime columns
df["duration"] = (df["end_time"] - df["start_time"]).dt.total_seconds()

# ══════════════════════════════════════════════════════════════════════════════
# 9. SORTING
# ══════════════════════════════════════════════════════════════════════════════

df.sort_values("duration_sec")
df.sort_values("duration_sec", ascending=False)
df.sort_values(["start_station", "duration_sec"])          # multi-column
df.sort_values("duration_sec", na_position="first")        # nulls first

df.nlargest(5, "duration_sec")    # top 5 rows by column  ← fast alternative to sort+head
df.nsmallest(5, "duration_sec")

# ══════════════════════════════════════════════════════════════════════════════
# 10. DUPLICATES
# ══════════════════════════════════════════════════════════════════════════════

df.duplicated()                           # boolean mask of duplicate rows
df.duplicated(subset=["trip_id"])         # duplicates based on column
df.duplicated(subset=["trip_id"]).sum()   # count duplicates
df.drop_duplicates()
df.drop_duplicates(subset=["trip_id"])
df.drop_duplicates(subset=["trip_id"], keep="last")  # keep last occurrence

# ══════════════════════════════════════════════════════════════════════════════
# 11. GROUPBY & AGGREGATION
# ══════════════════════════════════════════════════════════════════════════════

df.groupby("user_type")["duration_sec"].mean()
df.groupby("user_type")["duration_sec"].agg(["mean", "sum", "count", "max"])

# Named aggregations  ← clean and readable
df.groupby("user_type").agg(
    trip_count   =("trip_id",      "count"),
    avg_duration =("duration_sec", "mean"),
    total_dur    =("duration_sec", "sum"),
    max_duration =("duration_sec", "max"),
)

# Multiple group keys
df.groupby(["start_station", "user_type"])["duration_sec"].mean()

# Count occurrences per group
df.groupby("start_station").size().reset_index(name="trip_count")

# Filter groups — keep only groups with count > 1  ← useful
df.groupby("start_station").filter(lambda x: len(x) > 1)

# Transform — returns same-shape df (good for adding group stats back)  ← useful
df["group_avg"] = df.groupby("user_type")["duration_sec"].transform("mean")
df["rank_in_group"] = df.groupby("user_type")["duration_sec"].rank(ascending=False)

# ══════════════════════════════════════════════════════════════════════════════
# 12. MERGING / JOINING
# ══════════════════════════════════════════════════════════════════════════════

stations = pd.DataFrame({
    "station_id":   ["A", "B", "C"],
    "station_name": ["Central Park", "Times Square", "Brooklyn Bridge"],
    "capacity":     [30, 50, 20],
})

# Inner join (default)
pd.merge(df, stations, left_on="start_station", right_on="station_id")

# Left join — keep all rows from left
pd.merge(df, stations, left_on="start_station", right_on="station_id", how="left")

# Join on same column name
pd.merge(df, stations, on="station_id")

# Multiple keys
pd.merge(df1, df2, on=["date", "station_id"])

# Merge and check for unmatched  ← useful
merged = pd.merge(df, stations, left_on="start_station", right_on="station_id", how="left", indicator=True)
merged[merged["_merge"] == "left_only"]  # rows with no match

# Stack two dataframes vertically
pd.concat([df1, df2], ignore_index=True)
pd.concat([df1, df2], ignore_index=True, axis=0)

# Stack horizontally
pd.concat([df1, df2], axis=1)

# ══════════════════════════════════════════════════════════════════════════════
# 13. APPLY / MAP
# ══════════════════════════════════════════════════════════════════════════════

# apply on a column (Series)
df["duration_min"] = df["duration_sec"].apply(lambda x: x / 60)

# apply on multiple columns (row-wise)  ← slower, avoid in large data
df["route"] = df.apply(lambda row: f"{row['start_station']}→{row['end_station']}", axis=1)

# map — for simple value replacement  ← useful
df["user_type"].map({"Subscriber": "sub", "Customer": "cust"})

# replace — similar to map but more flexible  ← useful
df["user_type"].replace({"Subscriber": 1, "Customer": 0})
df.replace({"user_type": {"Subscriber": 1, "Customer": 0}})

# np.where — fast conditional column  ← very useful
df["is_long"] = np.where(df["duration_sec"] > 600, "long", "short")

# np.select — multiple conditions  ← very useful
conditions = [
    df["duration_sec"] < 300,
    df["duration_sec"] < 900,
]
choices = ["short", "medium"]
df["trip_category"] = np.select(conditions, choices, default="long")

# ══════════════════════════════════════════════════════════════════════════════
# 14. RESHAPING  ← uncommon but useful
# ══════════════════════════════════════════════════════════════════════════════

# pivot_table — like Excel pivot
df.pivot_table(
    values="duration_sec",
    index="start_station",
    columns="user_type",
    aggfunc="mean",
    fill_value=0,
)

# melt — wide to long format
pd.melt(df, id_vars=["trip_id"], value_vars=["duration_sec"], var_name="metric", value_name="value")

# crosstab — frequency table between two columns  ← useful
pd.crosstab(df["start_station"], df["user_type"])
pd.crosstab(df["start_station"], df["user_type"], normalize="index")  # as %

# ══════════════════════════════════════════════════════════════════════════════
# 15. BINNING  ← uncommon but very useful for data prep
# ══════════════════════════════════════════════════════════════════════════════

# cut — bin into fixed ranges
df["duration_bucket"] = pd.cut(
    df["duration_sec"],
    bins=[0, 300, 600, 900, float("inf")],
    labels=["short", "medium", "long", "very long"]
)

# qcut — bin into equal-sized quantile buckets
df["duration_quartile"] = pd.qcut(df["duration_sec"], q=4, labels=["Q1", "Q2", "Q3", "Q4"])

# ══════════════════════════════════════════════════════════════════════════════
# 16. WINDOW / ROLLING  ← uncommon but useful
# ══════════════════════════════════════════════════════════════════════════════

df["rolling_avg"] = df["duration_sec"].rolling(window=3).mean()     # 3-row moving avg
df["rolling_sum"] = df["duration_sec"].rolling(window=3).sum()
df["cumsum"]      = df["duration_sec"].cumsum()                      # cumulative sum
df["cummax"]      = df["duration_sec"].cummax()                      # running max

# Expanding window — grows from start
df["expanding_mean"] = df["duration_sec"].expanding().mean()

# ══════════════════════════════════════════════════════════════════════════════
# 17. RANKING  ← useful for top-N per group problems
# ══════════════════════════════════════════════════════════════════════════════

df["rank"]       = df["duration_sec"].rank(ascending=False)
df["rank_dense"] = df["duration_sec"].rank(method="dense", ascending=False)

# Top N per group  ← very useful pattern
df["rank_in_station"] = df.groupby("start_station")["duration_sec"].rank(ascending=False)
top1_per_station = df[df["rank_in_station"] == 1]

# ══════════════════════════════════════════════════════════════════════════════
# 18. USEFUL MISC  ← uncommon gems
# ══════════════════════════════════════════════════════════════════════════════

# pipe — chain operations cleanly
def remove_nulls(df): return df.dropna()
def add_duration_min(df): return df.assign(duration_min=df["duration_sec"]/60)

result = df.pipe(remove_nulls).pipe(add_duration_min)

# explode — expand list values into rows  ← useful when a cell has a list
df2 = pd.DataFrame({"id": [1, 2], "tags": [["sql", "python"], ["pandas"]]})
df2.explode("tags")

# get_dummies — one-hot encode categorical column  ← useful for ML prep
pd.get_dummies(df["user_type"])
pd.get_dummies(df, columns=["user_type"])

# where — keep values where condition is True, else replace  ← useful
df["duration_sec"].where(df["duration_sec"] > 0, other=0)

# clip — cap values at min/max  ← useful for outlier handling
df["duration_sec"].clip(lower=0, upper=3600)

# memory_usage — check dataframe size
df.memory_usage(deep=True).sum()

# convert_dtypes — auto-infer best dtypes  ← useful
df.convert_dtypes()

# ══════════════════════════════════════════════════════════════════════════════
# 19. OUTPUT
# ══════════════════════════════════════════════════════════════════════════════

df.to_csv("out.csv", index=False)
df.to_json("out.json", orient="records", indent=2)
df.to_dict("records")           # list of dicts  ← most common for passing data around
df.to_dict("list")              # dict of lists
df.values.tolist()              # list of lists (no column names)
df.to_string(index=False)       # pretty print as string

# Reset index (after groupby/filter operations)
df.reset_index(drop=True)
df.reset_index()                # turns index into a column
