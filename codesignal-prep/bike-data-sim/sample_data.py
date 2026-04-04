"""
Realistic sample bike trip data using the Divvy / Citi Bike new schema.
This is the format most likely used in your CodeSignal test.

Use this to build and test your solutions locally before the test.
"""

import pandas as pd
import numpy as np

# ── TRIPS TABLE ───────────────────────────────────────────────────────────────
# Mirrors Divvy / Citi Bike new format exactly

trips_raw = pd.DataFrame({
    "ride_id": [
        "R001","R002","R003","R004","R005",
        "R006","R007","R008","R009","R010",
        "R011","R012","R002",             # R002 is a duplicate
    ],
    "rideable_type": [
        "classic_bike","electric_bike","classic_bike","docked_bike","electric_bike",
        "classic_bike","electric_bike","classic_bike","classic_bike","electric_bike",
        "classic_bike","docked_bike","electric_bike",
    ],
    "started_at": pd.to_datetime([
        "2024-03-01 07:05:23","2024-03-01 08:47:10","2024-03-01 12:15:00",
        "2024-03-01 17:32:45","2024-03-01 22:10:05","2024-03-02 07:58:30",
        "2024-03-02 09:01:00","2024-03-02 13:22:18","2024-03-02 18:05:55",
        "2024-03-03 08:12:40","2024-03-03 08:50:00","2024-03-03 19:40:10",
        "2024-03-01 08:47:10",  # duplicate
    ]),
    "ended_at": pd.to_datetime([
        "2024-03-01 07:25:23","2024-03-01 09:32:10","2024-03-01 12:55:00",
        "2024-03-01 17:20:45",  # end < start → invalid
        "2024-03-01 22:48:05","2024-03-02 08:30:30",
        "2024-03-02 09:46:00","2024-03-02 14:10:18","2024-03-02 18:35:55",
        "2024-03-03 08:42:40","2024-03-03 10:15:00","2024-03-03 20:00:10",
        "2024-03-01 09:32:10",  # duplicate
    ]),
    "start_station_name": [
        "Streeter Dr & Grand Ave","Millennium Park","Clark St & Elm St",
        "Kingsbury St & Kinzie St","Streeter Dr & Grand Ave","Clark St & Elm St",
        None,                           # null — electric bike (dockless)
        "Millennium Park","Streeter Dr & Grand Ave","Clark St & Elm St",
        "Kingsbury St & Kinzie St","Millennium Park",
        "Millennium Park",
    ],
    "start_station_id": [
        "13022","13008","KA1504000135","KA1504000123","13022","KA1504000135",
        None,
        "13008","13022","KA1504000135",
        "KA1504000123","13008",
        "13008",
    ],
    "end_station_name": [
        "Millennium Park","Clark St & Elm St","Streeter Dr & Grand Ave",
        "Millennium Park","Clark St & Elm St","Streeter Dr & Grand Ave",
        "Millennium Park",              # electric bike ended at station
        "Kingsbury St & Kinzie St","Clark St & Elm St",
        None,                           # null end station
        "Streeter Dr & Grand Ave","Clark St & Elm St",
        "Clark St & Elm St",
    ],
    "end_station_id": [
        "13008","KA1504000135","13022",
        "13008","KA1504000135","13022",
        "13008",
        "KA1504000123","KA1504000135",
        None,
        "13022","KA1504000135",
        "KA1504000135",
    ],
    "start_lat": [41.8923, 41.8827, 41.9026, 41.8892, 41.8923,
                  41.9026, 41.8900, 41.8827, 41.8923, 41.9026,
                  41.8892, 41.8827, 41.8827],
    "start_lng": [-87.6120,-87.6233,-87.6313,-87.6383,-87.6120,
                  -87.6313,-87.6350,-87.6233,-87.6120,-87.6313,
                  -87.6383,-87.6233,-87.6233],
    "end_lat":   [41.8827, 41.9026, 41.8923, 41.8827, 41.9026,
                  41.8923, 41.8827, 41.8892, 41.9026, None,
                  41.8923, 41.9026, 41.9026],
    "end_lng":   [-87.6233,-87.6313,-87.6120,-87.6233,-87.6313,
                  -87.6120,-87.6233,-87.6383,-87.6313, None,
                  -87.6120,-87.6313,-87.6313],
    "member_casual": [
        "member","casual","member","member","casual",
        "member","casual","member","casual","member",
        "member","casual","casual",
    ],
})

# ── STATIONS TABLE ────────────────────────────────────────────────────────────
stations = pd.DataFrame({
    "station_id":   ["13022","13008","KA1504000135","KA1504000123"],
    "station_name": ["Streeter Dr & Grand Ave","Millennium Park",
                     "Clark St & Elm St","Kingsbury St & Kinzie St"],
    "capacity":     [23, 35, 15, 19],
    "lat":          [41.8923, 41.8827, 41.9026, 41.8892],
    "lng":          [-87.6120,-87.6233,-87.6313,-87.6383],
    "region":       ["Lakefront","Loop","Near North","River North"],
})


# ── HELPER: CLEAN TRIPS ───────────────────────────────────────────────────────
def get_clean_trips(df=trips_raw):
    """
    Standard cleaning pipeline for the trips table:
    - Remove duplicates (keep first ride_id)
    - Remove rows where ended_at <= started_at
    - Remove rows missing both start AND end station
    - Add duration_sec column
    """
    df = df.drop_duplicates(subset=["ride_id"], keep="first").copy()
    df["duration_sec"] = (df["ended_at"] - df["started_at"]).dt.total_seconds()
    df = df[df["duration_sec"] > 0].reset_index(drop=True)
    # keep rows that have at least one of start/end station (not both null)
    df = df[~(df["start_station_id"].isna() & df["end_station_id"].isna())]
    return df


if __name__ == "__main__":
    print("=== RAW TRIPS ===")
    print(f"Shape: {trips_raw.shape}")
    print(trips_raw[["ride_id","started_at","ended_at","start_station_name","member_casual"]].to_string())

    print("\n=== CLEAN TRIPS ===")
    clean = get_clean_trips()
    print(f"Shape: {clean.shape}")
    print(f"Removed: {len(trips_raw) - len(clean)} rows")
    print(clean[["ride_id","duration_sec","start_station_name","end_station_name","member_casual"]].to_string())

    print("\n=== STATIONS ===")
    print(stations.to_string())
