# Real Bike-Share Dataset Schemas

These are the actual column names from the most popular public bike-share datasets.
Your CodeSignal test will likely mirror one of these schemas closely.

---

## 1. Citi Bike NYC — citibikenyc.com/system-data

### New Format (2021 onwards) — most likely to appear in tests
```
ride_id             — unique trip identifier (string)
rideable_type       — "classic_bike" | "electric_bike" | "docked_bike"
started_at          — trip start timestamp  (YYYY-MM-DD HH:MM:SS)
ended_at            — trip end timestamp    (YYYY-MM-DD HH:MM:SS)
start_station_name  — name of start station
start_station_id    — station ID (string)
end_station_name    — name of end station
end_station_id      — station ID (string)
start_lat           — start latitude
start_lng           — start longitude
end_lat             — end latitude
end_lng             — end longitude
member_casual       — "member" | "casual"
```

### Old Format (pre-2021) — also seen in practice problems
```
tripduration        — seconds (int)
starttime           — YYYY-MM-DD HH:MM:SS.ffffff
stoptime            — YYYY-MM-DD HH:MM:SS.ffffff
start station id    — int
start station name  — string
start station latitude
start station longitude
end station id
end station name
end station latitude
end station longitude
bikeid              — int
usertype            — "Subscriber" | "Customer"
birth year          — int (may be null)
gender              — 0=Unknown, 1=Male, 2=Female
```

---

## 2. Divvy — Chicago — divvybikes.com/system-data

Same new format as Citi Bike (both use Lyft/PBSC platform):
```
ride_id             — unique trip ID (string)
rideable_type       — "classic_bike" | "electric_bike" | "docked_bike"
started_at          — YYYY-MM-DD HH:MM:SS
ended_at            — YYYY-MM-DD HH:MM:SS
start_station_name
start_station_id
end_station_name
end_station_id
start_lat
start_lng
end_lat
end_lng
member_casual       — "member" | "casual"
```
Note: No trip duration column — must calculate from started_at / ended_at.
Note: Null stations are common for electric bikes (dockless).

---

## 3. Capital Bikeshare — DC — capitalbikeshare.com/system-data

```
Duration            — seconds (int)
Start date          — YYYY-MM-DD HH:MM:SS
End date            — YYYY-MM-DD HH:MM:SS
Start station number
Start station       — station name
End station number
End station         — station name
Bike number
Member type         — "Member" | "Casual"
```

---

## 4. Metro Bike Share — LA — bikeshare.metro.net/about/data

```
trip_id
duration            — seconds
start_time          — MM/DD/YYYY HH:MM    (note: different date format!)
end_time            — MM/DD/YYYY HH:MM
start_station
start_lat
start_lon
end_station
end_lat
end_lon
bike_id
plan_duration       — 0, 30, 365 (subscription type in days)
trip_route_category — "One Way" | "Round Trip"
passholder_type     — "Walk-Up" | "Monthly Pass" | "Annual Pass" | "Staff Annual"
bike_type           — "standard" | "electric"
```

---

## Key Differences to Know

| System      | Duration col? | User type values        | Timestamp col names     | Station nulls? |
|-------------|---------------|-------------------------|-------------------------|----------------|
| Citi Bike (new) | No (calculate) | member / casual     | started_at / ended_at   | Yes (e-bikes)  |
| Citi Bike (old) | Yes (tripduration) | Subscriber / Customer | starttime / stoptime | Rare        |
| Divvy       | No (calculate) | member / casual         | started_at / ended_at   | Yes (e-bikes)  |
| Capital     | Yes (Duration) | Member / Casual         | Start date / End date   | Rare           |
| Metro LA    | Yes (duration) | Walk-Up / Monthly / Annual | start_time / end_time | Rare        |

---

## Common Data Quality Issues (seen across all datasets)

- **Missing station names/IDs** — especially for electric/dockless bikes
- **Negative or zero duration** — data entry errors, must filter out
- **Very short trips** (< 60 sec) — usually removed by providers but may appear
- **Very long trips** (> 24 hrs) — usually stolen or lost bikes
- **Duplicate ride_ids** — rare but happens
- **Inconsistent date formats** — especially Metro LA uses MM/DD/YYYY
- **Null end locations** — bike not returned to station

---

## Download Links

- Citi Bike NYC: https://citibikenyc.com/system-data
- Divvy Chicago: https://divvybikes.com/system-data
- Capital Bikeshare DC: https://capitalbikeshare.com/system-data
- Metro Bike Share LA: https://bikeshare.metro.net/about/data/
- Kaggle (Divvy 2024): https://www.kaggle.com/datasets/ranjanrakesh51/divvy-bike-sharing-data-jan-24-to-mar-25
- Kaggle (Capital 2020-2024): https://www.kaggle.com/datasets/taweilo/capital-bikeshare-dataset-202005202408
