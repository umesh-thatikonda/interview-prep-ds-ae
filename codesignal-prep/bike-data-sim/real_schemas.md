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

## 5. Bay Wheels — San Francisco — lyft.com/bikes/bay-wheels/system-data

Same Lyft/PBSC platform as Divvy and Citi Bike:
```
ride_id
rideable_type       — "classic_bike" | "electric_bike"
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
Note: Identical schema to Divvy — same Lyft platform across all three cities.

---

## 6. Bluebikes — Boston — bluebikes.com/system-data

```
tripduration        — seconds (int)
starttime           — YYYY-MM-DD HH:MM:SS
stoptime            — YYYY-MM-DD HH:MM:SS
start station id
start station name
start station latitude
start station longitude
end station id
end station name
end station latitude
end station longitude
bikeid
usertype            — "Subscriber" | "Customer"
birth year          — int (may be null)
gender              — 0=Unknown, 1=Male, 2=Female
postal code
```
Note: Same old Citi Bike format — duration pre-calculated, has demographic fields.

---

## 7. Bike Share Toronto — open.toronto.ca

```
Trip Id
Trip Duration       — seconds
Start Station Id
Start Time          — MM/DD/YYYY HH:MM
Start Station Name
End Station Id
End Time            — MM/DD/YYYY HH:MM
End Station Name
Bike Id
User Type           — "Annual Member" | "Casual Member" | "Walk-up"
```
Note: Date format is MM/DD/YYYY — watch out when parsing.

---

## 8. UCI Bike Sharing Dataset — archive.uci.edu (hourly/daily aggregated)

Different type — NOT trip-level. Hourly/daily demand counts with weather.
```
instant             — row index
dteday              — date
season              — 1=spring, 2=summer, 3=fall, 4=winter
yr                  — 0=2011, 1=2012
mnth                — month (1-12)
hr                  — hour (0-23)  [only in hourly version]
holiday             — 0/1
weekday             — 0-6
workingday          — 0/1
weathersit          — 1=clear, 2=mist, 3=light snow/rain, 4=heavy rain
temp                — normalized temperature
atemp               — normalized feeling temperature
hum                 — normalized humidity
windspeed           — normalized windspeed
casual              — casual user count
registered          — registered user count
cnt                 — total count (casual + registered)
```
Use case: regression, time series, demand forecasting practice.

---

## Key Differences to Know

| System         | Duration col?      | User type values              | Timestamp format     | Station nulls? |
|----------------|--------------------|-------------------------------|----------------------|----------------|
| Citi Bike new  | No (calculate)     | member / casual               | YYYY-MM-DD HH:MM:SS  | Yes (e-bikes)  |
| Citi Bike old  | Yes (tripduration) | Subscriber / Customer         | YYYY-MM-DD HH:MM:SS  | Rare           |
| Divvy          | No (calculate)     | member / casual               | YYYY-MM-DD HH:MM:SS  | Yes (e-bikes)  |
| Bay Wheels     | No (calculate)     | member / casual               | YYYY-MM-DD HH:MM:SS  | Yes (e-bikes)  |
| Capital DC     | Yes (Duration)     | Member / Casual               | YYYY-MM-DD HH:MM:SS  | Rare           |
| Bluebikes      | Yes (tripduration) | Subscriber / Customer         | YYYY-MM-DD HH:MM:SS  | Rare           |
| Metro LA       | Yes (duration)     | Walk-Up / Monthly / Annual    | MM/DD/YYYY HH:MM     | Rare           |
| Toronto        | Yes (Trip Duration)| Annual Member / Casual / Walk-up | MM/DD/YYYY HH:MM  | Rare           |
| UCI (hourly)   | N/A (aggregated)   | casual / registered / cnt     | YYYY-MM-DD           | None           |

---

## Common Data Quality Issues (seen across all datasets)

- **Missing station names/IDs** — especially for electric/dockless bikes
- **Negative or zero duration** — data entry errors, must filter out
- **Very short trips** (< 60 sec) — usually removed by providers but may appear
- **Very long trips** (> 24 hrs) — usually stolen or lost bikes
- **Duplicate ride_ids** — rare but happens
- **Inconsistent date formats** — Metro LA and Toronto use MM/DD/YYYY (not ISO)
- **Null end locations** — bike not returned to station
- **Demographic fields** (gender, birth year) — present in old formats, often null

---

## Download Links

| Dataset | URL |
|---|---|
| Citi Bike NYC | https://citibikenyc.com/system-data |
| Divvy Chicago | https://divvybikes.com/system-data |
| Bay Wheels SF | https://www.lyft.com/bikes/bay-wheels/system-data |
| Capital Bikeshare DC | https://capitalbikeshare.com/system-data |
| Bluebikes Boston | https://bluebikes.com/system-data |
| Metro Bike Share LA | https://bikeshare.metro.net/about/data/ |
| Bike Share Toronto | https://open.toronto.ca/dataset/bike-share-toronto-ridership-data/ |
| UCI Bike Sharing | https://archive.uci.edu/dataset/275/bike+sharing+dataset |
| Kaggle — Divvy 2024 | https://www.kaggle.com/datasets/ranjanrakesh51/divvy-bike-sharing-data-jan-24-to-mar-25 |
| Kaggle — Capital 2020–2024 | https://www.kaggle.com/datasets/taweilo/capital-bikeshare-dataset-202005202408 |
| Kaggle — SF Bay Area | https://www.kaggle.com/datasets/benhamner/sf-bay-area-bike-share |
| Kaggle — Bluebikes Boston | https://www.kaggle.com/datasets/jackdaoud/bluebikes-in-boston |
| Data.gov — Bikeshare Systems | https://catalog.data.gov/dataset/bikeshare-scooter-systems2 |
