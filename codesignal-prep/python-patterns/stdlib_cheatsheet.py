# Python Standard Library — Speed Reference for CodeSignal
# These are the patterns you'll reach for most in a data engineering test.

# ─── CSV ──────────────────────────────────────────────────────────────────────
import csv

# Read CSV into list of dicts
with open("trips.csv") as f:
    rows = list(csv.DictReader(f))

# Write list of dicts to CSV
with open("out.csv", "w", newline="") as f:
    writer = csv.DictWriter(f, fieldnames=["trip_id", "duration"])
    writer.writeheader()
    writer.writerows(rows)

# ─── DATETIME ─────────────────────────────────────────────────────────────────
from datetime import datetime, timedelta

# Parse timestamps (common formats)
dt = datetime.strptime("2024-01-15 08:30:00", "%Y-%m-%d %H:%M:%S")
dt = datetime.strptime("2024-01-15T08:30:00", "%Y-%m-%dT%H:%M:%S")

# Duration in seconds
duration = (end_dt - start_dt).total_seconds()

# Format back to string
dt.strftime("%Y-%m-%d %H:%M:%S")

# Extract parts
dt.year, dt.month, dt.day, dt.hour, dt.weekday()  # weekday: 0=Mon

# ─── COLLECTIONS ──────────────────────────────────────────────────────────────
from collections import defaultdict, Counter

# Count occurrences
counts = Counter(row["station_id"] for row in rows)
most_common = counts.most_common(5)

# Group rows by key
by_station = defaultdict(list)
for row in rows:
    by_station[row["start_station"]].append(row)

# ─── SORTING ──────────────────────────────────────────────────────────────────
# Sort by single field
rows.sort(key=lambda r: r["duration"])

# Sort by multiple fields
rows.sort(key=lambda r: (r["start_station"], r["start_time"]))

# ─── FILTERING / CLEANING ─────────────────────────────────────────────────────
# Remove rows with missing values in key columns
clean = [r for r in rows if r["start_station"] and r["end_station"]]

# Remove rows with invalid duration (negative or zero)
clean = [r for r in rows if float(r["duration"]) > 0]

# Cast types safely
def to_int(val, default=None):
    try:
        return int(val)
    except (ValueError, TypeError):
        return default

def to_float(val, default=None):
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

# ─── JSON ─────────────────────────────────────────────────────────────────────
import json

with open("data.json") as f:
    data = json.load(f)

with open("out.json", "w") as f:
    json.dump(data, f, indent=2)

# ─── STATISTICS (stdlib) ──────────────────────────────────────────────────────
import statistics

vals = [float(r["duration"]) for r in rows]
statistics.mean(vals)
statistics.median(vals)
statistics.stdev(vals)

# ─── STRING OPS ───────────────────────────────────────────────────────────────
s.strip()           # remove whitespace
s.lower()           # normalize case
s.replace(" ", "_") # clean column names
s.split(",")        # parse delimited strings
