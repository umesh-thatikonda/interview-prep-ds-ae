# Level 4 Simulation — Basic Pipeline Logic
# Ingest raw data → validate → transform → output summary.
# This simulates a simplified ETL pipeline. Aim: under 20 minutes.

import csv
import json
from datetime import datetime
from collections import defaultdict


class BikeSharePipeline:
    def __init__(self):
        self.raw = []
        self.clean = []
        self.errors = []

    # ── INGEST ────────────────────────────────────────────────────────────────
    def ingest_csv(self, filepath):
        """Load raw rows from CSV."""
        with open(filepath) as f:
            self.raw = list(csv.DictReader(f))
        return self

    def ingest_records(self, records):
        """Load from list of dicts (for testing)."""
        self.raw = records
        return self

    # ── VALIDATE & CLEAN ──────────────────────────────────────────────────────
    def process(self):
        """Clean raw data. Log errors. Populate self.clean."""
        fmt = "%Y-%m-%d %H:%M:%S"
        seen = set()

        for row in self.raw:
            trip_id = row.get("trip_id", "").strip()

            # Dedup
            if trip_id in seen:
                self.errors.append({"trip_id": trip_id, "reason": "duplicate"})
                continue
            seen.add(trip_id)

            # Required fields
            for field in ["start_station_id", "end_station_id", "start_time", "end_time"]:
                if not row.get(field, "").strip():
                    self.errors.append({"trip_id": trip_id, "reason": f"missing {field}"})
                    break
            else:
                # Duration
                try:
                    start = datetime.strptime(row["start_time"].strip(), fmt)
                    end   = datetime.strptime(row["end_time"].strip(), fmt)
                    duration = int((end - start).total_seconds())
                    if duration <= 0:
                        raise ValueError("non-positive duration")
                except Exception as e:
                    self.errors.append({"trip_id": trip_id, "reason": str(e)})
                    continue

                self.clean.append({
                    "trip_id": trip_id,
                    "start_station_id": row["start_station_id"].strip(),
                    "end_station_id":   row["end_station_id"].strip(),
                    "duration_sec":     duration,
                    "user_type":        row.get("user_type", "Unknown").strip(),
                    "start_time":       row["start_time"].strip(),
                })

        return self

    # ── TRANSFORM / AGGREGATE ─────────────────────────────────────────────────
    def summary(self):
        """Return dict summary of clean data."""
        if not self.clean:
            return {}

        durations = [r["duration_sec"] for r in self.clean]
        station_counts = defaultdict(int)
        for r in self.clean:
            station_counts[r["start_station_id"]] += 1

        top_station = max(station_counts, key=station_counts.get)

        return {
            "total_trips": len(self.clean),
            "error_count": len(self.errors),
            "avg_duration_sec": round(sum(durations) / len(durations), 2),
            "max_duration_sec": max(durations),
            "min_duration_sec": min(durations),
            "top_start_station": top_station,
            "top_start_station_trips": station_counts[top_station],
        }

    # ── OUTPUT ────────────────────────────────────────────────────────────────
    def write_clean(self, filepath):
        if not self.clean:
            return
        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=self.clean[0].keys())
            writer.writeheader()
            writer.writerows(self.clean)

    def write_summary(self, filepath):
        with open(filepath, "w") as f:
            json.dump(self.summary(), f, indent=2)


# ── TEST ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    SAMPLE = [
        {"trip_id": "1", "start_time": "2024-01-01 08:00:00", "end_time": "2024-01-01 08:30:00",
         "start_station_id": "S1", "end_station_id": "S2", "user_type": "Subscriber"},
        {"trip_id": "2", "start_time": "2024-01-01 09:00:00", "end_time": "2024-01-01 08:50:00",  # bad duration
         "start_station_id": "S2", "end_station_id": "S3", "user_type": "Customer"},
        {"trip_id": "3", "start_time": "2024-01-01 10:00:00", "end_time": "2024-01-01 10:20:00",
         "start_station_id": "S1", "end_station_id": "S3", "user_type": "Subscriber"},
        {"trip_id": "1", "start_time": "2024-01-01 08:00:00", "end_time": "2024-01-01 08:30:00",  # dup
         "start_station_id": "S1", "end_station_id": "S2", "user_type": "Subscriber"},
    ]

    pipeline = BikeSharePipeline()
    pipeline.ingest_records(SAMPLE).process()

    print(f"Clean: {len(pipeline.clean)}, Errors: {len(pipeline.errors)}")
    print("Errors:", pipeline.errors)
    print("Summary:", pipeline.summary())

    assert len(pipeline.clean) == 2
    assert len(pipeline.errors) == 2
    assert pipeline.summary()["top_start_station"] == "S1"
    print("All assertions passed.")
