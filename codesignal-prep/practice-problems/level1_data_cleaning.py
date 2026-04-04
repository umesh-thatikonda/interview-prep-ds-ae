# Level 1 Simulation — Data Cleaning
# Typical first level: load raw bike trip data, clean it, return valid rows.
# Practice writing this fast — aim for under 15 minutes.

import csv
from datetime import datetime


RAW_DATA = [
    {"trip_id": "1", "start_time": "2024-01-01 08:00:00", "end_time": "2024-01-01 08:30:00",
     "start_station": "A", "end_station": "B", "bike_id": "101", "user_type": "Subscriber"},
    {"trip_id": "2", "start_time": "2024-01-01 09:00:00", "end_time": "2024-01-01 08:50:00",  # end < start
     "start_station": "B", "end_station": "C", "bike_id": "102", "user_type": "Customer"},
    {"trip_id": "3", "start_time": "2024-01-01 10:00:00", "end_time": "2024-01-01 10:45:00",
     "start_station": "",  "end_station": "D", "bike_id": "103", "user_type": "Subscriber"},  # missing station
    {"trip_id": "4", "start_time": "2024-01-01 11:00:00", "end_time": "2024-01-01 11:20:00",
     "start_station": "C", "end_station": "A", "bike_id": "104", "user_type": "Subscriber"},
    {"trip_id": "1", "start_time": "2024-01-01 08:00:00", "end_time": "2024-01-01 08:30:00",  # duplicate
     "start_station": "A", "end_station": "B", "bike_id": "101", "user_type": "Subscriber"},
]


class BikeDataProcessor:
    def __init__(self, data):
        self.data = data

    def clean(self):
        """
        Return cleaned trip records. Rules:
        - Remove rows with missing start_station or end_station
        - Remove rows where end_time <= start_time (invalid duration)
        - Remove duplicate trip_ids (keep first occurrence)
        - Add duration_sec column (int)
        """
        seen_ids = set()
        result = []
        fmt = "%Y-%m-%d %H:%M:%S"

        for row in self.data:
            # Dedup
            if row["trip_id"] in seen_ids:
                continue
            seen_ids.add(row["trip_id"])

            # Missing stations
            if not row["start_station"] or not row["end_station"]:
                continue

            # Invalid duration
            start = datetime.strptime(row["start_time"], fmt)
            end = datetime.strptime(row["end_time"], fmt)
            duration = int((end - start).total_seconds())
            if duration <= 0:
                continue

            row = dict(row)
            row["duration_sec"] = duration
            result.append(row)

        return result


if __name__ == "__main__":
    processor = BikeDataProcessor(RAW_DATA)
    clean = processor.clean()
    print(f"Input: {len(RAW_DATA)} rows  →  Clean: {len(clean)} rows")
    for r in clean:
        print(r)

    # Expected: trip_ids 1 and 4 only (2 has bad duration, 3 missing station, second 1 is dup)
    assert len(clean) == 2
    assert {r["trip_id"] for r in clean} == {"1", "4"}
    assert clean[0]["duration_sec"] == 1800
    print("All assertions passed.")
