# Level 2 Simulation — Joining Datasets
# Enrich cleaned trip data with station metadata.
# Aim: under 15 minutes.

TRIPS = [
    {"trip_id": "1", "start_station_id": "S1", "end_station_id": "S2", "duration_sec": 1800, "user_type": "Subscriber"},
    {"trip_id": "2", "start_station_id": "S2", "end_station_id": "S3", "duration_sec": 600,  "user_type": "Customer"},
    {"trip_id": "3", "start_station_id": "S1", "end_station_id": "S4", "duration_sec": 2400, "user_type": "Subscriber"},
    {"trip_id": "4", "start_station_id": "S9", "end_station_id": "S2", "duration_sec": 900,  "user_type": "Customer"},  # S9 unknown
]

STATIONS = [
    {"station_id": "S1", "station_name": "Central Park", "capacity": 30},
    {"station_id": "S2", "station_name": "Times Square",  "capacity": 50},
    {"station_id": "S3", "station_name": "Brooklyn Bridge","capacity": 20},
]


class TripEnricher:
    def __init__(self, trips, stations):
        self.trips = trips
        # Build lookup dict for O(1) access — always do this for joins
        self.station_lookup = {s["station_id"]: s for s in stations}

    def enrich(self):
        """
        Join trips with stations on start and end station.
        - Include only trips where BOTH stations are known (inner join)
        - Add: start_station_name, end_station_name, start_capacity, end_capacity
        """
        result = []
        for trip in self.trips:
            start = self.station_lookup.get(trip["start_station_id"])
            end   = self.station_lookup.get(trip["end_station_id"])
            if not start or not end:
                continue
            enriched = dict(trip)
            enriched["start_station_name"] = start["station_name"]
            enriched["end_station_name"]   = end["station_name"]
            enriched["start_capacity"]     = start["capacity"]
            enriched["end_capacity"]       = end["capacity"]
            result.append(enriched)
        return result

    def top_routes(self, n=3):
        """Return top N routes by trip count as list of (start_name, end_name, count)."""
        from collections import Counter
        enriched = self.enrich()
        counts = Counter((r["start_station_name"], r["end_station_name"]) for r in enriched)
        return [(s, e, c) for (s, e), c in counts.most_common(n)]

    def avg_duration_by_user_type(self):
        """Return dict of user_type -> avg duration_sec (enriched trips only)."""
        from collections import defaultdict
        enriched = self.enrich()
        buckets = defaultdict(list)
        for r in enriched:
            buckets[r["user_type"]].append(r["duration_sec"])
        return {ut: sum(v)/len(v) for ut, v in buckets.items()}


if __name__ == "__main__":
    enricher = TripEnricher(TRIPS, STATIONS)

    enriched = enricher.enrich()
    print(f"Enriched trips: {len(enriched)}")  # 3 (trip_id 4 has unknown S9)
    assert len(enriched) == 3

    routes = enricher.top_routes()
    print(f"Top routes: {routes}")

    avg = enricher.avg_duration_by_user_type()
    print(f"Avg duration by user type: {avg}")
    assert avg["Subscriber"] == (1800 + 2400) / 2
    print("All assertions passed.")
