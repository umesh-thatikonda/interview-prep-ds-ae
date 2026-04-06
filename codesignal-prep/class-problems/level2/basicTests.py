import unittest
from bikeEnricher import BikeEnricher


TRIPS = [
    {"ride_id": "R001", "started_at": "2024-03-01 07:00:00", "ended_at": "2024-03-01 07:20:00",
     "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},

    {"ride_id": "R002", "started_at": "2024-03-01 08:00:00", "ended_at": "2024-03-01 08:45:00",
     "start_station_id": "S2", "end_station_id": "S3", "member_casual": "casual"},

    {"ride_id": "R003", "started_at": "2024-03-01 09:00:00", "ended_at": "2024-03-01 09:30:00",
     "start_station_id": "S1", "end_station_id": "S3", "member_casual": "member"},

    {"ride_id": "R004", "started_at": "2024-03-01 10:00:00", "ended_at": "2024-03-01 10:10:00",
     "start_station_id": "S9", "end_station_id": "S2", "member_casual": "casual"},   # S9 unknown

    {"ride_id": "R005", "started_at": "2024-03-01 11:00:00", "ended_at": "2024-03-01 10:50:00",
     "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},   # bad duration
]

STATIONS = [
    {"station_id": "S1", "station_name": "Central Park",   "region": "North", "capacity": 20},
    {"station_id": "S2", "station_name": "Times Square",   "region": "Central", "capacity": 35},
    {"station_id": "S3", "station_name": "Brooklyn Bridge","region": "South",  "capacity": 15},
]


class TestBikeEnricherBasic(unittest.TestCase):

    def setUp(self):
        self.e = BikeEnricher(TRIPS, STATIONS)

    def test_enriched_count(self):
        # R001, R002, R003 valid + both stations known. R004 unknown S9. R005 bad duration.
        self.assertEqual(len(self.e.get_enriched_trips()), 3)

    def test_enriched_has_station_name(self):
        enriched = self.e.get_enriched_trips()
        for t in enriched:
            self.assertIn("start_station_name", t)
            self.assertIn("end_station_name", t)
            self.assertIn("start_region", t)
            self.assertIn("end_region", t)

    def test_enriched_correct_names(self):
        enriched = {t["ride_id"]: t for t in self.e.get_enriched_trips()}
        self.assertEqual(enriched["R001"]["start_station_name"], "Central Park")
        self.assertEqual(enriched["R001"]["end_station_name"],   "Times Square")
        self.assertEqual(enriched["R001"]["start_region"],       "North")

    def test_trips_per_region(self):
        result = self.e.get_trips_per_region()
        self.assertEqual(result["North"],   2)   # R001, R003 start from S1 (North)
        self.assertEqual(result["Central"], 1)   # R002 starts from S2 (Central)

    def test_most_popular_route(self):
        # R001 and R003 both go from Central Park to different places
        # R001: Central Park → Times Square
        # R002: Times Square → Brooklyn Bridge
        # R003: Central Park → Brooklyn Bridge
        # All routes appear once — alphabetical tiebreak
        route = self.e.get_most_popular_route()
        self.assertIsInstance(route, tuple)
        self.assertEqual(len(route), 2)

    def test_avg_duration_by_region(self):
        result = self.e.get_avg_duration_by_region()
        # R001=1200s, R003=1800s both from North → avg=1500.0
        self.assertEqual(result["North"], 1500.0)
        # R002=2700s from Central → avg=2700.0
        self.assertEqual(result["Central"], 2700.0)

    def test_station_summary_sorted(self):
        summary = self.e.get_station_summary()
        self.assertIsInstance(summary, list)
        self.assertGreater(len(summary), 0)
        # First station should have highest trip_count
        self.assertGreaterEqual(summary[0]["trip_count"], summary[-1]["trip_count"])

    def test_station_summary_fields(self):
        summary = self.e.get_station_summary()
        for s in summary:
            self.assertIn("station_id",   s)
            self.assertIn("station_name", s)
            self.assertIn("region",       s)
            self.assertIn("trip_count",   s)
            self.assertIn("avg_duration", s)

    def test_empty_trips(self):
        e = BikeEnricher([], STATIONS)
        self.assertEqual(e.get_enriched_trips(), [])
        self.assertEqual(e.get_trips_per_region(), {})
        self.assertIsNone(e.get_most_popular_route())
        self.assertEqual(e.get_station_summary(), [])

    def test_empty_stations(self):
        e = BikeEnricher(TRIPS, [])
        self.assertEqual(e.get_enriched_trips(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
