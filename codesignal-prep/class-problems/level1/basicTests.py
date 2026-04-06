import unittest
from bikeProcessor import BikeProcessor


TRIPS = [
    {"ride_id": "R001", "started_at": "2024-03-01 07:00:00", "ended_at": "2024-03-01 07:20:00",
     "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},

    {"ride_id": "R002", "started_at": "2024-03-01 08:00:00", "ended_at": "2024-03-01 07:50:00",
     "start_station_id": "S2", "end_station_id": "S3", "member_casual": "casual"},   # bad duration

    {"ride_id": "R003", "started_at": "2024-03-01 09:00:00", "ended_at": "2024-03-01 09:30:00",
     "start_station_id": "",   "end_station_id": "S1", "member_casual": "member"},   # empty station

    {"ride_id": "R004", "started_at": "2024-03-01 10:00:00", "ended_at": "2024-03-01 10:45:00",
     "start_station_id": "S3", "end_station_id": "S1", "member_casual": "casual"},

    {"ride_id": "R001", "started_at": "2024-03-01 11:00:00", "ended_at": "2024-03-01 11:30:00",
     "start_station_id": "S1", "end_station_id": "S3", "member_casual": "member"},   # duplicate R001

    {"ride_id": "R005", "started_at": "2024-03-01 12:00:00", "ended_at": "2024-03-01 12:10:00",
     "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},
]


class TestBikeProcessorBasic(unittest.TestCase):

    def setUp(self):
        self.p = BikeProcessor(TRIPS)

    def test_total_count(self):
        self.assertEqual(self.p.total_count(), 6)

    def test_duration_sec(self):
        self.assertEqual(self.p.get_duration_sec(TRIPS[0]), 1200.0)

    def test_duration_negative(self):
        self.assertLess(self.p.get_duration_sec(TRIPS[1]), 0)

    def test_valid_trips_count(self):
        # R001(first), R004, R005 are valid — R002 bad dur, R003 empty station, R001(dup) removed
        self.assertEqual(len(self.p.get_valid_trips()), 3)

    def test_valid_excludes_bad_duration(self):
        ids = [t["ride_id"] for t in self.p.get_valid_trips()]
        self.assertNotIn("R002", ids)

    def test_valid_excludes_empty_station(self):
        ids = [t["ride_id"] for t in self.p.get_valid_trips()]
        self.assertNotIn("R003", ids)

    def test_valid_excludes_duplicate_keeps_first(self):
        valid = self.p.get_valid_trips()
        r001 = [t for t in valid if t["ride_id"] == "R001"]
        self.assertEqual(len(r001), 1)
        self.assertEqual(r001[0]["started_at"], "2024-03-01 07:00:00")  # first one kept

    def test_average_duration(self):
        # R001=1200, R004=2700, R005=600 → avg=1500.0
        self.assertEqual(self.p.get_average_duration(), 1500.0)

    def test_count_by_user_type(self):
        result = self.p.get_count_by_user_type()
        self.assertEqual(result["member"], 2)   # R001, R005
        self.assertEqual(result["casual"], 1)   # R004

    def test_top_start_stations(self):
        top = self.p.get_top_start_stations(2)
        self.assertEqual(top[0], "S1")          # S1 appears twice (R001, R005)
        self.assertEqual(len(top), 2)

    def test_empty_input(self):
        p = BikeProcessor([])
        self.assertEqual(p.total_count(), 0)
        self.assertEqual(p.get_valid_trips(), [])
        self.assertEqual(p.get_average_duration(), 0.0)
        self.assertEqual(p.get_count_by_user_type(), {})
        self.assertEqual(p.get_top_start_stations(3), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
