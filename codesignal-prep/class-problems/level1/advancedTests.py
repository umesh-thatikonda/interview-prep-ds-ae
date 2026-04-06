import unittest
from bikeProcessor import BikeProcessor


class TestBikeProcessorAdvanced(unittest.TestCase):

    def test_all_invalid(self):
        trips = [
            {"ride_id": "X1", "started_at": "2024-01-01 10:00:00", "ended_at": "2024-01-01 09:00:00",
             "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},
            {"ride_id": "X2", "started_at": "2024-01-01 10:00:00", "ended_at": "2024-01-01 10:30:00",
             "start_station_id": None, "end_station_id": "S2", "member_casual": "casual"},
        ]
        p = BikeProcessor(trips)
        self.assertEqual(p.get_valid_trips(), [])
        self.assertEqual(p.get_average_duration(), 0.0)
        self.assertEqual(p.get_count_by_user_type(), {})

    def test_zero_duration_is_invalid(self):
        trips = [
            {"ride_id": "Z1", "started_at": "2024-01-01 10:00:00", "ended_at": "2024-01-01 10:00:00",
             "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},
        ]
        p = BikeProcessor(trips)
        self.assertEqual(p.get_valid_trips(), [])

    def test_none_station_is_invalid(self):
        trips = [
            {"ride_id": "N1", "started_at": "2024-01-01 08:00:00", "ended_at": "2024-01-01 08:30:00",
             "start_station_id": None, "end_station_id": "S2", "member_casual": "member"},
        ]
        p = BikeProcessor(trips)
        self.assertEqual(p.get_valid_trips(), [])

    def test_top_stations_tie_broken_alphabetically(self):
        # S1 and S2 both appear once — alphabetical tiebreak
        trips = [
            {"ride_id": "A1", "started_at": "2024-01-01 08:00:00", "ended_at": "2024-01-01 08:30:00",
             "start_station_id": "S2", "end_station_id": "S3", "member_casual": "member"},
            {"ride_id": "A2", "started_at": "2024-01-01 09:00:00", "ended_at": "2024-01-01 09:30:00",
             "start_station_id": "S1", "end_station_id": "S3", "member_casual": "casual"},
        ]
        p = BikeProcessor(trips)
        top = p.get_top_start_stations(2)
        self.assertEqual(top, ["S1", "S2"])     # alphabetical when tied

    def test_top_n_larger_than_unique_stations(self):
        trips = [
            {"ride_id": "B1", "started_at": "2024-01-01 08:00:00", "ended_at": "2024-01-01 08:30:00",
             "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},
        ]
        p = BikeProcessor(trips)
        top = p.get_top_start_stations(10)
        self.assertEqual(len(top), 1)           # only 1 unique station exists

    def test_many_duplicates(self):
        trips = [
            {"ride_id": "D1", "started_at": "2024-01-01 08:00:00", "ended_at": "2024-01-01 08:30:00",
             "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},
            {"ride_id": "D1", "started_at": "2024-01-01 09:00:00", "ended_at": "2024-01-01 09:30:00",
             "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},
            {"ride_id": "D1", "started_at": "2024-01-01 10:00:00", "ended_at": "2024-01-01 10:30:00",
             "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},
        ]
        p = BikeProcessor(trips)
        self.assertEqual(len(p.get_valid_trips()), 1)   # only first D1 kept

    def test_average_duration_precision(self):
        trips = [
            {"ride_id": "P1", "started_at": "2024-01-01 08:00:00", "ended_at": "2024-01-01 08:00:01",
             "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},
            {"ride_id": "P2", "started_at": "2024-01-01 09:00:00", "ended_at": "2024-01-01 09:00:02",
             "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},
        ]
        p = BikeProcessor(trips)
        self.assertEqual(p.get_average_duration(), 1.5)


if __name__ == "__main__":
    unittest.main(verbosity=2)
