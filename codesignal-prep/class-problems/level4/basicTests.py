import unittest
from bikePipeline import BikePipeline


TRIPS = [
    {"ride_id": "R001", "started_at": "2024-03-01 07:05:00", "ended_at": "2024-03-01 07:25:00",
     "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},

    {"ride_id": "R002", "started_at": "2024-03-01 08:00:00", "ended_at": "2024-03-01 08:45:00",
     "start_station_id": "S2", "end_station_id": "S1", "member_casual": "casual"},

    {"ride_id": "R003", "started_at": "2024-03-01 09:00:00", "ended_at": "2024-03-01 08:50:00",
     "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},   # bad duration

    {"ride_id": "R004", "started_at": "2024-03-02 07:30:00", "ended_at": "2024-03-02 08:00:00",
     "start_station_id": "S1", "end_station_id": "S2", "member_casual": "member"},

    {"ride_id": "R005", "started_at": "2024-03-02 09:00:00", "ended_at": "2024-03-02 09:20:00",
     "start_station_id": "",   "end_station_id": "S2", "member_casual": "casual"},   # empty station

    {"ride_id": "R001", "started_at": "2024-03-02 10:00:00", "ended_at": "2024-03-02 10:30:00",
     "start_station_id": "S2", "end_station_id": "S1", "member_casual": "casual"},   # duplicate
]

STATIONS = [
    {"station_id": "S1", "station_name": "Central Park",  "region": "North",   "capacity": 20},
    {"station_id": "S2", "station_name": "Times Square",  "region": "Central", "capacity": 35},
]


class TestBikePipelineBasic(unittest.TestCase):

    def setUp(self):
        self.pipeline = BikePipeline(TRIPS, STATIONS).run()

    def test_run_returns_self(self):
        p = BikePipeline(TRIPS, STATIONS)
        self.assertIs(p.run(), p)

    def test_clean_trip_count(self):
        # Valid: R001, R002, R004 — R003 bad dur, R005 empty station, R001 dup removed
        self.assertEqual(len(self.pipeline.get_clean_trips()), 3)

    def test_error_count(self):
        self.assertEqual(self.pipeline.get_error_count(), 3)

    def test_transform_adds_duration(self):
        trips = self.pipeline.get_clean_trips()
        for t in trips:
            self.assertIn("duration_sec", t)
            self.assertGreater(t["duration_sec"], 0)

    def test_transform_adds_hour(self):
        trips = {t["ride_id"]: t for t in self.pipeline.get_clean_trips()}
        self.assertEqual(trips["R001"]["hour_of_day"], 7)
        self.assertEqual(trips["R002"]["hour_of_day"], 8)

    def test_transform_adds_day_of_week(self):
        trips = {t["ride_id"]: t for t in self.pipeline.get_clean_trips()}
        self.assertEqual(trips["R001"]["day_of_week"], "Friday")

    def test_transform_adds_date(self):
        trips = {t["ride_id"]: t for t in self.pipeline.get_clean_trips()}
        self.assertEqual(trips["R001"]["date"], "2024-03-01")

    def test_transform_trip_length_short(self):
        # R001 = 1200s → medium
        trips = {t["ride_id"]: t for t in self.pipeline.get_clean_trips()}
        self.assertEqual(trips["R001"]["trip_length"], "medium")

    def test_transform_trip_length_long(self):
        # R002 = 2700s → long
        trips = {t["ride_id"]: t for t in self.pipeline.get_clean_trips()}
        self.assertEqual(trips["R002"]["trip_length"], "long")

    def test_daily_summary_dates(self):
        summary = self.pipeline.get_daily_summary()
        dates = [s["date"] for s in summary]
        self.assertIn("2024-03-01", dates)
        self.assertIn("2024-03-02", dates)
        self.assertEqual(dates, sorted(dates))   # sorted ascending

    def test_daily_summary_fields(self):
        summary = self.pipeline.get_daily_summary()
        for s in summary:
            for field in ["date","trip_count","avg_duration","pct_member","busiest_hour"]:
                self.assertIn(field, s)

    def test_daily_trip_counts(self):
        summary = {s["date"]: s for s in self.pipeline.get_daily_summary()}
        self.assertEqual(summary["2024-03-01"]["trip_count"], 2)  # R001, R002
        self.assertEqual(summary["2024-03-02"]["trip_count"], 1)  # R004

    def test_station_daily_report_fields(self):
        report = self.pipeline.get_station_daily_report()
        for r in report:
            for field in ["date","station_id","station_name","trip_count","avg_duration","cumulative_trips"]:
                self.assertIn(field, r)

    def test_station_daily_cumulative(self):
        report = self.pipeline.get_station_daily_report()
        s1 = [r for r in report if r["station_id"] == "S1"]
        # S1 appears on 2024-03-01 (R001) and 2024-03-02 (R004)
        self.assertEqual(len(s1), 2)
        s1_sorted = sorted(s1, key=lambda x: x["date"])
        self.assertEqual(s1_sorted[0]["cumulative_trips"], 1)
        self.assertEqual(s1_sorted[1]["cumulative_trips"], 2)

    def test_raises_before_run(self):
        p = BikePipeline(TRIPS, STATIONS)   # run() not called
        with self.assertRaises(RuntimeError):
            p.get_clean_trips()
        with self.assertRaises(RuntimeError):
            p.get_daily_summary()

    def test_empty_input(self):
        p = BikePipeline([], STATIONS).run()
        self.assertEqual(p.get_clean_trips(), [])
        self.assertEqual(p.get_error_count(), 0)
        self.assertEqual(p.get_daily_summary(), [])
        self.assertEqual(p.get_station_daily_report(), [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
