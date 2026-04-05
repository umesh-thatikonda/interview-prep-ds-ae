"""
Python Class + unittest Refresher
Run this file directly: python3 class_and_unittest_refresher.py
All tests will execute and tell you what passed/failed.
"""

import unittest
import csv
import json
from datetime import datetime
from collections import defaultdict, Counter


# ══════════════════════════════════════════════════════════════════════════════
# PART 1 — PYTHON CLASS BASICS
# ══════════════════════════════════════════════════════════════════════════════

"""
A class is a blueprint. __init__ runs when you create an object.
self refers to the current instance — always the first argument in every method.
"""

class Dog:
    def __init__(self, name, breed):   # runs on Dog("Rex", "Lab")
        self.name  = name              # stored on the instance
        self.breed = breed
        self.tricks = []               # mutable default — always init in __init__

    def learn(self, trick):
        self.tricks.append(trick)
        return self                    # returning self allows method chaining

    def describe(self):
        return f"{self.name} ({self.breed}) knows: {self.tricks}"


# Usage
rex = Dog("Rex", "Labrador")
rex.learn("sit").learn("shake")        # chaining works because learn() returns self
print(rex.describe())
# → Rex (Labrador) knows: ['sit', 'shake']


# ══════════════════════════════════════════════════════════════════════════════
# PART 2 — CODESIGNAL FORMAT (what you'll actually see)
# ══════════════════════════════════════════════════════════════════════════════

"""
CodeSignal gives you a class with:
- __init__ that receives raw data
- method stubs with docstrings telling you what to return
- a test suite that calls those methods and checks results

Your job: fill in the method bodies. That's it.
"""

class TripProcessor:
    def __init__(self, trips):
        """
        trips: list of dicts, each with keys:
            ride_id, started_at, ended_at,
            start_station_id, end_station_id, member_casual
        Timestamps are strings: "YYYY-MM-DD HH:MM:SS"
        """
        self.trips = trips
        self._fmt  = "%Y-%m-%d %H:%M:%S"

    # ── helper (private by convention, not enforced) ──────────────────────────
    def _parse(self, ts):
        return datetime.strptime(ts, self._fmt)

    def _duration(self, trip):
        return (self._parse(trip["ended_at"]) - self._parse(trip["started_at"])).total_seconds()

    # ── public methods (what the test suite calls) ────────────────────────────

    def count(self):
        """Return total number of trips."""
        return len(self.trips)

    def valid_trips(self):
        """
        Return list of trips where:
        - duration > 0
        - start_station_id and end_station_id are non-empty strings
        Preserve original order.
        """
        result = []
        for t in self.trips:
            if (t.get("start_station_id") and
                t.get("end_station_id") and
                self._duration(t) > 0):
                result.append(t)
        return result

    def average_duration(self):
        """
        Return average trip duration in seconds across valid trips.
        Return 0 if no valid trips.
        Round to 2 decimal places.
        """
        valid = self.valid_trips()
        if not valid:
            return 0
        return round(sum(self._duration(t) for t in valid) / len(valid), 2)

    def trips_by_user_type(self):
        """
        Return dict: { "member": count, "casual": count }
        Only count valid trips.
        """
        counts = Counter(t["member_casual"] for t in self.valid_trips())
        return dict(counts)

    def top_stations(self, n=3):
        """
        Return list of top-n start_station_ids by trip count (valid trips only).
        Sorted descending by count. Return station_id strings only.
        """
        counts = Counter(t["start_station_id"] for t in self.valid_trips())
        return [s for s, _ in counts.most_common(n)]


# ══════════════════════════════════════════════════════════════════════════════
# PART 3 — UNITTEST BASICS
# ══════════════════════════════════════════════════════════════════════════════

"""
unittest is Python's built-in test framework — no install needed.

Key ideas:
- Write a class that inherits from unittest.TestCase
- Each test method must start with test_
- Use self.assertEqual, self.assertTrue, self.assertIn etc to assert
- Run with: python3 filename.py   OR   python3 -m unittest filename
"""

# ── Shared test data ──────────────────────────────────────────────────────────
SAMPLE_TRIPS = [
    {
        "ride_id": "R001",
        "started_at":       "2024-03-01 07:05:00",
        "ended_at":         "2024-03-01 07:25:00",  # 1200 sec
        "start_station_id": "S1",
        "end_station_id":   "S2",
        "member_casual":    "member",
    },
    {
        "ride_id": "R002",
        "started_at":       "2024-03-01 09:00:00",
        "ended_at":         "2024-03-01 08:50:00",  # INVALID — end < start
        "start_station_id": "S2",
        "end_station_id":   "S3",
        "member_casual":    "casual",
    },
    {
        "ride_id": "R003",
        "started_at":       "2024-03-01 10:00:00",
        "ended_at":         "2024-03-01 10:45:00",  # 2700 sec
        "start_station_id": "",                      # INVALID — empty station
        "end_station_id":   "S1",
        "member_casual":    "member",
    },
    {
        "ride_id": "R004",
        "started_at":       "2024-03-01 12:00:00",
        "ended_at":         "2024-03-01 12:30:00",  # 1800 sec
        "start_station_id": "S1",
        "end_station_id":   "S3",
        "member_casual":    "casual",
    },
    {
        "ride_id": "R005",
        "started_at":       "2024-03-01 14:00:00",
        "ended_at":         "2024-03-01 14:20:00",  # 1200 sec
        "start_station_id": "S1",
        "end_station_id":   "S2",
        "member_casual":    "member",
    },
]


class TestTripProcessor(unittest.TestCase):

    def setUp(self):
        """Runs before EVERY test method. Fresh processor each time."""
        self.processor = TripProcessor(SAMPLE_TRIPS)

    # ── test methods ──────────────────────────────────────────────────────────

    def test_count_total(self):
        """Total trips including invalid ones."""
        self.assertEqual(self.processor.count(), 5)

    def test_valid_trips_excludes_bad_duration(self):
        """R002 has end < start — should be excluded."""
        valid_ids = [t["ride_id"] for t in self.processor.valid_trips()]
        self.assertNotIn("R002", valid_ids)

    def test_valid_trips_excludes_empty_station(self):
        """R003 has empty start_station_id — should be excluded."""
        valid_ids = [t["ride_id"] for t in self.processor.valid_trips()]
        self.assertNotIn("R003", valid_ids)

    def test_valid_trips_count(self):
        """Only R001, R004, R005 are valid."""
        self.assertEqual(len(self.processor.valid_trips()), 3)

    def test_average_duration(self):
        """Valid trips: R001=1200, R004=1800, R005=1200 → avg=1400.0"""
        self.assertEqual(self.processor.average_duration(), 1400.0)

    def test_trips_by_user_type(self):
        """R001=member, R004=casual, R005=member → member:2, casual:1"""
        result = self.processor.trips_by_user_type()
        self.assertEqual(result["member"], 2)
        self.assertEqual(result["casual"], 1)

    def test_top_stations(self):
        """S1 appears in R001, R004, R005 → should be first. Only 1 unique start station in valid trips."""
        top = self.processor.top_stations(n=3)
        self.assertEqual(top[0], "S1")
        self.assertIn("S1", top)

    def test_empty_input(self):
        """Edge case: no trips at all."""
        p = TripProcessor([])
        self.assertEqual(p.count(), 0)
        self.assertEqual(p.valid_trips(), [])
        self.assertEqual(p.average_duration(), 0)
        self.assertEqual(p.trips_by_user_type(), {})

    def test_all_invalid(self):
        """Edge case: all trips are invalid."""
        bad = [SAMPLE_TRIPS[1], SAMPLE_TRIPS[2]]  # bad duration + empty station
        p = TripProcessor(bad)
        self.assertEqual(p.valid_trips(), [])
        self.assertEqual(p.average_duration(), 0)


# ══════════════════════════════════════════════════════════════════════════════
# PART 4 — UNITTEST ASSERTION CHEATSHEET
# ══════════════════════════════════════════════════════════════════════════════

"""
self.assertEqual(a, b)          — a == b
self.assertNotEqual(a, b)       — a != b
self.assertTrue(x)              — bool(x) is True
self.assertFalse(x)             — bool(x) is False
self.assertIn(item, container)  — item in container
self.assertNotIn(item, container)
self.assertIsNone(x)            — x is None
self.assertIsNotNone(x)
self.assertAlmostEqual(a, b, places=2)  — floats: round(a-b, 2) == 0
self.assertGreater(a, b)        — a > b
self.assertGreaterEqual(a, b)   — a >= b
self.assertRaises(ValueError, fn, arg)  — fn(arg) raises ValueError
"""


# ══════════════════════════════════════════════════════════════════════════════
# PART 5 — HOW CODESIGNAL TESTS LOOK (what the grader runs)
# ══════════════════════════════════════════════════════════════════════════════

"""
You won't see the test file — CodeSignal hides it.
But it looks exactly like Part 3 above.

It will do things like:
    processor = BikeShareProcessor(raw_data)
    result = processor.clean_trips()
    assert result == expected_output   ← pass or fail

Your strategy:
1. Copy the class stub into your editor
2. Read each docstring CAREFULLY — that is your spec
3. Implement one method at a time
4. Run the visible tests often (no penalty)
5. The hidden tests check edge cases — always handle:
   - empty input []
   - all invalid data
   - duplicate IDs
   - null / empty string fields
   - negative or zero durations
"""


# ══════════════════════════════════════════════════════════════════════════════
# RUN TESTS
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    unittest.main(verbosity=2)
