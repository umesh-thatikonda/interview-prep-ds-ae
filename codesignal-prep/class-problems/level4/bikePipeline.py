from datetime import datetime
from collections import defaultdict, Counter


class BikePipeline:
    """
    A full ETL pipeline for bike-share data.

    Simulates ingesting raw trip data, validating it,
    transforming it with time features, and producing
    daily and station-level aggregated reports.

    trips: list of dicts with keys:
        ride_id, started_at, ended_at,
        start_station_id, end_station_id, member_casual

    stations: list of dicts with keys:
        station_id, station_name, region, capacity
    """

    def __init__(self, trips: list, stations: list):
        """
        Initialize pipeline. Build station lookup dict.
        Do NOT clean or transform data yet — that happens in run().
        """
        pass

    def run(self) -> "BikePipeline":
        """
        Execute the full pipeline in order:
            1. ingest   — store raw trips
            2. validate — remove invalid trips (bad duration, empty stations, duplicates)
            3. transform — add derived fields to each valid trip
        Return self to allow chaining: pipeline.run().get_daily_summary()
        """
        pass

    def _validate(self, trips: list) -> list:
        """
        Return only valid trips:
        - duration > 0
        - start_station_id and end_station_id non-empty and not None
        - no duplicate ride_ids (keep first occurrence)
        """
        pass

    def _transform(self, trips: list) -> list:
        """
        Add the following derived fields to each trip dict:
            duration_sec  : float  — trip duration in seconds
            hour_of_day   : int    — hour started_at (0-23)
            day_of_week   : str    — e.g. "Monday"
            date          : str    — "YYYY-MM-DD"
            trip_length   : str    — "short" (<600s), "medium" (600-1800s), "long" (>1800s)
        Return the transformed list.
        """
        pass

    def get_clean_trips(self) -> list:
        """
        Return the list of validated + transformed trips.
        Raise RuntimeError if run() has not been called yet.
        """
        pass

    def get_error_count(self) -> int:
        """
        Return number of trips that were removed during validation.
        Raise RuntimeError if run() has not been called yet.
        """
        pass

    def get_daily_summary(self) -> list:
        """
        Return list of dicts, one per date, with:
            date          : str
            trip_count    : int
            avg_duration  : float  (seconds, rounded to 2dp)
            pct_member    : float  (%, rounded to 1dp)
            busiest_hour  : int    (hour with most trips that day)
        Sorted by date ascending.
        Raise RuntimeError if run() has not been called yet.
        """
        pass

    def get_station_daily_report(self) -> list:
        """
        Return list of dicts, one per (date, start_station_id), with:
            date              : str
            station_id        : str
            station_name      : str   (from stations lookup, or "Unknown")
            trip_count        : int
            avg_duration      : float (seconds, rounded to 2dp)
            cumulative_trips  : int   (running total for this station across dates)
        Sorted by station_id ASC, then date ASC.
        Raise RuntimeError if run() has not been called yet.
        """
        pass
