from datetime import datetime
from collections import defaultdict, Counter


class BikeEnricher:
    """
    Enriches valid bike-share trip data with station metadata
    and produces aggregated summaries.

    trips: list of dicts with keys:
        ride_id, started_at, ended_at,
        start_station_id, end_station_id, member_casual

    stations: list of dicts with keys:
        station_id, station_name, region, capacity
    """

    def __init__(self, trips: list, stations: list):
        """
        Initialize with raw trips and station metadata.
        On init, automatically:
            - Remove invalid trips (duration <= 0, empty stations, duplicates)
            - Build a station lookup dict for O(1) access
        Store cleaned trips and station lookup as instance variables.
        """
        pass

    def _get_duration_sec(self, trip: dict) -> float:
        """Return duration in seconds. Negative if ended_at < started_at."""
        fmt = "%Y-%m-%d %H:%M:%S"
        return (datetime.strptime(trip["ended_at"], fmt) -
                datetime.strptime(trip["started_at"], fmt)).total_seconds()

    def get_enriched_trips(self) -> list:
        """
        Return list of valid trips enriched with station data.
        Each returned dict must include ALL original trip fields PLUS:
            start_station_name : str
            start_region       : str
            end_station_name   : str
            end_region         : str
        Only include trips where BOTH start and end stations exist in stations list.
        Preserve original order.
        """
        pass

    def get_trips_per_region(self) -> dict:
        """
        Return dict of { region: trip_count } for start station regions.
        Use enriched trips only (both stations must be known).
        """
        pass

    def get_most_popular_route(self) -> tuple:
        """
        Return the most popular (start_station_name, end_station_name) pair
        as a tuple, based on enriched trips.
        If tie, return the one that comes first alphabetically by start_station_name,
        then end_station_name.
        Return None if no enriched trips exist.
        """
        pass

    def get_avg_duration_by_region(self) -> dict:
        """
        Return dict of { region: avg_duration_sec } for start station region.
        Use enriched trips only. Round to 2 decimal places.
        """
        pass

    def get_station_summary(self) -> list:
        """
        Return a list of dicts, one per station that appears as a start station
        in enriched trips. Each dict has:
            station_id   : str
            station_name : str
            region       : str
            trip_count   : int
            avg_duration : float  (seconds, rounded to 2 dp)
        Sorted by trip_count descending, then station_name ascending.
        """
        pass
