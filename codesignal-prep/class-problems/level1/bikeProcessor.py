from datetime import datetime


class BikeProcessor:
    """
    A processor for bike-share trip data.

    Each trip is a dict with the following keys:
        ride_id         : str   — unique trip identifier
        started_at      : str   — format "YYYY-MM-DD HH:MM:SS"
        ended_at        : str   — format "YYYY-MM-DD HH:MM:SS"
        start_station_id: str   — may be empty string or None
        end_station_id  : str   — may be empty string or None
        member_casual   : str   — "member" or "casual"
    """

    def __init__(self, trips: list):
        """
        Initialize with a list of raw trip dicts.
        """
        self.trips = trips

    def total_count(self) -> int:
        """
        Return the total number of trips including invalid ones.
        """
        return len(self.trips)

    def get_duration_sec(self, trip: dict) -> float:
        """
        Return the duration of the given trip in seconds.
        Duration = ended_at - started_at.
        Timestamps are strings in format "YYYY-MM-DD HH:MM:SS".
        """
        fmt = "%Y-%m-%d %H:%M:%S"
        start = datetime.strptime(trip["started_at"], fmt)
        end = datetime.strptime(trip["ended_at"], fmt)
        return (end - start).total_seconds()

    def get_valid_trips(self) -> list:
        """
        Return list of valid trips preserving original order.
        A trip is invalid if ANY of the following:
            - duration <= 0  (ended_at <= started_at)
            - start_station_id is empty string or None
            - end_station_id is empty string or None
            - ride_id is a duplicate (keep first occurrence only)
        """
        seen_ids = set()
        valid = []
        for t in self.trips:
            if t["ride_id"] in seen_ids:
                continue
            seen_ids.add(t["ride_id"])
            if self.get_duration_sec(t) <= 0:
                continue
            if not t["start_station_id"]:
                continue
            if not t["end_station_id"]:
                continue
            valid.append(t)
        return valid

    def get_average_duration(self) -> float:
        """
        Return average duration in seconds of valid trips only.
        Round to 2 decimal places.
        Return 0.0 if there are no valid trips.
        """
        return round(sum(self.get_duration_sec(t) for t in self.get_valid_trips()) / max(len(self.get_valid_trips()), 1), 2)

    def get_count_by_user_type(self) -> dict:
        """
        Return a dict with counts of valid trips per user type.
        Example: {"member": 10, "casual": 5}
        Only include user types that appear in valid trips.
        """
        val = dict()

        for t in self.trips:

            if t["member_casual"] not in val and t in self.get_valid_trips():
                val[t["member_casual"]] = 0
            if t in self.get_valid_trips():
                val[t["member_casual"]] += 1
        return val


    def get_top_start_stations(self, n: int) -> list:
        """
        Return list of top-n start_station_ids by trip count
        across valid trips only.
        Sorted descending by count.
        If two stations have the same count, sort alphabetically ascending.
        Return station IDs only (not counts).
        """
        result = dict()
        for t in self.trips:
            if t in self.get_valid_trips():
                if t["start_station_id"] not in result:
                    result[t["start_station_id"]] = 0
                result[t["start_station_id"]] += 1
        sorted_result = sorted(result.items(), key=lambda x: (-x[1], x[0]))
        return [x[0] for x in sorted_result[:n]]    
