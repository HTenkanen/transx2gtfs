import hashlib

import pandas as pd

DIRECTIONS = {
    "inbound": 0,
    "outbound": 1,
    "inboundAndOutbound": 0,
    "circular": 0,
    "clockwise": 0,
    "antiClockwise": 0,
}


def get_direction(direction_id):
    """Return boolean direction id"""
    if direction_id in DIRECTIONS:
        return DIRECTIONS[direction_id]
    raise ValueError("Cannot determine direction from %s" % direction_id)


def get_stop_times(gtfs_info):
    """Extract stop_times attributes from GTFS info DataFrame"""
    stop_times_cols = [
        "trip_id",
        "arrival_time",
        "departure_time",
        "stop_id",
        "stop_sequence",
        "timepoint",
    ]

    # Select columns
    stop_times = gtfs_info[stop_times_cols].copy()

    # Drop duplicates (there should not be any but make sure)
    stop_times = stop_times.drop_duplicates()

    # Ensure correct data types
    int_types = ["stop_sequence", "timepoint"]
    for col in int_types:
        stop_times[col] = stop_times[col].astype(int)

    # If there is only a single sequence for a trip, do not export it
    stops_per_trip = stop_times.groupby("trip_id")["stop_id"].transform("size")
    for trip_id in stop_times.loc[stops_per_trip <= 1, "trip_id"].unique():
        print(
            "Trip '%s' does not include a sequence of stops, excluding from GTFS."
            % trip_id
        )
    return stop_times[stops_per_trip > 1].reset_index(drop=True)


def get_frequencies(gtfs_info):
    """frequencies.txt rows for frequency-based journeys, or None"""
    if "frequency_end_time" not in gtfs_info.columns:
        return None
    trips = gtfs_info.dropna(subset=["frequency_end_time"]).drop_duplicates("trip_id")
    if len(trips) == 0:
        return None
    frequencies = pd.DataFrame(
        {
            "trip_id": trips["trip_id"].to_list(),
            "start_time": trips["frequency_start_time"].to_list(),
            "end_time": trips["frequency_end_time"].to_list(),
            "headway_secs": trips["frequency_headway_secs"].astype(int).to_list(),
            "exact_times": 0,
        }
    )
    return frequencies


def make_service_id(service_ref, start_date, end_date, weekdays, exceptions):
    """
    service_id of a calendar: '<service>_<start>_<end>_<weekdays>', with a short
    hash of the encoded exceptions appended when the calendar has exceptions.
    """
    service_id = "%s_%s_%s_%s" % (service_ref, start_date, end_date, weekdays)
    if exceptions:
        service_id = "%s_%s" % (service_id, exceptions_digest(exceptions))
    return service_id


def exceptions_digest(exceptions):
    """Short stable hash of an encoded exception list"""
    return hashlib.sha1(exceptions.encode("utf-8")).hexdigest()[:8]


def generate_service_id(stop_times):
    """Generate service_id into stop_times DataFrame"""
    exceptions = (
        stop_times["exceptions"]
        if "exceptions" in stop_times.columns
        else pd.Series("", index=stop_times.index)
    )
    stop_times["service_id"] = [
        make_service_id(service_ref, start, end, weekdays, exc)
        for service_ref, start, end, weekdays, exc in zip(
            stop_times["service_ref"],
            stop_times["start_date"],
            stop_times["end_date"],
            stop_times["weekdays"],
            exceptions.fillna(""),
        )
    ]
    return stop_times
