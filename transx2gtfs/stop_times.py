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


def generate_service_id(stop_times):
    """Generate service_id into stop_times DataFrame"""

    # Create column for service_id
    stop_times["service_id"] = None

    # Parse calendar info
    calendar_info = stop_times.drop_duplicates(subset=["vehicle_journey_id"])

    # Group by weekdays
    calendar_groups = calendar_info.groupby("weekdays")

    # Iterate over groups and create a service_id
    for _, cgroup in calendar_groups:
        # Parse all vehicle journey ids
        vehicle_journey_ids = cgroup["vehicle_journey_id"].to_list()

        # Parse other items
        service_ref = cgroup["service_ref"].unique()[0]
        daygroup = cgroup["weekdays"].unique()[0]
        start_d = cgroup["start_date"].unique()[0]
        end_d = cgroup["end_date"].unique()[0]

        # Generate service_id
        service_id = "%s_%s_%s_%s" % (service_ref, start_d, end_d, daygroup)

        # Update stop_times service_id
        stop_times.loc[
            stop_times["vehicle_journey_id"].isin(vehicle_journey_ids), "service_id"
        ] = service_id
    return stop_times
