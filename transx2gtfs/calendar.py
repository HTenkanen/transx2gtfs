import pandas as pd

WEEKDAYS = [
    "monday",
    "tuesday",
    "wednesday",
    "thursday",
    "friday",
    "saturday",
    "sunday",
]


def join_names(names):
    """Encode a list of tag names as 'A' / 'A|B'; None stays None"""
    if names is None:
        return None
    if len(names) == 1:
        return names[0]
    return "|".join(names)


def get_weekday_info(operating_profile):
    """Weekday info ('Weekend', 'MondayToFriday', 'Saturday|Sunday', ...) of an
    OperatingProfile, or None if the profile has no DaysOfWeek"""
    if operating_profile is None:
        return None
    return join_names(operating_profile.days_of_week)


def get_service_operative_days_info(doc):
    """
    Weekday info from the service's OperatingProfile (first Service of the
    document). Used if a VehicleJourney does not carry its own profile.
    """
    if not doc.services:
        return None
    return get_weekday_info(doc.services[0].operating_profile)


def parse_active_days(dayinfo):
    """Parse a TransXChange DaysOfWeek value into a {weekday: 0/1} dict"""
    weekday_to_num = {day: i for i, day in enumerate(WEEKDAYS)}

    # Containers
    active_days = []

    # Process 'weekend'
    if "weekend" in dayinfo.strip().lower():
        active_days.append(5)
        active_days.append(6)

    # Check if dayinfo is specified as day-range
    elif "To" in dayinfo:
        day_range = dayinfo.split("To")
        start_i = weekday_to_num[day_range[0].lower()]
        end_i = weekday_to_num[day_range[1].lower()]

        # Get days when the service is active
        for idx in range(start_i, end_i + 1):
            # Get days
            active_days.append(idx)

    # Process a collection of individual weekdays
    elif "|" in dayinfo:
        days = dayinfo.split("|")
        for day in days:
            active_days.append(weekday_to_num[day.lower()])

    # If input is only a single day
    else:
        active_days.append(weekday_to_num[dayinfo.lower()])

    return {day: int(i in active_days) for i, day in enumerate(WEEKDAYS)}


def parse_day_range(dayinfo):
    """Parse day range from TransXChange DayOfWeek element into a one-row frame"""
    return pd.DataFrame([parse_active_days(dayinfo)])


def get_calendar(gtfs_info):
    """Parse calendar attributes from GTFS info DataFrame"""
    # Parse calendar
    use_cols = ["service_id", "weekdays", "start_date", "end_date"]
    calendar = gtfs_info.drop_duplicates(subset=use_cols)
    calendar = calendar[use_cols].copy()
    calendar = calendar.reset_index(drop=True)

    rows = []
    for _, row in calendar.iterrows():
        dayrow = {"service_id": row["service_id"]}
        dayrow.update(parse_active_days(row["weekdays"]))
        dayrow["start_date"] = row["start_date"]
        dayrow["end_date"] = row["end_date"]
        rows.append(dayrow)

    col_order = ["service_id"] + WEEKDAYS + ["start_date", "end_date"]
    gtfs_calendar = pd.DataFrame(rows, columns=col_order)

    # Ensure correct datatypes
    for col in WEEKDAYS:
        gtfs_calendar[col] = gtfs_calendar[col].astype(int)

    return gtfs_calendar
