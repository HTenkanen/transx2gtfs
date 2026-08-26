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


def _weekday_names(days_of_week_element):
    weekdays = [elem._name for elem in days_of_week_element.get_elements()]
    if len(weekdays) == 1:
        return weekdays[0]
    return "|".join(weekdays)


def get_service_operative_days_info(data):
    """
    Get operating profile information from Services.Service.

    This is used if VehicleJourney does not contain the information.
    """
    try:
        service = data.TransXChange.Services.Service
        return _weekday_names(service.OperatingProfile.RegularDayType.DaysOfWeek)
    except Exception:
        # If service does not have OperatingProfile available, return None
        return None


def get_weekday_info(vehicle_journey_element):
    """Parses weekday info from TransXChange VehicleJourney element"""
    j = vehicle_journey_element
    try:
        return _weekday_names(j.OperatingProfile.RegularDayType.DaysOfWeek)
    except Exception:
        # If journey does not have OperatingProfile available, return None
        return None


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
