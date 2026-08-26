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
_WEEKDAY_NUMBERS = {day: i for i, day in enumerate(WEEKDAYS)}
_RANGES = {
    "mondaytofriday": [0, 1, 2, 3, 4],
    "mondaytosaturday": [0, 1, 2, 3, 4, 5],
    "mondaytosunday": [0, 1, 2, 3, 4, 5, 6],
    "weekend": [5, 6],
    "saturdaysundayholidaysonly": [5, 6],
    "holidaysonly": [],
}


def join_names(names):
    """Encode a list of tag names as 'A' / 'A|B'; None stays None"""
    if names is None:
        return None
    if len(names) == 1:
        return names[0]
    return "|".join(names)


def get_weekday_info(operating_profile):
    """Weekday info ('Weekend', 'MondayToFriday', 'Saturday|Sunday', ...) of an
    OperatingProfile, or None if the profile has no DaysOfWeek; 'HolidaysOnly'
    for a profile that runs on holidays only"""
    if operating_profile is None:
        return None
    if operating_profile.holidays_only and not operating_profile.days_of_week:
        return "HolidaysOnly"
    return join_names(operating_profile.days_of_week)


def get_service_operative_days_info(doc):
    """
    Weekday info from the service's OperatingProfile (first Service of the
    document). Used if a VehicleJourney does not carry its own profile.
    """
    if not doc.services:
        return None
    return get_weekday_info(doc.services[0].operating_profile)


def _active_day_numbers(token):
    """Weekday numbers denoted by one DaysOfWeek element name"""
    key = token.strip().lower()
    if key in _RANGES:
        return _RANGES[key]
    if key in _WEEKDAY_NUMBERS:
        return [_WEEKDAY_NUMBERS[key]]
    if key.startswith("not") and key[3:] in _WEEKDAY_NUMBERS:
        return [i for i in range(7) if i != _WEEKDAY_NUMBERS[key[3:]]]
    if "to" in key:
        start, _, end = key.partition("to")
        if start in _WEEKDAY_NUMBERS and end in _WEEKDAY_NUMBERS:
            return list(range(_WEEKDAY_NUMBERS[start], _WEEKDAY_NUMBERS[end] + 1))
    raise ValueError("Unknown DaysOfWeek value '%s'." % token)


def parse_active_days(dayinfo):
    """
    Parse a TransXChange DaysOfWeek value into a {weekday: 0/1} dict.

    ``dayinfo`` is the encoded string ('Weekend', 'MondayToFriday',
    'Monday|Wednesday', 'NotSaturday', 'HolidaysOnly', ...); None or '' means
    every day (a journey without any day information).
    """
    if dayinfo is None or dayinfo == "":
        active = set(range(7))
    else:
        tokens = dayinfo.split("|")
        if all(t.strip().lower().startswith("not") for t in tokens):
            active = set(range(7))
            for token in tokens:
                active &= set(_active_day_numbers(token))
        else:
            active = set()
            for token in tokens:
                active.update(_active_day_numbers(token))
    return {day: int(i in active) for i, day in enumerate(WEEKDAYS)}


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
