import pandas as pd

from transx2gtfs.calendar import join_names


def get_calendar_dates_exceptions(operating_profile):
    """Bank holiday non-operation days ('AllBankHolidays', 'ChristmasDay|BoxingDay',
    ...) of an OperatingProfile, or None if it has no DaysOfNonOperation"""
    if operating_profile is None:
        return None
    return join_names(operating_profile.bank_holiday_days_of_non_operation)


def get_service_calendar_dates_exceptions(doc):
    """Bank holiday non-operation days of the first Service of the document"""
    if not doc.services:
        return None
    return get_calendar_dates_exceptions(doc.services[0].operating_profile)


def encode_exceptions(added, removed):
    """
    Encode calendar exceptions as 'YYYYMMDD:1|YYYYMMDD:2|...' (sorted by date);
    '' when there are none. ``added``/``removed`` are sets of datetime.date.
    """
    items = [(day, 1) for day in added] + [(day, 2) for day in removed]
    return "|".join(
        "%s:%d" % (day.strftime("%Y%m%d"), kind) for day, kind in sorted(items)
    )


def get_calendar_dates(gtfs_info):
    """
    Calendar dates (service exceptions) from the GTFS info DataFrame, whose
    ``exceptions`` column holds the encoded exceptions of every journey (see
    :func:`encode_exceptions`). Returns None when there are no exceptions.
    """
    if "exceptions" not in gtfs_info.columns:
        return None
    services = gtfs_info[["service_id", "exceptions"]].drop_duplicates("service_id")

    rows = []
    for service_id, exceptions in zip(services["service_id"], services["exceptions"]):
        if not exceptions:
            continue
        for item in exceptions.split("|"):
            day, kind = item.split(":")
            rows.append(dict(service_id=service_id, date=day, exception_type=int(kind)))

    if not rows:
        return None
    calendar_dates = pd.DataFrame(
        rows, columns=["service_id", "date", "exception_type"]
    )
    calendar_dates["exception_type"] = calendar_dates["exception_type"].astype(int)
    return calendar_dates
