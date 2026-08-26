import warnings

import pandas as pd

from transx2gtfs.bank_holidays import get_bank_holiday_dates
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


def get_calendar_dates(gtfs_info):
    """
    Parse calendar dates attributes from GTFS info DataFrame.

    TransXChange typically indicates exception in operation using 'AllBankHolidays'
    as an attribute. Hence, Bank holiday information is retrieved from
    "https://www.gov.uk/" site that should keep the data up-to-date. If the file
    (or internet) is not available, a static version of the same file will be
    used that is bundled with the package.

    There are different bank holidays in different regions in UK.
    Available regions are: 'england-and-wales', 'scotland', 'northern-ireland'

    """
    # Known exceptions and their counterparts in bankholiday table
    known_holidays = {
        "SpringBank": "Spring bank holiday",
        "LateSummerBankHolidayNotScotland": "Summer bank holiday",
        "MayDay": "Early May bank holiday",
        "GoodFriday": "Good Friday",
        "EasterMonday": "Easter Monday",
        "BoxingDay": "Boxing Day",
        "ChristmasDay": "Christmas Day",
        "NewYearsDay": "New Year’s Day",
    }

    # Get initial info about non-operative days
    gtfs_info = gtfs_info.copy()
    gtfs_info = gtfs_info.dropna(subset=["non_operative_days"])
    non_operative_values = list(gtfs_info["non_operative_days"].unique())

    # Container for all info
    non_operatives = []

    # Parse all non operative ones
    for info in non_operative_values:
        # Check if info consists of multiple values
        if isinstance(info, str) and "|" in info:
            split = info.split("|")
            non_operatives += split
        else:
            # Add individual value
            if info is not None and info != "":
                non_operatives.append(info)

    # Remove duplicates
    non_operatives = list(set(non_operatives))

    # Check if there exists some exceptions that are not known bank holidays
    for holiday in non_operatives:
        if (holiday not in known_holidays.keys()) and (holiday != "AllBankHolidays"):
            warnings.warn(
                "Did not recognize following holiday: %s" % holiday,
                UserWarning,
                stacklevel=2,
            )

    if len(non_operatives) > 0:
        # Get bank holidays that are during the operative period of the feed
        bank_holidays = get_bank_holiday_dates(gtfs_info)
    else:
        return None

    # Return None if no bank holiday happens to be during the operative period
    if bank_holidays is None:
        return None

    # Select distinct (service_id) rows that have bank holiday determined
    calendar_info = gtfs_info[["service_id", "non_operative_days"]].copy()
    calendar_info = calendar_info.drop_duplicates(subset=["service_id"])

    # One row per service and bank holiday; the exception always indicates
    # a non-operative service (value 2)
    rows = []
    for service_id in calendar_info["service_id"]:
        for date in bank_holidays:
            rows.append(dict(service_id=service_id, date=date, exception_type=2))

    calendar_dates = pd.DataFrame(
        rows, columns=["service_id", "date", "exception_type"]
    )
    calendar_dates["exception_type"] = calendar_dates["exception_type"].astype(int)

    return calendar_dates
