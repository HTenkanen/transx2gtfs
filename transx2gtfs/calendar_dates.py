import pandas as pd
from transx2gtfs.bank_holidays import get_bank_holiday_dates
import warnings


def get_service_calendar_dates_exceptions(data):
    """Parses calendar dates exception info from TransXChange VehicleJourney element"""
    try:
        non_operative_days = data.TransXChange.Services.Service.OperatingProfile.BankHolidayOperation.DaysOfNonOperation.get_elements()
        weekdays = []
        for elem in non_operative_days:
            weekdays.append(elem._name)
        if len(weekdays) == 1:
            return weekdays[0]
        else:
            return "|".join(weekdays)
    except:
        return None


def get_calendar_dates_exceptions(vehicle_journey_element):
    """Parses calendar dates exception info from TransXChange VehicleJourney element"""
    j = vehicle_journey_element
    try:
        non_operative_days = j.OperatingProfile.BankHolidayOperation.DaysOfNonOperation.get_elements()
        weekdays = []
        for elem in non_operative_days:
            weekdays.append(elem._name)
        if len(weekdays) == 1:
            return weekdays[0]
        else:
            return "|".join(weekdays)
    except:
        return None

def get_calendar_dates(gtfs_info):
    """
    Parse calendar dates attributes from GTFS info DataFrame.

    TransXChange typically indicates exception in operation using 'AllBankHolidays' as an attribute.
    Hence, Bank holiday information is retrieved from "https://www.gov.uk/" site that should keep the data up-to-date.
    If the file (or internet) is not available, a static version of the same file will be used that is bundled with the package.

    There are different bank holidays in different regions in UK.
    Available regions are: 'england-and-wales', 'scotland', 'northern-ireland'

    """
    # Known exceptions and their counterparts in bankholiday table
    known_holidays = {'SpringBank': 'Spring bank holiday',
                      'LateSummerBankHolidayNotScotland': 'Summer bank holiday',
                      'MayDay': 'Early May bank holiday',
                      'GoodFriday': 'Good Friday',
                      'EasterMonday': 'Easter Monday'}

    # Get initial info about non-operative days
    gtfs_info = gtfs_info.copy()
    gtfs_info = gtfs_info.dropna(subset=['non_operative_days'])
    non_operative_values = list(gtfs_info['non_operative_days'].unique())

    # Container for all info
    non_operatives = []

    # Parse all non operative ones
    for info in non_operative_values:
        # Check if info consists of multiple values
        if isinstance(info, str) and "|" in info:
            split = info.split('|')
            non_operatives += split
        else:
            # Add individual value
            if info is not None and info != '':
                non_operatives.append(info)

    # Remove duplicates
    non_operatives = list(set(non_operatives))

    # Check if there exists some exceptions that are not known bank holidays
    for holiday in non_operatives:
        if (holiday not in known_holidays.keys()) and (holiday != 'AllBankHolidays'):
            warnings.warn("Did not recognize following holiday: %s" % holiday,
                          UserWarning,
                          stacklevel=2)

    if len(non_operatives) > 0:
        # Get bank holidays that are during the operative period of the feed
        bank_holidays = get_bank_holiday_dates(gtfs_info)
    else:
        return None

    # Return None if no bank holiday happens to be during the operative period
    if bank_holidays is None:
        return None

    # Otherwise produce calendar_dates data

    # Select distinct (service_id) rows that have bank holiday determined
    calendar_info = gtfs_info[['service_id', 'non_operative_days']].copy()
    calendar_info = calendar_info.drop_duplicates(subset=['service_id'])

    # Create columns for date and exception_type
    calendar_info['date'] = None

    # The exception will always be indicating non-operative service (value 2)
    calendar_info['exception_type'] = 2

    # Container for calendar_dates
    calendar_dates = pd.DataFrame()

    # Iterate over services and produce rows having exception with given bank holiday dates
    for idx, row in calendar_info.iterrows():
        # Iterate over exception dates
        for date in bank_holidays:
            # Generate row
            row = dict(service_id=row['service_id'],
                       date=date,
                       exception_type=row['exception_type'])
            # Add to container
            calendar_dates = calendar_dates.append(row, ignore_index=True, sort=False)

    # Ensure correct datatype
    calendar_dates['exception_type'] = calendar_dates['exception_type'].astype(int)

    return calendar_dates

