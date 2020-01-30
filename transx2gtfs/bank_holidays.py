import pandas as pd
from datetime import datetime
from urllib.error import HTTPError


def get_bank_holiday_dates(gtfs_info):
    """
    Retrieve information about UK bank holidays that are during the feed operative period.

    Available regions: 'england-and-wales', 'scotland', 'northern-ireland'
    """
    available_regions = ['england-and-wales', 'scotland', 'northern-ireland']

    # Get bank holidays from UK Gov
    bank_holidays_url = "https://www.gov.uk/bank-holidays.json"

    # Read data from URL by default
    try:
        bholidays = pd.read_json(bank_holidays_url)
    # If url is unreachable use static file from the package
    except HTTPError:
        print("Could not read bank holidays via Internet, using static file instead.")
        bholidays = pd.read_json("data/bank-holidays.json")

    # Get bank holidays of all regions
    bank_holidays = pd.DataFrame()
    for region in available_regions:
        region_data = pd.DataFrame(bholidays.loc['events', region])
        region_data['region'] = region
        bank_holidays = bank_holidays.append(region_data, ignore_index=True,
                                             sort=False)

    # Drop duplicates
    bank_holidays = bank_holidays.drop_duplicates(subset=['date'])

    # Sort
    bank_holidays = bank_holidays.sort_values(by='date').reset_index(drop=True)

    # Make datetime from date and make index
    bank_holidays['dt'] = pd.to_datetime(bank_holidays['date'], infer_datetime_format=True)
    bank_holidays = bank_holidays.set_index('dt', drop=False)

    # Get start and end date of the GTFS feed
    start_date_min = datetime.strptime(gtfs_info['start_date'].min(), "%Y%m%d")
    end_date_max = datetime.strptime(gtfs_info['end_date'].max(), "%Y%m%d")

    # Select bank holidays that fit the time range
    selected_bank_holidays = bank_holidays[start_date_min:end_date_max]

    # Check if there were any bank holidays during the feed time
    if len(selected_bank_holidays) == 0:
        return None

    # If there are return get the dates
    dates = selected_bank_holidays['dt'].to_list()
    # Parse in GTFS date format
    gtfs_dates = [date.strftime("%Y%m%d") for date in dates]

    return gtfs_dates