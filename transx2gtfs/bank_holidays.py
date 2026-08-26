import io
import os
import urllib.request
from datetime import datetime
from urllib.error import URLError

import pandas as pd

from transx2gtfs.data import get_path

BANK_HOLIDAYS_URL = "https://www.gov.uk/bank-holidays.json"
BANK_HOLIDAYS_PATH_ENV = "TRANSX2GTFS_BANK_HOLIDAYS_PATH"
REGIONS = ["england-and-wales", "scotland", "northern-ireland"]


def read_bank_holidays():
    """
    Read the gov.uk bank holiday table for all regions, one row per date.

    Uses ``TRANSX2GTFS_BANK_HOLIDAYS_PATH`` if set, otherwise downloads from
    gov.uk and falls back to the file bundled with the package.
    """
    local_path = os.environ.get(BANK_HOLIDAYS_PATH_ENV)
    if local_path:
        bholidays = pd.read_json(local_path)
    else:
        try:
            with urllib.request.urlopen(BANK_HOLIDAYS_URL, timeout=30) as response:
                bholidays = pd.read_json(io.BytesIO(response.read()))
        # URLError is an OSError; ValueError covers a non-JSON response
        except (URLError, OSError, ValueError):
            print("Could not read bank holidays via Internet, using static file.")
            bholidays = pd.read_json(get_path("bank_holidays"))

    frames = []
    for region in REGIONS:
        region_data = pd.DataFrame(bholidays.loc["events", region])
        region_data["region"] = region
        frames.append(region_data)
    bank_holidays = pd.concat(frames, ignore_index=True)

    bank_holidays = bank_holidays.drop_duplicates(subset=["date"])
    bank_holidays = bank_holidays.sort_values(by="date").reset_index(drop=True)
    bank_holidays["dt"] = pd.to_datetime(bank_holidays["date"])
    return bank_holidays.set_index("dt", drop=False)


def get_bank_holiday_dates(gtfs_info):
    """
    Retrieve UK bank holidays (all regions) during the feed operative period,
    as a list of GTFS dates (YYYYMMDD), or None if there are none.
    """
    bank_holidays = read_bank_holidays()

    # Get start and end date of the GTFS feed
    start_date_min = datetime.strptime(gtfs_info["start_date"].min(), "%Y%m%d")
    end_date_max = datetime.strptime(gtfs_info["end_date"].max(), "%Y%m%d")

    # Select bank holidays that fit the time range
    selected_bank_holidays = bank_holidays.loc[start_date_min:end_date_max]

    if len(selected_bank_holidays) == 0:
        return None

    dates = selected_bank_holidays["dt"].to_list()
    return [date.strftime("%Y%m%d") for date in dates]
