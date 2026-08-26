"""
UK bank holidays as TransXChange names.

The table for a division ("england-and-wales" or "scotland") maps every
TransXChange bank-holiday element name to the dates it denotes within a period.
Fixed-date holidays are generated for each year; movable ones come from the
gov.uk bank-holiday feed (``TRANSX2GTFS_BANK_HOLIDAYS_PATH``, the live URL, or
the copy bundled with the package); the displacement names (``ChristmasDayHoliday``
and friends) are the gov.uk entries observed on a day other than the fixed date.
"""

import io
import os
import tempfile
import urllib.request
import warnings
from datetime import date, timedelta
from urllib.error import URLError

import pandas as pd

from transx2gtfs.data import get_path

BANK_HOLIDAYS_URL = "https://www.gov.uk/bank-holidays.json"
BANK_HOLIDAYS_PATH_ENV = "TRANSX2GTFS_BANK_HOLIDAYS_PATH"
ENGLAND_AND_WALES = "england-and-wales"
SCOTLAND = "scotland"
DIVISIONS = (ENGLAND_AND_WALES, SCOTLAND)

# Fixed-date names: (month, day); Scotland-only ones listed separately
FIXED_DATES = {
    "NewYearsDay": (1, 1),
    "ChristmasEve": (12, 24),
    "ChristmasDay": (12, 25),
    "BoxingDay": (12, 26),
    "NewYearsEve": (12, 31),
}
FIXED_DATES_SCOTLAND = {"Jan2ndScotland": (1, 2), "StAndrewsDay": (11, 30)}

# gov.uk titles of movable holidays -> TransXChange name (per division where they differ)
MOVABLE_TITLES = {
    "Good Friday": "GoodFriday",
    "Easter Monday": "EasterMonday",
    "Early May bank holiday": "MayDay",
    "Early May bank holiday (VE day)": "MayDay",
    "Spring bank holiday": "SpringBank",
    "Summer bank holiday": {
        ENGLAND_AND_WALES: "LateSummerBankHolidayNotScotland",
        SCOTLAND: "AugustBankHolidayScotland",
    },
}

# gov.uk titles of fixed-date holidays -> (base name, displacement name)
DISPLACEMENT_TITLES = {
    "New Year's Day": ("NewYearsDay", "NewYearsDayHoliday"),
    "Christmas Day": ("ChristmasDay", "ChristmasDayHoliday"),
    "Boxing Day": ("BoxingDay", "BoxingDayHoliday"),
    "2nd January": ("Jan2ndScotland", "Jan2ndScotlandHoliday"),
    "St Andrew's Day": ("StAndrewsDay", "StAndrewsDayHoliday"),
}

DISPLACEMENT_NAMES = tuple(names[1] for names in DISPLACEMENT_TITLES.values())
CHRISTMAS_NAMES = (
    "ChristmasDay",
    "BoxingDay",
    "ChristmasDayHoliday",
    "BoxingDayHoliday",
)
EARLY_RUN_OFF_NAMES = ("ChristmasEve", "NewYearsEve")
GROUP_SELECTORS = (
    "AllBankHolidays",
    "AllHolidaysExceptChristmas",
    "Christmas",
    "DisplacementHolidays",
    "EarlyRunOffDays",
    "HolidayMondays",
)
# Every TransXChange bank-holiday element name (a valid name may have no date in
# a given period, e.g. a displacement holiday in a year without substitute days)
KNOWN_NAMES = frozenset(
    list(FIXED_DATES)
    + list(FIXED_DATES_SCOTLAND)
    + [
        n
        for v in MOVABLE_TITLES.values()
        for n in ([v] if isinstance(v, str) else v.values())
    ]
    + [n for names in DISPLACEMENT_TITLES.values() for n in names]
    + list(GROUP_SELECTORS)
    + ["OtherPublicHoliday"]
)


def _normalise_title(title):
    return title.replace("’", "'").replace("â€™", "'").strip()


def _bank_holidays_bytes():
    """The feed to use: TRANSX2GTFS_BANK_HOLIDAYS_PATH, else gov.uk, else the
    packaged copy; validated as JSON before it is returned."""
    local_path = _bank_holidays_override or os.environ.get(BANK_HOLIDAYS_PATH_ENV)
    if local_path:
        with open(local_path, "rb") as f:
            data = f.read()
    else:
        try:
            with urllib.request.urlopen(BANK_HOLIDAYS_URL, timeout=30) as response:
                data = response.read()
            pd.read_json(io.BytesIO(data))
        # URLError is an OSError; ValueError covers a non-JSON response
        except (URLError, OSError, ValueError):
            print("Could not read bank holidays via Internet, using static file.")
            with open(get_path("bank_holidays"), "rb") as f:
                data = f.read()
    pd.read_json(io.BytesIO(data))
    return data


# Path set in worker processes by the conversion (takes precedence over the
# environment) so that every worker of one conversion reads the same snapshot
_bank_holidays_override = None


def set_bank_holidays_path(path):
    global _bank_holidays_override
    _bank_holidays_override = path


def snapshot_bank_holidays_data():
    """
    Write the bank holiday feed to use into a new private directory and return
    the file path; ``convert()`` hands it to its workers and removes it when
    they are done, so one conversion reads one immutable snapshot.
    """
    data = _bank_holidays_bytes()
    directory = tempfile.mkdtemp(prefix="transx2gtfs-")
    snapshot = os.path.join(directory, "bank-holidays.json")
    with open(snapshot, "wb") as f:
        f.write(data)
    return snapshot


def remove_bank_holidays_snapshot(snapshot):
    """Remove a snapshot written by :func:`snapshot_bank_holidays_data`"""
    for target in (snapshot, os.path.dirname(snapshot)):
        try:
            os.remove(target) if os.path.isfile(target) else os.rmdir(target)
        except OSError:
            pass


def read_bank_holidays():
    """
    Read the gov.uk bank holiday feed as a DataFrame with columns
    ``division``, ``title``, ``date`` (datetime.date).

    Uses the snapshot handed to a worker, else ``TRANSX2GTFS_BANK_HOLIDAYS_PATH``
    if set, else gov.uk, else the packaged copy.
    """
    bholidays = pd.read_json(io.BytesIO(_bank_holidays_bytes()))

    frames = []
    for division in bholidays.columns:
        events = pd.DataFrame(bholidays.loc["events", division])
        if len(events) == 0:
            continue
        events["division"] = division
        frames.append(events[["division", "title", "date"]])
    holidays = pd.concat(frames, ignore_index=True)
    holidays["title"] = holidays["title"].map(_normalise_title)
    holidays["date"] = pd.to_datetime(holidays["date"]).dt.date
    return holidays


def bank_holiday_table(division, start, end, holidays=None):
    """
    Map every TransXChange bank-holiday name to its dates within [start, end].

    ``division`` is "england-and-wales" or "scotland"; ``start``/``end`` are
    datetime.date; ``holidays`` is the frame from :func:`read_bank_holidays`.
    Group selectors (AllBankHolidays, Christmas, …) are included as well.
    """
    if division not in DIVISIONS:
        raise ValueError("Unknown bank holiday division '%s'." % division)
    if holidays is None:
        holidays = read_bank_holidays()
    # Every known name is present, empty when it has no date in the period
    table = {name: set() for name in KNOWN_NAMES if name not in GROUP_SELECTORS}
    table.pop("OtherPublicHoliday")

    def add(name, day):
        if start <= day <= end:
            table.setdefault(name, set()).add(day)

    fixed = dict(FIXED_DATES)
    if division == SCOTLAND:
        fixed.update(FIXED_DATES_SCOTLAND)
    for year in range(start.year, end.year + 1):
        for name, (month, day) in fixed.items():
            add(name, date(year, month, day))

    events = holidays[holidays["division"] == division]
    feed_end = events["date"].max() if len(events) else None
    if feed_end is not None and end > feed_end:
        warnings.warn(
            "The bank holiday feed ends on %s; movable holidays after that date "
            "are unknown for the period ending %s."
            % (feed_end.isoformat(), end.isoformat()),
            UserWarning,
            stacklevel=2,
        )
    # The bank holidays proper are the gov.uk dates (observed days, one-offs
    # included); the fixed-date names above are TransXChange's own selectors
    bank_holidays = {day for day in events["date"] if start <= day <= end}
    for title, day in zip(events["title"], events["date"]):
        base_title = title.replace("(substitute day)", "").strip()
        if base_title in DISPLACEMENT_TITLES:
            base_name, displacement_name = DISPLACEMENT_TITLES[base_title]
            if base_name in fixed and (day.month, day.day) != fixed[base_name]:
                add(displacement_name, day)
        elif base_title in MOVABLE_TITLES:
            name = MOVABLE_TITLES[base_title]
            if isinstance(name, dict):
                name = name[division]
            add(name, day)
        # Other gov.uk entries (jubilees, coronations, funerals) have no TXC name

    for name in table:
        table[name] = sorted(table[name])

    def dates_of(names):
        return sorted({d for name in names for d in table.get(name, [])})

    table["AllBankHolidays"] = sorted(bank_holidays)
    table["Christmas"] = dates_of(CHRISTMAS_NAMES)
    table["EarlyRunOffDays"] = dates_of(EARLY_RUN_OFF_NAMES)
    table["DisplacementHolidays"] = dates_of(DISPLACEMENT_NAMES)
    table["AllHolidaysExceptChristmas"] = [
        d for d in table["AllBankHolidays"] if d not in set(table["Christmas"])
    ]
    table["HolidayMondays"] = [d for d in table["AllBankHolidays"] if d.weekday() == 0]
    return table


def expand_bank_holiday_names(names, table):
    """Dates denoted by a list of TransXChange bank-holiday element names."""
    dates = set()
    for name in names or []:
        if name in table:
            dates.update(table[name])
        elif name not in KNOWN_NAMES:
            warnings.warn(
                "Did not recognize following holiday: %s" % name,
                UserWarning,
                stacklevel=2,
            )
    return dates


def detect_division(doc):
    """
    Bank holiday division of a document: Scotland when most of its stops have a
    Scottish ATCO area code (6xx), England and Wales otherwise.
    """
    codes = [p.atco_code for p in doc.stop_points if p.atco_code]
    if not codes:
        return ENGLAND_AND_WALES
    scottish = sum(1 for code in codes if code.startswith("6"))
    return SCOTLAND if scottish * 2 > len(codes) else ENGLAND_AND_WALES


def get_bank_holiday_dates(gtfs_info):
    """
    All bank holidays (both divisions) during the feed operative period as GTFS
    dates (YYYYMMDD), or None if there are none.
    """
    start = pd.to_datetime(gtfs_info["start_date"].min(), format="%Y%m%d").date()
    end = pd.to_datetime(gtfs_info["end_date"].max(), format="%Y%m%d").date()
    holidays = read_bank_holidays()
    dates = set()
    for division in DIVISIONS:
        dates.update(
            bank_holiday_table(division, start, end, holidays)["AllBankHolidays"]
        )
    if not dates:
        return None
    return [day.strftime("%Y%m%d") for day in sorted(dates)]


def daterange(start, end):
    """Every date from start to end inclusive."""
    day = start
    while day <= end:
        yield day
        day += timedelta(days=1)
