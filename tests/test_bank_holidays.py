"""Tests for the TransXChange bank-holiday table (transx2gtfs.bank_holidays)."""

import logging
import os
from datetime import date

import pandas as pd
import pytest

from transx2gtfs import bank_holidays as bh_module
from transx2gtfs.data import get_path
from transx2gtfs.bank_holidays import (
    GROUP_SELECTORS,
    KNOWN_NAMES,
    bank_holiday_table,
    detect_division,
    expand_bank_holiday_names,
    get_bank_holiday_dates,
    read_bank_holidays,
)
from transx2gtfs.txc import StopPoint, TxcDocument


def iso(table, name):
    return [d.isoformat() for d in table[name]]


def test_bank_holiday_table_names_and_selectors():
    holidays = read_bank_holidays()
    assert list(holidays.columns) == ["division", "title", "date"]
    ew = bank_holiday_table(
        "england-and-wales", date(2027, 1, 1), date(2027, 12, 31), holidays
    )
    assert iso(ew, "ChristmasDay") == ["2027-12-25"]
    assert iso(ew, "ChristmasDayHoliday") == ["2027-12-27"]
    assert iso(ew, "BoxingDay") == ["2027-12-26"]
    assert iso(ew, "BoxingDayHoliday") == ["2027-12-28"]
    assert iso(ew, "NewYearsDayHoliday") == []
    assert iso(ew, "EarlyRunOffDays") == ["2027-12-24", "2027-12-31"]
    assert iso(ew, "LateSummerBankHolidayNotScotland") == ["2027-08-30"]
    assert "2027-12-24" not in iso(ew, "AllBankHolidays")
    assert "2027-12-31" not in iso(ew, "AllBankHolidays")
    assert iso(ew, "Christmas") == [
        "2027-12-25",
        "2027-12-26",
        "2027-12-27",
        "2027-12-28",
    ]
    assert set(iso(ew, "AllHolidaysExceptChristmas")) == set(
        iso(ew, "AllBankHolidays")
    ) - set(iso(ew, "Christmas"))
    assert iso(ew, "HolidayMondays") == [
        "2027-03-29",
        "2027-05-03",
        "2027-05-31",
        "2027-08-30",
        "2027-12-27",
    ]
    assert iso(ew, "DisplacementHolidays") == ["2027-12-27", "2027-12-28"]

    scot = bank_holiday_table(
        "scotland", date(2027, 1, 1), date(2027, 12, 31), holidays
    )
    assert iso(scot, "Jan2ndScotland") == ["2027-01-02"]
    assert iso(scot, "Jan2ndScotlandHoliday") == ["2027-01-04"]
    assert iso(scot, "AugustBankHolidayScotland") == ["2027-08-02"]
    assert iso(scot, "StAndrewsDay") == ["2027-11-30"]
    scot_all = set(iso(scot, "AllBankHolidays"))
    assert {"2027-12-27", "2027-12-28"} <= scot_all
    assert "2027-12-25" not in scot_all
    assert scot["EasterMonday"] == []

    # one-off holidays (state funeral 2022, coronation 2023) are bank holidays too
    ew_2022 = bank_holiday_table(
        "england-and-wales", date(2022, 1, 1), date(2023, 12, 31), holidays
    )
    assert {"2022-09-19", "2023-05-08"} <= set(iso(ew_2022, "AllBankHolidays"))

    with pytest.raises(ValueError, match="Unknown bank holiday division"):
        bank_holiday_table("northern-ireland", date(2027, 1, 1), date(2027, 12, 31))


EXPECTED_2027 = {
    "england-and-wales": {
        "NewYearsDay": ["2027-01-01"],
        "GoodFriday": ["2027-03-26"],
        "EasterMonday": ["2027-03-29"],
        "MayDay": ["2027-05-03"],
        "SpringBank": ["2027-05-31"],
        "LateSummerBankHolidayNotScotland": ["2027-08-30"],
        "ChristmasEve": ["2027-12-24"],
        "ChristmasDay": ["2027-12-25"],
        "BoxingDay": ["2027-12-26"],
        "ChristmasDayHoliday": ["2027-12-27"],
        "BoxingDayHoliday": ["2027-12-28"],
        "NewYearsEve": ["2027-12-31"],
        "NewYearsDayHoliday": [],
        "Jan2ndScotland": [],
        "Jan2ndScotlandHoliday": [],
        "StAndrewsDay": [],
        "StAndrewsDayHoliday": [],
        "AugustBankHolidayScotland": [],
    },
    "scotland": {
        "NewYearsDay": ["2027-01-01"],
        "Jan2ndScotland": ["2027-01-02"],
        "Jan2ndScotlandHoliday": ["2027-01-04"],
        "GoodFriday": ["2027-03-26"],
        "EasterMonday": [],
        "MayDay": ["2027-05-03"],
        "SpringBank": ["2027-05-31"],
        "AugustBankHolidayScotland": ["2027-08-02"],
        "LateSummerBankHolidayNotScotland": [],
        "StAndrewsDay": ["2027-11-30"],
        "StAndrewsDayHoliday": [],
        "ChristmasEve": ["2027-12-24"],
        "ChristmasDay": ["2027-12-25"],
        "BoxingDay": ["2027-12-26"],
        "ChristmasDayHoliday": ["2027-12-27"],
        "BoxingDayHoliday": ["2027-12-28"],
        "NewYearsEve": ["2027-12-31"],
        "NewYearsDayHoliday": [],
    },
}


@pytest.mark.parametrize("division", sorted(EXPECTED_2027))
def test_every_holiday_name_in_2027(division):
    table = bank_holiday_table(
        division, date(2027, 1, 1), date(2027, 12, 31), read_bank_holidays()
    )
    produced = {name: iso(table, name) for name in table}
    for name, expected in EXPECTED_2027[division].items():
        assert produced[name] == expected, name
    names = {n for n in table if n not in GROUP_SELECTORS}
    assert names == set(EXPECTED_2027[division])
    assert names | set(GROUP_SELECTORS) | {"OtherPublicHoliday"} == KNOWN_NAMES


def test_expand_names_unions_dates_and_warns_on_unknown_names():
    table = bank_holiday_table(
        "england-and-wales", date(2027, 1, 1), date(2027, 12, 31)
    )
    expected = set(table["HolidayMondays"]) | set(table["ChristmasEve"])
    assert expand_bank_holiday_names(["HolidayMondays", "ChristmasEve"], table) == (
        expected
    )
    assert expand_bank_holiday_names(None, table) == set()
    # a known name that has no dates in the period is silently empty
    assert expand_bank_holiday_names(["NewYearsDayHoliday"], table) == set()
    with pytest.warns(UserWarning, match="Did not recognize following holiday: X"):
        assert expand_bank_holiday_names(["X"], table) == set()


def test_operating_period_beyond_the_holiday_feed_warns():
    holidays = read_bank_holidays()
    last = holidays["date"].max()
    with pytest.warns(
        UserWarning, match="bank holiday feed ends on %s" % last.isoformat()
    ):
        bank_holiday_table(
            "england-and-wales",
            date(last.year, 1, 1),
            date(last.year + 2, 12, 31),
            holidays,
        )


def test_bank_holidays_fall_back_to_the_packaged_copy_offline(monkeypatch):
    monkeypatch.delenv("TRANSX2GTFS_BANK_HOLIDAYS_PATH", raising=False)
    calls = []

    def fail(*args, **kwargs):
        calls.append(args)
        raise OSError("offline")

    monkeypatch.setattr(bh_module.urllib.request, "urlopen", fail)
    assert len(read_bank_holidays()) > 0 and len(calls) == 1


def test_bank_holidays_fall_back_when_the_download_is_truncated(monkeypatch, caplog):
    monkeypatch.delenv("TRANSX2GTFS_BANK_HOLIDAYS_PATH", raising=False)

    class Truncated:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self):
            raise bh_module.http.client.IncompleteRead(b"{", 100)

    monkeypatch.setattr(
        bh_module.urllib.request, "urlopen", lambda *a, **k: Truncated()
    )
    caplog.set_level(logging.INFO, logger="transx2gtfs")
    assert len(read_bank_holidays()) > 0
    assert "using static file" in caplog.text


def test_detect_division_by_stop_area_codes():
    def doc(*codes):
        return TxcDocument(stop_points=[StopPoint(atco_code=c) for c in codes])

    assert detect_division(doc("639003662", "639003652")) == "scotland"
    assert detect_division(doc("9300WAS1", "490007705N")) == "england-and-wales"
    # majority decides; a tie is England and Wales
    assert detect_division(doc("639003662", "9300WAS1")) == "england-and-wales"
    assert detect_division(doc("639003662", "639003652", "9300WAS1")) == "scotland"
    assert detect_division(doc()) == "england-and-wales"


def test_get_bank_holiday_dates_covers_every_feed_division():
    info = pd.DataFrame({"start_date": ["20270701"], "end_date": ["20270831"]})
    # Battle of the Boyne (Northern Ireland), Scottish and England and Wales summer holidays
    assert get_bank_holiday_dates(info) == ["20270712", "20270802", "20270830"]
    march = pd.DataFrame({"start_date": ["20270301"], "end_date": ["20270331"]})
    # St Patrick's Day (Northern Ireland only), Good Friday, Easter Monday
    assert get_bank_holiday_dates(march) == ["20270317", "20270326", "20270329"]
    quiet = pd.DataFrame({"start_date": ["20270601"], "end_date": ["20270630"]})
    assert get_bank_holiday_dates(quiet) is None


def test_snapshots_are_private_copies_of_the_configured_feed(monkeypatch, tmp_path):
    monkeypatch.setattr(bh_module.tempfile, "gettempdir", lambda: str(tmp_path))
    # a known feed: the packaged copy, in a file of our own
    source = tmp_path / "feed.json"
    source.write_bytes(open(get_path("bank_holidays"), "rb").read())
    feed = source.read_bytes()
    monkeypatch.setenv("TRANSX2GTFS_BANK_HOLIDAYS_PATH", str(source))
    first = bh_module.snapshot_bank_holidays_data()
    second = bh_module.snapshot_bank_holidays_data()
    third = bh_module.snapshot_bank_holidays_data()
    assert first != second
    assert open(first, "rb").read() == feed
    # snapshots are copies: changing the source afterwards does not affect them
    source.write_bytes(b"{}")
    assert open(first, "rb").read() == feed
    # a directory that cannot be removed is reported, not hidden
    (tmp_path / "keep").mkdir()
    os.rename(tmp_path / "keep", os.path.join(os.path.dirname(third), "keep"))
    with pytest.warns(UserWarning, match="Could not remove the bank holiday snapshot"):
        bh_module.remove_bank_holidays_snapshot(third)
    assert not os.path.exists(third)
    os.rmdir(os.path.join(os.path.dirname(third), "keep"))
    os.rmdir(os.path.dirname(third))
    assert open(first, "rb").read() == open(second, "rb").read()
    assert first.startswith(str(tmp_path))
    if os.name != "nt":  # Windows has no POSIX directory modes
        assert oct(os.stat(os.path.dirname(first)).st_mode & 0o777) == "0o700"
    # a worker reads the snapshot it was given, whatever the environment says
    monkeypatch.setenv("TRANSX2GTFS_BANK_HOLIDAYS_PATH", str(tmp_path / "missing.json"))
    bh_module.set_bank_holidays_path(first)
    try:
        assert len(bh_module.read_bank_holidays()) > 0
    finally:
        bh_module.set_bank_holidays_path(None)
    bh_module.remove_bank_holidays_snapshot(first)
    bh_module.remove_bank_holidays_snapshot(second)
    assert list(tmp_path.glob("transx2gtfs-*")) == []
    # removing a snapshot twice is harmless
    bh_module.remove_bank_holidays_snapshot(first)


def test_a_failed_snapshot_leaves_no_directory_behind(monkeypatch, tmp_path):
    monkeypatch.setattr(bh_module.tempfile, "gettempdir", lambda: str(tmp_path))
    # the feed is read fine; writing the snapshot fails after mkdtemp
    monkeypatch.setattr(bh_module, "_bank_holidays_bytes", lambda: b"{}")

    def fail(*args, **kwargs):
        raise OSError("disk full")

    monkeypatch.setattr("builtins.open", fail)
    with pytest.raises(OSError, match="disk full"):
        bh_module.snapshot_bank_holidays_data()
    assert list(tmp_path.glob("transx2gtfs-*")) == []
