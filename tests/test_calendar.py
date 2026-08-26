import numpy as np
import pytest
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from transx2gtfs.calendar import (
    WEEKDAYS,
    get_calendar,
    get_service_operative_days_info,
    get_weekday_info,
    parse_active_days,
    parse_day_range,
)
from transx2gtfs.transxchange import get_gtfs_info
from transx2gtfs.txc import read_txc


def _day_frame(active):
    return DataFrame([{day: int(day in active) for day in WEEKDAYS}])


@pytest.mark.parametrize("fixture_name", ["tfl_file", "txc21_file"])
def test_service_operative_days(fixture_name, request):
    doc = read_txc(request.getfixturevalue(fixture_name))
    operative_days = get_service_operative_days_info(doc)
    assert operative_days == "Weekend"


@pytest.mark.parametrize("fixture_name", ["tfl_file", "txc21_file"])
def test_vehicle_journey_weekdays(fixture_name, request):
    doc = read_txc(request.getfixturevalue(fixture_name))
    correct_frames = {
        "Sunday": _day_frame(["sunday"]),
        "Saturday": _day_frame(["saturday"]),
    }

    for journey in doc.vehicle_journeys:
        weekdays = get_weekday_info(journey.operating_profile)
        assert weekdays in correct_frames
        assert_frame_equal(parse_day_range(weekdays), correct_frames[weekdays])


@pytest.mark.parametrize(
    "dayinfo, active",
    [
        ("Monday", ["monday"]),
        ("MondayToFriday", ["monday", "tuesday", "wednesday", "thursday", "friday"]),
        ("Weekend", ["saturday", "sunday"]),
        ("Monday|Wednesday|Sunday", ["monday", "wednesday", "sunday"]),
    ],
)
def test_parse_active_days(dayinfo, active):
    days = parse_active_days(dayinfo)
    assert [day for day in WEEKDAYS if days[day] == 1] == active


@pytest.mark.parametrize(
    "fixture_name, service_code, start, end",
    [
        ("tfl_file", "1-HAM-_-y05-2675925", "20190713", "20190714"),
        ("txc21_file", "99-PIC-B-y05-4", "20200201", "20200202"),
    ],
)
def test_get_calendar(fixture_name, service_code, start, end, request):
    doc = read_txc(request.getfixturevalue(fixture_name))
    gtfs_info = get_gtfs_info(doc)
    assert isinstance(gtfs_info, DataFrame)

    gtfs_calendar = get_calendar(gtfs_info)

    zeros = np.int64([0, 0])
    correct_frame = DataFrame(
        {
            "service_id": [
                "%s_%s_%s_Sunday" % (service_code, start, end),
                "%s_%s_%s_Saturday" % (service_code, start, end),
            ],
            "monday": zeros,
            "tuesday": zeros,
            "wednesday": zeros,
            "thursday": zeros,
            "friday": zeros,
            "saturday": np.int64([0, 1]),
            "sunday": np.int64([1, 0]),
            "start_date": [start, start],
            "end_date": [end, end],
        }
    )
    assert_frame_equal(gtfs_calendar, correct_frame, check_dtype=False)
