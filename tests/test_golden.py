"""The GTFS tables produced for the TfL fixtures must not change.

The golden CSVs were generated with the DOM-based implementation of 0.5.0 and
guard the streaming reader and every later refactor."""

import pandas as pd
import pytest
from pandas.testing import assert_frame_equal

from transx2gtfs.agency import get_agency
from transx2gtfs.calendar import get_calendar
from transx2gtfs.calendar_dates import get_calendar_dates
from transx2gtfs.routes import get_routes
from transx2gtfs.stop_times import get_stop_times
from transx2gtfs.stops import get_stops
from transx2gtfs.transxchange import get_gtfs_info
from transx2gtfs.trips import get_trips
from transx2gtfs.txc import read_txc

from conftest import DATA_DIR, UNPACKED_DIR

FIXTURES = ["tfl_1-HAM-_-y05-2675925", "tfl_33-RB5-_-y05-7", "tfl_99-PIC-B-y05-4"]
TABLES = [
    "agency",
    "stops",
    "routes",
    "trips",
    "stop_times",
    "calendar",
    "calendar_dates",
]


def gtfs_tables(doc):
    gtfs_info = get_gtfs_info(doc)
    return {
        "agency": get_agency(doc),
        "stops": get_stops(doc),
        "routes": get_routes(gtfs_info, doc),
        "trips": get_trips(gtfs_info),
        "stop_times": get_stop_times(gtfs_info),
        "calendar": get_calendar(gtfs_info),
        "calendar_dates": get_calendar_dates(gtfs_info),
    }


@pytest.mark.parametrize("fixture", FIXTURES)
def test_gtfs_tables_match_golden(fixture):
    tables = gtfs_tables(read_txc(UNPACKED_DIR / (fixture + ".xml")))
    for name in TABLES:
        golden = DATA_DIR / "golden" / fixture / (name + ".csv")
        if not golden.exists():
            # No golden file means the table was not produced (calendar_dates
            # when no bank holiday falls into the operating period)
            assert tables[name] is None, name
            continue
        expected = pd.read_csv(golden, dtype=str, keep_default_na=False)
        produced = tables[name].astype(str).reset_index(drop=True)
        assert_frame_equal(produced, expected, obj=name)
