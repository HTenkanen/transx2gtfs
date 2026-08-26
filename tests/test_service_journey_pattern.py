import pytest
from pandas import DataFrame

from transx2gtfs.transxchange import get_service_journey_pattern_info
from transx2gtfs.txc import read_txc

REQUIRED_COLUMNS = [
    "agency_id",
    "direction",
    "direction_id",
    "end_date",
    "journey_pattern_id",
    "jp_section_reference",
    "line_name",
    "route_id",
    "service_code",
    "service_description",
    "start_date",
    "travel_mode",
    "trip_headsign",
    "vehicle_description",
    "vehicle_type",
]


@pytest.mark.parametrize(
    "fixture_name, shape", [("txc21_file", (6, 15)), ("tfl_file", (43, 15))]
)
def test_reading_journey_patterns(fixture_name, shape, request):
    doc = read_txc(request.getfixturevalue(fixture_name))
    journey_patterns = get_service_journey_pattern_info(doc)

    assert isinstance(journey_patterns, DataFrame)
    assert journey_patterns.shape == shape

    for col in REQUIRED_COLUMNS:
        assert col in journey_patterns.columns, ("Not in", journey_patterns.columns)
        assert journey_patterns[col].hasnans is False
