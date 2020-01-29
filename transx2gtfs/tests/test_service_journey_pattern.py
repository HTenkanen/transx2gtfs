from transx2gtfs.data import get_path
import pytest

@pytest.fixture
def test_tfl_data():
    return get_path('test_tfl_format')


@pytest.fixture
def test_txc21_data():
    return get_path('test_txc21_format')


@pytest.fixture
def test_naptan_data():
    return get_path('naptan_stops')


def test_reading_journey_patterns_from_txc21(test_txc21_data, test_naptan_data):
    from transx2gtfs.transxchange import get_service_journey_pattern_info
    from pandas import DataFrame
    import untangle

    data = untangle.parse(test_txc21_data)
    journey_patterns = get_service_journey_pattern_info(data)

    # Test type
    assert isinstance(journey_patterns, DataFrame)

    # Test shape
    assert journey_patterns.shape == (6, 14)

    # Test that required columns exist
    required_columns = ['agency_id',
                        'direction_id',
                        'end_date',
                        'journey_pattern_id',
                        'jp_section_reference',
                        'line_name',
                        'route_id',
                        'service_code',
                        'service_description',
                        'start_date',
                        'travel_mode',
                        'trip_headsign',
                        'vehicle_description',
                        'vehicle_type'
                        ]

    for col in required_columns:
        assert col in journey_patterns.columns, ("Not in", journey_patterns.columns)

    # Test that there are no missing data
    for col in required_columns:
        assert journey_patterns[col].hasnans is False


def test_reading_journey_patterns_from_tfl_format(test_tfl_data, test_naptan_data):
    from transx2gtfs.transxchange import get_service_journey_pattern_info
    from pandas import DataFrame
    import untangle

    data = untangle.parse(test_tfl_data)
    journey_patterns = get_service_journey_pattern_info(data)

    # Test type
    assert isinstance(journey_patterns, DataFrame)

    # Test shape
    assert journey_patterns.shape == (43, 14)

    # Test that required columns exist
    required_columns = ['agency_id',
                        'direction_id',
                        'end_date',
                        'journey_pattern_id',
                        'jp_section_reference',
                        'line_name',
                        'route_id',
                        'service_code',
                        'service_description',
                        'start_date',
                        'travel_mode',
                        'trip_headsign',
                        'vehicle_description',
                        'vehicle_type'
                        ]

    for col in required_columns:
        assert col in journey_patterns.columns, ("Not in", journey_patterns.columns)

    # Test that there are no missing data
    for col in required_columns:
        assert journey_patterns[col].hasnans is False