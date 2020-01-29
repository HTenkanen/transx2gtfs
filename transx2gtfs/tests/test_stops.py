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


def test_reading_stops_from_txc21(test_txc21_data, test_naptan_data):
    from transx2gtfs.stops import _get_txc_21_style_stops
    from pandas import DataFrame
    import untangle

    data = untangle.parse(test_txc21_data)
    stops = _get_txc_21_style_stops(data, naptan_stops_fp=test_naptan_data)

    # Test type
    assert isinstance(stops, DataFrame)

    # Test shape
    assert stops.shape == (3, 7)

    # Test that required columns exist
    required_columns = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
    for col in required_columns:
        assert col in stops.columns

    # Test that there are no missing data
    for col in required_columns:
        assert stops[col].hasnans is False


def test_reading_stops_from_tfl(test_tfl_data, test_naptan_data):
    from transx2gtfs.stops import _get_tfl_style_stops
    from pandas import DataFrame
    import untangle

    data = untangle.parse(test_tfl_data)
    stops = _get_tfl_style_stops(data, naptan_stops_fp=test_naptan_data)

    # Test type
    assert isinstance(stops, DataFrame)

    # Test shape
    assert stops.shape == (43, 6)

    # Test that required columns exist
    required_columns = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
    for col in required_columns:
        assert col in stops.columns

    # Test that there are no missing data
    for col in required_columns:
        assert stops[col].hasnans is False