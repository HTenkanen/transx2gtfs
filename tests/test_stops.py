import pandas as pd
import untangle
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from transx2gtfs.stops import (
    _get_tfl_style_stops,
    _get_txc_21_style_stops,
    get_stops,
    read_naptan_stops,
)

REQUIRED_COLUMNS = ["stop_id", "stop_name", "stop_lat", "stop_lon"]


def test_read_naptan_stops():
    stops = read_naptan_stops()
    assert list(stops.columns) == REQUIRED_COLUMNS
    assert len(stops) == 49
    assert stops["stop_id"].is_unique
    assert pd.api.types.is_string_dtype(stops["stop_id"])


def test_reading_stops_from_txc21(txc21_file):
    data = untangle.parse(txc21_file)
    stops = _get_txc_21_style_stops(data)

    assert isinstance(stops, DataFrame)
    assert stops.shape == (3, 4)
    assert list(stops.columns) == REQUIRED_COLUMNS
    for col in REQUIRED_COLUMNS:
        assert stops[col].hasnans is False


def test_reading_stops_from_tfl(tfl_file):
    data = untangle.parse(tfl_file)
    stops = _get_tfl_style_stops(data)

    assert isinstance(stops, DataFrame)
    assert stops.shape == (43, 4)
    assert list(stops.columns) == REQUIRED_COLUMNS
    for col in REQUIRED_COLUMNS:
        assert stops[col].hasnans is False

    # Coordinates come from NaPTAN and are in London
    assert stops["stop_lat"].between(51.3, 51.7).all()
    assert stops["stop_lon"].between(-0.6, 0.3).all()


def test_get_stops_detects_style(tfl_file, txc21_file):
    tfl = untangle.parse(tfl_file)
    assert_frame_equal(get_stops(tfl), _get_tfl_style_stops(tfl))

    txc21 = untangle.parse(txc21_file)
    assert_frame_equal(get_stops(txc21), _get_txc_21_style_stops(txc21))
