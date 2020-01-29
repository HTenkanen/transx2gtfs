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


def test_calendar_weekday_info_tfl(test_tfl_data):
    from transx2gtfs.calendar import get_service_operative_days_info
    import untangle

    data = untangle.parse(test_tfl_data)
    operative_days = get_service_operative_days_info(data)

    # Should return text
    assert isinstance(operative_days, str)

    # Should contain text 'Weekend'
    assert operative_days == 'Weekend'


def test_calendar_weekday_info_txc21(test_txc21_data):
    from transx2gtfs.calendar import get_service_operative_days_info
    import untangle

    data = untangle.parse(test_txc21_data)
    operative_days = get_service_operative_days_info(data)

    # Should return text
    assert isinstance(operative_days, str)

    # Should contain text 'Weekend'
    assert operative_days == 'Weekend'


def test_calendar_dataframe_tfl(test_tfl_data):
    from transx2gtfs.calendar import get_weekday_info, parse_day_range
    from pandas import DataFrame
    from pandas.testing import assert_frame_equal
    import untangle
    data = untangle.parse(test_tfl_data)

    # Get vehicle journeys
    vjourneys = data.TransXChange.VehicleJourneys.VehicleJourney

    correct_frames = {'Sunday': DataFrame({'friday': 0.0, 'monday': 0.0, 'saturday': 0.0,
                               'sunday': 1.0, 'thursday': 0.0,
                               'tuesday': 0.0, 'wednesday': 0.0}, index=[0]),

                      'Saturday': DataFrame({'friday': 0.0, 'monday': 0.0, 'saturday': 1.0,
                                           'sunday': 0.0, 'thursday': 0.0,
                                           'tuesday': 0.0, 'wednesday': 0.0}, index=[0])
                      }


    for i, journey in enumerate(vjourneys):
        # Parse weekday operation times from VehicleJourney
        weekdays = get_weekday_info(journey)

        # Should return text
        assert isinstance(weekdays, str)

        # Should be either 'Sunday' or 'Saturday'
        assert weekdays in ['Sunday', 'Saturday']

        # Get a row of DataFrame
        calendar_info = parse_day_range(weekdays)

        assert_frame_equal(calendar_info, correct_frames[weekdays])


def test_calendar_dataframe_txc21(test_txc21_data):
    from transx2gtfs.calendar import get_weekday_info, parse_day_range
    from pandas import DataFrame
    from pandas.testing import assert_frame_equal
    import untangle
    data = untangle.parse(test_txc21_data)

    # Get vehicle journeys
    vjourneys = data.TransXChange.VehicleJourneys.VehicleJourney

    correct_frames = {'Sunday': DataFrame({'friday': 0.0, 'monday': 0.0, 'saturday': 0.0,
                               'sunday': 1.0, 'thursday': 0.0,
                               'tuesday': 0.0, 'wednesday': 0.0}, index=[0]),

                      'Saturday': DataFrame({'friday': 0.0, 'monday': 0.0, 'saturday': 1.0,
                                           'sunday': 0.0, 'thursday': 0.0,
                                           'tuesday': 0.0, 'wednesday': 0.0}, index=[0])
                      }


    for i, journey in enumerate(vjourneys):
        # Parse weekday operation times from VehicleJourney
        weekdays = get_weekday_info(journey)

        # Should return text
        assert isinstance(weekdays, str)

        # Should be either 'Sunday' or 'Saturday'
        assert weekdays in ['Sunday', 'Saturday']

        # Get a row of DataFrame
        calendar_info = parse_day_range(weekdays)

        assert_frame_equal(calendar_info, correct_frames[weekdays])


def test_get_calendar_tfl(test_tfl_data):
    from transx2gtfs.calendar import get_calendar
    from transx2gtfs.transxchange import get_gtfs_info
    from pandas import DataFrame
    from pandas.testing import assert_frame_equal
    import numpy as np
    import untangle
    data = untangle.parse(test_tfl_data)

    # Get gtfs info
    gtfs_info = get_gtfs_info(data)
    assert isinstance(gtfs_info, DataFrame)

    # Get GTFS calendar
    gtfs_calendar = get_calendar(gtfs_info)
    assert isinstance(gtfs_calendar, DataFrame)

    correct_frame = DataFrame({
        'service_id': ["1-HAM-_-y05-2675925_20190713_20190714_Sunday",
                       "1-HAM-_-y05-2675925_20190713_20190714_Saturday"],
        'monday': np.int32([0, 0]), 'tuesday': np.int32([0, 0]), 'wednesday': np.int32([0, 0]),
        'thursday': np.int32([0, 0]), 'friday': np.int32([0, 0]),
        'saturday': np.int32([0, 1]), 'sunday': np.int32([1, 0]),
        'start_date': ["20190713", "20190713"],
        'end_date': ["20190714", "20190714"],
    }, index=[0, 1])

    # Check that the frames match
    assert_frame_equal(gtfs_calendar, correct_frame)


def test_get_calendar_txc21(test_txc21_data):
    from transx2gtfs.calendar import get_calendar
    from transx2gtfs.transxchange import get_gtfs_info
    from pandas import DataFrame
    from pandas.testing import assert_frame_equal
    import numpy as np
    import untangle
    data = untangle.parse(test_txc21_data)

    # Get gtfs info
    gtfs_info = get_gtfs_info(data)
    assert isinstance(gtfs_info, DataFrame)

    # Get GTFS calendar
    gtfs_calendar = get_calendar(gtfs_info)
    assert isinstance(gtfs_calendar, DataFrame)

    correct_frame = DataFrame({
        'service_id': ["99-PIC-B-y05-4_20200201_20200202_Sunday",
                       "99-PIC-B-y05-4_20200201_20200202_Saturday"],
        'monday': np.int32([0, 0]), 'tuesday': np.int32([0, 0]), 'wednesday': np.int32([0, 0]),
        'thursday': np.int32([0, 0]), 'friday': np.int32([0, 0]),
        'saturday': np.int32([0, 1]), 'sunday': np.int32([1, 0]),
        'start_date': ["20200201", "20200201"],
        'end_date': ["20200202", "20200202"],
    }, index=[0, 1])

    # Check that the frames match
    assert_frame_equal(gtfs_calendar, correct_frame)
