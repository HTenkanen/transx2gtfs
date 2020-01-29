from transx2gtfs.data import get_path
import pytest


@pytest.fixture
def test_data():
    return get_path('test_data_dir')


@pytest.fixture
def test_tfl_data():
    return get_path('test_tfl_format')


@pytest.fixture
def test_txc21_data():
    return get_path('test_txc21_format')


@pytest.fixture
def test_naptan_data():
    return get_path('naptan_stops')


@pytest.fixture
def temp_output_filepath():
    import tempfile
    import os
    temp_dir = tempfile.gettempdir()
    temp_fp = os.path.join(temp_dir, 'test_gtfs.zip')
    return temp_fp


def test_data_dir_availability_for_testing(test_data):
    import os
    import glob
    assert os.path.isdir(test_data)
    files = glob.glob(os.path.join(test_data, '*.xml'))
    for file in files:
        assert os.path.isfile(file)


def test_tfl_data_availability_for_testing(test_tfl_data):
    import os
    assert os.path.isfile(test_tfl_data)


def test_txc21_data_availability_for_testing(test_txc21_data):
    import os
    assert os.path.isfile(test_txc21_data)


def test_naptan_data_availability_for_testing(test_naptan_data):
    import os
    assert os.path.isfile(test_naptan_data)
