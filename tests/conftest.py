import pathlib

import pytest

from transx2gtfs.data import get_path

DATA_DIR = pathlib.Path(__file__).parent / "data"
UNPACKED_DIR = DATA_DIR / "unpacked"


@pytest.fixture(autouse=True)
def offline_data(monkeypatch):
    """Serve NaPTAN stops and bank holidays from local files (no network)."""
    monkeypatch.setenv("TRANSX2GTFS_NAPTAN_PATH", str(DATA_DIR / "naptan_subset.csv"))
    monkeypatch.setenv("TRANSX2GTFS_BANK_HOLIDAYS_PATH", get_path("bank_holidays"))


@pytest.fixture
def data_dir():
    return str(UNPACKED_DIR)


@pytest.fixture
def tfl_file():
    """TfL style: StopPoint elements with Easting/Northing (underground)."""
    return str(UNPACKED_DIR / "tfl_1-HAM-_-y05-2675925.xml")


@pytest.fixture
def txc21_file():
    """TXC 2.1 style: AnnotatedStopPointRef elements (bus)."""
    return str(UNPACKED_DIR / "tfl_99-PIC-B-y05-4.xml")


@pytest.fixture
def ferry_file():
    return str(UNPACKED_DIR / "tfl_33-RB5-_-y05-7.xml")


@pytest.fixture
def packed_zip():
    return str(DATA_DIR / "packed.zip")


@pytest.fixture
def nested_zip():
    return str(DATA_DIR / "nested.zip")


@pytest.fixture
def dir_with_packed():
    return str(DATA_DIR / "dir_with_packed")
