import os
import io
from zipfile import ZipFile

import pandas as pd

import transx2gtfs
from transx2gtfs.__main__ import main
from transx2gtfs.agency import get_agency, get_agency_url
from transx2gtfs.txc import read_txc

REQUIRED_FILES = [
    "agency.txt",
    "stops.txt",
    "routes.txt",
    "trips.txt",
    "stop_times.txt",
    "calendar.txt",
]


def test_agency_url_lookup():
    assert get_agency_url("OId_LUL") == "https://tfl.gov.uk/maps/track/tube"
    assert get_agency_url("OId_CV") == "https://www.thamesclippers.com/"
    assert get_agency_url("OId_UNKNOWN") == "NA"


def test_get_agency(ferry_file):
    agency = get_agency(read_txc(ferry_file))
    assert len(agency) == 1
    row = agency.iloc[0]
    assert row["agency_id"] == "OId_CV"
    assert row["agency_name"] == "MBNA THAMES CLIPPERS"
    assert row["agency_url"] == "https://www.thamesclippers.com/"
    assert row["agency_timezone"] == "Europe/London"


def _read_gtfs(zip_path):
    with ZipFile(zip_path) as zf:
        assert set(zf.namelist()) >= set(REQUIRED_FILES)
        return {
            name: pd.read_csv(io.BytesIO(zf.read(name)), dtype=str)
            for name in zf.namelist()
        }


def test_converting_to_gtfs(data_dir, tmp_path):
    output = str(tmp_path / "test_gtfs.zip")

    transx2gtfs.convert(data_dir, output, worker_cnt=2)

    assert os.path.isfile(output)
    gtfs = _read_gtfs(output)

    assert len(gtfs["agency.txt"]) == 3
    assert len(gtfs["stops.txt"]) == 49
    assert gtfs["stops.txt"]["stop_id"].is_unique
    assert gtfs["trips.txt"]["trip_id"].is_unique

    # Referential integrity between the files
    stop_times = gtfs["stop_times.txt"]
    assert set(stop_times["trip_id"]) == set(gtfs["trips.txt"]["trip_id"])
    assert set(stop_times["stop_id"]) <= set(gtfs["stops.txt"]["stop_id"])
    assert set(gtfs["trips.txt"]["route_id"]) <= set(gtfs["routes.txt"]["route_id"])
    assert set(gtfs["trips.txt"]["service_id"]) == set(
        gtfs["calendar.txt"]["service_id"]
    )
    # No bank holiday falls inside the fixtures' operating periods
    assert "calendar_dates.txt" not in gtfs
    assert set(gtfs["routes.txt"]["agency_id"]) <= set(gtfs["agency.txt"]["agency_id"])
    assert set(gtfs["routes.txt"]["route_type"]) == {"1", "3", "4"}


def test_cli(data_dir, tmp_path):
    output = str(tmp_path / "cli_gtfs.zip")
    main([data_dir, output, "--workers", "1"])
    assert os.path.isfile(output)
    _read_gtfs(output)
