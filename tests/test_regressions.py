"""Regression tests for bugs fixed in transx2gtfs (one test per bug)."""

import io
import os
import shutil
import sqlite3
import zipfile
from urllib.error import URLError

import pandas as pd
import pytest

import transx2gtfs
from transx2gtfs import bank_holidays, converter, stops
from transx2gtfs.agency import get_agency
from transx2gtfs.bank_holidays import get_bank_holiday_dates
from transx2gtfs.calendar_dates import get_calendar_dates
from transx2gtfs.dataio import (
    generate_gtfs_export,
    get_xml_paths,
    read_xml_inside_nested_zip,
    read_xml_inside_zip,
)
from transx2gtfs.distribute import create_workers
from transx2gtfs.routes import get_routes
from transx2gtfs.stop_times import get_stop_times
from transx2gtfs.stops import (
    _get_tfl_style_stops,
    _get_txc_21_style_stops,
    get_stops,
    osgb36_to_wgs84,
    read_naptan_stops,
)
from transx2gtfs.transxchange import get_gtfs_info, parse_runtime_duration
from transx2gtfs.txc import read_txc
from transx2gtfs.trips import get_trips

# A document with exactly one Route, JourneyPatternSection, JourneyPatternTimingLink,
# Service, JourneyPattern and VehicleJourney (the old DOM reader returned a bare
# element instead of a list for those); two stops because a trip needs a sequence.
SINGLE_ELEMENT_TXC = """<?xml version="1.0" encoding="utf-8"?>
<TransXChange xmlns="http://www.transxchange.org.uk/" SchemaVersion="2.1">
  <StopPoints>
    <AnnotatedStopPointRef>
      <StopPointRef>9300WAS1</StopPointRef>
      <CommonName>Woolwich Arsenal Pier</CommonName>
    </AnnotatedStopPointRef>
    <AnnotatedStopPointRef>
      <StopPointRef>9300MIL2</StopPointRef>
      <CommonName>North Greenwich Pier</CommonName>
    </AnnotatedStopPointRef>
  </StopPoints>
  <Routes>
    <Route id="R_1">
      <PrivateCode>R_1</PrivateCode>
      <Description>Woolwich - North Greenwich</Description>
      <RouteSectionRef>RS_1</RouteSectionRef>
    </Route>
  </Routes>
  <JourneyPatternSections>
    <JourneyPatternSection id="JPS_1">
      <JourneyPatternTimingLink id="JPL_1">
        <From SequenceNumber="1"><StopPointRef>9300WAS1</StopPointRef></From>
        <To SequenceNumber="2"><StopPointRef>9300MIL2</StopPointRef></To>
        <RouteLinkRef>RL_1</RouteLinkRef>
        <RunTime>PT10M</RunTime>
      </JourneyPatternTimingLink>
    </JourneyPatternSection>
  </JourneyPatternSections>
  <Operators>
    <Operator id="OId_CV">
      <OperatorNameOnLicence>MBNA THAMES CLIPPERS</OperatorNameOnLicence>
    </Operator>
  </Operators>
  <Services>
    <Service>
      <ServiceCode>S_1</ServiceCode>
      <Lines>
        <Line id="L_1"><LineName>RB5</LineName></Line>
      </Lines>
      <OperatingPeriod>
        <StartDate>2019-02-23</StartDate>
        <EndDate>2019-12-22</EndDate>
      </OperatingPeriod>
      <OperatingProfile>
        <RegularDayType><DaysOfWeek><MondayToFriday /></DaysOfWeek></RegularDayType>
        <BankHolidayOperation>
          <DaysOfNonOperation><AllBankHolidays /></DaysOfNonOperation>
        </BankHolidayOperation>
      </OperatingProfile>
      <RegisteredOperatorRef>OId_CV</RegisteredOperatorRef>
      <Mode>ferry</Mode>
      <Description>Woolwich Arsenal - North Greenwich</Description>
      <StandardService>
        <Origin>Woolwich Arsenal Pier</Origin>
        <Destination>North Greenwich Pier</Destination>
        <JourneyPattern id="JP_1">
          <Direction>outbound</Direction>
          <RouteRef>R_1</RouteRef>
          <JourneyPatternSectionRefs>JPS_1</JourneyPatternSectionRefs>
        </JourneyPattern>
      </StandardService>
    </Service>
  </Services>
  <VehicleJourneys>
    <VehicleJourney>
      <VehicleJourneyCode>VJ_1</VehicleJourneyCode>
      <ServiceRef>S_1</ServiceRef>
      <LineRef>L_1</LineRef>
      <JourneyPatternRef>JP_1</JourneyPatternRef>
      <DepartureTime>11:02:00</DepartureTime>
    </VehicleJourney>
  </VehicleJourneys>
</TransXChange>
"""

LATIN1_XML = (
    b'<?xml version="1.0" encoding="ISO-8859-1"?>\n'
    b'<TransXChange><Operators><Operator id="O"><OperatorNameOnLicence>Caf\xe9'
    b"</OperatorNameOnLicence></Operator></Operators></TransXChange>\n"
)


@pytest.fixture
def no_download(monkeypatch):
    """Fail the test if a NaPTAN download is attempted."""

    def fail(*args, **kwargs):
        raise AssertionError("NaPTAN download should not be triggered")

    monkeypatch.setattr(stops, "download_naptan", fail)


def test_bank_holidays_fallback_to_bundled_file(monkeypatch, tmp_path):
    # Offline, and from a cwd where the old relative path does not exist
    monkeypatch.delenv("TRANSX2GTFS_BANK_HOLIDAYS_PATH")
    monkeypatch.chdir(tmp_path)

    def offline(*args, **kwargs):
        raise URLError("no network")

    monkeypatch.setattr(bank_holidays.urllib.request, "urlopen", offline)

    gtfs_info = pd.DataFrame({"start_date": ["20191220"], "end_date": ["20191231"]})
    assert get_bank_holiday_dates(gtfs_info) == ["20191225", "20191226"]


def test_zipped_xml_honours_declared_encoding(tmp_path):
    zip_path = str(tmp_path / "packed.zip")
    with zipfile.ZipFile(zip_path, "w") as zf:
        zf.writestr("latin1.xml", LATIN1_XML)
    data, size, name = read_xml_inside_zip({"latin1.xml": zip_path})
    assert data.operators[0].name_on_licence == "Café"
    assert size == len(LATIN1_XML)
    assert name == "latin1.xml"

    nested_path = str(tmp_path / "nested.zip")
    with zipfile.ZipFile(nested_path, "w") as zf:
        zf.write(zip_path, "packed.zip")
    data, size, name = read_xml_inside_nested_zip(
        {nested_path: {"packed.zip": "latin1.xml"}}
    )
    assert data.operators[0].name_on_licence == "Café"
    assert size == len(LATIN1_XML)
    assert name == "latin1.xml"


def test_upper_case_extensions_are_found(tmp_path, ferry_file, packed_zip):
    """.XML/.ZIP names (common on Windows) are discovered like lower-case ones."""
    shutil.copy(ferry_file, tmp_path / "FERRY.XML")
    shutil.copy(packed_zip, tmp_path / "PACKED.ZIP")
    inner = io.BytesIO()
    with zipfile.ZipFile(inner, "w") as zf:
        zf.writestr("INNER.XML", LATIN1_XML)
    with zipfile.ZipFile(tmp_path / "NESTED.ZIP", "w") as zf:
        zf.writestr("INNER.ZIP", inner.getvalue())
        zf.writestr("README.TXT", "not an xml file")

    paths = get_xml_paths(str(tmp_path))
    assert len(paths) == 5
    assert paths[0] == str(tmp_path / "FERRY.XML")

    nested = [p for p in paths if str(tmp_path / "NESTED.ZIP") in p]
    assert nested == [{str(tmp_path / "NESTED.ZIP"): {"INNER.ZIP": "INNER.XML"}}]
    data, size, name = read_xml_inside_nested_zip(nested[0])
    assert data.operators[0].name_on_licence == "Café"

    assert len(get_xml_paths(str(tmp_path / "PACKED.ZIP"))) == 3


class SerialPool:
    """Stand-in for multiprocessing.Pool that records the process count."""

    processes = []

    def __init__(self, processes=None, initializer=None, initargs=()):
        SerialPool.processes.append(processes)
        if initializer is not None:
            initializer(*initargs)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return False

    def map(self, func, iterable):
        return list(map(func, iterable))


def test_convert_honours_worker_cnt(monkeypatch, data_dir, tmp_path):
    monkeypatch.setattr(converter.multiprocessing, "Pool", SerialPool)
    SerialPool.processes.clear()

    transx2gtfs.convert(data_dir, str(tmp_path / "gtfs.zip"), worker_cnt=1)
    assert SerialPool.processes == [1]

    transx2gtfs.convert(data_dir, str(tmp_path / "gtfs.zip"), worker_cnt=2)
    assert SerialPool.processes == [1, 2]


def test_create_workers_drops_empty_batches():
    workers = create_workers(["a.xml", "b.xml"], worker_cnt=4)
    assert [w.input_files for w in workers] == [["a.xml"], ["b.xml"]]
    assert len(create_workers(["a.xml"], worker_cnt=10**9)) == 1
    assert create_workers([], worker_cnt=2) == []

    with pytest.raises(ValueError):
        create_workers(["a.xml"], worker_cnt=0)
    with pytest.raises(TypeError):
        create_workers(["a.xml"], worker_cnt="2")


def test_convert_without_xml_files_raises(tmp_path):
    with pytest.raises(ValueError, match="Did not find any TransXChange"):
        transx2gtfs.convert(str(tmp_path), str(tmp_path / "gtfs.zip"))

    with pytest.raises(ValueError, match="not a directory or a .zip file"):
        transx2gtfs.convert(str(tmp_path / "missing"), str(tmp_path / "gtfs.zip"))


def test_single_element_document(no_download):
    data = read_txc(SINGLE_ELEMENT_TXC.encode())

    stop_data = get_stops(data)
    assert list(stop_data["stop_id"]) == ["9300WAS1", "9300MIL2"]

    assert get_agency(data)["agency_id"].to_list() == ["OId_CV"]

    gtfs_info = get_gtfs_info(data)
    stop_times = get_stop_times(gtfs_info)
    assert stop_times["stop_sequence"].to_list() == [1, 2]
    assert stop_times["arrival_time"].to_list() == ["11:02:00", "11:12:00"]

    trips = get_trips(gtfs_info)
    # AllBankHolidays non-operation gives the calendar exceptions, hence a hashed id
    (service_id,) = trips["service_id"].to_list()
    assert service_id.startswith("S_1_20190223_20191222_MondayToFriday_")

    routes = get_routes(gtfs_info, data)
    assert routes["route_id"].to_list() == ["R_1"]
    assert routes["route_type"].to_list() == [4]

    # Service-level bank holiday exceptions within 2019-02-23..2019-12-22
    calendar_dates = get_calendar_dates(gtfs_info)
    assert set(calendar_dates["service_id"]) == set(trips["service_id"])
    assert set(calendar_dates["exception_type"]) == {2}
    dates = set(calendar_dates["date"])
    assert {"20190419", "20190826"} <= dates
    # Outside the operating period
    assert "20190101" not in dates
    assert "20191225" not in dates


def test_tfl_stop_coordinates_fall_back_to_the_file(no_download):
    """A StopPoint missing from NaPTAN gets its coordinates from Easting/Northing."""
    naptan = read_naptan_stops()
    known = naptan.loc[naptan["stop_id"] == "490007705N"].iloc[0]

    xml = """<?xml version="1.0" encoding="utf-8"?>
<TransXChange>
  <StopPoints>
    <StopPoint>
      <AtcoCode>490007705N</AtcoCode>
      <Descriptor><CommonName>Known stop</CommonName></Descriptor>
    </StopPoint>
    <StopPoint>
      <AtcoCode>NOT_IN_NAPTAN</AtcoCode>
      <Descriptor><CommonName>Unknown stop</CommonName></Descriptor>
      <Place><Location><Easting>523285</Easting><Northing>178520</Northing></Location></Place>
    </StopPoint>
    <StopPoint>
      <AtcoCode>NO_LOCATION</AtcoCode>
      <Descriptor><CommonName>Unlocatable stop</CommonName></Descriptor>
    </StopPoint>
  </StopPoints>
</TransXChange>
"""
    with pytest.warns(UserWarning, match="NO_LOCATION"):
        stop_data = _get_tfl_style_stops(read_txc(xml.encode()))

    assert stop_data["stop_id"].to_list() == ["490007705N", "NOT_IN_NAPTAN"]
    assert stop_data.iloc[0]["stop_name"] == known["stop_name"]

    # Same grid reference as the known NaPTAN stop, so the coordinates must match
    fallback = stop_data.iloc[1]
    assert fallback["stop_name"] == "Unknown stop"
    assert fallback["stop_lon"] == pytest.approx(known["stop_lon"], abs=1e-4)
    assert fallback["stop_lat"] == pytest.approx(known["stop_lat"], abs=1e-4)


def test_osgb36_to_wgs84_uses_one_cached_transformer(monkeypatch):
    calls = []
    real_from_crs = stops.Transformer.from_crs

    def spy(*args, **kwargs):
        calls.append((args, kwargs))
        return real_from_crs(*args, **kwargs)

    monkeypatch.setattr(stops.Transformer, "from_crs", spy)
    monkeypatch.setattr(stops, "_osgb36_to_wgs84", None)

    lon, lat = osgb36_to_wgs84(523481, 178564)
    assert lon == pytest.approx(-0.22273, abs=1e-4)
    assert lat == pytest.approx(51.49255, abs=1e-4)

    osgb36_to_wgs84(539550, 180055)
    assert calls == [(("EPSG:27700", "EPSG:4326"), {"always_xy": True})]


@pytest.mark.parametrize(
    "runtime, seconds",
    [("PT30S", 30), ("PT10M", 600), ("PT1M30S", 90), ("PT1H2M3S", 3723), ("PT0S", 0)],
)
def test_parse_runtime_duration(runtime, seconds):
    assert parse_runtime_duration(runtime) == seconds


def test_single_stop_elements(no_download):
    txc21 = """<TransXChange><StopPoints>
      <AnnotatedStopPointRef>
        <StopPointRef>9300WAS1</StopPointRef><CommonName>Woolwich</CommonName>
      </AnnotatedStopPointRef>
    </StopPoints></TransXChange>"""
    stop_data = get_stops(read_txc(txc21.encode()))
    assert stop_data["stop_id"].to_list() == ["9300WAS1"]

    tfl = """<TransXChange><StopPoints>
      <StopPoint>
        <AtcoCode>9300MIL2</AtcoCode>
        <Descriptor><CommonName>North Greenwich Pier</CommonName></Descriptor>
      </StopPoint>
    </StopPoints></TransXChange>"""
    stop_data = get_stops(read_txc(tfl.encode()))
    assert stop_data["stop_id"].to_list() == ["9300MIL2"]


def test_stop_times_advance_by_the_arriving_links_run_time(no_download):
    """Each stop is reached after the run time of the link arriving at it."""
    xml = SINGLE_ELEMENT_TXC.replace(
        """      <JourneyPatternTimingLink id="JPL_1">
        <From SequenceNumber="1"><StopPointRef>9300WAS1</StopPointRef></From>
        <To SequenceNumber="2"><StopPointRef>9300MIL2</StopPointRef></To>
        <RouteLinkRef>RL_1</RouteLinkRef>
        <RunTime>PT10M</RunTime>
      </JourneyPatternTimingLink>
""",
        """      <JourneyPatternTimingLink id="JPL_1">
        <From SequenceNumber="1"><StopPointRef>9300WAS1</StopPointRef></From>
        <To SequenceNumber="2"><StopPointRef>9300MIL2</StopPointRef></To>
        <RouteLinkRef>RL_1</RouteLinkRef>
        <RunTime>PT5M</RunTime>
      </JourneyPatternTimingLink>
      <JourneyPatternTimingLink id="JPL_2">
        <From SequenceNumber="2"><StopPointRef>9300MIL2</StopPointRef></From>
        <To SequenceNumber="3"><StopPointRef>9300MIL1</StopPointRef></To>
        <RouteLinkRef>RL_2</RouteLinkRef>
        <RunTime>PT7M</RunTime>
      </JourneyPatternTimingLink>
      <JourneyPatternTimingLink id="JPL_3">
        <From SequenceNumber="3"><StopPointRef>9300MIL1</StopPointRef></From>
        <To SequenceNumber="4"><StopPointRef>9300WAS1</StopPointRef></To>
        <RouteLinkRef>RL_3</RouteLinkRef>
        <RunTime>PT2M</RunTime>
      </JourneyPatternTimingLink>
""",
    )
    assert "PT7M" in xml
    stop_times = get_stop_times(get_gtfs_info(read_txc(xml.encode())))
    assert stop_times["stop_sequence"].to_list() == [1, 2, 3, 4]
    assert stop_times["arrival_time"].to_list() == [
        "11:02:00",
        "11:07:00",
        "11:14:00",
        "11:16:00",
    ]


def test_route_without_journeys_is_skipped(no_download):
    xml = SINGLE_ELEMENT_TXC.replace(
        "  </Routes>",
        '    <Route id="R_UNUSED"><PrivateCode>R_UNUSED</PrivateCode>'
        "<Description>Unused</Description><RouteSectionRef>RS_2</RouteSectionRef>"
        "</Route>\n  </Routes>",
    )
    data = read_txc(xml.encode())
    gtfs_info = get_gtfs_info(data)
    with pytest.warns(UserWarning, match="R_UNUSED"):
        routes = get_routes(gtfs_info, data)
    assert routes["route_id"].to_list() == ["R_1"]


def test_export_keeps_every_calendar_exception(tmp_path):
    db = str(tmp_path / "gtfs.db")
    conn = sqlite3.connect(db)
    one_row = {
        "stops": {"stop_id": ["S1"]},
        "agency": {"agency_id": ["A1"]},
        "routes": {"route_id": ["R1"]},
        "trips": {"trip_id": ["T1"]},
        "stop_times": {"trip_id": ["T1"], "stop_id": ["S1"]},
        "calendar": {"service_id": ["SV1"]},
    }
    for table, columns in one_row.items():
        pd.DataFrame(columns).to_sql(table, conn, index=False)
    calendar_dates = pd.DataFrame(
        {
            "service_id": ["SV1", "SV1", "SV1"],
            "date": ["20190419", "20190422", "20190419"],
            "exception_type": [2, 2, 2],
        }
    )
    calendar_dates.to_sql("calendar_dates", conn, index=False)
    conn.close()

    exported = generate_gtfs_export(db)["calendar_dates"]
    assert exported["date"].to_list() == ["20190419", "20190422"]


def test_unknown_txc21_stop_is_skipped_without_download(no_download):
    xml = """<?xml version="1.0" encoding="utf-8"?>
<TransXChange>
  <StopPoints>
    <AnnotatedStopPointRef>
      <StopPointRef>NOT_IN_NAPTAN</StopPointRef>
      <CommonName>Unknown stop</CommonName>
    </AnnotatedStopPointRef>
    <AnnotatedStopPointRef>
      <StopPointRef>49001643031</StopPointRef>
      <CommonName>Heathrow Terminal 5</CommonName>
    </AnnotatedStopPointRef>
  </StopPoints>
</TransXChange>
"""
    with pytest.warns(UserWarning, match="NOT_IN_NAPTAN"):
        stop_data = _get_txc_21_style_stops(read_txc(xml.encode()))
    assert stop_data["stop_id"].to_list() == ["49001643031"]


def test_naptan_path_env_var_must_exist(monkeypatch, tmp_path, no_download):
    monkeypatch.setenv("TRANSX2GTFS_NAPTAN_PATH", str(tmp_path / "missing.csv"))
    with pytest.raises(FileNotFoundError):
        read_naptan_stops()


def test_naptan_frame_is_cached_per_file(tmp_path, no_download):
    first = read_naptan_stops()
    assert read_naptan_stops() is first

    # A different file (or a modified one) is re-read
    other = tmp_path / "naptan.csv"
    other.write_text(
        "ATCOCode,CommonName,Latitude,Longitude,Extra\n" "X1,Stop X,51.5,-0.1,foo\n"
    )
    stops_x = read_naptan_stops(str(other))
    assert stops_x["stop_id"].to_list() == ["X1"]
    assert list(stops_x.columns) == ["stop_id", "stop_name", "stop_lat", "stop_lon"]

    # A file without the required columns is rejected with a clear message
    other.write_text("Code,Name\nX1,Stop X\n")
    os.utime(other, (0, 0))
    with pytest.raises(ValueError, match="must contain the columns"):
        read_naptan_stops(str(other))
