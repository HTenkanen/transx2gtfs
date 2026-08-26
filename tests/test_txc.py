"""Tests for the streaming TransXChange reader (transx2gtfs.txc)."""

import io
import os
import subprocess
import sys
import textwrap

import pytest

from transx2gtfs.agency import get_agency
from transx2gtfs.stop_times import get_stop_times
from transx2gtfs.transxchange import get_gtfs_info
from transx2gtfs.trips import get_trips
from transx2gtfs.txc import OperatingProfile, TxcDocument, read_txc

MINIMAL = """<?xml version="1.0" encoding="utf-8"?>
<TransXChange xmlns="http://www.transxchange.org.uk/" SchemaVersion="2.1">
  <NptgLocalities>{filler}</NptgLocalities>
  <StopPoints>
    <AnnotatedStopPointRef><StopPointRef>S1</StopPointRef><CommonName>One</CommonName></AnnotatedStopPointRef>
    <AnnotatedStopPointRef><StopPointRef>S2</StopPointRef><CommonName>Two</CommonName></AnnotatedStopPointRef>
    <AnnotatedStopPointRef><StopPointRef>S3</StopPointRef><CommonName>Three</CommonName></AnnotatedStopPointRef>
  </StopPoints>
  <RouteSections>{filler}</RouteSections>
  <Routes>
    <Route id="R_1"><PrivateCode>R_1</PrivateCode><Description>One - Three</Description>
      <RouteSectionRef>RS_1</RouteSectionRef></Route>
  </Routes>
  <JourneyPatternSections>
    <JourneyPatternSection id="JPS_A">
      <JourneyPatternTimingLink id="L_A">
        <From SequenceNumber="1"><StopPointRef>S1</StopPointRef></From>
        <To SequenceNumber="2"><StopPointRef>S2</StopPointRef></To>
        <RouteLinkRef>RL_1</RouteLinkRef><RunTime>PT5M</RunTime>
      </JourneyPatternTimingLink>
    </JourneyPatternSection>
    <JourneyPatternSection id="JPS_B">
      <JourneyPatternTimingLink id="L_B">
        <From SequenceNumber="2"><StopPointRef>S2</StopPointRef></From>
        <To SequenceNumber="3"><StopPointRef>S3</StopPointRef></To>
        <RouteLinkRef>RL_2</RouteLinkRef><RunTime>PT7M</RunTime>
      </JourneyPatternTimingLink>
    </JourneyPatternSection>
  </JourneyPatternSections>
  <Operators>
    <Operator id="OId_X"><OperatorCode>X</OperatorCode>{operator_names}</Operator>
  </Operators>
  <Services>
    <Service>
      <ServiceCode>SVC</ServiceCode>
      <Lines><Line id="L1"><LineName>1</LineName></Line></Lines>
      <OperatingPeriod><StartDate>2020-01-01</StartDate><EndDate>2020-12-31</EndDate></OperatingPeriod>
      <OperatingProfile><RegularDayType><DaysOfWeek><MondayToFriday /></DaysOfWeek>
      </RegularDayType></OperatingProfile>
      <RegisteredOperatorRef>OId_X</RegisteredOperatorRef>
      <Mode>bus</Mode>
      <Description>One - Three</Description>
      <StandardService>
        <Origin>One</Origin><Destination>Three</Destination>
        <JourneyPattern id="JP_AB">
          <Direction>outbound</Direction><RouteRef>R_1</RouteRef>
          <JourneyPatternSectionRefs>JPS_A</JourneyPatternSectionRefs>
          <JourneyPatternSectionRefs>JPS_B</JourneyPatternSectionRefs>
        </JourneyPattern>
        <JourneyPattern id="JP_A">
          <Direction>outbound</Direction><RouteRef>R_1</RouteRef>
          <JourneyPatternSectionRefs>JPS_A</JourneyPatternSectionRefs>
        </JourneyPattern>
      </StandardService>
    </Service>
  </Services>
  <VehicleJourneys>
    <VehicleJourney>
      <VehicleJourneyCode>VJ_AB</VehicleJourneyCode><ServiceRef>SVC</ServiceRef>
      <LineRef>L1</LineRef><JourneyPatternRef>JP_AB</JourneyPatternRef>
      <DepartureTime>08:00:00</DepartureTime>
    </VehicleJourney>
    <VehicleJourney>
      <VehicleJourneyCode>VJ_A</VehicleJourneyCode><ServiceRef>SVC</ServiceRef>
      <LineRef>L1</LineRef><JourneyPatternRef>JP_A</JourneyPatternRef>
      <DepartureTime>08:00:00</DepartureTime>
    </VehicleJourney>
  </VehicleJourneys>
</TransXChange>
"""


def minimal(
    filler="", operator_names="<OperatorNameOnLicence>X Ltd</OperatorNameOnLicence>"
):
    return MINIMAL.format(filler=filler, operator_names=operator_names).encode()


def test_read_from_path_bytes_and_file(ferry_file):
    from_path = read_txc(ferry_file)
    with open(ferry_file, "rb") as f:
        raw = f.read()
    from_bytes = read_txc(raw, file_name=os.path.basename(ferry_file))
    from_file = read_txc(io.BytesIO(raw), file_name=os.path.basename(ferry_file))

    assert isinstance(from_path, TxcDocument)
    assert from_path.file_name == "tfl_33-RB5-_-y05-7.xml"
    assert from_path == from_bytes == from_file
    assert from_path.schema_version == "2.1"
    assert [s.atco_code for s in from_path.stop_points] == [
        "9300MIL2",
        "9300WAS1",
        "9300MIL1",
    ]
    assert from_path.services[0].operating_profile == OperatingProfile(
        days_of_week=["Weekend"],
        bank_holiday_days_of_operation=["AllBankHolidays"],
        bank_holiday_days_of_non_operation=[],
    )
    assert from_path.vehicle_journeys[0].operating_profile is None


def test_namespace_is_optional():
    with_ns = read_txc(minimal())
    without_ns = read_txc(
        minimal().replace(b' xmlns="http://www.transxchange.org.uk/"', b"")
    )
    assert with_ns == without_ns
    assert without_ns.journey_pattern("JP_AB").section_refs == ["JPS_A", "JPS_B"]
    assert without_ns.stop_point_style == "AnnotatedStopPointRef"


def test_missing_required_element_is_reported():
    broken = minimal().replace(b"<ServiceCode>SVC</ServiceCode>", b"")
    with pytest.raises(
        ValueError, match="<Service> is missing required element ServiceCode"
    ):
        read_txc(broken)

    no_departure = minimal().replace(b"<DepartureTime>08:00:00</DepartureTime>", b"")
    with pytest.raises(
        ValueError, match="<VehicleJourney> is missing required element DepartureTime"
    ):
        read_txc(no_departure)

    no_run_time = minimal().replace(b"<RunTime>PT7M</RunTime>", b"")
    with pytest.raises(
        ValueError,
        match="<JourneyPatternTimingLink> is missing required element RunTime",
    ):
        read_txc(no_run_time)

    with pytest.raises(KeyError, match="JPS_missing"):
        read_txc(minimal()).journey_pattern_section("JPS_missing")


def test_unknown_service_reference_is_rejected():
    xml = minimal().replace(
        b"<VehicleJourneyCode>VJ_A</VehicleJourneyCode><ServiceRef>SVC</ServiceRef>",
        b"<VehicleJourneyCode>VJ_A</VehicleJourneyCode><ServiceRef>OTHER</ServiceRef>",
    )
    with pytest.raises(ValueError, match="VJ_A' refers to unknown Service 'OTHER'"):
        get_gtfs_info(read_txc(xml))


def test_external_entities_are_not_resolved(tmp_path):
    secret = tmp_path / "secret.txt"
    secret.write_text("SECRET")
    xml = (
        minimal()
        .replace(
            b"<TransXChange ",
            b'<!DOCTYPE TransXChange [<!ENTITY leak SYSTEM "file://%s">]>\n<TransXChange '
            % str(secret).encode(),
        )
        .replace(b"<OperatorNameOnLicence>X Ltd<", b"<OperatorNameOnLicence>&leak;<")
    )
    doc = read_txc(xml)
    assert "SECRET" not in (doc.operators[0].name_on_licence or "")


def test_multi_section_journey_pattern_is_one_trip():
    gtfs_info = get_gtfs_info(read_txc(minimal()))
    stop_times = get_stop_times(gtfs_info)
    trips = get_trips(gtfs_info)

    ab = stop_times[stop_times["trip_id"] == "SVC_VJ_AB"]
    assert ab["stop_id"].to_list() == ["S1", "S2", "S3"]
    assert ab["stop_sequence"].to_list() == [1, 2, 3]
    assert ab["timepoint"].to_list() == [1, 0, 0]
    assert ab["arrival_time"].to_list() == ["08:00:00", "08:05:00", "08:12:00"]
    assert ab["departure_time"].to_list() == ["08:00:00", "08:05:00", "08:12:00"]

    # The single-section pattern sharing the first section keeps the legacy id
    a = stop_times[stop_times["trip_id"] == "JPS_A_MondayToFriday_0800"]
    assert a["stop_id"].to_list() == ["S1", "S2"]
    assert sorted(trips["trip_id"]) == ["JPS_A_MondayToFriday_0800", "SVC_VJ_AB"]


def test_identical_multi_section_journeys_get_distinct_trips():
    duplicate = """    <VehicleJourney>
      <VehicleJourneyCode>VJ_AB2</VehicleJourneyCode><ServiceRef>SVC</ServiceRef>
      <LineRef>L1</LineRef><JourneyPatternRef>JP_AB</JourneyPatternRef>
      <DepartureTime>08:00:00</DepartureTime>
    </VehicleJourney>
  </VehicleJourneys>"""
    xml = minimal().decode().replace("  </VehicleJourneys>", duplicate)
    trips = get_trips(get_gtfs_info(read_txc(xml.encode())))
    assert trips["trip_id"].is_unique
    assert {"SVC_VJ_AB", "SVC_VJ_AB2"} <= set(trips["trip_id"])


def test_operator_name_falls_back_to_short_name_and_code():
    short = get_agency(
        read_txc(minimal(operator_names="<OperatorShortName>X</OperatorShortName>"))
    )
    assert short["agency_name"].to_list() == ["X"]
    code_only = get_agency(read_txc(minimal(operator_names="")))
    assert code_only["agency_name"].to_list() == ["X"]


def test_journey_without_profile_uses_its_own_service():
    xml = minimal().decode()
    second_service = (
        xml[xml.index("    <Service>") : xml.index("  </Services>")]
        .replace("SVC", "SVC2")
        .replace("JP_AB", "JP2_AB")
        .replace('JP_A"', 'JP2_A"')
        .replace("<MondayToFriday />", "<Saturday />")
    )
    journey = """    <VehicleJourney>
      <VehicleJourneyCode>VJ2</VehicleJourneyCode><ServiceRef>SVC2</ServiceRef>
      <LineRef>L1</LineRef><JourneyPatternRef>JP2_A</JourneyPatternRef>
      <DepartureTime>09:00:00</DepartureTime>
    </VehicleJourney>
"""
    xml = xml.replace("  </Services>", second_service + "  </Services>")
    xml = xml.replace("  </VehicleJourneys>", journey + "  </VehicleJourneys>")
    gtfs_info = get_gtfs_info(read_txc(xml.encode()))
    by_journey = gtfs_info.drop_duplicates("vehicle_journey_id").set_index(
        "vehicle_journey_id"
    )
    assert by_journey.loc["VJ_A", "weekdays"] == "MondayToFriday"
    assert by_journey.loc["VJ2", "weekdays"] == "Saturday"


MEMORY_PROBE = textwrap.dedent("""
    import resource, sys
    from transx2gtfs.txc import read_txc

    path = sys.argv[1]
    before = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    doc = read_txc(path)
    after = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    scale = 1 if sys.platform == "darwin" else 1024  # bytes on macOS, KiB elsewhere
    print((after - before) * scale, len(doc.stop_points), len(doc.vehicle_journeys))
    """)


def _many_localities():
    """Filler of many independent depth-2 elements."""
    locality = (
        "<AnnotatedNptgLocalityRef><NptgLocalityRef>N%07d</NptgLocalityRef>"
        "<LocalityName>Locality number %d with a reasonably long name</LocalityName>"
        "</AnnotatedNptgLocalityRef>\n"
    )
    return "".join(locality % (i, i) for i in range(120_000))


def _one_huge_route_section():
    """Filler that is a single ignored depth-2 element with a huge subtree."""
    link = (
        '<RouteLink id="RL_%07d"><From><StopPointRef>S1</StopPointRef></From>'
        "<To><StopPointRef>S2</StopPointRef></To><Distance>%d</Distance>"
        "<Direction>outbound</Direction></RouteLink>\n"
    )
    links = "".join(link % (i, i) for i in range(120_000))
    return '<RouteSection id="RS_big">%s</RouteSection>' % links


@pytest.mark.skipif(sys.platform == "win32", reason="resource module is POSIX only")
@pytest.mark.parametrize("filler", [_many_localities, _one_huge_route_section])
def test_parsing_memory_is_bounded_by_the_model(tmp_path, filler):
    """Discarded content (localities, route sections) must not stay in memory."""
    path = tmp_path / "large.xml"
    path.write_bytes(minimal(filler=filler()))
    file_size = path.stat().st_size
    assert file_size > 20_000_000

    result = subprocess.run(
        [sys.executable, "-c", MEMORY_PROBE, str(path)],
        capture_output=True,
        text=True,
        check=True,
    )
    rss_increase, stop_count, journey_count = (int(v) for v in result.stdout.split())
    assert (stop_count, journey_count) == (3, 2)
    # A retained tree costs several times the file size; streaming costs a fraction
    assert rss_increase < file_size, (rss_increase, file_size)


def test_operator_fields_and_licensed_operators():
    xml = minimal(
        operator_names="<OperatorNameOnLicence>X Ltd</OperatorNameOnLicence>"
        "<NationalOperatorCode>XNOC</NationalOperatorCode>"
        "<LicenceNumber>PB0001</LicenceNumber>"
    ).replace(
        b"</Operators>",
        b'<LicensedOperator id="OId_Y"><OperatorCode>Y</OperatorCode></LicensedOperator>'
        b"</Operators>",
    )
    doc = read_txc(xml)
    assert [o.id for o in doc.operators] == ["OId_X", "OId_Y"]
    first = doc.operator("OId_X")
    assert (first.national_operator_code, first.licence_number) == ("XNOC", "PB0001")
    assert doc.operator("OId_Y").code == "Y"
    with pytest.raises(KeyError):
        doc.operator("OId_Z")


def test_root_dates_and_wait_times():
    xml = (
        minimal()
        .replace(
            b'SchemaVersion="2.1"',
            b'SchemaVersion="2.1" CreationDateTime="2027-01-01T00:00:00"',
        )
        .replace(
            b'<From SequenceNumber="1"><StopPointRef>S1</StopPointRef></From>',
            b'<From SequenceNumber="1"><StopPointRef>S1</StopPointRef>'
            b"<WaitTime>PT1M</WaitTime></From>",
        )
        .replace(
            b'<To SequenceNumber="2"><StopPointRef>S2</StopPointRef></To>',
            b'<To SequenceNumber="2"><StopPointRef>S2</StopPointRef>'
            b"<WaitTime>PT2M</WaitTime></To>",
        )
    )
    doc = read_txc(xml)
    assert doc.creation_date_time == "2027-01-01T00:00:00"
    assert doc.modification_date_time is None
    link = doc.journey_pattern_section("JPS_A").timing_links[0]
    assert (link.from_wait_time, link.to_wait_time) == ("PT1M", "PT2M")
    other = doc.journey_pattern_section("JPS_B").timing_links[0]
    assert (other.from_wait_time, other.to_wait_time) == (None, None)


def test_section_refs_may_be_whitespace_separated_idrefs():
    xml = minimal().replace(
        b"<JourneyPatternSectionRefs>JPS_A</JourneyPatternSectionRefs>"
        b"<JourneyPatternSectionRefs>JPS_B</JourneyPatternSectionRefs>",
        b"<JourneyPatternSectionRefs>JPS_A JPS_B</JourneyPatternSectionRefs>",
    )
    assert read_txc(xml).journey_pattern("JP_AB").section_refs == ["JPS_A", "JPS_B"]


def test_notes_in_both_forms():
    notes = (
        b"<Note><NoteCode>1</NoteCode><NoteText>Bookable in advance</NoteText></Note>"
        b"<Note> Direct text </Note>"
    )
    xml = minimal().replace(
        b"<DepartureTime>08:00:00</DepartureTime>",
        b"<DepartureTime>08:00:00</DepartureTime>" + notes,
        1,
    )
    doc = read_txc(xml)
    assert doc.vehicle_journeys[0].notes == ["Bookable in advance", "Direct text"]
    assert doc.vehicle_journeys[1].notes == []


def test_route_link_distances_and_sections_are_collected():
    sections = (
        '<RouteSection id="RS_1"><RouteLink id="RL_1"><Distance>1000</Distance></RouteLink>'
        '<RouteLink id="RL_2"></RouteLink></RouteSection>'
        '<RouteSection id="RS_2"><RouteLink id="RL_3"><Distance>250</Distance>'
        "<Track><Mapping><Location><Longitude>0</Longitude></Location></Mapping></Track>"
        "</RouteLink></RouteSection>"
    )
    doc = read_txc(
        minimal().replace(
            b"<RouteSections></RouteSections>",
            ("<RouteSections>%s</RouteSections>" % sections).encode(),
        )
    )
    assert doc.route_link_distances == {"RL_1": "1000", "RL_3": "250"}
    assert doc.route_link_sections == {"RL_1": "RS_1", "RL_2": "RS_1", "RL_3": "RS_2"}


FULL_PROFILE = (
    b"<OperatingProfile>"
    b"<RegularDayType><HolidaysOnly /></RegularDayType>"
    b"<SpecialDaysOperation>"
    b"<DaysOfOperation>"
    b"<DateRange><StartDate>2020-06-01</StartDate><EndDate>2020-06-05</EndDate>"
    b"</DateRange>"
    b"<DateRange><StartDate>2020-07-01</StartDate></DateRange>"
    b"</DaysOfOperation>"
    b"<DaysOfNonOperation>"
    b"<DateRange><StartDate>2020-08-10</StartDate><EndDate>2020-08-12</EndDate>"
    b"</DateRange>"
    b"</DaysOfNonOperation>"
    b"</SpecialDaysOperation>"
    b"<ServicedOrganisationDayType>"
    b"<DaysOfOperation>"
    b"<WorkingDays><ServicedOrganisationRef>ORG1</ServicedOrganisationRef>"
    b"</WorkingDays>"
    b"<Holidays><ServicedOrganisationRef>ORG2</ServicedOrganisationRef></Holidays>"
    b"</DaysOfOperation>"
    b"<DaysOfNonOperation>"
    b"<WorkingDays><ServicedOrganisationRef>ORG2</ServicedOrganisationRef>"
    b"</WorkingDays>"
    b"</DaysOfNonOperation>"
    b"</ServicedOrganisationDayType>"
    b"<BankHolidayOperation>"
    b"<DaysOfOperation><ChristmasDay />"
    b"<OtherPublicHoliday><Description>Fair</Description><Date>2020-09-01</Date>"
    b"</OtherPublicHoliday>"
    b"</DaysOfOperation>"
    b"<DaysOfNonOperation><GoodFriday /><EasterMonday />"
    b"<OtherPublicHoliday><Description>Gala</Description><Date>2020-10-01</Date>"
    b"</OtherPublicHoliday>"
    b"</DaysOfNonOperation>"
    b"</BankHolidayOperation>"
    b"</OperatingProfile>"
)

SERVICE_PROFILE = (
    b"<OperatingProfile><RegularDayType><DaysOfWeek><MondayToFriday /></DaysOfWeek>\n"
    b"      </RegularDayType></OperatingProfile>"
)


def test_operating_profile_reads_every_day_type():
    xml = minimal().replace(SERVICE_PROFILE, FULL_PROFILE)
    profile = read_txc(xml).services[0].operating_profile
    assert profile.days_of_week is None
    assert profile.holidays_only is True
    assert profile.bank_holiday_days_of_operation == ["ChristmasDay"]
    assert profile.bank_holiday_days_of_non_operation == ["GoodFriday", "EasterMonday"]
    assert profile.other_public_holidays_of_operation == [("Fair", "2020-09-01")]
    assert profile.other_public_holidays_of_non_operation == [("Gala", "2020-10-01")]
    assert profile.special_days_of_operation == [
        ("2020-06-01", "2020-06-05"),
        ("2020-07-01", "2020-07-01"),
    ]
    assert profile.special_days_of_non_operation == [("2020-08-10", "2020-08-12")]
    assert profile.serviced_organisation_days_of_operation == [
        ("ORG1", "WorkingDays"),
        ("ORG2", "Holidays"),
    ]
    assert profile.serviced_organisation_days_of_non_operation == [
        ("ORG2", "WorkingDays")
    ]

    plain = read_txc(minimal()).services[0].operating_profile
    assert plain.holidays_only is False
    assert plain.bank_holiday_days_of_operation is None
    assert plain.other_public_holidays_of_operation == []
    assert plain.special_days_of_operation == []
    assert plain.serviced_organisation_days_of_non_operation == []


def test_serviced_organisations_are_read():
    organisations = (
        b"<ServicedOrganisations>"
        b"<ServicedOrganisation><OrganisationCode>ORG1</OrganisationCode>"
        b"<Name>School</Name>"
        b"<WorkingDays>"
        b"<DateRange><StartDate>2020-01-06</StartDate><EndDate>2020-02-14</EndDate>"
        b"</DateRange>"
        b"<DateRange><StartDate>2020-02-24</StartDate><EndDate>2020-04-03</EndDate>"
        b"</DateRange>"
        b"</WorkingDays>"
        b"<Holidays>"
        b"<DateRange><StartDate>2020-02-17</StartDate><EndDate>2020-02-21</EndDate>"
        b"</DateRange>"
        b"</Holidays>"
        b"</ServicedOrganisation>"
        b"<ServicedOrganisation><OrganisationCode>ORG2</OrganisationCode>"
        b"</ServicedOrganisation>"
        b"</ServicedOrganisations>"
        b"<Services>"
    )
    doc = read_txc(minimal().replace(b"<Services>", organisations))
    assert [o.code for o in doc.serviced_organisations] == ["ORG1", "ORG2"]
    school = doc.serviced_organisation("ORG1")
    assert school.name == "School"
    assert school.working_days == [
        ("2020-01-06", "2020-02-14"),
        ("2020-02-24", "2020-04-03"),
    ]
    assert school.holidays == [("2020-02-17", "2020-02-21")]
    other = doc.serviced_organisation("ORG2")
    assert (other.name, other.working_days, other.holidays) == (None, [], [])
    with pytest.raises(KeyError):
        doc.serviced_organisation("ORG9")
    assert read_txc(minimal()).serviced_organisations == []


VJ_A_TAIL = b"<JourneyPatternRef>JP_A</JourneyPatternRef>\n"


def test_vehicle_journey_reference_frequency_and_overrides():
    extra = (
        b"<VehicleJourneyRef>VJ_AB</VehicleJourneyRef>"
        b"<OperatorRef>OId_X</OperatorRef>"
        b"<Frequency><EndTime>10:00:00</EndTime>"
        b"<Interval><ScheduledFrequency>PT10M</ScheduledFrequency></Interval>"
        b"</Frequency>"
        b"<VehicleJourneyTimingLink>"
        b"<JourneyPatternTimingLinkRef>L_A</JourneyPatternTimingLinkRef>"
        b"<RunTime>PT6M</RunTime>"
        b"<From><WaitTime>PT1M</WaitTime></From>"
        b"<To><WaitTime>PT2M</WaitTime></To>"
        b"</VehicleJourneyTimingLink>"
        b"<VehicleJourneyTimingLink>"
        b"<JourneyPatternTimingLinkRef>L_B</JourneyPatternTimingLinkRef>"
        b"</VehicleJourneyTimingLink>"
    )
    doc = read_txc(minimal().replace(VJ_A_TAIL, VJ_A_TAIL + extra))
    journey = doc.vehicle_journey("VJ_A")
    assert journey.vehicle_journey_ref == "VJ_AB"
    assert journey.operator_ref == "OId_X"
    assert (journey.frequency.end_time, journey.frequency.interval) == (
        "10:00:00",
        "PT10M",
    )
    first, second = journey.timing_links
    assert first.journey_pattern_timing_link_ref == "L_A"
    assert (first.run_time, first.from_wait_time, first.to_wait_time) == (
        "PT6M",
        "PT1M",
        "PT2M",
    )
    assert second.journey_pattern_timing_link_ref == "L_B"
    assert (second.run_time, second.from_wait_time, second.to_wait_time) == (
        None,
        None,
        None,
    )

    plain = doc.vehicle_journey("VJ_AB")
    assert (plain.vehicle_journey_ref, plain.operator_ref) == (None, None)
    assert (plain.frequency, plain.timing_links) == (None, [])
    with pytest.raises(KeyError):
        doc.vehicle_journey("VJ_9")


def test_departure_time_is_optional_only_with_a_journey_reference():
    departure = b"<DepartureTime>08:00:00</DepartureTime>"
    without = minimal().replace(
        VJ_A_TAIL + b"      " + departure, VJ_A_TAIL + b"      "
    )
    assert without != minimal()
    with pytest.raises(ValueError, match="missing required element DepartureTime"):
        read_txc(without)

    ref = b"<VehicleJourneyRef>VJ_AB</VehicleJourneyRef>"
    doc = read_txc(without.replace(VJ_A_TAIL, VJ_A_TAIL + ref))
    assert doc.vehicle_journey("VJ_A").departure_time is None
