"""Tests for TransXChange 2.4/2.5 conversion, on documents built from parts."""

import pytest

from transx2gtfs.agency import get_agency
from transx2gtfs.calendar import get_calendar, parse_active_days
from transx2gtfs.calendar_dates import get_calendar_dates
from transx2gtfs.routes import get_routes
from transx2gtfs.stop_times import get_stop_times
from transx2gtfs.transxchange import get_gtfs_info
from transx2gtfs.trips import get_trips
from transx2gtfs.txc import read_txc

STOPS = ["9300WAS1", "9300MIL2", "9300MIL1", "490007705N"]


def document(
    links=(("9300WAS1", "9300MIL2", "PT5M"), ("9300MIL2", "9300MIL1", "PT7M")),
    journeys=None,
    service_profile="",
    operating_period="<StartDate>2027-01-04</StartDate><EndDate>2027-03-31</EndDate>",
    routes=None,
    route_sections="",
    journey_patterns=None,
    serviced_organisations="",
    operator=(
        '<Operator id="OId_CV"><OperatorNameOnLicence>Op Ltd</OperatorNameOnLicence>'
        "</Operator>"
    ),
    lines='<Line id="L1"><LineName>1</LineName></Line>',
    mode="<Mode>bus</Mode>",
    root_attributes=(
        ' CreationDateTime="2027-01-10T10:00:00" '
        'ModificationDateTime="2027-02-01T10:00:00"'
    ),
):
    """A TransXChange 2.4 document from building blocks (all stops in the subset)."""
    stop_ids = sorted({s for link in links for s in link[:2]})
    stop_points = "".join(
        "<AnnotatedStopPointRef><StopPointRef>%s</StopPointRef>"
        "<CommonName>%s</CommonName>"
        "</AnnotatedStopPointRef>" % (s, s)
        for s in stop_ids
    )
    timing_links = ""
    for i, link in enumerate(links, start=1):
        from_stop, to_stop, run_time = link[:3]
        extra = link[3] if len(link) > 3 else {}
        from_wait = (
            "<WaitTime>%s</WaitTime>" % extra["from_wait"]
            if "from_wait" in extra
            else ""
        )
        to_wait = (
            "<WaitTime>%s</WaitTime>" % extra["to_wait"] if "to_wait" in extra else ""
        )
        route_link = extra.get("route_link", "RL_%d" % i)
        timing_links += (
            '<JourneyPatternTimingLink id="JPL_%d"><From>'
            "<StopPointRef>%s</StopPointRef>%s</From>"
            "<To><StopPointRef>%s</StopPointRef>%s</To><RouteLinkRef>%s</RouteLinkRef>"
            "<RunTime>%s</RunTime></JourneyPatternTimingLink>"
            % (i, from_stop, from_wait, to_stop, to_wait, route_link, run_time)
        )
    if routes is None:
        routes = (
            '<Route id="R_1"><PrivateCode>R_1</PrivateCode>'
            "<Description>A - B</Description>"
            "<RouteSectionRef>RS_1</RouteSectionRef></Route>"
        )
    if journey_patterns is None:
        journey_patterns = (
            '<JourneyPattern id="JP_1"><Direction>outbound</Direction>'
            "<RouteRef>R_1</RouteRef>"
            "<JourneyPatternSectionRefs>JPS_1</JourneyPatternSectionRefs>"
            "</JourneyPattern>"
        )
    if journeys is None:
        journeys = [journey("VJ_1")]
    return (
        '<?xml version="1.0" encoding="utf-8"?>'
        '<TransXChange xmlns="http://www.transxchange.org.uk/" SchemaVersion="2.4"%s>'
        "<StopPoints>%s</StopPoints><RouteSections>%s</RouteSections>"
        "<Routes>%s</Routes>"
        "<JourneyPatternSections>"
        '<JourneyPatternSection id="JPS_1">%s</JourneyPatternSection>'
        "</JourneyPatternSections><Operators>%s</Operators>"
        "<ServicedOrganisations>%s</ServicedOrganisations>"
        "<Services><Service><ServiceCode>S1</ServiceCode><Lines>%s</Lines>"
        "<OperatingPeriod>%s</OperatingPeriod>%s"
        "<RegisteredOperatorRef>OId_CV</RegisteredOperatorRef>%s<Description>A "
        "to B</Description>"
        "<StandardService><Origin>A</Origin>"
        "<Destination>B</Destination>%s</StandardService>"
        "</Service></Services><VehicleJourneys>%s</VehicleJourneys></TransXChange>"
        % (
            root_attributes,
            stop_points,
            route_sections,
            routes,
            timing_links,
            operator,
            serviced_organisations,
            lines,
            operating_period,
            service_profile,
            mode,
            journey_patterns,
            "".join(journeys),
        )
    ).encode()


def journey(
    code, departure="08:00:00", profile="", pattern="JP_1", extra="", line="L1"
):
    return (
        "<VehicleJourney>%s<VehicleJourneyCode>%s</VehicleJourneyCode>"
        "<ServiceRef>S1</ServiceRef>"
        "<LineRef>%s</LineRef>%s%s%s</VehicleJourney>"
        % (
            profile,
            code,
            line,
            "<JourneyPatternRef>%s</JourneyPatternRef>" % pattern if pattern else "",
            "<DepartureTime>%s</DepartureTime>" % departure if departure else "",
            extra,
        )
    )


def profile(days="<MondayToFriday />", bank_holidays="", special_days="", serviced=""):
    return (
        "<OperatingProfile><RegularDayType><DaysOfWeek>%s</DaysOfWeek></RegularDayType>"
        "%s%s%s</OperatingProfile>" % (days, special_days, serviced, bank_holidays)
    )


def tables(xml):
    doc = read_txc(xml)
    info = get_gtfs_info(doc)
    return dict(
        info=info,
        stop_times=get_stop_times(info),
        trips=get_trips(info),
        calendar=get_calendar(info),
        calendar_dates=get_calendar_dates(info),
        routes=get_routes(info, doc),
        agency=get_agency(doc),
        doc=doc,
    )


# Services ----------------------------------------------------------------------


def test_missing_end_date_defaults_to_a_year_after_the_latest_date():
    t = tables(document(operating_period="<StartDate>2027-01-04</StartDate>"))
    assert t["calendar"][["start_date", "end_date"]].to_dict("records") == [
        {
            "start_date": "20270104",
            "end_date": "20280201",
        }  # modification 2027-02-01 + 365 days
    ]
    t = tables(
        document(
            operating_period="<StartDate>2027-03-01</StartDate>", root_attributes=""
        )
    )
    assert t["calendar"]["end_date"].to_list() == ["20280229"]


@pytest.mark.parametrize(
    "direction, expected",
    [
        ("inbound", 0),
        ("outbound", 1),
        ("inboundAndOutbound", 0),
        ("circular", 0),
        ("clockwise", 0),
        ("antiClockwise", 0),
    ],
)
def test_direction_variants(direction, expected):
    pattern = (
        '<JourneyPattern id="JP_1"><Direction>%s</Direction><RouteRef>R_1</RouteRef>'
        "<JourneyPatternSectionRefs>JPS_1</JourneyPatternSectionRefs></JourneyPattern>"
        % direction
    )
    t = tables(document(journey_patterns=pattern))
    assert t["trips"]["direction_id"].to_list() == [expected]
    assert t["info"]["direction"].unique().tolist() == [direction]


def test_missing_mode_and_operator_names():
    with pytest.warns(UserWarning, match="no Mode, assuming bus"):
        t = tables(document(mode=""))
    assert t["routes"]["route_type"].to_list() == [3]
    t = tables(document(mode="<Mode>trolleyBus</Mode>"))
    assert t["routes"]["route_type"].to_list() == [11]
    with pytest.raises(ValueError, match="Unknown Mode 'hovercraft'"):
        tables(document(mode="<Mode>hovercraft</Mode>"))

    operator = (
        '<Operator id="OId_CV"><OperatorCode>CV</OperatorCode>'
        "<OperatorShortName>Short</OperatorShortName>"
        "<TradingName>Trade</TradingName></Operator>"
    )
    assert tables(document(operator=operator))["agency"]["agency_name"].to_list() == [
        "Trade"
    ]
    operator = (
        '<LicensedOperator id="OId_CV"><OperatorCode>CV</OperatorCode>'
        "</LicensedOperator>"
    )
    assert tables(document(operator=operator))["agency"]["agency_name"].to_list() == [
        "CV"
    ]


def test_every_operator_gets_an_agency_row():
    operators = (
        '<Operator id="OId_CV"><OperatorNameOnLicence>Op Ltd</OperatorNameOnLicence>'
        '</Operator><Operator id="OId_2"><OperatorShortName>Two</OperatorShortName>'
        "</Operator>"
    )
    agency = tables(document(operator=operators))["agency"]
    assert agency["agency_id"].to_list() == ["OId_CV", "OId_2"]
    assert agency["agency_name"].to_list() == ["Op Ltd", "Two"]
    with pytest.raises(ValueError, match="Operator 'OId_2' does not have a name"):
        tables(document(operator='<Operator id="OId_2" />'))


def test_service_required_elements_still_raise():
    cases = (
        ("<LineName>1</LineName>", "Line 'L1' LineName", "outbound"),
        ("<Origin>A</Origin>", "Origin", "inbound"),
        ("<Destination>B</Destination>", "Destination", "outbound"),
    )
    for element, message, direction in cases:
        broken = document().replace(element.encode(), b"")
        broken = broken.replace(
            b"<Direction>outbound</Direction>",
            b"<Direction>%s</Direction>" % direction.encode(),
        )
        with pytest.raises(ValueError, match="Service 'S1' %s is missing" % message):
            tables(broken)
    lines = '<Line id="L1"><LineName>1</LineName></Line><Line id="L2"></Line>'
    with pytest.raises(ValueError, match="Line 'L2' LineName is missing"):
        tables(document(lines=lines))
    # the service Description is optional (BODS files omit it)
    info = tables(document().replace(b"<Description>A to B</Description>", b""))["info"]
    assert len(info) > 0


# Calendars ------------------------------------------------------------------------


@pytest.mark.parametrize(
    "dayinfo, active",
    [
        ("MondayToSaturday", "1111110"),
        ("MondayToSunday", "1111111"),
        ("Weekend", "0000011"),
        ("HolidaysOnly", "0000000"),
        ("SaturdaySundayHolidaysOnly", "0000011"),
        ("NotSaturday", "1111101"),
        ("NotSaturday|NotSunday", "1111100"),
        ("Monday|Wednesday", "1010000"),
        ("TuesdayToThursday", "0111000"),
        (None, "1111111"),
        ("", "1111111"),
    ],
)
def test_day_patterns(dayinfo, active):
    assert "".join(str(v) for v in parse_active_days(dayinfo).values()) == active


@pytest.mark.parametrize("day_index", range(7))
def test_every_not_weekday_keyword(day_index):
    from transx2gtfs.calendar import WEEKDAYS

    keyword = "Not" + WEEKDAYS[day_index].capitalize()
    expected = [0 if i == day_index else 1 for i in range(7)]
    assert list(parse_active_days(keyword).values()) == expected


def test_unknown_day_pattern_raises():
    with pytest.raises(ValueError, match="Unknown DaysOfWeek value 'Someday'"):
        parse_active_days("Someday")


def test_journey_without_days_runs_every_day():
    t = tables(document(journeys=[journey("VJ_1", profile="")]))
    assert t["calendar"].iloc[0][["monday", "sunday"]].to_list() == [1, 1]
    assert t["trips"]["service_id"].to_list() == ["S1_20270104_20270331_MondayToSunday"]
    # the service profile is used when the journey has none
    t = tables(document(service_profile=profile("<Saturday />")))
    assert t["calendar"].iloc[0][["saturday", "sunday"]].to_list() == [1, 0]
    # HolidaysOnly is a day pattern without weekdays
    t = tables(
        document(
            journeys=[
                journey(
                    "VJ_1",
                    profile="<OperatingProfile><RegularDayType><HolidaysOnly />"
                    "</RegularDayType></OperatingProfile>",
                )
            ]
        )
    )
    assert t["calendar"].iloc[0][["monday", "sunday"]].to_list() == [0, 0]
    assert t["trips"]["service_id"].to_list() == ["S1_20270104_20270331_HolidaysOnly"]
