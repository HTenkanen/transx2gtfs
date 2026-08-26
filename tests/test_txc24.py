"""Tests for TransXChange 2.4/2.5 conversion, on documents built from parts."""

import warnings

import pytest

from transx2gtfs.agency import get_agency
from transx2gtfs.bank_holidays import detect_division
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


def bh(operation="", non_operation=""):
    return "<BankHolidayOperation>%s%s</BankHolidayOperation>" % (
        "<DaysOfOperation>%s</DaysOfOperation>" % operation if operation else "",
        (
            "<DaysOfNonOperation>%s</DaysOfNonOperation>" % non_operation
            if non_operation
            else ""
        ),
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


def exception_dates(calendar_dates, kind):
    if calendar_dates is None:
        return set()
    return set(calendar_dates.loc[calendar_dates["exception_type"] == kind, "date"])


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


def test_journey_without_days_runs_every_day_and_holidays_only_runs_on_holidays():
    t = tables(document(journeys=[journey("VJ_1", profile="")]))
    assert t["calendar"].iloc[0][["monday", "sunday"]].to_list() == [1, 1]
    assert t["trips"]["service_id"].to_list() == ["S1_20270104_20270331_MondayToSunday"]
    # the service profile is used when the journey has none
    t = tables(document(service_profile=profile("<Saturday />")))
    assert t["calendar"].iloc[0][["saturday", "sunday"]].to_list() == [1, 0]

    holidays_only = journey(
        "VJ_1",
        profile="<OperatingProfile><RegularDayType><HolidaysOnly /></RegularDayType>"
        "</OperatingProfile>",
    )
    t = tables(document(journeys=[holidays_only]))
    assert (
        t["calendar"]["service_id"]
        .iloc[0]
        .startswith("S1_20270104_20270331_HolidaysOnly_")
    )
    assert t["calendar"].iloc[0][["monday", "saturday"]].to_list() == [0, 0]
    # runs exactly on the bank holidays of the period (Good Friday, Easter Monday)
    assert exception_dates(t["calendar_dates"], 1) == {"20270326", "20270329"}

    weekend_and_holidays = journey(
        "VJ_1", profile=profile("<SaturdaySundayHolidaysOnly />")
    )
    t = tables(document(journeys=[weekend_and_holidays]))
    assert t["calendar"].iloc[0][["friday", "saturday", "sunday"]].to_list() == [
        0,
        1,
        1,
    ]
    assert exception_dates(t["calendar_dates"], 1) == {"20270326", "20270329"}

    # explicit removals win: HolidaysOnly minus all holidays never runs
    never = journey(
        "VJ_1",
        profile=profile(
            "<HolidaysOnly />", bank_holidays=bh(non_operation="<AllBankHolidays />")
        ),
    )
    assert tables(document(journeys=[never]))["calendar_dates"] is None


def test_bank_holiday_exceptions_and_precedence():
    # Good Friday 2027-03-26 (Friday, inside MondayToFriday) is removed; Easter
    # Monday 2027-03-29 likewise; a Saturday holiday would be added
    t = tables(
        document(
            journeys=[
                journey(
                    "VJ_1",
                    profile=profile(
                        bank_holidays=bh(non_operation="<GoodFriday /><EasterMonday />")
                    ),
                )
            ]
        )
    )
    assert exception_dates(t["calendar_dates"], 2) == {"20270326", "20270329"}
    assert (
        t["calendar"]["service_id"]
        .iloc[0]
        .startswith("S1_20270104_20270331_MondayToFriday_")
    )

    weekend = journey(
        "VJ_1",
        profile=profile(
            "<Weekend />", bank_holidays=bh(operation="<AllBankHolidays />")
        ),
    )
    t = tables(document(journeys=[weekend]))
    assert exception_dates(t["calendar_dates"], 1) == {"20270326", "20270329"}

    both = journey(
        "VJ_1",
        profile=profile(
            "<Weekend />",
            bank_holidays=bh(
                operation="<GoodFriday />", non_operation="<GoodFriday />"
            ),
        ),
    )
    assert (
        tables(document(journeys=[both]))["calendar_dates"] is None
    )  # cancellation wins

    other = journey(
        "VJ_1",
        profile=profile(
            "<Weekend />",
            bank_holidays=bh(
                operation=(
                    "<OtherPublicHoliday><Description>Local</Description>"
                    "<Date>2027-02-03</Date></OtherPublicHoliday>"
                )
            ),
        ),
    )
    assert exception_dates(tables(document(journeys=[other]))["calendar_dates"], 1) == {
        "20270203"
    }

    with pytest.warns(
        UserWarning, match="Did not recognize following holiday: Whitsun"
    ):
        tables(
            document(
                journeys=[
                    journey(
                        "VJ_1",
                        profile=profile(bank_holidays=bh(non_operation="<Whitsun />")),
                    )
                ]
            )
        )
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        # a valid name without a date in the period is fine
        tables(
            document(
                journeys=[
                    journey(
                        "VJ_1",
                        profile=profile(
                            bank_holidays=bh(non_operation="<ChristmasDayHoliday />")
                        ),
                    )
                ]
            )
        )


def test_journey_profile_inherits_bank_holidays_from_the_service():
    # the journey sets its days only; the service's bank-holiday rule still applies
    service_profile = profile(bank_holidays=bh(non_operation="<AllBankHolidays />"))
    t = tables(
        document(
            service_profile=service_profile,
            journeys=[journey("VJ_1", profile=profile("<MondayToSunday />"))],
        )
    )
    assert exception_dates(t["calendar_dates"], 2) == {"20270326", "20270329"}
    # each list is inherited on its own: a journey setting only DaysOfOperation
    # keeps the service's DaysOfNonOperation
    own = profile("<MondayToSunday />", bank_holidays=bh(operation="<GoodFriday />"))
    t = tables(
        document(
            service_profile=service_profile, journeys=[journey("VJ_1", profile=own)]
        )
    )
    assert exception_dates(t["calendar_dates"], 2) == {"20270326", "20270329"}
    # a journey setting both lists overrides the service's
    own = profile(
        "<MondayToSunday />",
        bank_holidays=bh(operation="<GoodFriday />", non_operation="<EasterMonday />"),
    )
    t = tables(
        document(
            service_profile=service_profile, journeys=[journey("VJ_1", profile=own)]
        )
    )
    assert exception_dates(t["calendar_dates"], 2) == {"20270329"}


def test_other_public_holidays_are_inherited_with_their_day_type():
    local = (
        "<OtherPublicHoliday><Description>Local</Description>"
        "<Date>2027-02-03</Date></OtherPublicHoliday>"
    )
    service_profile = profile(bank_holidays=bh(operation=local))
    t = tables(
        document(
            service_profile=service_profile,
            journeys=[journey("VJ_1", profile=profile("<Weekend />"))],
        )
    )
    assert exception_dates(t["calendar_dates"], 1) == {"20270203"}
    # a journey with its own DaysOfOperation does not inherit the service's dates
    own = profile("<Weekend />", bank_holidays=bh(operation="<GoodFriday />"))
    t = tables(
        document(
            service_profile=service_profile, journeys=[journey("VJ_1", profile=own)]
        )
    )
    assert exception_dates(t["calendar_dates"], 1) == {"20270326"}


def test_scottish_documents_use_the_scottish_calendar():
    scottish = document(links=(("639003662", "639003652", "PT5M"),))
    assert detect_division(read_txc(scottish)) == "scotland"
    assert detect_division(read_txc(document())) == "england-and-wales"
    # 2027-08-02 is the Scottish summer holiday (a Monday inside MondayToFriday)
    t = tables(
        document(
            links=(("639003662", "639003652", "PT5M"),),
            operating_period="<StartDate>2027-07-01</StartDate><EndDate>2027-09-30</EndDate>",
            journeys=[
                journey(
                    "VJ_1",
                    profile=profile(
                        bank_holidays=bh(non_operation="<AllBankHolidays />")
                    ),
                )
            ],
        )
    )
    assert exception_dates(t["calendar_dates"], 2) == {"20270802"}


def test_other_public_holidays_count_only_inside_the_period():
    outside = (
        "<OtherPublicHoliday><Description>Far</Description><Date>2028-02-03</Date>"
        "</OtherPublicHoliday>"
    )
    prof = profile("<Weekend />", bank_holidays=bh(operation=outside))
    assert (
        tables(document(journeys=[journey("VJ_1", profile=prof)]))["calendar_dates"]
        is None
    )


def test_service_ids_distinguish_calendars_within_a_service():
    plain = journey("VJ_1")
    with_exception = journey(
        "VJ_2",
        departure="09:00:00",
        profile=profile(
            "<MondayToSunday />", bank_holidays=bh(non_operation="<GoodFriday />")
        ),
    )
    t = tables(document(journeys=[plain, with_exception]))
    ids = t["trips"]["service_id"].to_list()
    assert ids[0] == "S1_20270104_20270331_MondayToSunday"
    assert (
        ids[1].startswith("S1_20270104_20270331_MondayToSunday_") and ids[1] != ids[0]
    )
    assert len(t["calendar"]) == 2
    assert set(t["calendar_dates"]["service_id"]) == {ids[1]}


def test_same_time_journeys_with_different_calendars_keep_separate_trips():
    plain = journey("VJ_1")
    school = journey(
        "VJ_2",
        profile=profile(
            "<MondayToSunday />", bank_holidays=bh(non_operation="<GoodFriday />")
        ),
    )
    t = tables(document(journeys=[plain, school]))
    assert len(t["trips"]) == 2 and t["trips"]["trip_id"].is_unique
    assert t["trips"]["trip_id"].iloc[0] == "JPS_1_MondayToSunday_0800"
    assert t["trips"]["trip_id"].iloc[1].startswith("JPS_1_MondayToSunday_0800_")
