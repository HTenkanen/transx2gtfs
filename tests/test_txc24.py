"""Tests for TransXChange 2.4/2.5 conversion, on documents built from parts."""

import warnings

import pytest

from transx2gtfs.agency import get_agency
from transx2gtfs.bank_holidays import detect_division
from transx2gtfs.calendar import get_calendar, parse_active_days
from transx2gtfs.calendar_dates import get_calendar_dates
from transx2gtfs.routes import get_routes
from transx2gtfs.stop_times import get_frequencies, get_stop_times
from transx2gtfs.transxchange import (
    GTFS_INFO_COLUMNS,
    get_gtfs_info,
    parse_runtime_duration,
)
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


def special(operation="", non_operation=""):
    def ranges(items):
        return "".join(
            "<DateRange><StartDate>%s</StartDate><EndDate>%s</EndDate></DateRange>" % r
            for r in items
        )

    return "<SpecialDaysOperation>%s%s</SpecialDaysOperation>" % (
        (
            "<DaysOfOperation>%s</DaysOfOperation>" % ranges(operation)
            if operation
            else ""
        ),
        (
            "<DaysOfNonOperation>%s</DaysOfNonOperation>" % ranges(non_operation)
            if non_operation
            else ""
        ),
    )


ORGANISATIONS = (
    "<ServicedOrganisation><OrganisationCode>SCH</OrganisationCode><Name>School</Name>"
    "<WorkingDays><DateRange><StartDate>2027-01-04</StartDate>"
    "<EndDate>2027-02-12</EndDate></DateRange>"
    "<DateRange><StartDate>2027-02-22</StartDate>"
    "<EndDate>2027-03-26</EndDate></DateRange></WorkingDays>"
    "<Holidays><DateRange><StartDate>2027-02-15</StartDate>"
    "<EndDate>2027-02-19</EndDate></DateRange></Holidays>"
    "</ServicedOrganisation>"
    "<ServicedOrganisation><OrganisationCode>COL</OrganisationCode><Name>College</Name>"
    "<WorkingDays><DateRange><StartDate>2027-03-29</StartDate>"
    "<EndDate>2027-03-31</EndDate></DateRange></WorkingDays>"
    "</ServicedOrganisation>"
)


def serviced(operation=(), non_operation=()):
    def refs(items):
        return "".join(
            "<%s><ServicedOrganisationRef>%s</ServicedOrganisationRef></%s>"
            % (kind, code, kind)
            for code, kind in items
        )

    return "<ServicedOrganisationDayType>%s%s</ServicedOrganisationDayType>" % (
        "<DaysOfOperation>%s</DaysOfOperation>" % refs(operation) if operation else "",
        (
            "<DaysOfNonOperation>%s</DaysOfNonOperation>" % refs(non_operation)
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
        frequencies=get_frequencies(info),
        agency=get_agency(doc),
        doc=doc,
    )


ROUTE_SECTIONS = (
    '<RouteSection id="RS_1"><RouteLink id="RL_1">'
    "<Distance>1000</Distance></RouteLink></RouteSection>"
    '<RouteSection id="RS_2"><RouteLink id="RL_2">'
    "<Distance>3000</Distance></RouteLink></RouteSection>"
)


def arrivals(t):
    return t["stop_times"][["arrival_time", "departure_time"]].values.tolist()


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
    with pytest.warns(UserWarning, match="'VJ_1' never operates"):
        t = tables(document(journeys=[never, journey("VJ_2", departure="09:00:00")]))
    assert t["trips"]["trip_id"].to_list() == ["JPS_1_MondayToSunday_0900"]


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


# Special days and serviced organisations -----------------------------------------


def test_special_days_trim_exclude_add_and_remove():
    # non-operation at the start and end shortens the period, in the middle it excludes
    prof = profile(
        special_days=special(
            non_operation=[
                ("2027-01-01", "2027-01-08"),
                ("2027-02-01", "2027-02-02"),
                ("2027-03-25", "2027-04-10"),
            ]
        )
    )
    t = tables(document(journeys=[journey("VJ_1", profile=prof)]))
    assert t["calendar"][["start_date", "end_date"]].values.tolist() == [
        ["20270109", "20270324"]
    ]
    assert exception_dates(t["calendar_dates"], 2) == {"20270201", "20270202"}

    prof = profile(
        special_days=special(operation=[("2027-01-09", "2027-01-10")])
    )  # a weekend
    t = tables(document(journeys=[journey("VJ_1", profile=prof)]))
    assert exception_dates(t["calendar_dates"], 1) == {"20270109", "20270110"}

    prof = profile(special_days=special(non_operation=[("2026-12-01", "2027-04-30")]))
    with pytest.warns(UserWarning, match="never operates"):
        t = tables(
            document(
                journeys=[
                    journey("VJ_1", profile=prof),
                    journey("VJ_2", departure="09:00:00"),
                ]
            )
        )
    assert len(t["trips"]) == 1


def test_huge_date_ranges_are_bounded_to_the_period():
    prof = profile(
        special_days=special(
            non_operation=[("0001-01-01", "2027-01-06")],
            operation=[("2027-03-27", "9999-12-31")],
        )
    )
    t = tables(document(journeys=[journey("VJ_1", profile=prof)]))
    assert t["calendar"]["start_date"].to_list() == ["20270107"]
    assert exception_dates(t["calendar_dates"], 1) == {"20270327", "20270328"}


def test_only_special_days_trim_the_period_edges():
    # New Year's Day 2027 is the first Friday of a period starting 2027-01-01
    prof = profile(bank_holidays=bh(non_operation="<NewYearsDay />"))
    t = tables(
        document(
            operating_period="<StartDate>2027-01-01</StartDate><EndDate>2027-01-31</EndDate>",
            journeys=[journey("VJ_1", profile=prof)],
        )
    )
    assert t["calendar"]["start_date"].to_list() == ["20270101"]
    assert exception_dates(t["calendar_dates"], 2) == {"20270101"}


def test_serviced_organisation_restriction_union_and_exclusion():
    prof = profile(serviced=serviced(operation=[("SCH", "WorkingDays")]))
    t = tables(
        document(
            serviced_organisations=ORGANISATIONS,
            journeys=[journey("VJ_1", profile=prof)],
        )
    )
    assert t["calendar"][["start_date", "end_date"]].values.tolist() == [
        ["20270104", "20270326"]
    ]
    # the half-term week (15-19 Feb) is outside the working days: excluded
    assert exception_dates(t["calendar_dates"], 2) == {
        "20270215",
        "20270216",
        "20270217",
        "20270218",
        "20270219",
    }

    prof = profile(
        serviced=serviced(operation=[("SCH", "WorkingDays"), ("COL", "WorkingDays")])
    )
    t = tables(
        document(
            serviced_organisations=ORGANISATIONS,
            journeys=[journey("VJ_1", profile=prof)],
        )
    )
    assert t["calendar"]["end_date"].to_list() == [
        "20270331"
    ]  # union extends to the college dates
    assert "20270329" not in exception_dates(t["calendar_dates"], 2)

    prof = profile(
        serviced=serviced(operation=[("SCH", "WorkingDays"), ("SCH", "Holidays")])
    )
    t = tables(
        document(
            serviced_organisations=ORGANISATIONS,
            journeys=[journey("VJ_1", profile=prof)],
        )
    )
    assert t["calendar_dates"] is None  # working days plus holidays cover every weekday

    prof = profile(serviced=serviced(non_operation=[("SCH", "Holidays")]))
    t = tables(
        document(
            serviced_organisations=ORGANISATIONS,
            journeys=[journey("VJ_1", profile=prof)],
        )
    )
    assert exception_dates(t["calendar_dates"], 2) == {
        "20270215",
        "20270216",
        "20270217",
        "20270218",
        "20270219",
    }

    with pytest.raises(ValueError, match="Unknown ServicedOrganisation 'NOPE'"):
        tables(
            document(
                serviced_organisations=ORGANISATIONS,
                journeys=[
                    journey(
                        "VJ_1",
                        profile=profile(
                            serviced=serviced(operation=[("NOPE", "WorkingDays")])
                        ),
                    )
                ],
            )
        )


def test_explicit_addition_overrides_an_implied_gap_but_not_a_removal():
    # 2027-02-17 (Wednesday) is a half-term gap; adding it explicitly keeps it
    prof = profile(
        serviced=serviced(operation=[("SCH", "WorkingDays")]),
        special_days=special(operation=[("2027-02-17", "2027-02-17")]),
    )
    t = tables(
        document(
            serviced_organisations=ORGANISATIONS,
            journeys=[journey("VJ_1", profile=prof)],
        )
    )
    assert "20270217" not in exception_dates(t["calendar_dates"], 2)
    assert "20270217" not in exception_dates(
        t["calendar_dates"], 1
    )  # inside the weekday pattern: no row
    # an explicit removal on the same date wins
    prof = profile(
        special_days=special(
            operation=[("2027-02-17", "2027-02-17")],
            non_operation=[("2027-02-17", "2027-02-17")],
        )
    )
    t = tables(document(journeys=[journey("VJ_1", profile=prof)]))
    assert exception_dates(t["calendar_dates"], 2) == {"20270217"}


def test_same_time_journeys_with_different_calendar_bounds_keep_separate_trips():
    plain = journey("VJ_1")
    clipped = journey(
        "VJ_2", profile=profile(serviced=serviced(operation=[("COL", "WorkingDays")]))
    )
    t = tables(
        document(serviced_organisations=ORGANISATIONS, journeys=[plain, clipped])
    )
    assert t["calendar_dates"] is None  # the college dates need no exception rows
    assert len(t["trips"]) == 2 and t["trips"]["trip_id"].is_unique
    assert sorted(t["calendar"]["end_date"]) == ["20270331", "20270331"]
    assert sorted(t["calendar"]["start_date"]) == ["20270104", "20270329"]


def test_document_whose_journeys_never_operate_yields_an_empty_table():
    prof = profile(special_days=special(non_operation=[("2026-12-01", "2027-04-30")]))
    with pytest.warns(UserWarning, match="no vehicle journey operates"):
        info = get_gtfs_info(
            read_txc(document(journeys=[journey("VJ_1", profile=prof)]))
        )
    assert len(info) == 0 and list(info.columns) == GTFS_INFO_COLUMNS
    assert len(get_stop_times(info)) == 0
    assert get_calendar_dates(info) is None and get_frequencies(info) is None


def test_journeys_whose_every_operating_day_is_removed_never_operate():
    # Wednesdays only, every Wednesday of the period removed
    wednesdays = [
        ("2027-01-06", "2027-01-06"),
        ("2027-01-13", "2027-01-13"),
        ("2027-01-20", "2027-01-20"),
    ]
    prof = profile("<Wednesday />", special_days=special(non_operation=wednesdays))
    period = "<StartDate>2027-01-04</StartDate><EndDate>2027-01-24</EndDate>"
    with pytest.warns(UserWarning, match="'VJ_1' never operates"):
        t = tables(
            document(
                operating_period=period,
                journeys=[journey("VJ_1", profile=prof), journey("VJ_2")],
            )
        )
    assert len(t["trips"]) == 1
    # an explicit addition keeps the journey alive
    prof = profile(
        "<Wednesday />",
        special_days=special(
            non_operation=wednesdays, operation=[("2027-01-10", "2027-01-10")]
        ),
    )
    t = tables(
        document(operating_period=period, journeys=[journey("VJ_1", profile=prof)])
    )
    assert exception_dates(t["calendar_dates"], 1) == {"20270110"}
    assert exception_dates(t["calendar_dates"], 2) == {
        "20270106",
        "20270113",
        "20270120",
    }
    assert t["calendar"][["start_date", "end_date"]].values.tolist() == [
        ["20270104", "20270124"]
    ]


def test_explicit_additions_outside_the_organisation_calendar_are_kept():
    # the college runs 29-31 March; a Saturday in January is added explicitly
    prof = profile(
        serviced=serviced(operation=[("COL", "WorkingDays")]),
        special_days=special(operation=[("2027-01-09", "2027-01-09")]),
    )
    t = tables(
        document(
            serviced_organisations=ORGANISATIONS,
            journeys=[journey("VJ_1", profile=prof)],
        )
    )
    assert t["calendar"][["start_date", "end_date"]].values.tolist() == [
        ["20270329", "20270331"]
    ]
    assert exception_dates(t["calendar_dates"], 1) == {"20270109"}
    assert exception_dates(t["calendar_dates"], 2) == set()


def test_organisation_without_dates_in_the_period_keeps_explicit_additions():
    prof = profile(serviced=serviced(operation=[("COL", "WorkingDays")]))
    period = "<StartDate>2027-01-04</StartDate><EndDate>2027-01-31</EndDate>"
    with pytest.warns(UserWarning, match="'VJ_1' never operates"):
        t = tables(
            document(
                serviced_organisations=ORGANISATIONS,
                operating_period=period,
                journeys=[journey("VJ_1", profile=prof), journey("VJ_2")],
            )
        )
    assert len(t["trips"]) == 1
    prof = profile(
        serviced=serviced(operation=[("COL", "WorkingDays")]),
        special_days=special(operation=[("2027-01-09", "2027-01-09")]),
    )
    t = tables(
        document(
            serviced_organisations=ORGANISATIONS,
            operating_period=period,
            journeys=[journey("VJ_1", profile=prof)],
        )
    )
    assert exception_dates(t["calendar_dates"], 1) == {"20270109"}
    # every weekday of the period is outside the college dates
    assert len(exception_dates(t["calendar_dates"], 2)) == 20


def test_special_days_and_organisations_are_inherited_from_the_service():
    service_profile = profile(
        special_days=special(non_operation=[("2027-02-01", "2027-02-02")]),
        serviced=serviced(non_operation=[("SCH", "Holidays")]),
    )
    t = tables(
        document(
            serviced_organisations=ORGANISATIONS,
            service_profile=service_profile,
            journeys=[journey("VJ_1", profile=profile("<MondayToSunday />"))],
        )
    )
    assert exception_dates(t["calendar_dates"], 2) == {
        "20270201",
        "20270202",
        "20270215",
        "20270216",
        "20270217",
        "20270218",
        "20270219",
    }
    # a journey with its own special days keeps the service's organisation rule only
    own = profile(
        "<MondayToSunday />",
        special_days=special(non_operation=[("2027-03-01", "2027-03-01")]),
    )
    t = tables(
        document(
            serviced_organisations=ORGANISATIONS,
            service_profile=service_profile,
            journeys=[journey("VJ_1", profile=own)],
        )
    )
    assert "20270201" not in exception_dates(t["calendar_dates"], 2)
    assert {"20270301", "20270215"} <= exception_dates(t["calendar_dates"], 2)


def test_periods_at_the_edge_of_the_calendar_do_not_overflow():
    period = "<StartDate>9999-12-31</StartDate><EndDate>9999-12-31</EndDate>"
    prof = profile(
        "<MondayToSunday />",
        special_days=special(non_operation=[("9999-12-31", "9999-12-31")]),
    )
    with pytest.warns(UserWarning, match="'VJ_1' never operates"):
        t = tables(
            document(
                operating_period=period,
                root_attributes="",
                journeys=[journey("VJ_1", profile=prof), journey("VJ_2")],
            )
        )
    assert t["calendar"]["start_date"].to_list() == ["99991231"]


def test_daterange_reaches_the_last_representable_date():
    from datetime import date

    from transx2gtfs.bank_holidays import daterange

    assert list(daterange(date(9999, 12, 30), date.max)) == [
        date(9999, 12, 30),
        date.max,
    ]
    assert list(daterange(date(2027, 1, 2), date(2027, 1, 1))) == []


# Timing ---------------------------------------------------------------------------


def test_wait_times_and_journey_timing_link_overrides():
    links = (
        ("9300WAS1", "9300MIL2", "PT5M", {"from_wait": "PT1M"}),
        ("9300MIL2", "9300MIL1", "PT7M", {"to_wait": "PT2M", "from_wait": "PT1M"}),
        ("9300MIL1", "490007705N", "PT3M"),
    )
    t = tables(document(links=links))
    assert arrivals(t) == [
        ["08:00:00", "08:00:00"],
        ["08:05:00", "08:06:00"],  # From wait of link 2
        ["08:13:00", "08:15:00"],  # To wait of link 2
        ["08:18:00", "08:18:00"],
    ]
    override = (
        "<VehicleJourneyTimingLink>"
        "<JourneyPatternTimingLinkRef>JPL_2</JourneyPatternTimingLinkRef>"
        "<RunTime>PT9M</RunTime><To><WaitTime>PT0M</WaitTime></To>"
        "</VehicleJourneyTimingLink>"
    )
    t = tables(document(links=links, journeys=[journey("VJ_1", extra=override)]))
    assert arrivals(t)[2] == ["08:15:00", "08:15:00"]
    # the journey departs its first stop at DepartureTime: a From wait on the
    # first link does not delay it
    links = (("9300WAS1", "9300MIL2", "PT5M", {"from_wait": "PT2M"}),)
    assert arrivals(tables(document(links=links))) == [
        ["08:00:00", "08:00:00"],
        ["08:05:00", "08:05:00"],
    ]
    # each stop carries the link leaving it, the last stop the link reaching it
    links = (
        ("9300WAS1", "9300MIL2", "PT5M"),
        ("9300MIL2", "9300MIL1", "PT7M"),
        ("9300MIL1", "490007705N", "PT3M"),
    )
    info = tables(document(links=links))["info"]
    assert info["route_link_ref"].to_list() == ["RL_1", "RL_2", "RL_3", "RL_3"]
    # rows are the From stops of the links (then the last To stop), even when
    # adjacent links disagree about the stop between them
    links = (("9300WAS1", "9300MIL2", "PT5M"), ("9300MIL1", "490007705N", "PT3M"))
    info = tables(document(links=links))["info"]
    assert info["stop_id"].to_list() == ["9300WAS1", "9300MIL1", "490007705N"]


def test_same_minute_journeys_with_different_seconds_keep_separate_trips():
    t = tables(
        document(journeys=[journey("VJ_1"), journey("VJ_2", departure="08:00:30")])
    )
    assert t["trips"]["trip_id"].iloc[0] == "JPS_1_MondayToSunday_0800"
    assert t["trips"]["trip_id"].iloc[1].startswith("JPS_1_MondayToSunday_0800_")
    assert t["trips"]["trip_id"].is_unique
    first = t["stop_times"].groupby("trip_id")["departure_time"].first()
    assert sorted(first) == ["08:00:00", "08:00:30"]


def test_trips_crossing_midnight_keep_counting_hours():
    links = (("9300WAS1", "9300MIL2", "PT1H30M"),)
    t = tables(document(links=links, journeys=[journey("VJ_1", departure="23:15:00")]))
    assert arrivals(t) == [["23:15:00", "23:15:00"], ["24:45:00", "24:45:00"]]


def test_departure_times_are_parsed_strictly():
    from transx2gtfs.transxchange import parse_time

    assert [parse_time(v) for v in ("08:00", "08:00:30", "24:00:00", "08:00:29.5")] == [
        28800,
        28830,
        86400,
        28830,
    ]
    for value in ("8", "8:00", "08:90:00", "08:00:60", "25:00:00", "08:00:00:x", ""):
        with pytest.raises(ValueError, match="Not a time"):
            parse_time(value)
    with pytest.raises(ValueError, match="Not a time: '08:00:00:00'"):
        tables(document(journeys=[journey("VJ_1", departure="08:00:00:00")]))


def test_runtime_durations():
    values = ("PT0S", "PT1H2M3S", "P1DT1H", None, "")
    assert [parse_runtime_duration(v) for v in values] == [0, 3723, 90000, 0, 0]
    with pytest.warns(UserWarning, match="Negative duration '-PT5M'"):
        assert parse_runtime_duration("-PT5M") == 300


def test_fractional_and_invalid_durations():
    assert parse_runtime_duration("PT0.5H") == 1800
    assert parse_runtime_duration("PT1.5M") == 90
    assert parse_runtime_duration("PT2.4S") == 2
    assert parse_runtime_duration("P0.5D") == 43200
    # rounded once, exactly: 30 digits just below half a second stay 0
    assert parse_runtime_duration("PT0.499999999999999999999999999999S") == 0
    assert parse_runtime_duration("PT0.5S") == 1
    assert parse_runtime_duration("PT2.5S") == 3
    invalid = (
        "10 minutes",
        "PT",
        "P",
        "-P",
        "-PT",
        "P1DT",
        "PT5",
        "PT1.5H30M",  # a fraction only on the lowest-order component
        "P0.5DT1H",
    )
    for value in invalid:
        with pytest.raises(ValueError, match="Not an ISO-8601 duration"):
            parse_runtime_duration(value)


def test_same_time_journeys_with_overrides_keep_separate_trips():
    override = (
        "<VehicleJourneyTimingLink>"
        "<JourneyPatternTimingLinkRef>JPL_2</JourneyPatternTimingLinkRef>"
        "<RunTime>PT9M</RunTime></VehicleJourneyTimingLink>"
    )
    t = tables(document(journeys=[journey("VJ_1"), journey("VJ_2", extra=override)]))
    assert len(t["trips"]) == 2 and t["trips"]["trip_id"].is_unique
    assert t["trips"]["trip_id"].iloc[0] == "JPS_1_MondayToSunday_0800"
    assert t["trips"]["trip_id"].iloc[1].startswith("JPS_1_MondayToSunday_0800_")
    by_trip = t["stop_times"].groupby("trip_id")["arrival_time"].last()
    assert sorted(by_trip) == ["08:12:00", "08:14:00"]


# Interpolation --------------------------------------------------------------------


def test_interpolation_equal_and_by_distance():
    links = (("9300WAS1", "9300MIL2", "PT0S"), ("9300MIL2", "9300MIL1", "PT10M"))
    t = tables(document(links=links))
    assert [a for a, _ in arrivals(t)] == ["08:00:00", "08:05:00", "08:10:00"]
    t = tables(document(links=links, route_sections=ROUTE_SECTIONS))
    assert [a for a, _ in arrivals(t)] == ["08:00:00", "08:02:30", "08:10:00"]
    # several zero links in a row share the run
    links = (
        ("9300WAS1", "9300MIL2", "PT0S"),
        ("9300MIL2", "9300MIL1", "PT0S"),
        ("9300MIL1", "490007705N", "PT9M"),
    )
    t = tables(document(links=links))
    assert [a for a, _ in arrivals(t)] == [
        "08:00:00",
        "08:03:00",
        "08:06:00",
        "08:09:00",
    ]


def test_interpolation_keeps_times_monotonic_with_waits_and_trailing_zero_run():
    links = (
        ("9300WAS1", "9300MIL2", "PT0S", {"to_wait": "PT1M"}),
        ("9300MIL2", "9300MIL1", "PT10M"),
        ("9300MIL1", "490007705N", "PT0S"),
    )
    t = tables(document(links=links))
    assert arrivals(t) == [
        ["08:00:00", "08:00:00"],
        ["08:05:00", "08:06:00"],  # the dwell at B is added to the 10-minute run
        ["08:11:00", "08:11:00"],
        ["08:11:00", "08:11:00"],  # trailing zero run keeps the anchor time
    ]


def test_non_finite_distances_fall_back_to_equal_spacing():
    sections = (
        '<RouteSection id="RS_1"><RouteLink id="RL_1"><Distance>1e400</Distance>'
        "</RouteLink></RouteSection>"
        '<RouteSection id="RS_2"><RouteLink id="RL_2"><Distance>3000</Distance>'
        "</RouteLink></RouteSection>"
    )
    links = (("9300WAS1", "9300MIL2", "PT0S"), ("9300MIL2", "9300MIL1", "PT10M"))
    t = tables(document(links=links, route_sections=sections))
    assert [a for a, _ in arrivals(t)] == ["08:00:00", "08:05:00", "08:10:00"]
    # very large finite distances (whose sum overflows) are weighed relatively
    sections = (
        '<RouteSection id="RS_1"><RouteLink id="RL_1"><Distance>1e308</Distance>'
        "</RouteLink></RouteSection>"
        '<RouteSection id="RS_2"><RouteLink id="RL_2"><Distance>1.5e308</Distance>'
        "</RouteLink></RouteSection>"
    )
    t = tables(document(links=links, route_sections=sections))
    assert [a for a, _ in arrivals(t)] == ["08:00:00", "08:04:00", "08:10:00"]
    # a missing or zero distance on one link of the run likewise
    sections = (
        '<RouteSection id="RS_1"><RouteLink id="RL_1"><Distance>0</Distance>'
        "</RouteLink></RouteSection>"
    )
    t = tables(document(links=links, route_sections=sections))
    assert [a for a, _ in arrivals(t)] == ["08:00:00", "08:05:00", "08:10:00"]


# Journey references ---------------------------------------------------------------


def test_vehicle_journey_ref_inherits_from_the_referenced_journey():
    journeys = [
        journey("VJ_1", profile=profile("<Saturday />")),
        journey(
            "VJ_2",
            departure="09:30:00",
            pattern=None,
            extra="<VehicleJourneyRef>VJ_1</VehicleJourneyRef>",
        ),
    ]
    t = tables(document(journeys=journeys))
    by_trip = t["stop_times"].groupby("trip_id")["arrival_time"].first()
    assert by_trip.to_dict() == {
        "JPS_1_Saturday_0800": "08:00:00",
        "JPS_1_Saturday_0930": "09:30:00",
    }
    assert set(t["trips"]["service_id"]) == {"S1_20270104_20270331_Saturday"}
    # what the referencing journey sets itself wins
    journeys[1] = journey(
        "VJ_2",
        departure="09:30:00",
        pattern=None,
        profile=profile("<Sunday />"),
        extra="<VehicleJourneyRef>VJ_1</VehicleJourneyRef>",
    )
    t = tables(document(journeys=journeys))
    assert sorted(t["trips"]["service_id"]) == [
        "S1_20270104_20270331_Saturday",
        "S1_20270104_20270331_Sunday",
    ]


def test_vehicle_journey_ref_without_departure_time_and_cycles():
    inherit_all = journey(
        "VJ_2",
        departure=None,
        pattern=None,
        extra="<VehicleJourneyRef>VJ_1</VehicleJourneyRef>",
    )
    t = tables(document(journeys=[journey("VJ_1"), inherit_all]))
    assert len(t["trips"]) == 1  # same pattern, days and time: one trip

    cycle = [
        journey(
            "VJ_1",
            departure=None,
            pattern=None,
            extra="<VehicleJourneyRef>VJ_2</VehicleJourneyRef>",
        ),
        journey(
            "VJ_2",
            departure=None,
            pattern=None,
            extra="<VehicleJourneyRef>VJ_1</VehicleJourneyRef>",
        ),
    ]
    with pytest.raises(ValueError, match="circular VehicleJourneyRef"):
        tables(document(journeys=cycle))

    unknown = journey(
        "VJ_1",
        departure=None,
        pattern=None,
        extra="<VehicleJourneyRef>VJ_9</VehicleJourneyRef>",
    )
    with pytest.raises(ValueError, match="unknown VehicleJourney 'VJ_9'"):
        tables(document(journeys=[unknown]))

    with pytest.raises(ValueError, match="missing required element DepartureTime"):
        read_txc(document(journeys=[journey("VJ_1", departure=None)]))
    # a reference chain that never provides a pattern or a departure time
    no_pattern = journey("VJ_1", pattern=None)
    with pytest.raises(ValueError, match="'VJ_1' has no JourneyPatternRef"):
        tables(document(journeys=[no_pattern]))
    chain = [
        journey(
            "VJ_1", departure=None, extra="<VehicleJourneyRef>VJ_2</VehicleJourneyRef>"
        ),
        journey(
            "VJ_2", departure=None, extra="<VehicleJourneyRef>VJ_3</VehicleJourneyRef>"
        ),
        journey(
            "VJ_3", departure=None, extra="<VehicleJourneyRef>VJ_1</VehicleJourneyRef>"
        ),
    ]
    with pytest.raises(ValueError, match="circular VehicleJourneyRef"):
        tables(document(journeys=chain))


def test_timing_link_overrides_are_merged_per_link_along_references():
    def override(ref, run=None, to_wait=None):
        return (
            "<VehicleJourneyTimingLink>"
            "<JourneyPatternTimingLinkRef>%s</JourneyPatternTimingLinkRef>%s%s"
            "</VehicleJourneyTimingLink>"
            % (
                ref,
                "<RunTime>%s</RunTime>" % run if run else "",
                "<To><WaitTime>%s</WaitTime></To>" % to_wait if to_wait else "",
            )
        )

    parent = journey("VJ_1", extra=override("JPL_1", run="PT8M", to_wait="PT1M"))
    # the child overrides link 2 only: link 1 keeps the parent's override
    child = journey(
        "VJ_2",
        departure="09:00:00",
        pattern=None,
        extra="<VehicleJourneyRef>VJ_1</VehicleJourneyRef>" + override("JPL_2", "PT9M"),
    )
    t = tables(document(journeys=[parent, child]))
    by_trip = t["stop_times"].groupby("trip_id")["arrival_time"].apply(list)
    assert sorted(by_trip.to_list()) == [
        ["08:00:00", "08:08:00", "08:16:00"],
        ["09:00:00", "09:08:00", "09:18:00"],
    ]
    # a partial override of the same link inherits the parent's other fields
    child = journey(
        "VJ_2",
        departure="09:00:00",
        pattern=None,
        extra="<VehicleJourneyRef>VJ_1</VehicleJourneyRef>" + override("JPL_1", "PT6M"),
    )
    t = tables(document(journeys=[parent, child]))
    rows = t["stop_times"][t["stop_times"]["trip_id"].str.contains("_0900")]
    assert rows[["arrival_time", "departure_time"]].values.tolist() == [
        ["09:00:00", "09:00:00"],
        ["09:06:00", "09:07:00"],  # run overridden, the To wait inherited
        ["09:14:00", "09:14:00"],
    ]


def test_long_reference_chains_resolve_without_recursion():
    journeys = [journey("VJ_0", profile=profile("<Saturday />"))]
    for i in range(1, 3000):
        journeys.append(
            journey(
                "VJ_%d" % i,
                departure=None,
                pattern=None,
                extra="<VehicleJourneyRef>VJ_%d</VehicleJourneyRef>" % (i - 1),
            )
        )
    t = tables(document(journeys=journeys))
    assert len(t["trips"]) == 1  # all inherit pattern, time and days: one trip


def test_journey_operator_other_than_the_service_operator_warns():
    other = journey("VJ_1", extra="<OperatorRef>OId_OTHER</OperatorRef>")
    with pytest.warns(UserWarning, match="names operator 'OId_OTHER'"):
        t = tables(document(journeys=[other]))
    assert t["routes"]["agency_id"].to_list() == ["OId_CV"]
    with warnings.catch_warnings():
        warnings.simplefilter("error")
        same = journey("VJ_1", extra="<OperatorRef>OId_CV</OperatorRef>")
        tables(document(journeys=[same]))
    # warned even for a journey that never operates
    never = journey(
        "VJ_1",
        profile=profile(
            special_days=special(non_operation=[("2026-12-01", "2027-04-30")])
        ),
        extra="<OperatorRef>OId_OTHER</OperatorRef>",
    )
    with pytest.warns(UserWarning, match="names operator 'OId_OTHER'"):
        with pytest.warns(UserWarning, match="never operates"):
            tables(document(journeys=[never, journey("VJ_2")]))


# Frequencies ----------------------------------------------------------------------

FREQUENCY = (
    "<Frequency><EndTime>10:00:00</EndTime><Interval>"
    "<ScheduledFrequency>PT15M</ScheduledFrequency></Interval></Frequency>"
)


def test_frequency_journey_becomes_a_frequencies_row():
    t = tables(
        document(
            journeys=[
                journey("VJ_1", extra=FREQUENCY),
                journey("VJ_2", departure="12:00:00"),
            ]
        )
    )
    (row,) = t["frequencies"].to_dict("records")
    # a frequency journey never shares the plain id of a scheduled journey
    assert row["trip_id"].startswith("JPS_1_MondayToSunday_0800_")
    assert row["trip_id"] in set(t["trips"]["trip_id"])
    assert {
        k: row[k] for k in ("start_time", "end_time", "headway_secs", "exact_times")
    } == {
        "start_time": "08:00:00",
        "end_time": "10:00:00",
        "headway_secs": 900,
        "exact_times": 0,
    }
    assert len(t["trips"]) == 2
    # the template trip keeps its stop times
    assert t["stop_times"]["trip_id"].str.startswith(row["trip_id"]).sum() == 3
    assert tables(document())["frequencies"] is None


def test_overnight_frequency_window_runs_into_the_next_day():
    freq = (
        "<Frequency><EndTime>01:00:00</EndTime><Interval>"
        "<ScheduledFrequency>PT30M</ScheduledFrequency></Interval></Frequency>"
    )
    t = tables(document(journeys=[journey("VJ_1", departure="23:00:00", extra=freq)]))
    assert t["frequencies"][["start_time", "end_time"]].values.tolist() == [
        ["23:00:00", "25:00:00"]
    ]


def test_same_time_journeys_with_frequency_keep_separate_trips():
    t = tables(document(journeys=[journey("VJ_1"), journey("VJ_3", extra=FREQUENCY)]))
    assert len(t["trips"]) == 2 and t["trips"]["trip_id"].is_unique
    assert t["trips"]["trip_id"].iloc[0] == "JPS_1_MondayToSunday_0800"
    assert t["frequencies"]["trip_id"].iloc[0] != "JPS_1_MondayToSunday_0800"


def test_frequency_without_a_positive_interval_raises():
    for interval in ("", "PT0S", "-PT15M"):
        freq = "<Frequency><EndTime>10:00:00</EndTime>%s</Frequency>" % (
            "<Interval><ScheduledFrequency>%s</ScheduledFrequency></Interval>"
            % interval
            if interval
            else ""
        )
        with pytest.raises(ValueError, match="'VJ_1' has no positive Frequency"):
            tables(document(journeys=[journey("VJ_1", extra=freq)]))
    # a Frequency without EndTime is not a frequency journey
    freq = (
        "<Frequency><Interval><ScheduledFrequency>PT15M</ScheduledFrequency>"
        "</Interval></Frequency>"
    )
    assert (
        tables(document(journeys=[journey("VJ_1", extra=freq)]))["frequencies"] is None
    )
