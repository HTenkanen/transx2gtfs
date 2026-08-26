"""TransXChange 2.4/2.5 rules: journey references, frequencies, calendars,
bank holidays, serviced organisations, timing, routes without RouteRef."""

import io
import os
import warnings
from datetime import date
from zipfile import ZipFile

import pandas as pd
import pytest

import transx2gtfs
from transx2gtfs.agency import get_agency
from transx2gtfs.bank_holidays import (
    bank_holiday_table,
    detect_division,
    expand_bank_holiday_names,
    read_bank_holidays,
)
from transx2gtfs.calendar import get_calendar, parse_active_days
from transx2gtfs.calendar_dates import get_calendar_dates
from transx2gtfs.routes import get_routes
from transx2gtfs.stop_times import get_frequencies, get_stop_times
from transx2gtfs.transxchange import get_gtfs_info, parse_runtime_duration
from transx2gtfs.trips import get_trips
from transx2gtfs.txc import read_txc

from conftest import DATA_DIR

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
    link_extra=None,
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
        frequencies=get_frequencies(info),
        agency=get_agency(doc),
        doc=doc,
    )


def exception_dates(calendar_dates, kind):
    if calendar_dates is None:
        return set()
    return set(calendar_dates.loc[calendar_dates["exception_type"] == kind, "date"])


# Journeys ----------------------------------------------------------------------


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


def test_frequency_journey_becomes_a_frequencies_row():
    freq = (
        "<Frequency><EndTime>10:00:00</EndTime><Interval>"
        "<ScheduledFrequency>PT15M</ScheduledFrequency></Interval></Frequency>"
    )
    t = tables(
        document(
            journeys=[
                journey("VJ_1", extra=freq),
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


def test_missing_mode_and_operator_names():
    with pytest.warns(UserWarning, match="no Mode, assuming bus"):
        t = tables(document(mode=""))
    assert t["routes"]["route_type"].to_list() == [3]
    t = tables(document(mode="<Mode>trolleyBus</Mode>"))
    assert t["routes"]["route_type"].to_list() == [11]

    operator = (
        '<Operator id="OId_CV"><OperatorCode>CV</OperatorCode>'
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


def test_line_ref_selects_the_line_name():
    lines = (
        '<Line id="L1"><LineName>1</LineName></Line><Line id="L2">'
        "<LineName>1A</LineName></Line>"
    )
    t = tables(
        document(
            lines=lines,
            journeys=[
                journey("VJ_1"),
                journey("VJ_2", departure="09:00:00", line="L2"),
            ],
        )
    )
    names = (
        t["info"]
        .drop_duplicates("vehicle_journey_id")
        .set_index("vehicle_journey_id")["line_name"]
    )
    assert names.to_dict() == {"VJ_1": "1", "VJ_2": "1A"}
    assert t["routes"]["route_id"].to_list() == ["R_1"]


# Routes without RouteRef ---------------------------------------------------------


def pattern_without_route():
    return (
        '<JourneyPattern id="JP_1"><Direction>outbound</Direction>'
        "<JourneyPatternSectionRefs>JPS_1</JourneyPatternSectionRefs></JourneyPattern>"
    )


ROUTE_SECTIONS = (
    '<RouteSection id="RS_1"><RouteLink id="RL_1">'
    "<Distance>1000</Distance></RouteLink></RouteSection>"
    '<RouteSection id="RS_2"><RouteLink id="RL_2">'
    "<Distance>3000</Distance></RouteLink></RouteSection>"
)


def test_route_matched_by_section_sequence():
    routes = (
        '<Route id="R_short"><PrivateCode>R_short</PrivateCode>'
        "<Description>Short</Description>"
        "<RouteSectionRef>RS_1</RouteSectionRef></Route>"
        '<Route id="R_full"><PrivateCode>R_full</PrivateCode>'
        "<Description>Full</Description>"
        "<RouteSectionRef>RS_1</RouteSectionRef>"
        "<RouteSectionRef>RS_2</RouteSectionRef></Route>"
    )
    t = tables(
        document(
            routes=routes,
            route_sections=ROUTE_SECTIONS,
            journey_patterns=pattern_without_route(),
        )
    )
    assert t["trips"]["route_id"].to_list() == [
        "R_full"
    ]  # exact sequence wins over prefix
    assert t["routes"]["route_id"].to_list() == ["R_full"]


def test_route_matched_by_unique_prefix_when_no_exact_match():
    routes = (
        '<Route id="R_long"><PrivateCode>R_long</PrivateCode><Description>L</Description>'
        "<RouteSectionRef>RS_1</RouteSectionRef><RouteSectionRef>RS_2</RouteSectionRef>"
        "<RouteSectionRef>RS_9</RouteSectionRef></Route>"
        '<Route id="R_other"><PrivateCode>R_other</PrivateCode><Description>O</Description>'
        "<RouteSectionRef>RS_1</RouteSectionRef><RouteSectionRef>RS_8</RouteSectionRef></Route>"
    )
    t = tables(
        document(
            routes=routes,
            route_sections=ROUTE_SECTIONS,
            journey_patterns=pattern_without_route(),
        )
    )
    assert t["trips"]["route_id"].to_list() == [
        "R_long"
    ]  # only R_long starts with RS_1, RS_2


def test_routes_sharing_the_pattern_prefix_but_diverging_are_unmatched():
    # both routes start with the pattern's whole sequence (RS_1, RS_2): ambiguous
    routes = (
        '<Route id="R_a"><PrivateCode>R_a</PrivateCode><Description>A</Description>'
        "<RouteSectionRef>RS_1</RouteSectionRef><RouteSectionRef>RS_2</RouteSectionRef>"
        "<RouteSectionRef>RS_9</RouteSectionRef></Route>"
        '<Route id="R_b"><PrivateCode>R_b</PrivateCode><Description>B</Description>'
        "<RouteSectionRef>RS_1</RouteSectionRef><RouteSectionRef>RS_2</RouteSectionRef>"
        "<RouteSectionRef>RS_8</RouteSectionRef></Route>"
    )
    t = tables(
        document(
            routes=routes,
            route_sections=ROUTE_SECTIONS,
            journey_patterns=pattern_without_route(),
        )
    )
    assert t["trips"]["route_id"].to_list() == ["S1_L1"]  # ambiguous: synthesised
    assert t["routes"]["route_id"].to_list() == ["S1_L1"]


def test_synthetic_route_when_nothing_matches():
    t = tables(document(routes="", journey_patterns=pattern_without_route()))
    assert t["trips"]["route_id"].to_list() == ["S1_L1"]
    assert t["routes"].to_dict("records") == [
        {
            "route_id": "S1_L1",
            "agency_id": "OId_CV",
            "route_short_name": "1",
            "route_long_name": "A - B",
            "route_type": 3,
        }
    ]


def test_route_without_description_uses_line_and_service():
    routes = '<Route id="R_1"><RouteSectionRef>RS_1</RouteSectionRef></Route>'
    t = tables(document(routes=routes))
    assert t["routes"][["route_short_name", "route_long_name"]].to_dict("records") == [
        {"route_short_name": "1", "route_long_name": "A - B"}
    ]


# Timing ---------------------------------------------------------------------------


def arrivals(t):
    return t["stop_times"][["arrival_time", "departure_time"]].values.tolist()


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


def test_interpolation_equal_and_by_distance():
    links = (("9300WAS1", "9300MIL2", "PT0S"), ("9300MIL2", "9300MIL1", "PT10M"))
    t = tables(document(links=links))
    assert [a for a, _ in arrivals(t)] == ["08:00:00", "08:05:00", "08:10:00"]
    t = tables(document(links=links, route_sections=ROUTE_SECTIONS))
    assert [a for a, _ in arrivals(t)] == ["08:00:00", "08:02:30", "08:10:00"]


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


def test_runtime_durations():
    values = ("PT0S", "PT1H2M3S", "P1DT1H", None)
    assert [parse_runtime_duration(v) for v in values] == [0, 3723, 90000, 0]
    with pytest.warns(UserWarning, match="Negative duration '-PT5M'"):
        assert parse_runtime_duration("-PT5M") == 300


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
    ],
)
def test_day_patterns(dayinfo, active):
    assert "".join(str(v) for v in parse_active_days(dayinfo).values()) == active


def test_journey_without_days_runs_every_day_and_holidays_only_runs_on_holidays():
    t = tables(document(journeys=[journey("VJ_1", profile="")]))
    assert t["calendar"].iloc[0][["monday", "sunday"]].to_list() == [1, 1]

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


def bh(operation="", non_operation=""):
    return "<BankHolidayOperation>%s%s</BankHolidayOperation>" % (
        "<DaysOfOperation>%s</DaysOfOperation>" % operation if operation else "",
        (
            "<DaysOfNonOperation>%s</DaysOfNonOperation>" % non_operation
            if non_operation
            else ""
        ),
    )


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


def test_bank_holiday_table_names_and_selectors():
    holidays = read_bank_holidays()
    ew = bank_holiday_table(
        "england-and-wales", date(2027, 1, 1), date(2027, 12, 31), holidays
    )
    iso = lambda name: [d.isoformat() for d in ew[name]]  # noqa: E731
    assert iso("ChristmasDay") == ["2027-12-25"] and iso("ChristmasDayHoliday") == [
        "2027-12-27"
    ]
    assert iso("BoxingDay") == ["2027-12-26"] and iso("BoxingDayHoliday") == [
        "2027-12-28"
    ]
    assert iso("NewYearsDayHoliday") == [] and iso("EarlyRunOffDays") == [
        "2027-12-24",
        "2027-12-31",
    ]
    assert iso("LateSummerBankHolidayNotScotland") == ["2027-08-30"]
    assert "2027-12-24" not in iso("AllBankHolidays") and "2027-12-31" not in iso(
        "AllBankHolidays"
    )
    assert iso("Christmas") == ["2027-12-25", "2027-12-26", "2027-12-27", "2027-12-28"]
    assert set(iso("AllHolidaysExceptChristmas")) == set(iso("AllBankHolidays")) - set(
        iso("Christmas")
    )
    assert iso("HolidayMondays") == [
        "2027-03-29",
        "2027-05-03",
        "2027-05-31",
        "2027-08-30",
        "2027-12-27",
    ]
    assert iso("DisplacementHolidays") == ["2027-12-27", "2027-12-28"]

    scot = bank_holiday_table(
        "scotland", date(2027, 1, 1), date(2027, 12, 31), holidays
    )
    assert [d.isoformat() for d in scot["Jan2ndScotland"]] == ["2027-01-02"]
    assert [d.isoformat() for d in scot["Jan2ndScotlandHoliday"]] == ["2027-01-04"]
    assert [d.isoformat() for d in scot["AugustBankHolidayScotland"]] == ["2027-08-02"]
    assert [d.isoformat() for d in scot["StAndrewsDay"]] == ["2027-11-30"]
    scot_all = {d.isoformat() for d in scot["AllBankHolidays"]}
    assert {"2027-12-27", "2027-12-28"} <= scot_all and "2027-12-25" not in scot_all
    assert [d.isoformat() for d in scot["ChristmasDayHoliday"]] == ["2027-12-27"]
    assert [d.isoformat() for d in scot["BoxingDayHoliday"]] == ["2027-12-28"]
    assert scot["EasterMonday"] == []

    # one-off holidays are bank holidays too
    ew_2022 = bank_holiday_table(
        "england-and-wales", date(2022, 1, 1), date(2023, 12, 31), holidays
    )
    all_dates = {d.isoformat() for d in ew_2022["AllBankHolidays"]}
    assert {"2022-09-19", "2023-05-08"} <= all_dates
    assert expand_bank_holiday_names(["HolidayMondays", "ChristmasEve"], ew) == set(
        ew["HolidayMondays"]
    ) | set(ew["ChristmasEve"])


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


# End to end ----------------------------------------------------------------------


def test_convert_txc24_fixtures(tmp_path):
    output = str(tmp_path / "gtfs.zip")
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        transx2gtfs.convert(str(DATA_DIR / "txc24"), output, worker_cnt=2)
    with ZipFile(output) as zf:
        names = set(zf.namelist())
        gtfs = {n: pd.read_csv(io.BytesIO(zf.read(n)), dtype=str) for n in names}
    assert {
        "agency.txt",
        "stops.txt",
        "routes.txt",
        "trips.txt",
        "stop_times.txt",
        "calendar.txt",
        "calendar_dates.txt",
        "frequencies.txt",
    } <= names
    stop_times = gtfs["stop_times.txt"]
    assert set(stop_times["trip_id"]) == set(gtfs["trips.txt"]["trip_id"])
    # one London stop of the 403 is not in the current NaPTAN
    assert set(stop_times["stop_id"]) - set(gtfs["stops.txt"]["stop_id"]) == {
        "490006706C13"
    }
    assert set(gtfs["trips.txt"]["route_id"]) <= set(gtfs["routes.txt"]["route_id"])
    assert set(gtfs["trips.txt"]["service_id"]) == set(
        gtfs["calendar.txt"]["service_id"]
    )
    assert set(gtfs["calendar_dates.txt"]["service_id"]) <= set(
        gtfs["calendar.txt"]["service_id"]
    )
    assert set(gtfs["frequencies.txt"]["trip_id"]) <= set(gtfs["trips.txt"]["trip_id"])
    assert len(gtfs["agency.txt"]) == 4


# Round-1 review findings ---------------------------------------------------------


def test_section_refs_may_be_whitespace_separated_idrefs():
    xml = (
        document()
        .replace(
            b"<JourneyPatternSectionRefs>JPS_1</JourneyPatternSectionRefs>",
            b"<JourneyPatternSectionRefs>JPS_1 JPS_2</JourneyPatternSectionRefs>",
        )
        .replace(
            b"</JourneyPatternSections>",
            b'<JourneyPatternSection id="JPS_2"><JourneyPatternTimingLink id="JPL_9">'
            b"<From><StopPointRef>9300MIL1</StopPointRef></From>"
            b"<To><StopPointRef>490007705N</StopPointRef></To><RunTime>PT2M</RunTime>"
            b"</JourneyPatternTimingLink></JourneyPatternSection></JourneyPatternSections>",
        )
    )
    doc = read_txc(xml)
    assert doc.journey_pattern("JP_1").section_refs == ["JPS_1", "JPS_2"]
    assert get_stop_times(get_gtfs_info(doc))["stop_id"].to_list() == [
        "9300WAS1",
        "9300MIL2",
        "9300MIL1",
        "490007705N",
    ]


def test_document_whose_journeys_never_operate_converts_to_nothing(tmp_path):
    prof = profile(special_days=special(non_operation=[("2026-12-01", "2027-04-30")]))
    with pytest.warns(UserWarning, match="no vehicle journey operates"):
        info = get_gtfs_info(
            read_txc(document(journeys=[journey("VJ_1", profile=prof)]))
        )
    assert len(info) == 0 and "trip_id" in info.columns
    assert len(get_stop_times(info)) == 0
    assert get_calendar_dates(info) is None and get_frequencies(info) is None

    (tmp_path / "never.xml").write_bytes(
        document(journeys=[journey("VJ_1", profile=prof)])
    )
    with pytest.raises(ValueError, match="did not produce any trips"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            transx2gtfs.convert(str(tmp_path), str(tmp_path / "gtfs.zip"), worker_cnt=1)


def test_overnight_frequency_window_runs_into_the_next_day():
    freq = (
        "<Frequency><EndTime>01:00:00</EndTime><Interval>"
        "<ScheduledFrequency>PT30M</ScheduledFrequency></Interval></Frequency>"
    )
    t = tables(document(journeys=[journey("VJ_1", departure="23:00:00", extra=freq)]))
    assert t["frequencies"][["start_time", "end_time"]].values.tolist() == [
        ["23:00:00", "25:00:00"]
    ]


def test_fractional_and_invalid_durations():
    assert parse_runtime_duration("PT0.5H") == 1800
    assert parse_runtime_duration("PT1.5M") == 90
    assert parse_runtime_duration("PT2.4S") == 2
    assert parse_runtime_duration("P0.5D") == 43200
    for invalid in ("10 minutes", "PT", "P", "-P", "-PT", "P1DT", "PT5"):
        with pytest.raises(ValueError, match="Not an ISO-8601 duration"):
            parse_runtime_duration(invalid)


def test_raw_direction_is_kept_in_gtfs_info():
    pattern = (
        '<JourneyPattern id="JP_1"><Direction>antiClockwise</Direction><RouteRef>R_1</RouteRef>'
        "<JourneyPatternSectionRefs>JPS_1</JourneyPatternSectionRefs></JourneyPattern>"
    )
    info = tables(document(journey_patterns=pattern))["info"]
    assert set(info["direction"]) == {"antiClockwise"} and set(
        info["direction_id"]
    ) == {0}


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


def test_bank_holidays_fall_back_to_the_packaged_copy_offline(monkeypatch):
    from transx2gtfs import bank_holidays as bh_module

    monkeypatch.delenv("TRANSX2GTFS_BANK_HOLIDAYS_PATH")
    calls = []

    def fail(*args, **kwargs):
        calls.append(args)
        raise OSError("offline")

    monkeypatch.setattr(bh_module.urllib.request, "urlopen", fail)
    assert len(bh_module.read_bank_holidays()) > 0 and len(calls) == 1


def test_operating_period_beyond_the_holiday_feed_warns():
    holidays = read_bank_holidays()
    last = holidays["date"].max()
    with pytest.warns(
        UserWarning, match="bank holiday feed ends on %s" % last.isoformat()
    ):
        bank_holiday_table(
            "england-and-wales",
            date(last.year, 1, 1),
            date(last.year + 2, 12, 31),
            holidays,
        )


EXPECTED_2027 = {
    "england-and-wales": {
        "NewYearsDay": ["2027-01-01"],
        "GoodFriday": ["2027-03-26"],
        "EasterMonday": ["2027-03-29"],
        "MayDay": ["2027-05-03"],
        "SpringBank": ["2027-05-31"],
        "LateSummerBankHolidayNotScotland": ["2027-08-30"],
        "ChristmasEve": ["2027-12-24"],
        "ChristmasDay": ["2027-12-25"],
        "BoxingDay": ["2027-12-26"],
        "ChristmasDayHoliday": ["2027-12-27"],
        "BoxingDayHoliday": ["2027-12-28"],
        "NewYearsEve": ["2027-12-31"],
        "NewYearsDayHoliday": [],
        "Jan2ndScotland": [],
        "Jan2ndScotlandHoliday": [],
        "StAndrewsDay": [],
        "StAndrewsDayHoliday": [],
        "AugustBankHolidayScotland": [],
    },
    "scotland": {
        "NewYearsDay": ["2027-01-01"],
        "Jan2ndScotland": ["2027-01-02"],
        "Jan2ndScotlandHoliday": ["2027-01-04"],
        "GoodFriday": ["2027-03-26"],
        "EasterMonday": [],
        "MayDay": ["2027-05-03"],
        "SpringBank": ["2027-05-31"],
        "AugustBankHolidayScotland": ["2027-08-02"],
        "LateSummerBankHolidayNotScotland": [],
        "StAndrewsDay": ["2027-11-30"],
        "StAndrewsDayHoliday": [],
        "ChristmasEve": ["2027-12-24"],
        "ChristmasDay": ["2027-12-25"],
        "BoxingDay": ["2027-12-26"],
        "ChristmasDayHoliday": ["2027-12-27"],
        "BoxingDayHoliday": ["2027-12-28"],
        "NewYearsEve": ["2027-12-31"],
        "NewYearsDayHoliday": [],
    },
}
GROUPS = (
    "AllBankHolidays",
    "AllHolidaysExceptChristmas",
    "Christmas",
    "DisplacementHolidays",
    "EarlyRunOffDays",
    "HolidayMondays",
)


@pytest.mark.parametrize("division", sorted(EXPECTED_2027))
def test_every_holiday_name_in_2027(division):
    table = bank_holiday_table(
        division, date(2027, 1, 1), date(2027, 12, 31), read_bank_holidays()
    )
    produced = {name: [d.isoformat() for d in dates] for name, dates in table.items()}
    for name, expected in EXPECTED_2027[division].items():
        assert produced[name] == expected, name
    assert {n for n in table if n not in GROUPS} == set(EXPECTED_2027[division])


@pytest.mark.parametrize("day_index", range(7))
def test_every_not_weekday_keyword(day_index):
    from transx2gtfs.calendar import WEEKDAYS

    keyword = "Not" + WEEKDAYS[day_index].capitalize()
    expected = [0 if i == day_index else 1 for i in range(7)]
    assert list(parse_active_days(keyword).values()) == expected


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


def test_snapshots_are_private_copies_of_the_configured_feed(monkeypatch, tmp_path):
    from transx2gtfs import bank_holidays as bh_module

    monkeypatch.setattr(bh_module.tempfile, "gettempdir", lambda: str(tmp_path))
    first = bh_module.snapshot_bank_holidays_data()
    second = bh_module.snapshot_bank_holidays_data()
    assert first != second
    assert open(first, "rb").read() == open(second, "rb").read()
    assert first.startswith(str(tmp_path))
    if os.name != "nt":  # Windows has no POSIX directory modes
        assert oct(os.stat(os.path.dirname(first)).st_mode & 0o777) == "0o700"
    # a worker reads the snapshot it was given, whatever the environment says
    monkeypatch.setenv("TRANSX2GTFS_BANK_HOLIDAYS_PATH", str(tmp_path / "missing.json"))
    bh_module.set_bank_holidays_path(first)
    try:
        assert len(bh_module.read_bank_holidays()) > 0
    finally:
        bh_module.set_bank_holidays_path(None)
    bh_module.remove_bank_holidays_snapshot(first)
    bh_module.remove_bank_holidays_snapshot(second)
    assert list(tmp_path.glob("transx2gtfs-*")) == []


def test_pattern_with_an_unmapped_link_is_not_matched_to_a_route():
    routes = (
        '<Route id="R_1"><PrivateCode>R_1</PrivateCode><Description>A - B</Description>'
        "<RouteSectionRef>RS_1</RouteSectionRef></Route>"
    )
    # the second link has no RouteLinkRef in any RouteSection
    links = (
        ("9300WAS1", "9300MIL2", "PT5M"),
        ("9300MIL2", "9300MIL1", "PT7M", {"route_link": "RL_unknown"}),
    )
    t = tables(
        document(
            links=links,
            routes=routes,
            route_sections=ROUTE_SECTIONS,
            journey_patterns=pattern_without_route(),
        )
    )
    assert t["trips"]["route_id"].to_list() == ["S1_L1"]


def test_convert_removes_its_snapshot_and_leaves_the_environment_alone(
    monkeypatch, tmp_path, data_dir
):
    from transx2gtfs import bank_holidays as bh_module

    monkeypatch.setattr(bh_module.tempfile, "gettempdir", lambda: str(tmp_path))
    before = dict(os.environ)
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        transx2gtfs.convert(data_dir, str(tmp_path / "gtfs.zip"), worker_cnt=1)
    assert dict(os.environ) == before
    assert list(tmp_path.glob("transx2gtfs-*")) == []


def test_notes_in_both_forms():
    notes = (
        "<Note><NoteCode>1</NoteCode><NoteText>Bookable in advance</NoteText></Note>"
        "<Note> Direct text </Note>"
    )
    doc = read_txc(document(journeys=[journey("VJ_1", extra=notes)]))
    assert doc.vehicle_journeys[0].notes == ["Bookable in advance", "Direct text"]


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


def test_synthetic_route_ids_are_unique_across_services_and_lines():
    from transx2gtfs.routes import synthetic_route_ids
    from transx2gtfs.txc import Line, Route, Service, TxcDocument

    doc = TxcDocument(
        routes=[Route(id="A_B_C")],
        services=[
            Service(code="A", lines=[Line(id="B_C")]),
            Service(code="A_B", lines=[Line(id="C")]),
        ],
    )
    assert synthetic_route_ids(doc) == {("A", "B_C"): "A_B_C_", ("A_B", "C"): "A_B_C__"}


def test_synthetic_route_id_never_collides_with_a_declared_route():
    routes = (
        '<Route id="S1_L1"><PrivateCode>S1_L1</PrivateCode><Description>Declared</Description>'
        "<RouteSectionRef>RS_9</RouteSectionRef></Route>"
    )
    t = tables(document(routes=routes, journey_patterns=pattern_without_route()))
    assert t["trips"]["route_id"].to_list() == ["S1_L1_"]
    assert t["routes"]["route_id"].to_list() == ["S1_L1_"]


def test_same_time_journeys_with_overrides_or_frequency_keep_separate_trips():
    override = (
        "<VehicleJourneyTimingLink><JourneyPatternTimingLinkRef>JPL_2</JourneyPatternTimingLinkRef>"
        "<RunTime>PT9M</RunTime></VehicleJourneyTimingLink>"
    )
    freq = (
        "<Frequency><EndTime>10:00:00</EndTime><Interval>"
        "<ScheduledFrequency>PT15M</ScheduledFrequency></Interval></Frequency>"
    )
    t = tables(
        document(
            journeys=[
                journey("VJ_1"),
                journey("VJ_2", extra=override),
                journey("VJ_3", extra=freq),
            ]
        )
    )
    assert len(t["trips"]) == 3 and t["trips"]["trip_id"].is_unique
    assert t["trips"]["trip_id"].iloc[0] == "JPS_1_MondayToSunday_0800"
    assert t["frequencies"]["trip_id"].iloc[0] != "JPS_1_MondayToSunday_0800"


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


def test_same_time_journeys_on_different_lines_get_separate_synthetic_trips():
    lines = (
        '<Line id="L1"><LineName>1</LineName></Line>'
        '<Line id="L2"><LineName>1A</LineName></Line>'
    )
    t = tables(
        document(
            lines=lines,
            routes="",
            journey_patterns=pattern_without_route(),
            journeys=[journey("VJ_1"), journey("VJ_2", line="L2")],
        )
    )
    assert t["trips"]["trip_id"].is_unique
    assert sorted(t["trips"]["route_id"]) == ["S1_L1", "S1_L2"]
    assert sorted(t["routes"]["route_id"]) == ["S1_L1", "S1_L2"]


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
    # the service Description is optional (BODS files omit it)
    info = tables(document().replace(b"<Description>A to B</Description>", b""))["info"]
    assert len(info) > 0


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


def test_unknown_line_ref_and_missing_line_names_raise():
    with pytest.raises(ValueError, match="VJ_1' refers to unknown Line 'L9'"):
        tables(document(journeys=[journey("VJ_1", line="L9")]))
    lines = '<Line id="L1"><LineName>1</LineName></Line><Line id="L2"></Line>'
    with pytest.raises(ValueError, match="Line 'L2' LineName is missing"):
        tables(document(lines=lines))
