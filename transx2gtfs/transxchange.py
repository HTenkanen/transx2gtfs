import dataclasses
import math
import re
import warnings
from datetime import date, datetime, timedelta
from fractions import Fraction

import pandas as pd

from transx2gtfs.bank_holidays import (
    bank_holiday_table,
    daterange,
    detect_division,
    expand_bank_holiday_names,
    read_bank_holidays,
)
from transx2gtfs.calendar import get_weekday_info, parse_active_days
from transx2gtfs.calendar_dates import encode_exceptions
from transx2gtfs.routes import get_mode
from transx2gtfs.stop_times import exceptions_digest, generate_service_id, get_direction

DEFAULT_WEEKDAYS = "MondayToSunday"
OPERATING_PERIOD_DEFAULT_DAYS = 365
GTFS_INFO_COLUMNS = [
    "stop_id",
    "stop_sequence",
    "timepoint",
    "arrival_time",
    "departure_time",
    "route_link_ref",
    "agency_id",
    "trip_id",
    "route_id",
    "vehicle_journey_id",
    "service_ref",
    "direction_id",
    "direction",
    "line_name",
    "travel_mode",
    "trip_headsign",
    "vehicle_type",
    "start_date",
    "end_date",
    "weekdays",
    "exceptions",
    "service_id",
]


def parse_date(text):
    return datetime.strptime(text.strip()[:10], "%Y-%m-%d").date()


def gtfs_date(day):
    return day.strftime("%Y%m%d")


def service_end_date(doc, service, start):
    """
    End of the operating period: the service's EndDate, or one year after the
    latest of its StartDate and the document's creation/modification dates.
    """
    if service.end_date:
        return parse_date(service.end_date)
    latest = start
    for stamp in (doc.creation_date_time, doc.modification_date_time):
        if stamp:
            latest = max(latest, parse_date(stamp))
    return latest + timedelta(days=OPERATING_PERIOD_DEFAULT_DAYS)


# Calendars
# ---------


class _Calendars:
    """Bank holiday tables of a document, one per operating period"""

    def __init__(self, doc):
        self.division = detect_division(doc)
        self._holidays = None
        self._tables = {}

    def table(self, start, end):
        key = (start, end)
        if key not in self._tables:
            if self._holidays is None:
                self._holidays = read_bank_holidays()
            self._tables[key] = bank_holiday_table(
                self.division, start, end, self._holidays
            )
        return self._tables[key]


def _range_dates(ranges, start, end):
    """Dates of the (start, end) ISO ranges, bounded to the period [start, end]"""
    dates = set()
    for range_start, range_end in ranges:
        first = max(parse_date(range_start), start)
        last = min(parse_date(range_end), end)
        if first <= last:
            dates.update(daterange(first, last))
    return dates


def _organisation_dates(doc, refs, start, end):
    dates = set()
    for code, kind in refs:
        try:
            organisation = doc.serviced_organisation(code)
        except KeyError:
            raise ValueError("Unknown ServicedOrganisation '%s'." % code) from None
        ranges = (
            organisation.holidays if kind == "Holidays" else organisation.working_days
        )
        dates.update(_range_dates(ranges, start, end))
    return dates


def _inherit_profile(profile, fallback):
    """Fill the day, bank holiday, special day and serviced organisation rules a
    journey profile does not set from the service profile"""
    if profile is None:
        return fallback
    if fallback is None:
        return profile
    changes = {}
    if profile.days_of_week is None and not profile.holidays_only:
        changes["days_of_week"] = fallback.days_of_week
        changes["holidays_only"] = fallback.holidays_only
    # A missing DaysOf(Non)Operation element is inherited as a whole: its
    # holiday names and its OtherPublicHoliday dates
    if profile.bank_holiday_days_of_operation is None:
        changes["bank_holiday_days_of_operation"] = (
            fallback.bank_holiday_days_of_operation
        )
        changes["other_public_holidays_of_operation"] = (
            fallback.other_public_holidays_of_operation
        )
    if profile.bank_holiday_days_of_non_operation is None:
        changes["bank_holiday_days_of_non_operation"] = (
            fallback.bank_holiday_days_of_non_operation
        )
        changes["other_public_holidays_of_non_operation"] = (
            fallback.other_public_holidays_of_non_operation
        )
    # Special days and serviced organisations are inherited as a whole too
    if not (profile.special_days_of_operation or profile.special_days_of_non_operation):
        changes["special_days_of_operation"] = fallback.special_days_of_operation
        changes["special_days_of_non_operation"] = (
            fallback.special_days_of_non_operation
        )
    if not (
        profile.serviced_organisation_days_of_operation
        or profile.serviced_organisation_days_of_non_operation
    ):
        changes["serviced_organisation_days_of_operation"] = (
            fallback.serviced_organisation_days_of_operation
        )
        changes["serviced_organisation_days_of_non_operation"] = (
            fallback.serviced_organisation_days_of_non_operation
        )
    return dataclasses.replace(profile, **changes) if changes else profile


def journey_calendar(doc, calendars, profile, start, end):
    """
    Operating period, weekday pattern and exceptions of one journey.

    Returns ``(start_date, end_date, weekdays, exceptions)`` with GTFS dates and
    the encoded exceptions (dates added on days outside the weekday pattern,
    removed on days inside it; a removal wins over an addition), or None when
    the journey never operates.
    """
    weekdays = get_weekday_info(profile) or DEFAULT_WEEKDAYS
    active = {i for i, on in enumerate(parse_active_days(weekdays).values()) if on}

    added, removed = set(), set()
    # Only SpecialDaysOperation non-operation shortens the period at its edges
    special_removed = set()
    if profile is not None:
        holidays_only = profile.holidays_only or any(
            name.endswith("HolidaysOnly") for name in (profile.days_of_week or [])
        )
        if (
            holidays_only
            or profile.bank_holiday_days_of_operation
            or profile.bank_holiday_days_of_non_operation
        ):
            table = calendars.table(start, end)
            # HolidaysOnly / SaturdaySundayHolidaysOnly run on every bank holiday
            if holidays_only:
                added |= set(table["AllBankHolidays"])
            added |= expand_bank_holiday_names(
                profile.bank_holiday_days_of_operation, table
            )
            removed |= expand_bank_holiday_names(
                profile.bank_holiday_days_of_non_operation, table
            )
        added |= {
            parse_date(d)
            for _, d in profile.other_public_holidays_of_operation
            if d and start <= parse_date(d) <= end
        }
        removed |= {
            parse_date(d)
            for _, d in profile.other_public_holidays_of_non_operation
            if d and start <= parse_date(d) <= end
        }
        added |= _range_dates(profile.special_days_of_operation, start, end)
        special_removed = _range_dates(
            profile.special_days_of_non_operation, start, end
        )
        removed |= special_removed
        removed |= _organisation_dates(
            doc, profile.serviced_organisation_days_of_non_operation, start, end
        )

        # Serviced organisation days of operation restrict the journey to the
        # union of the organisations' dates (explicit additions still apply)
        if profile.serviced_organisation_days_of_operation:
            allowed = _organisation_dates(
                doc, profile.serviced_organisation_days_of_operation, start, end
            )
            if allowed:
                start = max(start, min(allowed))
                end = min(end, max(allowed))
            gaps = {
                day
                for day in daterange(start, end)
                if day.weekday() in active and day not in allowed
            }
            removed |= gaps - added

    # Special-day non-operation at the edges shortens the period instead of
    # adding exceptions; a period removed up to the last representable date
    # (or from the first one) is gone
    while start <= end and start in special_removed:
        if start == date.max:
            return None
        start += timedelta(days=1)
    while end >= start and end in special_removed:
        if end == date.min:
            return None
        end -= timedelta(days=1)
    if start > end:
        return None

    # A journey with no operating date left never operates
    operates = any(
        day.weekday() in active and day not in removed for day in daterange(start, end)
    ) or any(day not in removed for day in added)
    if not operates:
        return None

    in_period = lambda day: start <= day <= end  # noqa: E731
    type1 = {
        day
        for day in added
        if day not in removed and (day.weekday() not in active or not in_period(day))
    }
    type2 = {day for day in removed if in_period(day) and day.weekday() in active}
    return gtfs_date(start), gtfs_date(end), weekdays, encode_exceptions(type1, type2)


# Timing
# ------


def _journey_fingerprint(
    journey, departure_seconds, exceptions, period, service_period
):
    """What distinguishes same-minute journeys of one pattern: departure seconds,
    exceptions, a calendar clipped away from the service's period and timing link
    overrides; '' when none of them apply"""
    parts = []
    if departure_seconds % 60:
        parts.append("seconds:%d" % (departure_seconds % 60))
    if exceptions:
        parts.append(exceptions)
    if period != service_period:
        parts.append("period:%s:%s" % period)
    for tl in sorted(
        journey.timing_links, key=lambda t: t.journey_pattern_timing_link_ref
    ):
        parts.append(
            "%s:%s:%s:%s"
            % (
                tl.journey_pattern_timing_link_ref,
                tl.run_time or "",
                tl.from_wait_time or "",
                tl.to_wait_time or "",
            )
        )
    return "|".join(parts)


def _link_timings(journey, sections):
    """Effective (link, run seconds, from-wait seconds, to-wait seconds) per link"""
    overrides = {tl.journey_pattern_timing_link_ref: tl for tl in journey.timing_links}
    timings = []
    for section in sections:
        for link in section.timing_links:
            override = overrides.get(link.id)
            run_time = link.run_time
            from_wait = link.from_wait_time
            to_wait = link.to_wait_time
            if override is not None:
                run_time = override.run_time or run_time
                from_wait = override.from_wait_time or from_wait
                to_wait = override.to_wait_time or to_wait
            timings.append(
                (
                    link,
                    parse_runtime_duration(run_time),
                    parse_runtime_duration(from_wait),
                    parse_runtime_duration(to_wait),
                )
            )
    return timings


def _stop_offsets(timings):
    """
    Arrival and departure offsets (seconds after the journey's departure) of
    every stop of the journey: the first stop, then the To stop of every link.
    A stop's departure is its arrival plus the To wait of the link reaching it
    and the From wait of the link leaving it; the first stop departs at the
    journey's DepartureTime, so a From wait on the first link does not delay it.
    """
    n_links = len(timings)
    arrivals = [0] * (n_links + 1)
    departures = [0] * (n_links + 1)
    for k, (_link, run, _from_wait, to_wait) in enumerate(timings):
        arrivals[k + 1] = departures[k] + run
        next_from_wait = timings[k + 1][2] if k + 1 < n_links else 0
        departures[k + 1] = arrivals[k + 1] + to_wait + next_from_wait
    return arrivals, departures


# Main table
# ----------


def _service_profiles(doc):
    """OperatingProfile per service code, used when a journey has none"""
    return {service.code: service.operating_profile for service in doc.services}


def process_vehicle_journeys(doc, service_jp_info):
    """Build the stop_times-level GTFS info rows for every VehicleJourney"""

    vjourneys = doc.vehicle_journeys

    # Number of journeys to process
    journey_cnt = len(vjourneys)

    # Service-level operating profiles as fallback, and the bank holidays
    service_profiles = _service_profiles(doc)
    calendars = _Calendars(doc)

    # Container for gtfs_info rows
    rows = []

    # Iterate over VehicleJourneys
    for i, journey in enumerate(vjourneys):
        if i != 0 and i % 50 == 0:
            print("Processed %s / %s journeys." % (i, journey_cnt))
        # Get service reference
        service_ref = journey.service_ref

        # Journey pattern reference
        journey_pattern_id = journey.journey_pattern_ref

        # Vehicle journey id (part of multi-section trip ids)
        vehicle_journey_id = journey.code

        if service_ref not in service_profiles:
            raise ValueError(
                "VehicleJourney '%s' refers to unknown Service '%s'."
                % (vehicle_journey_id, service_ref)
            )

        # Operating profile of the journey, completed from its service's
        profile = _inherit_profile(
            journey.operating_profile, service_profiles[service_ref]
        )

        # Select service journey patterns for given service id
        service_journey_patterns = service_jp_info.loc[
            service_jp_info["journey_pattern_id"] == journey_pattern_id
        ]
        if len(service_journey_patterns) == 0:
            raise ValueError(
                "VehicleJourney '%s' refers to unknown JourneyPattern '%s'."
                % (vehicle_journey_id, journey_pattern_id)
            )

        # Journey pattern sections of this journey, in order
        sections = [
            doc.journey_pattern_section(section_id)
            for section_id in doc.journey_pattern(journey_pattern_id).section_refs
        ]

        # Parse direction, line_name, travel mode, trip_headsign, vehicle_type,
        # agency_id
        cols = [
            "agency_id",
            "route_id",
            "direction_id",
            "direction",
            "line_name",
            "travel_mode",
            "trip_headsign",
            "vehicle_type",
            "start_date",
            "end_date",
        ]
        (
            agency_id,
            route_id,
            direction_id,
            direction,
            line_name,
            travel_mode,
            trip_headsign,
            vehicle_type,
            start_date,
            end_date,
        ) = service_journey_patterns[cols].values[0]

        # Ensure integer values
        direction_id = int(direction_id)
        travel_mode = int(travel_mode)

        # Operating period, weekdays and exceptions of the journey
        service_period = (start_date, end_date)
        calendar = journey_calendar(
            doc,
            calendars,
            profile,
            datetime.strptime(start_date, "%Y%m%d").date(),
            datetime.strptime(end_date, "%Y%m%d").date(),
        )
        if calendar is None:
            warnings.warn(
                "VehicleJourney '%s' never operates, skipping." % vehicle_journey_id,
                UserWarning,
                stacklevel=2,
            )
            continue
        start_date, end_date, weekdays, exceptions = calendar

        # Get departure time
        departure_seconds = parse_time(journey.departure_time)
        hour, minute = departure_seconds // 3600, departure_seconds % 3600 // 60

        if not sections:
            continue

        # Generate trip_id: single-section patterns keep the legacy id from the
        # section (same section might occur with different calendar info, hence
        # weekday info is attached); multi-section patterns get an id unique per
        # vehicle journey, since section lists are reusable
        if len(sections) == 1:
            trip_id = "%s_%s_%s%s" % (
                sections[0].id,
                weekdays,
                str(hour).zfill(2),
                str(minute).zfill(2),
            )
            # Journeys at the same time with different calendars or timing
            # overrides must not share a trip
            fingerprint = _journey_fingerprint(
                journey,
                departure_seconds,
                exceptions,
                (start_date, end_date),
                service_period,
            )
            if fingerprint:
                trip_id = "%s_%s" % (trip_id, exceptions_digest(fingerprint))
        else:
            trip_id = "%s_%s" % (service_ref, vehicle_journey_id)

        # Attributes shared by all stop_times rows of this journey
        journey_info = dict(
            agency_id=agency_id,
            trip_id=trip_id,
            route_id=route_id,
            vehicle_journey_id=vehicle_journey_id,
            service_ref=service_ref,
            direction_id=direction_id,
            direction=direction,
            line_name=line_name,
            travel_mode=travel_mode,
            trip_headsign=trip_headsign,
            vehicle_type=vehicle_type,
            start_date=start_date,
            end_date=end_date,
            weekdays=weekdays,
            exceptions=exceptions,
        )

        # Walk the timing links of all sections of the journey pattern in order
        timings = _link_timings(journey, sections)
        arrivals, departures = _stop_offsets(timings)
        # A row is the From stop of the link leaving it (with that link), the
        # last row the To stop of the last link, as before
        stop_ids = [t[0].from_stop for t in timings] + [timings[-1][0].to_stop]
        route_link_refs = [t[0].route_link_ref for t in timings]
        route_link_refs = route_link_refs + route_link_refs[-1:]
        for stop_num, stop_id in enumerate(stop_ids, start=1):
            info = dict(
                stop_id=stop_id,
                stop_sequence=stop_num,
                timepoint=1 if stop_num == 1 else 0,
                arrival_time=format_time(departure_seconds + arrivals[stop_num - 1]),
                departure_time=format_time(
                    departure_seconds + departures[stop_num - 1]
                ),
                route_link_ref=route_link_refs[stop_num - 1],
            )
            info.update(journey_info)
            rows.append(info)

    if not rows:
        warnings.warn(
            "%s: no vehicle journey operates, nothing to convert."
            % (doc.file_name or "TransXChange document"),
            UserWarning,
            stacklevel=2,
        )
        return pd.DataFrame(columns=GTFS_INFO_COLUMNS)
    gtfs_info = pd.DataFrame(rows)

    # Generate service_id column into the table
    gtfs_info = generate_service_id(gtfs_info)

    return gtfs_info


def get_gtfs_info(doc):
    """
    Get GTFS info from TransXChange elements.

    Info:
        - VehicleJourney element includes the departure time information
        - JourneyPatternRef element includes information about the trip_id
        - JourneyPatternSections include the leg duration information
        - ServiceJourneyPatterns include information about which
          JourneyPatternSections belong to a given VehicleJourney.

    GTFS fields - required/optional available from TransXChange - <fieldName>
    shows foreign keys between layers:
        - Stop_times: <trip_id>, arrival_time, departure_time, stop_id,
          stop_sequence, (+ optional: shape_dist_travelled, timepoint)
        - Trips: <route_id>, service_id, <trip_id>, (+ optional: trip_headsign,
          direction_id, trip_shortname)
        - Routes: <route_id>, agency_id, route_type, route_short_name,
          route_long_name
    """
    # Get all service journey pattern info
    service_jp_info = get_service_journey_pattern_info(doc)

    # Process
    return process_vehicle_journeys(doc, service_jp_info)


_DURATION = re.compile(
    r"^(?P<sign>-)?P(?:(?P<days>\d+(?:\.\d+)?)D)?"
    r"(?:T(?:(?P<hours>\d+(?:\.\d+)?)H)?(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?$"
)


def parse_runtime_duration(runtime):
    """
    Parse an ISO-8601 duration (PT1H2M3S, PT0S, PT0.5H, P1D) into whole seconds
    (rounded once, at the end). An empty or missing value is 0; anything else
    that is not a duration raises ValueError.
    """
    if runtime is None or runtime.strip() == "":
        return 0
    value = runtime.strip()
    match = _DURATION.match(value)
    if match is None:
        raise ValueError("Not an ISO-8601 duration: '%s'" % runtime)
    parts = match.groupdict()
    components = [parts[k] for k in ("days", "hours", "minutes", "seconds")]
    time_components = components[1:]
    if not any(components) or ("T" in value and not any(time_components)):
        raise ValueError("Not an ISO-8601 duration: '%s'" % runtime)
    # Only the lowest-order component present may carry a fraction
    present = [c for c in components if c is not None]
    if any("." in c for c in present[:-1]):
        raise ValueError("Not an ISO-8601 duration: '%s'" % runtime)
    # Exact arithmetic; rounded half up once, at the end
    total = (
        Fraction(parts["days"] or 0) * 86400
        + Fraction(parts["hours"] or 0) * 3600
        + Fraction(parts["minutes"] or 0) * 60
        + Fraction(parts["seconds"] or 0)
    )
    if parts["sign"] and total != 0:
        # Negative run and wait times occur in published data; the sign is a
        # known data error (UK2GTFS strips it too)
        warnings.warn(
            "Negative duration '%s' treated as %s." % (runtime, runtime.strip()[1:]),
            UserWarning,
            stacklevel=2,
        )
    return int(math.floor(total + Fraction(1, 2)))


_TIME = re.compile(r"^(?P<h>\d{2}):(?P<m>\d{2})(?::(?P<s>\d{2}(?:\.\d+)?))?$")


def parse_time(text):
    """
    Seconds since midnight of a HH:MM:SS (or HH:MM) time; fractional seconds
    are rounded half up. Anything else raises ValueError.
    """
    match = _TIME.match((text or "").strip())
    if match is None:
        raise ValueError("Not a time: '%s'" % text)
    hours, minutes = int(match.group("h")), int(match.group("m"))
    seconds = Fraction(match.group("s") or 0)
    if hours > 24 or minutes > 59 or seconds >= 60:
        raise ValueError("Not a time: '%s'" % text)
    return int(math.floor(hours * 3600 + minutes * 60 + seconds + Fraction(1, 2)))


def format_time(seconds):
    """GTFS time HH:MM:SS; hours run past 24 for trips crossing midnight"""
    seconds = int(round(seconds))
    return "%02d:%02d:%02d" % (seconds // 3600, seconds % 3600 // 60, seconds % 60)


def _require(value, what):
    if value is None:
        raise ValueError("%s is missing." % what)
    return value


def get_service_journey_pattern_info(doc):
    """Retrieve a DataFrame of all Journey Pattern info of services"""
    rows = []

    for service in doc.services:
        service_code = service.code
        what = "Service '%s'" % service_code

        # Service description (optional in 2.4 files; an internal column only)
        service_description = service.description or ""

        # Travel mode
        mode = get_mode(service.mode)

        # Line name
        if not service.lines:
            raise ValueError(what + " has no Line.")
        for line in service.lines:
            _require(line.name, "%s Line '%s' LineName" % (what, line.id))
        line_name = service.lines[0].name

        # Operator reference code
        agency_id = _require(
            service.registered_operator_ref, what + " RegisteredOperatorRef"
        )

        # Start and end date
        start = parse_date(_require(service.start_date, what + " StartDate"))
        end = service_end_date(doc, service, start)
        start_date, end_date = gtfs_date(start), gtfs_date(end)

        for jp in service.journey_patterns:

            # Journey pattern id
            journey_pattern_id = jp.id
            jp_what = "JourneyPattern '%s'" % journey_pattern_id

            # Section references (one row per journey pattern)
            if not jp.section_refs:
                raise ValueError(jp_what + " has no JourneyPatternSectionRefs.")
            section_ref = "|".join(jp.section_refs)

            # Direction
            raw_direction = _require(jp.direction, jp_what + " Direction")
            direction = get_direction(raw_direction)

            # Headsign
            if direction == 0:
                headsign = _require(service.origin, what + " Origin")
            else:
                headsign = _require(service.destination, what + " Destination")
            # Route Reference
            route_ref = _require(jp.route_ref, jp_what + " RouteRef")

            vehicle_type = jp.vehicle_type_code
            vehicle_description = jp.vehicle_type_description

            rows.append(
                dict(
                    journey_pattern_id=journey_pattern_id,
                    service_code=service_code,
                    agency_id=agency_id,
                    line_name=line_name,
                    travel_mode=mode,
                    service_description=service_description,
                    trip_headsign=headsign,
                    # Links to trips
                    jp_section_reference=section_ref,
                    direction_id=direction,
                    direction=raw_direction,
                    # Route_id linking to routes
                    route_id=route_ref,
                    vehicle_type=vehicle_type,
                    vehicle_description=vehicle_description,
                    start_date=start_date,
                    end_date=end_date,
                )
            )
    return pd.DataFrame(rows)
