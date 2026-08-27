import logging
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
from transx2gtfs.routes import get_mode, synthetic_route_ids
from transx2gtfs.stop_times import exceptions_digest, generate_service_id, get_direction

log = logging.getLogger("transx2gtfs")

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
    "frequency_start_time",
    "frequency_end_time",
    "frequency_headway_secs",
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


# Vehicle journeys
# ----------------


def resolve_vehicle_journey(doc, journey, _index=None, _memo=None):
    """
    A journey with a VehicleJourneyRef inherits everything it does not set from
    the referenced journey (transitively). Returns a resolved copy. ``_index``
    (code -> journey) and ``_memo`` (code -> resolved journey) let a conversion
    resolve every journey of a document once.
    """
    if journey.vehicle_journey_ref is None:
        return journey
    if _index is None:
        _index = {j.code: j for j in doc.vehicle_journeys}
    if _memo is None:
        _memo = {}
    # Follow the chain (without recursion) until a resolved or root journey
    chain = [journey]
    seen = {journey.code}
    current = journey
    while current.vehicle_journey_ref is not None and current.code not in _memo:
        ref = current.vehicle_journey_ref
        if ref in seen:
            raise ValueError(
                "VehicleJourney '%s' has a circular VehicleJourneyRef." % journey.code
            )
        current = _index.get(ref)
        if current is None:
            raise ValueError(
                "VehicleJourney '%s' refers to unknown VehicleJourney '%s'."
                % (chain[-1].code, ref)
            )
        seen.add(current.code)
        chain.append(current)
    resolved = _memo.get(chain[-1].code, chain[-1])
    _memo[chain[-1].code] = resolved
    for child in reversed(chain[:-1]):
        inherited = {}
        for name in (
            "journey_pattern_ref",
            "departure_time",
            "operating_profile",
            "line_ref",
            "operator_ref",
            "frequency",
        ):
            if getattr(child, name) is None:
                inherited[name] = getattr(resolved, name)
        inherited["timing_links"] = _merge_timing_links(
            resolved.timing_links, child.timing_links
        )
        resolved = dataclasses.replace(child, vehicle_journey_ref=None, **inherited)
        _memo[child.code] = resolved
    return resolved


def _merge_timing_links(parent_links, child_links):
    """Timing-link overrides of a journey on top of the referenced journey's:
    per link, the child's values win and its unset fields are inherited"""
    if not child_links:
        return parent_links
    merged = {tl.journey_pattern_timing_link_ref: tl for tl in parent_links}
    for child in child_links:
        ref = child.journey_pattern_timing_link_ref
        parent = merged.get(ref)
        if parent is not None:
            child = dataclasses.replace(
                child,
                run_time=child.run_time or parent.run_time,
                from_wait_time=child.from_wait_time or parent.from_wait_time,
                to_wait_time=child.to_wait_time or parent.to_wait_time,
            )
        merged[ref] = child
    return list(merged.values())


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
    journey, departure_seconds, exceptions, period, service_period, synthetic_route
):
    """What distinguishes same-minute journeys of one pattern: departure seconds,
    exceptions, a calendar clipped away from the service's period, a line-derived
    route, timing link overrides and frequency data; '' when none of them apply"""
    parts = []
    if departure_seconds % 60:
        parts.append("seconds:%d" % (departure_seconds % 60))
    if exceptions:
        parts.append(exceptions)
    if period != service_period:
        parts.append("period:%s:%s" % period)
    if synthetic_route:
        parts.append("route:%s" % synthetic_route)
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
    if journey.frequency is not None and journey.frequency.end_time:
        parts.append(
            "freq:%s:%s" % (journey.frequency.end_time, journey.frequency.interval)
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


def _stop_offsets(doc, timings):
    """
    Arrival and departure offsets (seconds after the journey's departure) of
    every stop of the journey: the first stop, then the To stop of every link.
    A stop's departure is its arrival plus the To wait of the link reaching it
    and the From wait of the link leaving it; the first stop departs at the
    journey's DepartureTime, so a From wait on the first link does not delay it.
    Stops reached by zero-run-time links get interpolated times.
    """
    n_links = len(timings)
    arrivals = [0] * (n_links + 1)
    departures = [0] * (n_links + 1)
    for k, (_link, run, _from_wait, to_wait) in enumerate(timings):
        arrivals[k + 1] = departures[k] + run
        next_from_wait = timings[k + 1][2] if k + 1 < n_links else 0
        departures[k + 1] = arrivals[k + 1] + to_wait + next_from_wait

    # Interpolate stops reached by zero-run-time links between their anchors,
    # by route link distance when every link of the run has one, else equally:
    # only movement time is distributed, waits at the interpolated stops are
    # added back so that times stay monotonic
    k = 0
    while k < n_links:
        if timings[k][1] != 0:
            k += 1
            continue
        first_zero = k
        while k < n_links and timings[k][1] == 0:
            k += 1
        # links first_zero..k-1 are zero; link k (if any) closes the run
        if k >= n_links:
            break
        anchor_start, anchor_end = first_zero, k + 1
        links = [timings[i][0] for i in range(anchor_start, anchor_end)]
        weights = [_distance(doc, link.route_link_ref) for link in links]
        if any(w is None or w <= 0 for w in weights):
            weights = [1.0] * len(links)
        # Exact relative weights (at most 1 each): no overflow, no rounding
        # until the seconds are rounded
        largest = max(weights)
        weights = [Fraction(w) / Fraction(largest) for w in weights]
        total_weight = sum(weights)
        interior = range(anchor_start + 1, anchor_end)
        waits = {stop: departures[stop] - arrivals[stop] for stop in interior}
        movement = arrivals[anchor_end] - departures[anchor_start] - sum(waits.values())
        cumulative_weight = Fraction(0)
        cumulative_wait = 0
        for offset, stop in enumerate(interior):
            cumulative_weight += weights[offset]
            arrivals[stop] = (
                departures[anchor_start]
                + cumulative_wait
                + round(movement * cumulative_weight / total_weight)
            )
            departures[stop] = arrivals[stop] + waits[stop]
            cumulative_wait += waits[stop]
        k += 1
    return arrivals, departures


def _distance(doc, route_link_ref):
    """Route link Distance as a finite float, or None"""
    if route_link_ref is None:
        return None
    text = doc.route_link_distances.get(route_link_ref)
    if text is None:
        return None
    try:
        value = float(text)
    except ValueError:
        return None
    return value if math.isfinite(value) else None


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
    services = {service.code: service for service in doc.services}
    service_profiles = _service_profiles(doc)
    calendars = _Calendars(doc)
    synthetic_ids = synthetic_route_ids(doc)
    journey_index = {j.code: j for j in vjourneys}
    resolved = {}

    # Container for gtfs_info rows
    rows = []

    # Iterate over VehicleJourneys
    for i, journey in enumerate(vjourneys):
        if i != 0 and i % 50 == 0:
            log.info("Processed %s / %s journeys.", i, journey_cnt)

        journey = resolve_vehicle_journey(doc, journey, journey_index, resolved)

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
        service = services[service_ref]
        if journey_pattern_id is None:
            raise ValueError(
                "VehicleJourney '%s' has no JourneyPatternRef." % vehicle_journey_id
            )
        if journey.departure_time is None:
            raise ValueError(
                "VehicleJourney '%s' has no DepartureTime." % vehicle_journey_id
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

        # The journey's line (several lines per service are possible)
        line = next((ln for ln in service.lines if ln.id == journey.line_ref), None)
        if journey.line_ref is not None and line is None:
            raise ValueError(
                "VehicleJourney '%s' refers to unknown Line '%s'."
                % (vehicle_journey_id, journey.line_ref)
            )
        if line is not None:
            line_name = line.name
        # A pattern without a (matching) Route: one route per service and line
        synthetic_route = None
        if route_id is None:
            line_id = line.id if line is not None else service.lines[0].id
            route_id = synthetic_ids[(service.code, line_id)]
            synthetic_route = route_id

        if journey.operator_ref not in (
            None,
            "",
            agency_id,
            service.registered_operator_ref,
        ):
            warnings.warn(
                "VehicleJourney '%s' names operator '%s'; the route keeps its "
                "service's operator '%s'."
                % (vehicle_journey_id, journey.operator_ref, agency_id),
                UserWarning,
                stacklevel=2,
            )

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
                synthetic_route,
            )
            if fingerprint:
                trip_id = "%s_%s" % (trip_id, exceptions_digest(fingerprint))
        else:
            trip_id = "%s_%s" % (service_ref, vehicle_journey_id)

        # Frequency-based journey; an EndTime not after the departure is on the
        # following day
        if journey.frequency is not None and journey.frequency.end_time:
            frequency_start = format_time(departure_seconds)
            end_seconds = parse_time(journey.frequency.end_time)
            if end_seconds <= departure_seconds:
                end_seconds += 24 * 3600
            frequency_end = format_time(end_seconds)
            interval = journey.frequency.interval or ""
            # The shared parser strips a negative sign; a headway must be positive
            frequency_headway = (
                0
                if interval.strip().startswith("-")
                else parse_runtime_duration(interval)
            )
            if frequency_headway <= 0:
                raise ValueError(
                    "VehicleJourney '%s' has no positive Frequency interval."
                    % vehicle_journey_id
                )
        else:
            frequency_start = frequency_end = frequency_headway = None

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
            frequency_start_time=frequency_start,
            frequency_end_time=frequency_end,
            frequency_headway_secs=frequency_headway,
        )

        # Walk the timing links of all sections of the journey pattern in order
        timings = _link_timings(journey, sections)
        arrivals, departures = _stop_offsets(doc, timings)
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


def _route_for_pattern(doc, jp):
    """
    RouteRef of the pattern, else the Route whose section list equals the
    sequence of route sections of the pattern's links (or, failing that, the
    only route whose list starts with that sequence); None when no route matches.
    """
    if jp.route_ref:
        return jp.route_ref
    sequence = []
    for section_id in jp.section_refs:
        for link in doc.journey_pattern_section(section_id).timing_links:
            route_section = doc.route_link_sections.get(link.route_link_ref)
            if route_section is None:
                # A link without a route link: the path is unknown, no matching
                return None
            if not sequence or sequence[-1] != route_section:
                sequence.append(route_section)
    if not sequence:
        return None
    for route in doc.routes:
        if route.route_section_refs == sequence:
            return route.id
    # A route the pattern covers a prefix of; ambiguity means unmatched
    prefixed = [
        route
        for route in doc.routes
        if route.route_section_refs[: len(sequence)] == sequence
    ]
    if len(prefixed) == 1:
        return prefixed[0].id
    return None


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
            # Route (None: synthesised per line when the journeys are processed)
            route_ref = _route_for_pattern(doc, jp)

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
