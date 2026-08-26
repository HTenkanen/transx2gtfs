import dataclasses
import warnings
from datetime import date, datetime, timedelta, time

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


def get_last_stop_time_info(
    link, hour, current_date, current_dt, duration, stop_num, boarding_time
):
    # Parse stop_id for TO
    stop_id = link.to_stop
    # Get arrival time for the last one
    current_dt = current_dt + timedelta(seconds=duration)
    departure_dt = current_dt + timedelta(seconds=boarding_time)
    # Get hour info
    arrival_hour = current_dt.hour
    departure_hour = departure_dt.hour
    # Ensure trips passing midnight are formatted correctly
    arrival_hour, departure_hour = get_midnight_formatted_times(
        arrival_hour, departure_hour, hour, current_date, current_dt, departure_dt
    )
    # Convert to string
    arrival_t = "{arrival_hour}:{minsecs}".format(
        arrival_hour=str(arrival_hour).zfill(2), minsecs=current_dt.strftime("%M:%S")
    )
    departure_t = "{departure_hour}:{minsecs}".format(
        departure_hour=str(departure_hour).zfill(2),
        minsecs=departure_dt.strftime("%M:%S"),
    )

    info = dict(
        stop_id=stop_id,
        stop_sequence=stop_num,
        arrival_time=arrival_t,
        departure_time=departure_t,
    )
    return info


def get_midnight_formatted_times(
    arrival_hour, departure_hour, hour, current_date, current_dt, departure_dt
):
    # If the arrival / departure hour is smaller than the initialized hour,
    # it means that the trip is extending to the next day. In that case,
    # the hour info should be extending to numbers over 24. E.g. if trip starts
    # at 23:30 and ends at 00:25, the arrival_time should be determined as 24:25
    # to avoid negative time hops.
    if arrival_hour < hour:
        # Calculate time delta (in hours) between the initial trip datetime and the
        # current and add 1 to hop over the midnight to the next day
        last_second_of_day = datetime.combine(current_date, time(23, 59, 59))
        arrival_over_midnight_surplus = (
            int(((current_dt - last_second_of_day) / 60 / 60).seconds) + 1
        )
        departure_over_midnight_surplus = (
            int(((departure_dt - last_second_of_day) / 60 / 60).seconds) + 1
        )

        # Update the hour values with midnight surplus
        arrival_hour = 23 + arrival_over_midnight_surplus
        departure_hour = 23 + departure_over_midnight_surplus

    return arrival_hour, departure_hour


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


def _journey_fingerprint(exceptions, period, service_period):
    """What distinguishes same-time journeys of one pattern: exceptions and a
    calendar clipped away from the service's period; '' when neither applies"""
    parts = []
    if exceptions:
        parts.append(exceptions)
    if period != service_period:
        parts.append("period:%s:%s" % period)
    return "|".join(parts)


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

    # Get current date for time reference
    current_date = datetime.now().date()

    # If additional boarding time is needed, specify it here
    # Boarding time in seconds
    boarding_time = 0

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
        departure_time = journey.departure_time
        hour, minute, second = departure_time.split(":")
        hour, minute, second = int(hour), int(minute), int(second)

        current_dt = None

        # Attributes shared by all stop_times rows of this journey
        journey_info = dict(
            agency_id=agency_id,
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
            # Journeys at the same time with different calendars must not
            # share a trip
            fingerprint = _journey_fingerprint(
                exceptions, (start_date, end_date), service_period
            )
            if fingerprint:
                trip_id = "%s_%s" % (trip_id, exceptions_digest(fingerprint))
        else:
            trip_id = "%s_%s" % (service_ref, vehicle_journey_id)

        # Walk the timing links of all sections of the journey pattern in order.
        # A link's From stop is reached after the run time of the *previous* link;
        # the link's own run time is applied to its To stop (the next link's From
        # stop, or the last stop of the trip).
        stop_num = 1
        previous_duration = 0
        for section in sections:
            for link in section.timing_links:

                # Get leg runtime code
                runtime = link.run_time

                # Parse duration in seconds
                duration = int(parse_runtime_duration(runtime=runtime))

                # Generate datetime for the start time
                if current_dt is None:
                    # On the first stop arrival and departure time should be identical
                    current_dt = datetime.combine(
                        current_date, time(int(hour), int(minute))
                    )
                    departure_dt = current_dt
                    # Timepoint
                    timepoint = 1

                else:
                    current_dt = current_dt + timedelta(seconds=previous_duration)

                    # Timepoint
                    timepoint = 0

                    departure_dt = current_dt + timedelta(seconds=boarding_time)

                # Get hour info
                arrival_hour = current_dt.hour
                departure_hour = departure_dt.hour

                # Ensure trips passing midnight are formatted correctly
                arrival_hour, departure_hour = get_midnight_formatted_times(
                    arrival_hour,
                    departure_hour,
                    hour,
                    current_date,
                    current_dt,
                    departure_dt,
                )

                # Convert to string
                arrival_t = "{arrival_hour}:{minsecs}".format(
                    arrival_hour=str(arrival_hour).zfill(2),
                    minsecs=current_dt.strftime("%M:%S"),
                )
                departure_t = "{departure_hour}:{minsecs}".format(
                    departure_hour=str(departure_hour).zfill(2),
                    minsecs=departure_dt.strftime("%M:%S"),
                )

                # Parse stop_id for FROM
                stop_id = link.from_stop

                # Route link reference
                route_link_ref = link.route_link_ref

                # Create gtfs_info row
                info = dict(
                    stop_id=stop_id,
                    stop_sequence=stop_num,
                    timepoint=timepoint,
                    arrival_time=arrival_t,
                    departure_time=departure_t,
                    route_link_ref=route_link_ref,
                    trip_id=trip_id,
                )
                info.update(journey_info)
                rows.append(info)

                # Update stop number
                stop_num += 1
                previous_duration = duration

        # After all timing links have been iterated over,
        # the last stop needs to be added separately
        info = get_last_stop_time_info(
            link, hour, current_date, current_dt, duration, stop_num, boarding_time
        )

        info["timepoint"] = 0
        info["route_link_ref"] = route_link_ref
        info["trip_id"] = trip_id
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


def parse_runtime_duration(runtime):
    """Parse duration information from TransXChange runtime code"""

    # Converters
    HOUR_IN_SECONDS = 60 * 60
    MINUTE_IN_SECONDS = 60

    time = 0
    runtime = runtime.split("PT")[1]

    if "H" in runtime:
        split = runtime.split("H")
        time = time + int(split[0]) * HOUR_IN_SECONDS
        runtime = split[1]
    if "M" in runtime:
        split = runtime.split("M")
        time = time + int(split[0]) * MINUTE_IN_SECONDS
        runtime = split[1]
    if "S" in runtime:
        split = runtime.split("S")
        time = time + int(split[0])
    return time


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
