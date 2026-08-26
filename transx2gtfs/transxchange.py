import pandas as pd
from datetime import datetime, timedelta, time
from transx2gtfs.calendar import get_weekday_info
from transx2gtfs.calendar_dates import get_calendar_dates_exceptions
from transx2gtfs.stop_times import generate_service_id, get_direction
from transx2gtfs.routes import get_mode

DEFAULT_WEEKDAYS = "MondayToSunday"
OPERATING_PERIOD_DEFAULT_DAYS = 365


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


def _service_profiles(doc):
    """OperatingProfile per service code, used when a journey has none"""
    return {service.code: service.operating_profile for service in doc.services}


def process_vehicle_journeys(doc, service_jp_info):
    """Build the stop_times-level GTFS info rows for every VehicleJourney"""

    vjourneys = doc.vehicle_journeys

    # Number of journeys to process
    journey_cnt = len(vjourneys)

    # Service-level operating profiles as fallback
    service_profiles = _service_profiles(doc)

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

        # Vehicle journey id ==> will be used to generate service_id
        # (identifies operative weekdays)
        vehicle_journey_id = journey.code

        # Operating profile of the journey, falling back to its service's
        if service_ref not in service_profiles:
            raise ValueError(
                "VehicleJourney '%s' refers to unknown Service '%s'."
                % (vehicle_journey_id, service_ref)
            )
        service_profile = service_profiles[service_ref]

        # A journey without any day information runs every day
        weekdays = get_weekday_info(journey.operating_profile)
        if weekdays is None:
            weekdays = get_weekday_info(service_profile) or DEFAULT_WEEKDAYS

        non_operative_days = get_calendar_dates_exceptions(journey.operating_profile)
        if non_operative_days is None:
            non_operative_days = get_calendar_dates_exceptions(service_profile)

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
            non_operative_days=non_operative_days,
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
