import pandas as pd
from datetime import datetime, timedelta, time
from transx2gtfs.calendar import get_weekday_info, get_service_operative_days_info
from transx2gtfs.calendar_dates import get_calendar_dates_exceptions, get_service_calendar_dates_exceptions
from transx2gtfs.stop_times import generate_service_id, get_direction
from transx2gtfs.routes import get_mode


def get_last_stop_time_info(link, hour,
                            current_date, current_dt,
                            duration, stop_num, boarding_time):
    # Parse stop_id for TO
    stop_id = link.To.StopPointRef.cdata
    # Get arrival time for the last one
    current_dt = current_dt + timedelta(seconds=duration)
    departure_dt = current_dt + timedelta(seconds=boarding_time)
    # Get hour info
    arrival_hour = current_dt.hour
    departure_hour = departure_dt.hour
    # Ensure trips passing midnight are formatted correctly
    arrival_hour, departure_hour = get_midnight_formatted_times(arrival_hour, departure_hour,
                                                                hour, current_date, current_dt,
                                                                departure_dt)
    # Convert to string
    arrival_t = "{arrival_hour}:{minsecs}".format(arrival_hour=str(arrival_hour).zfill(2),
                                                  minsecs=current_dt.strftime("%M:%S"))
    departure_t = "{departure_hour}:{minsecs}".format(departure_hour=str(departure_hour).zfill(2),
                                                      minsecs=departure_dt.strftime("%M:%S"))

    return dict(
        stop_id=stop_id,
        stop_sequence=stop_num,
        arrival_time=arrival_t,
        departure_time=departure_t,
    )


def get_midnight_formatted_times(arrival_hour, departure_hour, hour, current_date, current_dt, departure_dt):
    # If the arrival / departure hour is smaller than the initialized hour,
    # it means that the trip is extending to the next day. In that case,
    # the hour info should be extending to numbers over 24. E.g. if trip starts
    # at 23:30 and ends at 00:25, the arrival_time should be determined as 24:25
    # to avoid negative time hops.
    if arrival_hour < hour:
        # Calculate time delta (in hours) between the initial trip datetime and the current
        # and add 1 to hop over the midnight to the next day
        last_second_of_day = datetime.combine(current_date, time(23, 59, 59))
        arrival_over_midnight_surplus = int(((current_dt - last_second_of_day) / 60 / 60).seconds) + 1
        departure_over_midnight_surplus = int(((departure_dt - last_second_of_day) / 60 / 60).seconds) + 1

        # Update the hour values with midnight surplus
        arrival_hour = 23 + arrival_over_midnight_surplus
        departure_hour = 23 + departure_over_midnight_surplus

    return arrival_hour, departure_hour

def process(vjourneys):
    """Process vehicle journeys"""
    # Number of journeys to process
    journey_cnt = len(vjourneys)

    # Container for gtfs_info
    gtfs_info = pd.DataFrame()

    # Iterate over VehicleJourneys
    for i, journey in enumerate(vjourneys):
        if i != 0 and i % 50 == 0:
            print(f"Processed {i} / {journey_cnt} journeys.")
        # Get service reference
        service_ref = journey.ServiceRef.cdata

        # Journey pattern reference
        journey_pattern_id = journey.JourneyPatternRef.cdata

        # Vehicle journey id ==> will be used to generate service_id (identifies operative weekdays)
        vehicle_journey_id = journey.VehicleJourneyCode.cdata

        # Parse weekday operation times from VehicleJourney
        weekdays = get_weekday_info(journey)

        # Parse calendar dates (exceptions in operation)
        non_operative_days = get_calendar_dates_exceptions(journey)

        # Create gtfs_info row como DataFrame directamente
        info = pd.DataFrame([{
            'vehicle_journey_id': vehicle_journey_id,
            'service_ref': service_ref,
            'journey_pattern_id': journey_pattern_id,
            'weekdays': weekdays,
            'non_operative_days': non_operative_days
        }])

        # Merge into stop times usando pd.concat
        gtfs_info = pd.concat([gtfs_info, info], ignore_index=True)

    return gtfs_info


def process_vehicle_journeys(vjourneys, service_jp_info, sections, service_operative_days, service_non_operative_days):
    """Process single vehicle journey instance"""

    # Number of journeys to process
    journey_cnt = len(vjourneys)

    # Container for gtfs_info
    gtfs_info = pd.DataFrame()

    # Get current date for time reference
    current_date = datetime.now().date()

    # If additional boarding time is needed, specify it here
    boarding_time = 0

    # Iterate over VehicleJourneys
    for i, journey in enumerate(vjourneys):
        if i != 0 and i % 50 == 0:
            print(f"Processed {i} / {journey_cnt} journeys.")

        # Get service reference
        service_ref = journey.ServiceRef.cdata

        # Journey pattern reference
        journey_pattern_id = journey.JourneyPatternRef.cdata

        # Vehicle journey id
        vehicle_journey_id = journey.VehicleJourneyCode.cdata

        # Parse weekday operation times from VehicleJourney
        weekdays = get_weekday_info(journey)
        if weekdays is None:
            weekdays = service_operative_days

        # Parse calendar dates (exceptions in operation)
        non_operative_days = get_calendar_dates_exceptions(journey)
        if non_operative_days is None:
            non_operative_days = service_non_operative_days

        # Select service journey patterns for given service id
        service_journey_patterns = service_jp_info.loc[service_jp_info['journey_pattern_id'] == journey_pattern_id]

        # Get Journey Pattern Section reference
        jp_section_references = service_journey_patterns['jp_section_reference'].to_list()

        # Parse direction, line_name, travel mode, trip_headsign, vehicle_type, agency_id
        cols = ['agency_id', 'route_id', 'direction_id', 'line_name',
                'travel_mode', 'trip_headsign', 'vehicle_type', 'start_date', 'end_date']
        agency_id, route_id, direction_id, line_name, travel_mode, \
        trip_headsign, vehicle_type, start_date, end_date = service_journey_patterns[cols].values[0]

        # Ensure integer values
        direction_id = int(direction_id)
        travel_mode = int(travel_mode) if travel_mode is not None else 3  # Default a 3 (Bus) si es None

        # Get departure time
        departure_time = journey.DepartureTime.cdata
        hour, minute, second = departure_time.split(':')
        hour, minute, second = int(hour), int(minute), int(second)

        current_dt = None

        # Iterate over a single departure section
        stop_num = 1
        for section in sections:
            section_id = section.get_attribute('id')
            trip_id = f"{section_id}_{weekdays}_{str(hour).zfill(2)}{str(minute).zfill(2)}"

            if section_id not in jp_section_references:
                continue

            timing_links = section.JourneyPatternTimingLink
            section_times = pd.DataFrame()

            for link in timing_links:
                runtime = link.RunTime.cdata
                duration = int(parse_runtime_duration(runtime=runtime))

                if current_dt is None:
                    current_dt = datetime.combine(current_date, time(hour, minute))
                    departure_dt = current_dt
                    timepoint = 1
                else:
                    current_dt = current_dt + timedelta(seconds=duration)
                    timepoint = 0
                    departure_dt = current_dt + timedelta(seconds=boarding_time)

                arrival_hour = current_dt.hour
                departure_hour = departure_dt.hour
                arrival_hour, departure_hour = get_midnight_formatted_times(arrival_hour, departure_hour,
                                                                           hour, current_date, current_dt,
                                                                           departure_dt)

                arrival_t = f"{str(arrival_hour).zfill(2)}:{current_dt.strftime('%M:%S')}"
                departure_t = f"{str(departure_hour).zfill(2)}:{departure_dt.strftime('%M:%S')}"

                stop_id = link.From.StopPointRef.cdata
                route_link_ref = link.RouteLinkRef.cdata

                info = pd.DataFrame([{
                    'stop_id': stop_id,
                    'stop_sequence': stop_num,
                    'timepoint': timepoint,
                    'arrival_time': arrival_t,
                    'departure_time': departure_t,
                    'route_link_ref': route_link_ref,
                    'agency_id': agency_id,
                    'trip_id': trip_id,
                    'route_id': route_id,
                    'vehicle_journey_id': vehicle_journey_id,
                    'service_ref': service_ref,
                    'direction_id': direction_id,
                    'line_name': line_name,
                    'travel_mode': travel_mode,
                    'trip_headsign': trip_headsign,
                    'vehicle_type': vehicle_type,
                    'start_date': start_date,
                    'end_date': end_date,
                    'weekdays': weekdays,
                    'non_operative_days': non_operative_days
                }])
                section_times = pd.concat([section_times, info], ignore_index=True)
                stop_num += 1

            # Último stop
            info = pd.DataFrame([get_last_stop_time_info(link, hour, current_date, current_dt, duration, stop_num, boarding_time)])
            info['timepoint'] = 0
            info['route_link_ref'] = route_link_ref
            info['agency_id'] = agency_id
            info['trip_id'] = trip_id
            info['route_id'] = route_id
            info['vehicle_journey_id'] = vehicle_journey_id
            info['service_ref'] = service_ref
            info['direction_id'] = direction_id
            info['line_name'] = line_name
            info['travel_mode'] = travel_mode
            info['trip_headsign'] = trip_headsign
            info['vehicle_type'] = vehicle_type
            info['start_date'] = start_date
            info['end_date'] = end_date
            info['weekdays'] = weekdays

            section_times = pd.concat([section_times, info], ignore_index=True)

        # Add to GTFS DataFrame
        gtfs_info = pd.concat([gtfs_info, section_times], ignore_index=True)

    # Generate service_id column into the table
    gtfs_info = generate_service_id(gtfs_info)

    return gtfs_info


import warnings

def get_gtfs_info(data):
    """
    Get GTFS info from TransXChange elements.

    Info:
        - VehicleJourney element includes the departure time information
        - JourneyPatternRef element includes information about the trip_id
        - JourneyPatternSections include the leg duration information
        - ServiceJourneyPatterns include information about which JourneyPatternSections belong to a given VehicleJourney.

    GTFS fields - required/optional available from TransXChange - <fieldName> shows foreign keys between layers:
        - Stop_times: <trip_id>, arrival_time, departure_time, stop_id, stop_sequence, (+ optional: shape_dist_travelled, timepoint)
        - Trips: <route_id>, service_id, <trip_id>, (+ optional: trip_headsign, direction_id, trip_shortname)
        - Routes: <route_id>, agency_id, route_type, route_short_name, route_long_name
    """
    # Access JourneyPatternSections and manage empty or multiple cases
    try:
        journey_sections = data.TransXChange.JourneyPatternSections
        if hasattr(journey_sections, 'JourneyPatternSection'):
            sections = journey_sections.JourneyPatternSection
            # If sections is only one element, convert to list for consistency
            if not isinstance(sections, list):
                sections = [sections]
        else:
            warnings.warn("Couldn't find JourneyPatternSection in JourneyPatternSections. Using empty list.")
            sections = []
    except AttributeError:
        warnings.warn("Couldn't find JourneyPatternSections in TransXChange file. Using empty list.")
        sections = []

    # Access VehicleJourneys and manage empty or multiple cases
    try:
        vehicle_journeys = data.TransXChange.VehicleJourneys
        if hasattr(vehicle_journeys, 'VehicleJourney'):
            vjourneys = vehicle_journeys.VehicleJourney
            # If vjourneys is only one element, convert to list for consistency
            if not isinstance(vjourneys, list):
                vjourneys = [vjourneys]
        else:
            warnings.warn("Couldn't find VehicleJourney in VehicleJourneys. Using empty list.")
            vjourneys = []
    except AttributeError:
        warnings.warn("Couldn't find VehicleJourneys in TransXChange file. Using empty list.")
        vjourneys = []

    # Get all service journey pattern info
    service_jp_info = get_service_journey_pattern_info(data)

    # Get service operative days
    service_operative_days = get_service_operative_days_info(data)

    # Get service non-operative days
    service_non_operative_days = get_service_calendar_dates_exceptions(data)

    return process_vehicle_journeys(
        vjourneys=vjourneys,
        service_jp_info=service_jp_info,
        sections=sections,
        service_operative_days=service_operative_days,
        service_non_operative_days=service_non_operative_days,
    )


def parse_runtime_duration(runtime):
    """Parse duration information from TransXChange runtime code"""

    # Converters
    HOUR_IN_SECONDS = 60 * 60
    MINUTE_IN_SECONDS = 60

    time = 0
    runtime = runtime.split("PT")[1]

    if 'H' in runtime:
        split = runtime.split("H")
        time += int(split[0]) * HOUR_IN_SECONDS
        runtime = split[1]
    if 'M' in runtime:
        split = runtime.split("M")
        time += int(split[0]) * MINUTE_IN_SECONDS
        runtime = split[1]
    if 'S' in runtime:
        split = runtime.split("S")
        time += int(split[0]) * MINUTE_IN_SECONDS
    return time


def get_service_journey_pattern_info(data):
    """Retrieve a DataFrame of all Journey Pattern info of services"""
    # Access services and manage empty or non-existent cases
    try:
        services_container = data.TransXChange.Services
        if hasattr(services_container, 'Service'):
            services = services_container.Service
            # If services is only one element, convert to list for consistency
            if not isinstance(services, list):
                services = [services]
        else:
            warnings.warn("Couldn't find Service elements in Services. Returning empty DataFrame.")
            return pd.DataFrame()
    except AttributeError:
        warnings.warn("Couldn't find Services element in TransXChange file. Returning empty DataFrame.")
        return pd.DataFrame()

    service_jp_info = pd.DataFrame()

    for service in services:
        # Service description (optional)
        service_description = getattr(service, 'Description', None)
        service_description = service_description.cdata if service_description else ""
        # Travel mode (optional)
        mode = getattr(service, 'Mode', None)
        mode = get_mode(mode.cdata) if mode else get_mode(None)
        # Line name - manage multiple lines
        lines = service.Lines.Line
        if isinstance(lines, list):
            # If there are multiple lines, take the first one
            line_name = lines[0].LineName.cdata if lines else ""
        else:
            # If there is only one line, take its name
            line_name = lines.LineName.cdata if hasattr(lines, 'LineName') else ""

        # Service code
        service_code = service.ServiceCode.cdata

        # Operator reference code
        agency_id = service.RegisteredOperatorRef.cdata

        # Start and end date
        start_date = datetime.strftime(datetime.strptime(service.OperatingPeriod.StartDate.cdata, '%Y-%m-%d'), '%Y%m%d')

        # End date (optional)
        end_date_obj = getattr(service.OperatingPeriod, 'EndDate', None)
        if end_date_obj:
            end_date = datetime.strftime(datetime.strptime(end_date_obj.cdata, '%Y-%m-%d'), '%Y%m%d')
        else:
            end_date = "20991231"  # Default value if not present

        # Retrieve journey patterns
        journey_patterns = service.StandardService.JourneyPattern
        if not isinstance(journey_patterns, list):
            journey_patterns = [journey_patterns]  # Convert to list if only one element

        for jp in journey_patterns:
            # Journey pattern id
            journey_pattern_id = jp.get_attribute('id')

            # Section reference
            section_refs = jp.JourneyPatternSectionRefs
            if isinstance(section_refs, list):
                if section_refs:
                    section_ref = section_refs[0].cdata  # Use the first reference
                    if len(section_refs) > 1:
                        warnings.warn(f"Multiple JourneyPatternSectionRefs found for journey_pattern_id {journey_pattern_id}. Using the first one.")
                else:
                    warnings.warn(f"No JourneyPatternSectionRefs found for journey_pattern_id {journey_pattern_id}. Using empty string.")
                    section_ref = ""
            else:
                section_ref = section_refs.cdata if hasattr(section_refs, 'cdata') else ""

            # Direction
            direction = get_direction(jp.Direction.cdata)

            # Headsign
            if direction == 0:
                headsign = service.StandardService.Origin.cdata
            else:
                headsign = service.StandardService.Destination.cdata

            # Route Reference
            route_ref = jp.RouteRef.cdata

            # Vehicle type code (optional)
            vehicle_type = None
            if hasattr(jp, 'Operational') and hasattr(jp.Operational, 'VehicleType'):
                vehicle_type = getattr(jp.Operational.VehicleType, 'VehicleTypeCode', None)
                if vehicle_type:
                    vehicle_type = vehicle_type.cdata

            # Vehicle description (optional)
            vehicle_description = None
            if hasattr(jp, 'Operational') and hasattr(jp.Operational, 'VehicleType'):
                vehicle_description = getattr(jp.Operational.VehicleType, 'Description', None)
                if vehicle_description:
                    vehicle_description = vehicle_description.cdata

            # Create row
            row = pd.DataFrame([{
                'journey_pattern_id': journey_pattern_id,
                'service_code': service_code,
                'agency_id': agency_id,
                'line_name': line_name,
                'travel_mode': mode,
                'service_description': service_description,
                'trip_headsign': headsign,
                'jp_section_reference': section_ref,
                'direction_id': direction,
                'route_id': route_ref,
                'vehicle_type': vehicle_type,
                'vehicle_description': vehicle_description,
                'start_date': start_date,
                'end_date': end_date
            }])

            service_jp_info = pd.concat([service_jp_info, row], ignore_index=True)

    return service_jp_info