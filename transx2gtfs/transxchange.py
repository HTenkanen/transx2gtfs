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

    info = dict(stop_id=stop_id,
                stop_sequence=stop_num,
                arrival_time=arrival_t,
                departure_time=departure_t)
    return info


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
            print("Processed %s / %s journeys." % (i, journey_cnt))
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

        # Create gtfs_info row
        info = dict(vehicle_journey_id=vehicle_journey_id,
                    service_ref=service_ref,
                    journey_pattern_id=journey_pattern_id,
                    weekdays=weekdays,
                    non_operative_days=non_operative_days)

        # Merge into stop times
        gtfs_info = gtfs_info.append(info, ignore_index=True, sort=False)

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
    # Boarding time in seconds
    boarding_time = 0

    # Iterate over VehicleJourneys
    for i, journey in enumerate(vjourneys):
        if i != 0 and i % 50 == 0:
            print("Processed %s / %s journeys." % (i, journey_cnt))
        # Get service reference
        service_ref = journey.ServiceRef.cdata

        # Journey pattern reference
        journey_pattern_id = journey.JourneyPatternRef.cdata

        # Vehicle journey id ==> will be used to generate service_id (identifies operative weekdays)
        vehicle_journey_id = journey.VehicleJourneyCode.cdata

        # Parse weekday operation times from VehicleJourney
        weekdays = get_weekday_info(journey)

        # If weekday operation times were not available from VehicleJourney, use Services.Service
        if weekdays is None:
            weekdays = service_operative_days

        # Parse calendar dates (exceptions in operation)
        non_operative_days = get_calendar_dates_exceptions(journey)

        # If exceptions were not available try using information from Services.Service
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
        travel_mode = int(travel_mode)

        # Get departure time
        departure_time = journey.DepartureTime.cdata
        hour, minute, second = departure_time.split(':')
        hour, minute, second = int(hour), int(minute), int(second)

        current_dt = None

        # Iterate over a single departure section
        stop_num = 1
        for section in sections:

            # Section reference
            section_id = section.get_attribute('id')

            # Generate trip_id (same section id might occur with different calendar info,
            # hence attach weekday info as part of trip_id)
            trip_id = "%s_%s_%s%s" % (section_id, weekdays,
                                      str(hour).zfill(2),
                                      str(minute).zfill(2))

            # Check that section-ids match
            if not section_id in jp_section_references:
                continue

            timing_links = section.JourneyPatternTimingLink

            section_times = pd.DataFrame()

            # For the given departure section calculate arrival/departure times
            # for all possible trip departure times
            for link in timing_links:

                # Get leg runtime code
                runtime = link.RunTime.cdata

                # Parse duration in seconds
                duration = int(parse_runtime_duration(runtime=runtime))

                # Generate datetime for the start time
                if current_dt is None:
                    # On the first stop arrival and departure time should be identical
                    current_dt = datetime.combine(current_date, time(int(hour), int(minute)))
                    departure_dt = current_dt
                    # Timepoint
                    timepoint = 1

                else:
                    current_dt = current_dt + timedelta(seconds=duration)

                    # Timepoint
                    timepoint = 0

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

                # Parse stop_id for FROM
                stop_id = link.From.StopPointRef.cdata

                # Route link reference
                route_link_ref = link.RouteLinkRef.cdata

                # Create gtfs_info row
                info = dict(stop_id=stop_id,
                            stop_sequence=stop_num,
                            timepoint=timepoint,
                            # Duration between stops in seconds (not needed - keep here for reference)
                            # duration=duration,
                            arrival_time=arrival_t,
                            departure_time=departure_t,
                            route_link_ref=route_link_ref,
                            agency_id=agency_id,
                            trip_id=trip_id,
                            route_id=route_id,
                            vehicle_journey_id=vehicle_journey_id,
                            service_ref=service_ref,
                            direction_id=direction_id,
                            line_name=line_name,
                            travel_mode=travel_mode,
                            trip_headsign=trip_headsign,
                            vehicle_type=vehicle_type,
                            start_date=start_date,
                            end_date=end_date,
                            weekdays=weekdays,
                            non_operative_days=non_operative_days)
                section_times = section_times.append(info, ignore_index=True, sort=False)

                # Update stop number
                stop_num += 1

            # After timing links have been iterated over,
            # the last stop needs to be added separately
            info = get_last_stop_time_info(link, hour,
                                           current_date, current_dt,
                                           duration, stop_num, boarding_time)

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
            section_times = section_times.append(info, ignore_index=True, sort=False)

        # Add to GTFS DataFrame
        gtfs_info = gtfs_info.append(section_times, ignore_index=True, sort=False)

    # Generate service_id column into the table
    gtfs_info = generate_service_id(gtfs_info)

    return gtfs_info


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
    sections = data.TransXChange.JourneyPatternSections.JourneyPatternSection
    vjourneys = data.TransXChange.VehicleJourneys.VehicleJourney

    # Get all service journey pattern info
    service_jp_info = get_service_journey_pattern_info(data)

    # Get service operative days
    service_operative_days = get_service_operative_days_info(data)

    # Get service non-operative days
    service_non_operative_days = get_service_calendar_dates_exceptions(data)

    # Process
    gtfs_info = process_vehicle_journeys(vjourneys=vjourneys,
                                         service_jp_info=service_jp_info,
                                         sections=sections,
                                         service_operative_days=service_operative_days,
                                         service_non_operative_days=service_non_operative_days
                                         )

    return gtfs_info


def parse_runtime_duration(runtime):
    """Parse duration information from TransXChange runtime code"""

    # Converters
    HOUR_IN_SECONDS = 60 * 60
    MINUTE_IN_SECONDS = 60

    time = 0
    runtime = runtime.split("PT")[1]

    if 'H' in runtime:
        split = runtime.split("H")
        time = time + int(split[0]) * HOUR_IN_SECONDS
        runtime = split[1]
    if 'M' in runtime:
        split = runtime.split("M")
        time = time + int(split[0]) * MINUTE_IN_SECONDS
        runtime = split[1]
    if 'S' in runtime:
        split = runtime.split("S")
        time = time + int(split[0]) * MINUTE_IN_SECONDS
    return time


def get_service_journey_pattern_info(data):
    """Retrieve a DataFrame of all Journey Pattern info of services"""
    services = data.TransXChange.Services.Service

    service_jp_info = pd.DataFrame()

    for service in services:
        
        try:
            # Service description
            service_description = service.Description.cdata
        except:
            service_description = None

        # Travel mode
        mode = get_mode(service.Mode.cdata)

        # Line name
        line_name = service.Lines.Line.LineName.cdata

        try:
            # Service code
            service_code = service.ServiceCode.cdata
        except:
            service_code=None

        if service_code==None:
            continue

        # Operator reference code
        agency_id = service.RegisteredOperatorRef.cdata

        # Start and end date
        start_date = datetime.strftime(datetime.strptime(service.OperatingPeriod.StartDate.cdata, '%Y-%m-%d'), '%Y%m%d')
        end_date = datetime.strftime(datetime.strptime(service.OperatingPeriod.EndDate.cdata, '%Y-%m-%d'), '%Y%m%d')

        # Retrieve journey patterns
        journey_patterns = service.StandardService.JourneyPattern

        for jp in journey_patterns:

            # Journey pattern id
            journey_pattern_id = jp.get_attribute('id')

            # Section reference
            section_ref = jp.JourneyPatternSectionRefs.cdata

            # Direction
            direction = get_direction(jp.Direction.cdata)

            # Headsign
            if direction == 0:
                headsign = service.StandardService.Origin.cdata
            else:
                headsign = service.StandardService.Destination.cdata
            # Route Reference
            route_ref = jp.RouteRef.cdata

            try:
                # Vehicle type code
                vehicle_type = jp.Operational.VehicleType.VehicleTypeCode.cdata
            except:
                vehicle_type = None

            try:
                # Vechicle description
                vehicle_description = jp.Operational.VehicleType.Description.cdata
            except:
                vehicle_description = None

            # Create row
            row = dict(journey_pattern_id=journey_pattern_id,
                       service_code=service_code,
                       agency_id=agency_id,
                       line_name=line_name,
                       travel_mode=mode,
                       service_description=service_description,
                       trip_headsign=headsign,
                       # Links to trips
                       jp_section_reference=section_ref,
                       direction_id=direction,
                       # Route_id linking to routes
                       route_id=route_ref,
                       vehicle_type=vehicle_type,
                       vehicle_description=vehicle_description,
                       start_date=start_date,
                       end_date=end_date
                       )
            # Add to DataFrame
            service_jp_info = service_jp_info.append(row, ignore_index=True, sort=False)
    return service_jp_info
