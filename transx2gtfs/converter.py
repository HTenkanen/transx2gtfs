# -*- coding: utf-8 -*-
"""
Convert transXchange data format to GTFS format.

TODO:
    - Parallelize the JourneyPattern iteration into multiple threads
    - Modularize into class and optimize (now duplicate information generated in the first phase)

See Python reference (not maintained) for conversion from: https://github.com/adamlukemooney/txc2gtfs

This is a Java version: https://github.com/jpf18/TransXChange2GTFS/tree/master/src/transxchange2GoogleTransitHandler

The TransXChange model) has seven basic concepts: Service, Registration, Operator, Route,
StopPoint, JourneyPattern, and VehicleJourney.
    - A Service brings together the information about a registered bus service, and may contain
        two types of component service: Standard or Flexible; a mix of both types is allowed within a
        single Service.
    - A normal bus schedule is described by a StandardService and a Route. A Route describes
        the physical path taken by buses on the service as a set of route links.
    - A FlexibleService describes a bus service that does not have a fixed route, but only a
        catchment area or a few variable stops with no prescribed pattern of use.
    - A StandardService has one or more JourneyPattern elements to describe the common
        logical path of traversal of the stops of the Route as a sequence of timing links (see later),
        and one or more VehicleJourney elements, which describe individual scheduled journeys by
        buses over the Route and JourneyPattern at a specific time.
    - Both types of service have a registered Operator, who runs the service. Other associated
        operator roles can also be specified.
    - Route, JourneyPattern and VehicleJoumey follow a sequence of NaPTAN StopPoints. A
        Route specifies in effect an ordered list of StopPoints. A JourneyPattern specifies an
        ordered list of links between these points, giving relative times between each stop; a
        VehicleJourney follows the same list of stops at specific absolute passing times. (The
        detailed timing Link and elements that connect VehicleJourneys, JourneyPatterns etc to
        StopPoints are not shown in Figure 3-1). StopPoints may be grouped within StopAreas.
    - The StopPoints used in a JourneyPattern or Route are either declared locally or by
        referenced to an external definition using an AnnotatedStopRef
    - A Registration specifies the registration details for a service. It is mandatory in the
        registration schema.

Column Conversion table:

    +------------------------------+-------------+
    | TransXChange attribute       | GTFS column |
    +------------------------------+-------------+
    | JourneyPatternSectionRefs    | trip_id     |
    +------------------------------+-------------+
    | VehicleJourneyCode           | service_id  | --> is aggregated to remove duplicate information
    +------------------------------+-------------+
    | RouteSectionRef              | route_id    |
    +------------------------------+-------------+
    | JourneyPatternRouteReference | route_id    |
    +------------------------------+-------------+

Created on Fri Apr 26 16:52:57 2019

@author: Dr. Henrikki Tenkanen, University College London
"""
import untangle
import pandas as pd
from datetime import datetime, timedelta, time
from time import time as timeit
from zipfile import ZipFile, ZIP_DEFLATED
import pyproj
import sqlite3
import glob
import csv
from multiprocessing import cpu_count
import math
import multiprocessing
from transx2gtfs.data import get_path
import os


class Parallel:
    def __init__(self, input_files, file_size_limit, stops_fp, gtfs_db):
        self.input_files = input_files
        self.file_size_limit = file_size_limit
        self.stops_fp = stops_fp
        self.gtfs_db = gtfs_db


def download_naptan_stops(url="http://naptan.app.dft.gov.uk/datarequest/GTFS.ashx", local_path=None):
    """Download a fresh set of Naptan stop points, or use a locally downloaded NAPTAN stopset csv-file"""
    if local_path is not None:
        if os.path.exists(local_path):
            # Read csv
            stops = pd.read_csv(local_path, sep='\t')
            return stops
        else:
            raise ValueError("Could not find stop file at:", local_path)
    else:
        NotImplementedError("TODO: Downloading stops from NAPTAN website")


def get_stop_type(data, stop_id):
    """Returns the stop type according GTFS reference"""
    for p in data.TransXChange.StopPoints.StopPoint:
        if p.AtcoCode.cdata == stop_id:
            stype = p.StopClassification.StopType.cdata
            if stype in ['RPL', 'RPLY']:
                return 0
            elif stype == 'PLT':
                return 1
            elif stype in ['BCP', 'BCT', 'HAR', 'FLX', 'MRK', 'CUS']:
                return 3
            elif stype == 'FBT':
                return 4
            else:
                return 999


def get_trip_headsign(data, service_ref):
    """Parse trip headsign based on service reference id"""
    service = data.TransXChange.Services.Service
    if service.ServiceCode == service_ref:
        return service.Description.cdata
    else:
        raise ValueError("Could not find trip headsign for", service_ref)


def get_mode(mode):
    """Parse mode from TransXChange value"""
    if mode in ['tram', 'trolleyBus']:
        return 0
    elif mode in ['underground', 'metro']:
        return 1
    elif mode == 'rail':
        return 2
    elif mode in ['bus', 'coach']:
        return 3
    elif mode == 'ferry':
        return 4


def get_route_type(data):
    """Returns the route type according GTFS reference"""
    mode = data.TransXChange.Services.Service.Mode.cdata
    return get_mode(mode)


def get_direction(direction_id):
    """Return boolean direction id"""
    if direction_id == 'inbound':
        return 0
    elif direction_id == 'outbound':
        return 1
    else:
        raise ValueError("Cannot determine direction from %s" % direction_id)


def get_stop_info(data):
    """Returns stop sequence data for routes"""
    # Container
    stop_info = pd.DataFrame()

    # Direction flag
    direction_flag = None

    # Stop sequence
    stop_seq = 1

    # Get route sections
    route_sections = data.TransXChange.RouteSections.RouteSection
    for section in route_sections:
        route_links = section.RouteLink

        # Keep track of stop sequence
        for link in route_links:
            # Get direction
            direction = get_direction(link.Direction.cdata)
            # Track the changing direction and reset stop sequence
            if direction_flag != direction:
                direction_flag = direction
                stop_seq = 1

            # Get from stops
            from_stop = link.From.StopPointRef.cdata
            to_stop = link.To.StopPointRef.cdata
            # Distance
            if 'Distance' in link.__dict__.keys():
                dist = int(link.Distance.cdata)
            else:
                dist = 0

            # Parse info
            info = dict(direction_id=direction,
                        leg_dist=dist,
                        from_stop_id=from_stop,
                        to_stop_id=to_stop,
                        stop_sequence=stop_seq)
            stop_info = stop_info.append(info, ignore_index=True, sort=False)

            # Update stop_seq
            stop_seq += 1
    # Ensure data types
    stop_info['stop_sequence'] = stop_info['stop_sequence'].astype(int)
    stop_info['direction_id'] = stop_info['direction_id'].astype(int)

    return stop_info


def get_agency_url(operator_code):
    """Get url for operators"""
    operator_urls = {
        'OId_LUL': "https://tfl.gov.uk/maps/track/tube",
        'OId_DLR': "https://tfl.gov.uk/modes/dlr/",
        'OId_CRC': "https://www.crownrivercruise.co.uk/",
        'OId_TRS': "https://www.thamesriverservices.co.uk/",
        'OId_CCR': "https://www.citycruises.com/",
        'OId_CV': "https://www.thamesclippers.com/",
        'OId_WFF': "https://tfl.gov.uk/modes/river/woolwich-ferry",
        'OId_TCL': "https://tfl.gov.uk/modes/trams/",
        'OId_EAL': "https://www.emiratesairline.co.uk/"
    }
    try:
        url = operator_urls[operator_code]
        return url
    except:
        return "NA"


def get_stops(data, naptan_stops_fp=None):
    """Parse stop data from TransXchange elements"""

    # Helper projections for transformations
    # Define the projection
    # The .srs here returns the Proj4-string presentation of the projection
    wgs84 = pyproj.Proj("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")
    osgb36 = pyproj.Proj(
        "+proj=tmerc +lat_0=49 +lon_0=-2 +k=0.999601 +x_0=400000 +y_0=-100000 +ellps=airy +towgs84=446.448,-125.157,542.060,0.1502,0.2470,0.8421,-20.4894 +units=m +no_defs <>")

    # Attributes
    _stop_id_col = 'stop_id'

    # Container
    stop_data = pd.DataFrame()

    # Get stop database
    naptan_stops = download_naptan_stops(local_path=naptan_stops_fp)

    # Iterate over stop points
    for p in data.TransXChange.StopPoints.StopPoint:
        # Name of the stop
        stop_name = p.Descriptor.CommonName.cdata

        # Ensure that stop_name does not have troublesome characters (for CSV export)
        # TODO: Implement this

        # Stop_id
        stop_id = p.AtcoCode.cdata

        # Get stop info
        stop = naptan_stops.loc[naptan_stops[_stop_id_col] == stop_id]

        # If NAPTAN db does not contain the info, try to parse from the data directly
        if len(stop) != 1:
            # print("Could not find stop_id '%s' from Naptan database. Using coordinates directly from TransXChange." % stop_id)
            # X and y coordinates - Notice: these might not be available! --> Use NAPTAN database
            # Spatial reference - TransXChange might use:
            #   - OSGB36 (epsg:7405) spatial reference: https://spatialreference.org/ref/epsg/osgb36-british-national-grid-odn-height/
            #   - WGS84 (epsg:4326)
            # Detected epsg
            detected_epsg = None
            x = float(p.Place.Location.Easting.cdata)
            y = float(p.Place.Location.Northing.cdata)

            # Detect the most probable CRS at the first iteration
            if detected_epsg is None:
                # Check if the coordinates are in meters
                if x > 180:
                    detected_epsg = 7405
                else:
                    detected_epsg = 4326

            # Convert point coordinates to WGS84 if they are in OSGB36
            if detected_epsg == 7405:
                x, y = pyproj.transform(p1=osgb36, p2=wgs84, x=x, y=y)

            # Get vehicle type
            vehicle_type = get_stop_type(data, stop_id=stop_id)

            # Create row
            stop = dict(stop_id=stop_id,
                        stop_code=None,
                        stop_name=stop_name,
                        stop_lat=y,
                        stop_lon=x,
                        stop_url=None,
                        vehicle_type=vehicle_type
                        )

        # Add to container
        stop_data = stop_data.append(stop, ignore_index=True, sort=False)

    # Ensure correct data types
    try:
        stop_data['vehicle_type'] = stop_data['vehicle_type'].astype(int)
    except:
        # Fill NaN values if they exist
        stop_data['vehicle_type'] = stop_data['vehicle_type'].fillna(999)
        stop_data['vehicle_type'] = stop_data['vehicle_type'].astype(int)

    return stop_data


def get_route_links(data):
    """Get route section information from TransXChange elements"""
    route_links = pd.DataFrame()

    # Iterate over route-links
    for rs in data.TransXChange.RouteSections.RouteSection:

        # Get route section code
        route_section_code = rs.get_attribute('id')

        rlinks = rs.RouteLink
        for rl in rlinks:
            direction = get_direction(rl.Direction.cdata)
            try:
                distance = int(rl.Distance.cdata)
            except:
                distance = None
            # Get route link code
            route_link_code = rl.get_attribute('id')

            # Get route link stop_id
            rl_stop_id = rl.From.StopPointRef.cdata

            # Generate row
            rlink = dict(route_section_id=route_section_code,
                         route_link_ref=route_link_code,
                         direction_id=direction,
                         distance=distance,
                         rl_stop_id=rl_stop_id)
            route_links = route_links.append(rlink, ignore_index=True, sort=False)
    return route_links


def get_routes(gtfs_info, data):
    """Get routes from TransXchange elements"""
    # Columns to use in output
    use_cols = ['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type']

    routes = pd.DataFrame()

    for r in data.TransXChange.Routes.Route:
        # Get route id
        route_id = r.get_attribute('id')

        # Get agency_id
        agency_id = gtfs_info.loc[gtfs_info['route_id'] == route_id, 'agency_id'].unique()[0]

        # Get route long name
        route_long_name = r.Description.cdata

        # Get route private id
        route_private_id = r.PrivateCode.cdata

        # Get route short name (test '-_-' separator)
        route_short_name = route_private_id.split('-_-')[0]

        # Route Section reference (might be needed somewhere)
        route_section_id = r.RouteSectionRef.cdata

        # Get route_type
        route_type = get_route_type(data)

        # Generate row
        route = dict(route_id=route_id,
                     agency_id=agency_id,
                     route_private_id=route_private_id,
                     route_long_name=route_long_name,
                     route_short_name=route_short_name,
                     route_type=route_type,
                     route_section_id=route_section_id
                     )
        routes = routes.append(route, ignore_index=True, sort=False)

    # Ensure that route type is integer
    routes['route_type'] = routes['route_type'].astype(int)

    # Select only required columns
    routes = routes[use_cols].copy()
    return routes


def get_agency(data):
    """Parse agency information from TransXchange elements"""
    # Container
    agency_data = pd.DataFrame()

    # Agency id
    # agency_id = data.TransXChange.Operators.Operator.OperatorCode.cdata
    agency_id = data.TransXChange.Operators.Operator.get_attribute('id')

    # Agency name
    agency_name = data.TransXChange.Operators.Operator.OperatorNameOnLicence.cdata

    # Agency url
    agency_url = get_agency_url(agency_id)

    # Agency timezone
    agency_tz = "Europe/London"

    # Agency langunage
    agency_lang = "en"

    # Parse row
    agency = dict(agency_id=agency_id,
                  agency_name=agency_name,
                  agency_url=agency_url,
                  agency_timezone=agency_tz,
                  agency_lang=agency_lang)

    agency_data = agency_data.append(agency, ignore_index=True, sort=False)
    return agency_data


def get_service_operative_days_info(data):
    """
    Get operating profile information from Services.Service.

    This is used if VehicleJourney does not contain the information.
    """
    try:
        reg_weekdays = data.TransXChange.Services.Service.OperatingProfile.RegularDayType.DaysOfWeek.get_elements()
        weekdays = []
        for elem in reg_weekdays:
            weekdays.append(elem._name)
        if len(weekdays) == 1:
            return weekdays[0]
        else:
            return "|".join(weekdays)
    except:
        # If service does not have OperatingProfile available, return None
        return None


def get_weekday_info(vehicle_journey_element):
    """Parses weekday info from TransXChange VehicleJourney element"""
    j = vehicle_journey_element
    try:
        reg_weekdays = j.OperatingProfile.RegularDayType.DaysOfWeek.get_elements()
        weekdays = []
        for elem in reg_weekdays:
            weekdays.append(elem._name)
        if len(weekdays) == 1:
            return weekdays[0]
        else:
            return "|".join(weekdays)
    except:
        # If journey does not have OperatingProfile available, return None
        return None


def get_service_calendar_dates_exceptions(data):
    """Parses calendar dates exception info from TransXChange VehicleJourney element"""
    try:
        non_operative_days = data.TransXChange.Services.Service.OperatingProfile.BankHolidayOperation.DaysOfNonOperation.get_elements()
        weekdays = []
        for elem in non_operative_days:
            weekdays.append(elem._name)
        if len(weekdays) == 1:
            return weekdays[0]
        else:
            return "|".join(weekdays)
    except:
        return None


def get_calendar_dates_exceptions(vehicle_journey_element):
    """Parses calendar dates exception info from TransXChange VehicleJourney element"""
    j = vehicle_journey_element
    try:
        non_operative_days = j.OperatingProfile.BankHolidayOperation.DaysOfNonOperation.get_elements()
        weekdays = []
        for elem in non_operative_days:
            weekdays.append(elem._name)
        if len(weekdays) == 1:
            return weekdays[0]
        else:
            return "|".join(weekdays)
    except:
        return None


def generate_service_id(stop_times):
    """Generate service_id into stop_times DataFrame"""

    # Create column for service_id
    stop_times['service_id'] = None

    # Parse calendar info
    calendar_info = stop_times.drop_duplicates(subset=['vehicle_journey_id'])

    # Group by weekdays
    calendar_groups = calendar_info.groupby('weekdays')

    # Iterate over groups and create a service_id
    for weekday, cgroup in calendar_groups:
        # Parse all vehicle journey ids
        vehicle_journey_ids = cgroup['vehicle_journey_id'].to_list()

        # Parse other items
        service_ref = cgroup['service_ref'].unique()[0]
        daygroup = cgroup['weekdays'].unique()[0]
        start_d = cgroup['start_date'].unique()[0]
        end_d = cgroup['end_date'].unique()[0]

        # Generate service_id
        service_id = "%s_%s_%s_%s" % (service_ref, start_d, end_d, daygroup)

        # Update stop_times service_id
        stop_times.loc[stop_times['vehicle_journey_id'].isin(vehicle_journey_ids), 'service_id'] = service_id
    return stop_times


def create_workers(input_files, stops_fp=None, gtfs_db=None, file_size_limit=1000):
    """Create workers for multiprocessing"""

    # Distribute the process into all cores
    core_cnt = cpu_count()

    # File count
    file_cnt = len(input_files)

    # Batch size
    batch_size = math.ceil(file_cnt / core_cnt)

    # Create journey workers
    workers = []
    start_i = 0
    end_i = batch_size

    for i in range(0, core_cnt):
        # On the last iteration ensure that all the rest will be added
        if i == core_cnt - 1:
            # Slice the list
            selection = input_files[start_i:]
        else:
            # Slice the list
            selection = input_files[start_i:end_i]

            print(start_i, end_i)

        workers.append(Parallel(input_files=selection, file_size_limit=file_size_limit,
                                stops_fp=stops_fp, gtfs_db=gtfs_db))

        # Update indices
        start_i += batch_size
        end_i += batch_size
    return workers


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

        # Get departure time
        departure_time = journey.DepartureTime.cdata
        hour, minute, second = departure_time.split(':')
        hour, minute, second = int(hour), int(minute), int(second)

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

        # Container for timing info
        journey_info = pd.DataFrame()

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

                    # If additional boarding time is needed, specify it here
                    # Boarding time in seconds
                    boarding_time = 0

                    departure_dt = current_dt + timedelta(seconds=boarding_time)

                # Get hour info
                arrival_hour = current_dt.hour
                departure_hour = departure_dt.hour

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

                    # Convert to string
                arrival_t = "{arrival_hour}:{minsecs}".format(arrival_hour=arrival_hour,
                                                              minsecs=current_dt.strftime("%M:%S"))
                departure_t = "{departure_hour}:{minsecs}".format(departure_hour=departure_hour,
                                                                  minsecs=departure_dt.strftime("%M:%S"))

                # Parse stop_id for FROM
                stop_id = link.From.StopPointRef.cdata

                # Parse stop sequence number (original TransXChange one - not used but kept here for reference)
                # orig_stop_seq = int(link.From.get_attribute('SequenceNumber'))

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

            # Add to journey DataFrame
            journey_info = journey_info.append(section_times, ignore_index=True, sort=False)

        # Merge into stop times
        gtfs_info = gtfs_info.append(journey_info, ignore_index=True, sort=False)

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

        # Service description
        service_description = service.Description.cdata

        # Travel mode
        mode = get_mode(service.Mode.cdata)

        # Line name
        line_name = service.Lines.Line.LineName.cdata

        # Service code
        service_code = service.ServiceCode.cdata

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


def get_trips(gtfs_info):
    """Extract trips attributes from GTFS info DataFrame"""
    trip_cols = ['route_id', 'service_id', 'trip_id', 'trip_headsign', 'direction_id']

    # Extract trips from GTFS info
    trips = gtfs_info.drop_duplicates(subset=['route_id', 'service_id', 'trip_id'])
    trips = trips[trip_cols].copy()
    trips = trips.reset_index(drop=True)

    # Ensure correct data types
    trips['direction_id'] = trips['direction_id'].astype(int)

    return trips


def get_stop_times(gtfs_info):
    """Extract stop_times attributes from GTFS info DataFrame"""
    stop_times_cols = ['trip_id', 'arrival_time', 'departure_time', 'stop_id', 'stop_sequence', 'timepoint']

    # Select columns
    stop_times = gtfs_info[stop_times_cols].copy()

    # Drop duplicates (there should not be any but make sure)
    stop_times = stop_times.drop_duplicates()

    # Ensure correct data types
    int_types = ['stop_sequence', 'timepoint']
    for col in int_types:
        stop_times[col] = stop_times[col].astype(int)

    # If there is only a single sequence for a trip, do not export it
    grouped = stop_times.groupby('trip_id')
    filtered_stop_times = pd.DataFrame()
    for idx, group in grouped:
        if len(group) > 1:
            filtered_stop_times = filtered_stop_times.append(group, ignore_index=True, sort=False)
        else:
            print("Trip '%s' does not include a sequence of stops, excluding from GTFS." % idx)

    return filtered_stop_times


def parse_day_range(dayinfo):
    """Parse day range from TransXChange DayOfWeek element"""
    # Converters
    weekday_to_num = {'monday': 0, 'tuesday': 1, 'wednesday': 2,
                      'thursday': 3, 'friday': 4, 'saturday': 5,
                      'sunday': 6}
    num_to_weekday = {0: 'monday', 1: 'tuesday', 2: 'wednesday',
                      3: 'thursday', 4: 'friday', 5: 'saturday',
                      6: 'sunday'}

    # Containers
    active_days = []
    day_info = pd.DataFrame()

    # Process 'weekend'
    if "weekend" in dayinfo.strip().lower():
        active_days.append('saturday')
        active_days.append('sunday')

    # Check if dayinfo is specified as day-range
    elif "To" in dayinfo:
        day_range = dayinfo.split('To')
        start_i = weekday_to_num[day_range[0].lower()]
        end_i = weekday_to_num[day_range[1].lower()]

        # Get days when the service is active
        for idx in range(start_i, end_i + 1):
            # Get days
            active_days.append(idx)

    # Process a collection of individual weekdays
    elif "|" in dayinfo:
        days = dayinfo.split('|')
        for day in days:
            active_days.append(weekday_to_num[day.lower()])

    # If input is only a single day
    else:
        active_days.append(weekday_to_num[dayinfo.lower()])

    # Generate calendar row
    row = {}
    # Create columns
    for daynum in range(0, 7):
        # Get day column
        daycol = num_to_weekday[daynum]

        # Check if service is operative or not
        if daynum in active_days:
            active = 1
        else:
            active = 0
        row[daycol] = active

    # Generate DF
    day_info = day_info.append(row, ignore_index=True, sort=False)
    return day_info


def get_calendar(gtfs_info):
    """Parse calendar attributes from GTFS info DataFrame"""
    # Parse calendar
    use_cols = ['service_id', 'weekdays', 'start_date', 'end_date']
    calendar = gtfs_info.drop_duplicates(subset=use_cols)
    calendar = calendar[use_cols].copy()
    calendar = calendar.reset_index(drop=True)

    # Container for final results
    gtfs_calendar = pd.DataFrame()

    # Parse weekday columns
    for idx, row in calendar.iterrows():
        # Get dayinfo
        dayinfo = row['weekdays']

        # Parse day information
        dayrow = parse_day_range(dayinfo)

        # Add service and operation range info
        dayrow['service_id'] = row['service_id']
        dayrow['start_date'] = row['start_date']
        dayrow['end_date'] = row['end_date']

        # Add to container
        gtfs_calendar = gtfs_calendar.append(dayrow, ignore_index=True, sort=False)

    # Fix column order
    col_order = ['service_id', 'monday', 'tuesday', 'wednesday',
                 'thursday', 'friday', 'saturday', 'sunday',
                 'start_date', 'end_date']
    gtfs_calendar = gtfs_calendar[col_order].copy()

    # Ensure correct datatypes
    int_types = ['monday', 'tuesday', 'wednesday',
                 'thursday', 'friday', 'saturday', 'sunday']
    for col in int_types:
        gtfs_calendar[col] = gtfs_calendar[col].astype(int)

    return gtfs_calendar


def get_bank_holiday_dates(gtfs_info, bank_holidays_region='england-and-wales'):
    """
    Retrieve information about UK bank holidays that are during the feed operative period.

    Available regions: 'england-and-wales', 'scotland', 'northern-ireland'
    """
    available_regions = ['england-and-wales', 'scotland', 'northern-ireland']
    assert bank_holidays_region in available_regions, "You need to use one of the following regions: %s" % available_regions

    # Get bank holidays from UK Gov
    bank_holidays_url = "https://www.gov.uk/bank-holidays.json"

    # Read data from URL by default
    try:
        bholidays = pd.read_json(bank_holidays_url)
    # If url is unreachable use static file from the package
    except:
        print("Could not read bank holidays via Internet, using static file instead.")
        bholidays = pd.read_json("data/bank-holidays.json")

    # Get bank holidays for specified region
    bank_holidays = pd.DataFrame(bholidays.loc['events', bank_holidays_region])

    # Make datetime from date and make index
    bank_holidays['dt'] = pd.to_datetime(bank_holidays['date'], infer_datetime_format=True)
    bank_holidays = bank_holidays.set_index('dt', drop=False)

    # Get start and end date of the GTFS feed
    start_date_min = datetime.strptime(gtfs_info['start_date'].min(), "%Y%m%d")
    end_date_max = datetime.strptime(gtfs_info['end_date'].max(), "%Y%m%d")

    # Select bank holidays that fit the time range
    selected_bank_holidays = bank_holidays[start_date_min:end_date_max]

    # Check if there were any bank holidays during the feed time
    if len(selected_bank_holidays) == 0:
        return None

    # If there are return get the dates
    dates = selected_bank_holidays['dt'].to_list()
    # Parse in GTFS date format
    gtfs_dates = [date.strftime("%Y%m%d") for date in dates]

    return gtfs_dates


def get_calendar_dates(gtfs_info, bank_holidays_region='england-and-wales'):
    """
    Parse calendar dates attributes from GTFS info DataFrame.

    TransXChange typically indicates exception in operation using 'AllBankHolidays' as an attribute.
    Hence, Bank holiday information is retrieved from "https://www.gov.uk/" site that should keep the data up-to-date.
    If the file (or internet) is not available, a static version of the same file will be used that is bundled with the package.

    There are different bank holidays in different regions in UK. Hence, you can (and should) define
    the region with <bank_holidays_region> -parameter.
    Available regions are: 'england-and-wales', 'scotland', 'northern-ireland'

    """
    # Known exceptions and their counterparts in bankholiday table
    known_holidays = {'SpringBank': 'Spring bank holiday',
                      'LateSummerBankHolidayNotScotland': 'Summer bank holiday',
                      'MayDay': 'Early May bank holiday',
                      'GoodFriday': 'Good Friday'}

    # Get initial info about non-operative days
    non_operative_values = list(gtfs_info['non_operative_days'].unique())

    # Container for all info
    non_operatives = []

    # Parse all non operative ones
    for info in non_operative_values:
        # Check if info consists of multiple values
        if (info is not None) and "|" in info:
            split = info.split('|')
            non_operatives += split
        else:
            # Add individual value
            if info is not None and info != '':
                non_operatives.append(info)

    # Remove duplicates
    non_operatives = list(set(non_operatives))

    # Check if there exists some exceptions that are not known bank holidays
    for holiday in non_operatives:
        if (holiday not in known_holidays.keys()) and (holiday != 'AllBankHolidays'):
            raise NotImplementedError("There were also other exceptions than typical BankHolidays:", holiday)

    if len(non_operatives) > 0:
        # Get bank holidays that are during the operative period of the feed (returns None if they do not exist)
        bank_holidays = get_bank_holiday_dates(gtfs_info, bank_holidays_region=bank_holidays_region)
    else:
        return None

    # Return None if no bank holiday happens to be during the operative period
    if bank_holidays is None:
        return None

    # Otherwise produce calendar_dates data

    # Select distinct (service_id) rows that have bank holiday determined
    calendar_info = gtfs_info[['service_id', 'non_operative_days']].copy()
    calendar_info = calendar_info.drop_duplicates(subset=['service_id'])

    # Create columns for date and exception_type
    calendar_info['date'] = None

    # The exception will always be indicating non-operative service (value 2)
    calendar_info['exception_type'] = 2

    # Container for calendar_dates
    calendar_dates = pd.DataFrame()

    # Iterate over services and produce rows having exception with given bank holiday dates
    for idx, row in calendar_info.iterrows():
        # Iterate over exception dates
        for date in bank_holidays:
            # Generate row
            row = dict(service_id=row['service_id'],
                       date=date,
                       exception_type=row['exception_type'])
            # Add to container
            calendar_dates = calendar_dates.append(row, ignore_index=True, sort=False)

    # Ensure correct datatype
    calendar_dates['exception_type'] = calendar_dates['exception_type'].astype(int)

    return calendar_dates


def generate_gtfs_export(gtfs_db_fp):
    """Reads the gtfs database and generates an export dictionary for GTFS"""
    # Initialize connection
    conn = sqlite3.connect(gtfs_db_fp)

    # Read database and produce the GTFS file
    # =======================================

    # Stops
    # -----
    stops = pd.read_sql_query("SELECT * FROM stops", conn)
    if 'index' in stops.columns:
        stops = stops.drop('index', axis=1)

    # Drop duplicates based on stop_id
    stops = stops.drop_duplicates(subset=['stop_id'])

    # Agency
    # ------
    agency = pd.read_sql_query("SELECT * FROM agency", conn)
    if 'index' in agency.columns:
        agency = agency.drop('index', axis=1)
    # Drop duplicates
    agency = agency.drop_duplicates(subset=['agency_id'])

    # Routes
    # ------
    routes = pd.read_sql_query("SELECT * FROM routes", conn)
    if 'index' in routes.columns:
        routes = routes.drop('index', axis=1)
    # Drop duplicates
    routes = routes.drop_duplicates(subset=['route_id'])

    # Trips
    # -----
    trips = pd.read_sql_query("SELECT * FROM trips", conn)
    if 'index' in trips.columns:
        trips = trips.drop('index', axis=1)

    # Drop duplicates
    trips = trips.drop_duplicates(subset=['trip_id'])

    # Stop_times
    # ----------
    stop_times = pd.read_sql_query("SELECT * FROM stop_times", conn)
    if 'index' in stop_times.columns:
        stop_times = stop_times.drop('index', axis=1)

    # Drop duplicates
    stop_times = stop_times.drop_duplicates()

    # Calendar
    # --------
    calendar = pd.read_sql_query("SELECT * FROM calendar", conn)
    if 'index' in calendar.columns:
        calendar = calendar.drop('index', axis=1)
    # Drop duplicates
    calendar = calendar.drop_duplicates(subset=['service_id'])

    # Calendar dates
    # --------------
    try:
        calendar_dates = pd.read_sql_query("SELECT * FROM calendar_dates", conn)
        if 'index' in calendar_dates.columns:
            calendar_dates = calendar_dates.drop('index', axis=1)
        # Drop duplicates
        calendar_dates = calendar_dates.drop_duplicates(subset=['service_id'])
    except:
        # If data is not available pass empty DataFrame
        calendar_dates = pd.DataFrame()

    # Create dictionary for GTFS data
    gtfs_data = dict(
        agency=agency.copy(),
        calendar=calendar.copy(),
        calendar_dates=calendar_dates.copy(),
        routes=routes.copy(),
        stops=stops.copy(),
        stop_times=stop_times.copy(),
        trips=trips.copy())

    # Close connection
    conn.close()

    return gtfs_data


def save_to_gtfs_zip(output_zip_fp, gtfs_data):
    """Export GTFS data to zip file.

    Parameters
    ----------

    output_zip_fp : str
        Full filepath to the GTFS zipfile that will be exported.
    gtfs_data : dict
        A dictionary containing DataFrames for different GTFS outputs.
    """
    print("Exporting GTFS\n----------------------")

    # Quoted attributes in stops
    _quote_attributes = ["stop_name", "stop_desc", "trip_headsign"]

    # Open stream
    with ZipFile(output_zip_fp, 'w') as zf:
        for name, data in gtfs_data.items():
            fname = "{filename}.txt".format(filename=name)

            # Add quotion marks for specific columns to ensure that naming of the stop does not
            # affect the output csv (e.g. stop-names might have commas that cause conflicts)
            #            if name == "stops":
            #                for col in _quote_attributes:
            #                    if col in data.columns:
            #                        data[col] = '"' + data[col] + '"'
            #
            #            if name == "stop_times":
            #                for col in _quote_attributes:
            #                    if col in data.columns:
            #                        data[col] = '"' + data[col] + '"'

            if data is not None:
                if len(data) > 0:
                    print("Exporting:", fname)
                    # Save
                    buffer = data.to_csv(None, sep=',', index=False, quoting=csv.QUOTE_NONE, escapechar="'")

                    zf.writestr(fname, buffer, compress_type=ZIP_DEFLATED)
                else:
                    print("Skipping. No data available for:", fname)
            else:
                print("Skipping. No data available for:", fname)
    print("Success.")
    print("GTFS zipfile was saved to: %s" % output_zip_fp)


def process_files(parallel):
    # Get files from input instance
    files = parallel.input_files
    file_size_limit = parallel.file_size_limit
    naptan_stops_fp = parallel.stops_fp
    gtfs_db = parallel.gtfs_db

    for idx, fp in enumerate(files):

        # Filesize
        size = round((os.path.getsize(fp) / 1000000), 1)
        if file_size_limit < size:
            continue

        print("=================================================================")
        print("[%s / %s] Processing TransXChange file: %s" % (idx, len(files), os.path.basename(fp)))
        print("Size: %s MB" % size)
        # Log start time
        start_t = timeit()

        data = untangle.parse(fp)

        # Parse stops
        stop_data = get_stops(data, naptan_stops_fp=naptan_stops_fp)

        # Get additional info for stops (not needed necessary)
        # stop_info = get_stop_info(data)

        # Parse agency
        agency = get_agency(data)

        # Parse GTFS info containing data about trips, calendar, stop_times and calendar_dates
        gtfs_info = get_gtfs_info(data)

        # Parse stop_times
        stop_times = get_stop_times(gtfs_info)

        # Parse trips
        trips = get_trips(gtfs_info)

        # Parse calendar
        calendar = get_calendar(gtfs_info)

        # Parse calendar_dates
        calendar_dates = get_calendar_dates(gtfs_info)

        # Parse routes
        routes = get_routes(gtfs_info=gtfs_info, data=data)

        # Initialize database connection
        conn = sqlite3.connect(gtfs_db)

        # Push to database
        # TODO: Catch the errors if multiple processes try to write to database simultaniously

        stop_data.to_sql(name='stops', con=conn, index=False, if_exists='append')
        routes.to_sql(name='routes', con=conn, index=False, if_exists='append')
        agency.to_sql(name='agency', con=conn, index=False, if_exists='append')
        trips.to_sql(name='trips', con=conn, index=False, if_exists='append')
        if len(stop_times) > 0:
            stop_times.to_sql(name='stop_times', con=conn, index=False, if_exists='append')
        calendar.to_sql(name='calendar', con=conn, index=False, if_exists='append')

        if calendar_dates is not None:
            calendar_dates.to_sql(name='calendar_dates', con=conn, index=False, if_exists='append')

        # Close connection
        conn.close()

        # Log end time and parse duration
        end_t = timeit()
        duration = (end_t - start_t) / 60

        print("It took %s minutes." % round(duration, 1))

        # ===================
        # ===================
        # ===================


def convert(data_dir, output_filepath):
    # Total start
    tot_start_t = timeit()

    # Filepath for temporary gtfs db
    target_dir = os.path.dirname(output_filepath)
    gtfs_db = os.path.join(target_dir, "gtfs.db")

    # Append to same database?
    append_to_db = False

    # If append to database is false remove previous gtfs-database if it exists
    if append_to_db == False:
        if os.path.exists(gtfs_db):
            os.remove(gtfs_db)

    # NAPTAN stops
    naptan_stops_fp = get_path("naptan_stops")

    # Retrieve all TransXChange files
    files = glob.glob(os.path.join(data_dir, "*.xml"))

    # Iterate over files
    print("Populating database ..")

    # Limit the processed files by file size (in MB)
    # Files with lower filesize than below will be processed
    file_size_limit = 1000

    # Create workers
    workers = create_workers(input_files=files, file_size_limit=file_size_limit,
                             stops_fp=naptan_stops_fp, gtfs_db=gtfs_db)

    # Create Pool
    pool = multiprocessing.Pool()

    # Generate GTFS info to the database in parallel
    pool.map(process_files, workers)

    # Print information about the total time
    tot_end_t = timeit()
    tot_duration = (tot_end_t - tot_start_t) / 60
    print("===========================================================")
    print("It took %s minutes in total." % round(tot_duration, 1))

    # Generate output dictionary
    gtfs_data = generate_gtfs_export(gtfs_db)

    # Export to disk
    save_to_gtfs_zip(output_zip_fp=output_filepath, gtfs_data=gtfs_data)
