# -*- coding: utf-8 -*-
"""
Convert transXchange data format to GTFS format.

TODO:
    - Parallelize the JourneyPattern iteration into multiple threads

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


Author
------
Dr. Henrikki Tenkanen, University College London

License
-------

MIT.

"""
import untangle
from time import time as timeit
import sqlite3
import glob
from multiprocessing import cpu_count
import math
import os
import multiprocessing
from transx2gtfs.data import get_path
from transx2gtfs.stop_times import get_stop_times
from transx2gtfs.stops import get_stops
from transx2gtfs.trips import get_trips
from transx2gtfs.routes import get_routes
from transx2gtfs.agency import get_agency
from transx2gtfs.calendar import get_calendar
from transx2gtfs.calendar_dates import get_calendar_dates
from transx2gtfs.dataio import generate_gtfs_export, save_to_gtfs_zip
from transx2gtfs.transxchange import get_gtfs_info


class Parallel:
    def __init__(self, input_files, file_size_limit, stops_fp, gtfs_db):
        self.input_files = input_files
        self.file_size_limit = file_size_limit
        self.stops_fp = stops_fp
        self.gtfs_db = gtfs_db


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

        # Only export data into db if there exists valid stop_times data
        if len(stop_times) > 0:
            stop_times.to_sql(name='stop_times', con=conn, index=False, if_exists='append')
            stop_data.to_sql(name='stops', con=conn, index=False, if_exists='append')
            routes.to_sql(name='routes', con=conn, index=False, if_exists='append')
            agency.to_sql(name='agency', con=conn, index=False, if_exists='append')
            trips.to_sql(name='trips', con=conn, index=False, if_exists='append')
            calendar.to_sql(name='calendar', con=conn, index=False, if_exists='append')

            if calendar_dates is not None:
                calendar_dates.to_sql(name='calendar_dates', con=conn, index=False, if_exists='append')
        else:
            print(
                "UserWarning: File %s did not contain valid stop_sequence data, skipping." % (
                    os.path.basename(fp))
            )

        # Close connection
        conn.close()

        # Log end time and parse duration
        end_t = timeit()
        duration = (end_t - start_t) / 60

        print("It took %s minutes." % round(duration, 1))

        # ===================
        # ===================
        # ===================


def convert(data_dir, output_filepath, append_to_existing=False):
    """
    Converts TransXchange formatted schedule data into GTFS feed.

    data_dir : str
        Data directory containing one or multiple TransXchange .xml files.
    output_filepath : str
        Full filepath to the output GTFS zip-file, e.g. '/home/myuser/data/my_gtfs.zip'
    append_to_existing : bool (default is False)
        Flag for appending to existing gtfs-database. This might be useful if you have
        TransXchange .xml files distributed into multiple directories (e.g. separate files for
        train data, tube data and bus data) and you want to merge all those datasets into a single
        GTFS feed.
    """
    # Total start
    tot_start_t = timeit()

    # Filepath for temporary gtfs db
    target_dir = os.path.dirname(output_filepath)
    gtfs_db = os.path.join(target_dir, "gtfs.db")

    # If append to database is false remove previous gtfs-database if it exists
    if append_to_existing == False:
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
