# -*- coding: utf-8 -*-
"""
Convert transXchange data format to GTFS format.

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

Author
------
Dr. Henrikki Tenkanen, University College London

License
-------

MIT.
"""

from time import time as timeit
import sqlite3
import os
import multiprocessing
from transx2gtfs.stop_times import get_stop_times
from transx2gtfs.stops import get_stops
from transx2gtfs.trips import get_trips
from transx2gtfs.routes import get_routes
from transx2gtfs.agency import get_agency
from transx2gtfs.calendar import get_calendar
from transx2gtfs.calendar_dates import get_calendar_dates
from transx2gtfs.dataio import generate_gtfs_export, save_to_gtfs_zip, get_xml_paths
from transx2gtfs.dataio import read_xml_inside_nested_zip, read_xml_inside_zip, read_unpacked_xml
from transx2gtfs.transxchange import get_gtfs_info
from transx2gtfs.distribute import create_workers


def process_files(parallel):
    # Get files from input instance
    files = parallel.input_files
    file_size_limit = parallel.file_size_limit
    gtfs_db = parallel.gtfs_db

    for idx, path in enumerate(files):

        # If type is string, it is a direct filepath to XML
        if isinstance(path, str):
            data, file_size, xml_name = read_unpacked_xml(path)

        # If the type is dictionary contents are in a zip
        elif isinstance(path, dict):

            # If the type of value is a string the file can be read directly
            # from the given Zipfile path, with following structure:
            # {"transxchange_name.xml" : "/home/data/myzipfile.zip"}
            if isinstance(list(path.values())[0], str):
                data, file_size, xml_name = read_xml_inside_zip(path)


            # If the type of value is a dictionary the xml-file
            # is in a ZipFile which is inside another ZipFile.
            # In such cases, the path stucture is:
            # {"outermost_zipfile_path.zip": {"inner_zipfile.zip": "transxchange.xml"}}
            elif isinstance(list(path.values())[0], dict):
                data, file_size, xml_name = read_xml_inside_nested_zip(path)
            else:
                raise ValueError("Something is wrong with the input xml-file paths.")
        else:
            raise ValueError("Something is wrong with the input xml-file paths.")

        # Filesize
        size = round((file_size / 1000000), 1)
        if file_size_limit < size:
            continue

        print("=================================================================")
        print("[%s / %s] Processing TransXChange file: %s" % (idx, len(files), xml_name))
        print("Size: %s MB" % size)
        # Log start time
        start_t = timeit()

        # Parse stops
        stop_data = get_stops(data)

        if stop_data is None:
            print("Did not found any valid stops. Skipping..")
            continue

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
                    xml_name)
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


def convert(input_filepath, output_filepath, append_to_existing=False, worker_cnt=None,
            file_size_limit=2000):
    """
    Converts TransXchange formatted schedule data into GTFS feed.

    input_filepath : str
        File path to data directory or a ZipFile containing one or multiple TransXchange .xml files.
        Also nested ZipFiles are supported (i.e. a ZipFile with ZipFile(s) containing .xml files.)
    output_filepath : str
        Full filepath to the output GTFS zip-file, e.g. '/home/myuser/data/my_gtfs.zip'
    append_to_existing : bool (default is False)
        Flag for appending to existing gtfs-database. This might be useful if you have
        TransXchange .xml files distributed into multiple directories (e.g. separate files for
        train data, tube data and bus data) and you want to merge all those datasets into a single
        GTFS feed.
    worker_cnt : int
        Number of workers to distribute the conversion process. By default the number of CPUs is used.
    file_size_limit : int
        File size limit (in megabytes) can be used to skip larger-than-memory XML-files (should not happen).
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

    # Retrieve all TransXChange files
    files = get_xml_paths(input_filepath)

    # Iterate over files
    print("Populating database ..")

    # Create workers
    workers = create_workers(input_files=files, worker_cnt=worker_cnt,
                             file_size_limit=file_size_limit,
                             gtfs_db=gtfs_db)

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
