# -*- coding: utf-8 -*-
"""
Convert transXchange data format to GTFS format.

The TransXChange model) has seven basic concepts: Service, Registration, Operator,
Route, StopPoint, JourneyPattern, and VehicleJourney.
    - A Service brings together the information about a registered bus service, and may
        contain two types of component service: Standard or Flexible; a mix of both
        types is allowed within a single Service.
    - A normal bus schedule is described by a StandardService and a Route. A Route
        describes the physical path taken by buses on the service as a set of route
        links.
    - A FlexibleService describes a bus service that does not have a fixed route, but
        only a catchment area or a few variable stops with no prescribed pattern of use.
    - A StandardService has one or more JourneyPattern elements to describe the common
        logical path of traversal of the stops of the Route as a sequence of timing
        links (see later), and one or more VehicleJourney elements, which describe
        individual scheduled journeys by buses over the Route and JourneyPattern at a
        specific time.
    - Both types of service have a registered Operator, who runs the service. Other
        associated operator roles can also be specified.
    - Route, JourneyPattern and VehicleJoumey follow a sequence of NaPTAN StopPoints. A
        Route specifies in effect an ordered list of StopPoints. A JourneyPattern
        specifies an ordered list of links between these points, giving relative times
        between each stop; a VehicleJourney follows the same list of stops at specific
        absolute passing times. (The detailed timing Link and elements that connect
        VehicleJourneys, JourneyPatterns etc to StopPoints are not shown in Figure 3-1).
        StopPoints may be grouped within StopAreas.
    - The StopPoints used in a JourneyPattern or Route are either declared locally or by
        referenced to an external definition using an AnnotatedStopRef
    - A Registration specifies the registration details for a service. It is mandatory
        in the registration schema.

Author
------
Henrikki Tenkanen, Aalto University

License
-------

MIT.
"""

from time import time as timeit
import contextlib
import sqlite3
import os
import multiprocessing
from transx2gtfs.stop_times import get_stop_times, get_frequencies
from transx2gtfs.stops import get_stops, ensure_naptan_data
from transx2gtfs.bank_holidays import (
    remove_bank_holidays_snapshot,
    set_bank_holidays_path,
    snapshot_bank_holidays_data,
)
from transx2gtfs.trips import get_trips
from transx2gtfs.routes import get_routes
from transx2gtfs.agency import get_agency
from transx2gtfs.calendar import get_calendar
from transx2gtfs.calendar_dates import get_calendar_dates
from transx2gtfs.dataio import (
    _table_exists,
    generate_gtfs_export,
    read_xml_header,
    save_to_gtfs_zip,
    get_xml_paths,
)
from transx2gtfs.superseded import select_current_files
from transx2gtfs.txc import TxcHeader
from transx2gtfs.dataio import (
    read_xml_inside_nested_zip,
    read_xml_inside_zip,
    read_unpacked_xml,
)
from transx2gtfs.transxchange import get_gtfs_info
from transx2gtfs.distribute import create_workers

# Lock serialising database writes across worker processes (set by _init_worker)
_db_lock = None


def _row_count(gtfs_db, table):
    """Rows of a table of the database; 0 when the table (or database) is absent"""
    if not os.path.exists(gtfs_db):
        return 0
    conn = sqlite3.connect(gtfs_db)
    try:
        if not _table_exists(conn, table):
            return 0
        return conn.execute("SELECT COUNT(*) FROM %s" % table).fetchone()[0]
    finally:
        conn.close()


def _item_name(item):
    """The XML file name of an input item of any kind"""
    while isinstance(item, dict):
        key, item = next(iter(item.items()))
        if isinstance(item, str) and not item.lower().endswith(".zip"):
            return item
        if isinstance(item, str):
            return key
    return os.path.basename(item)


def select_files(files):
    """
    The input files to convert once superseded versions are left out: every
    file's header is read and, per service, only the current version is kept
    (see :mod:`transx2gtfs.superseded`). Dropped files are printed. A file whose
    header cannot be read is kept; the conversion reports it.
    """
    headers = []
    for item in files:
        try:
            header = read_xml_header(item)
        except Exception as error:  # noqa: BLE001 - any parse error: keep the file
            print("Could not read the header of %s: %s" % (_item_name(item), error))
            header = TxcHeader(file_name=_item_name(item))
        headers.append((item, header))
    selection = select_current_files(headers)
    for dropped in selection.dropped:
        print(
            "Skipping %s (ServiceCode %s, revision %s): superseded by %s (revision %s)"
            % (
                dropped.file_name,
                dropped.service_code,
                dropped.revision_number,
                dropped.superseded_by,
                dropped.superseded_by_revision,
            )
        )
    if selection.dropped:
        print("Skipped %d superseded file(s)." % len(selection.dropped))
    return selection.kept


def _init_worker(lock, bank_holidays_path=None):
    global _db_lock
    _db_lock = lock
    if bank_holidays_path is not None:
        set_bank_holidays_path(bank_holidays_path)


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
        print(
            "[%s / %s] Processing TransXChange file: %s" % (idx, len(files), xml_name)
        )
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
        routes = get_routes(gtfs_info=gtfs_info, doc=data)

        # Parse frequencies (headway-based journeys)
        frequencies = get_frequencies(gtfs_info)

        # Only export data into db if there exists valid stop_times data
        if len(stop_times) > 0:
            _write_to_db(
                gtfs_db,
                stop_times=stop_times,
                stops=stop_data,
                routes=routes,
                agency=agency,
                trips=trips,
                calendar=calendar,
                calendar_dates=calendar_dates,
                frequencies=frequencies,
            )
        else:
            print(
                "UserWarning: File %s did not contain valid stop_sequence data, skipping."
                % (xml_name)
            )

        # Log end time and parse duration
        end_t = timeit()
        duration = (end_t - start_t) / 60

        print("It took %s minutes." % round(duration, 1))


def _write_to_db(gtfs_db, **tables):
    """Append the GTFS tables of one file to the database (one writer at a time)"""
    lock = _db_lock if _db_lock is not None else contextlib.nullcontext()
    with lock:
        conn = sqlite3.connect(gtfs_db, timeout=120)
        try:
            for name, table in tables.items():
                if table is not None:
                    table.to_sql(name=name, con=conn, index=False, if_exists="append")
        finally:
            conn.close()


def convert(
    input_filepath,
    output_filepath,
    append_to_existing=False,
    worker_cnt=None,
    file_size_limit=2000,
    skip_superseded=True,
):
    """
    Converts TransXchange formatted schedule data into GTFS feed.

    input_filepath : str
        File path to data directory or a ZipFile containing one or multiple
        TransXchange .xml files. Also nested ZipFiles are supported (i.e. a ZipFile
        with ZipFile(s) containing .xml files.)
    output_filepath : str
        Full filepath to the output GTFS zip-file, e.g. '/home/myuser/data/my_gtfs.zip'
    append_to_existing : bool (default is False)
        Flag for appending to existing gtfs-database. This might be useful if you
        have TransXchange .xml files distributed into multiple directories (e.g.
        separate files for train data, tube data and bus data) and you want to merge
        all those datasets into a single GTFS feed.
    worker_cnt : int
        Number of worker processes. By default the number of CPUs minus one is used.
    file_size_limit : int
        File size limit (in megabytes) can be used to skip larger-than-memory
        XML-files (should not happen).
    skip_superseded : bool (default is True)
        Leave out files superseded by a newer version of the same service
        (change archives such as the Bus Open Data Service bulk download hold
        several revisions): per ServiceCode, among versions whose operating
        periods overlap only the highest RevisionNumber is converted. Dropped
        files are printed.
    """
    # Total start
    tot_start_t = timeit()

    # Filepath for temporary gtfs db
    target_dir = os.path.dirname(output_filepath)
    gtfs_db = os.path.join(target_dir, "gtfs.db")

    # If append to database is false remove previous gtfs-database if it exists
    if not append_to_existing:
        if os.path.exists(gtfs_db):
            os.remove(gtfs_db)

    # Retrieve all TransXChange files
    files = get_xml_paths(input_filepath)
    if len(files) == 0:
        raise ValueError(
            "Did not find any TransXChange .xml files from '%s'." % input_filepath
        )
    if skip_superseded:
        files = select_files(files)

    # Make sure the NaPTAN stop data and one immutable bank holiday snapshot are
    # available before the workers start; the snapshot only lives while they run
    ensure_naptan_data()
    rows_before = _row_count(gtfs_db, "stop_times")
    snapshot = snapshot_bank_holidays_data()
    try:
        # Iterate over files
        print("Populating database ..")

        # Create workers
        workers = create_workers(
            input_files=files,
            worker_cnt=worker_cnt,
            file_size_limit=file_size_limit,
            gtfs_db=gtfs_db,
        )

        # Generate GTFS info to the database in parallel; workers take turns writing
        lock = multiprocessing.Lock()
        with multiprocessing.Pool(
            processes=len(workers),
            initializer=_init_worker,
            initargs=(lock, snapshot),
        ) as pool:
            pool.map(process_files, workers)
    finally:
        # An in-process pool runs the initializer in this process
        set_bank_holidays_path(None)
        remove_bank_holidays_snapshot(snapshot)

    # Print information about the total time
    tot_end_t = timeit()
    tot_duration = (tot_end_t - tot_start_t) / 60
    print("===========================================================")
    print("It took %s minutes in total." % round(tot_duration, 1))

    # Nothing to export when no file of this conversion produced trips (all
    # journeys skipped), whatever an existing database already holds
    if _row_count(gtfs_db, "stop_times") == rows_before:
        raise ValueError(
            "The TransXChange files in '%s' did not produce any trips." % input_filepath
        )

    # Generate output dictionary
    gtfs_data = generate_gtfs_export(gtfs_db)

    # Export to disk
    save_to_gtfs_zip(output_zip_fp=output_filepath, gtfs_data=gtfs_data)
