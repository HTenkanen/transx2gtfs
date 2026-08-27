import logging
import sqlite3
import pandas as pd
from zipfile import ZipFile, ZIP_DEFLATED
import csv
import io
import os

from transx2gtfs.txc import read_txc, read_txc_header

log = logging.getLogger("transx2gtfs")


def _has_extension(path, extension):
    """Case-insensitive extension check (e.g. both 'a.xml' and 'A.XML')."""
    return path.lower().endswith(extension)


def get_paths_from_zip(zip_filepath):
    """
    Extracts TransXchange xml-paths from ZipFile (also nested).
    """
    xml_contents = []
    z = ZipFile(zip_filepath)
    files_in_zip = z.namelist()

    for name in files_in_zip:
        if _has_extension(name, ".xml"):
            # Create dictionary with name as key and zip filepath value
            xml_contents.append({name: z.filename})

        # If the zip contained another zip take it's contents
        elif _has_extension(name, ".zip"):
            # Read inner zip to memory
            inner_zip = ZipFile(io.BytesIO(z.read(name)))
            # Read files from inner zip
            for inner_name in inner_zip.namelist():
                if _has_extension(inner_name, ".xml"):
                    xml_contents.append({z.filename: {name: inner_name}})
    return xml_contents


def get_xml_paths(filepath):
    """
    Retrieves XML paths from:
        - directory +
        - ZipFiles within a directory +
        - ZipFiles within a ZipFile

    Finds xml files with all combinations of the above.
    """
    # Input is directory
    # ------------------
    if os.path.isdir(filepath):
        # Read all XML and zip files
        entries = [
            os.path.join(filepath, name) for name in sorted(os.listdir(filepath))
        ]
        xml_contents = [p for p in entries if _has_extension(p, ".xml")]
        zip_contents = [p for p in entries if _has_extension(p, ".zip")]

        # Parse xml references inside zip files
        for zfp in zip_contents:
            xml_contents += get_paths_from_zip(zfp)

    # Input is a ZipFile
    elif os.path.isfile(filepath) and _has_extension(filepath, ".zip"):
        xml_contents = get_paths_from_zip(filepath)

    else:
        raise ValueError("Input '%s' is not a directory or a .zip file." % filepath)

    return xml_contents


def read_unpacked_xml(xml_path):
    """
    Reads a TransXChange XML file into a TxcDocument.
    """
    file_size = os.path.getsize(xml_path)
    return read_txc(xml_path), file_size, os.path.basename(xml_path)


def read_xml_header(xml_path):
    """
    The :class:`~transx2gtfs.txc.TxcHeader` of an input item of any kind
    :func:`get_xml_paths` produces: a path, a zip member or a nested zip member.
    """
    if isinstance(xml_path, str):
        return read_txc_header(xml_path)
    if isinstance(xml_path, dict):
        value = list(xml_path.values())[0]
        if isinstance(value, str):
            filename = list(xml_path.keys())[0]
            with ZipFile(value) as z, z.open(filename) as member:
                return read_txc_header(member, file_name=filename)
        if isinstance(value, dict):
            zip_filepath = list(xml_path.keys())[0]
            inner_zip_name = list(value.keys())[0]
            xml_name = list(value.values())[0]
            with ZipFile(zip_filepath) as z:
                inner_zip = ZipFile(io.BytesIO(z.read(inner_zip_name)))
            with inner_zip.open(xml_name) as member:
                return read_txc_header(member, file_name=xml_name)
    raise ValueError("Something is wrong with the input xml-file paths.")


def read_xml_inside_zip(xml_path):
    """
    Reads a TransXChange XML file inside a ZipFile into a TxcDocument.
    """
    zip_filepath = list(xml_path.values())[0]
    filename = list(xml_path.keys())[0]
    z = ZipFile(zip_filepath)
    file_size = z.getinfo(filename).file_size
    with z.open(filename) as member:
        doc = read_txc(member, file_name=filename)
    return doc, file_size, filename


def read_xml_inside_nested_zip(xml_path):
    """
    Reads a TransXChange XML file in a ZipFile inside another ZipFile.
    """
    zip_filepath = list(xml_path.keys())[0]
    inner_zip_info = list(xml_path.values())[0]
    inner_zip_name = list(inner_zip_info.keys())[0]
    xml_name = list(inner_zip_info.values())[0]

    # Read outer zip
    z = ZipFile(zip_filepath)

    # Read inner zip to memory
    inner_zip = ZipFile(io.BytesIO(z.read(inner_zip_name)))
    file_size = inner_zip.getinfo(xml_name).file_size
    with inner_zip.open(xml_name) as member:
        doc = read_txc(member, file_name=xml_name)
    return doc, file_size, xml_name


def _table_exists(conn, name):
    query = "SELECT name FROM sqlite_master WHERE type='table' AND name=?"
    return conn.execute(query, (name,)).fetchone() is not None


def make_route_names_unique(routes):
    """
    Routes of one agency sharing route_short_name and route_long_name (trip
    planners warn about them) are told apart: the first keeps its names, the
    later ones get ' (<route_id>)' appended to route_long_name — again, should
    that still clash with a name in use (route ids are unique, so this ends).
    """
    names = ["agency_id", "route_short_name", "route_long_name"]
    if len(routes) == 0 or any(column not in routes.columns for column in names):
        return routes
    routes = routes.reset_index(drop=True)
    renamed = pd.Series(False, index=routes.index)
    for _ in range(len(routes)):
        # A missing name and an empty one are the same empty field in GTFS
        duplicated = routes[names].fillna("").astype(str).duplicated(keep="first")
        if not duplicated.any():
            break
        if not renamed.any():
            routes = routes.copy()
        routes.loc[duplicated, "route_long_name"] = (
            routes.loc[duplicated, "route_long_name"].fillna("").astype(str)
            + " ("
            + routes.loc[duplicated, "route_id"].astype(str)
            + ")"
        )
        renamed |= duplicated
    if renamed.any():
        log.info(
            "%d route(s) shared their names with another route of the same agency; "
            "the route_id was appended to their route_long_name.",
            int(renamed.sum()),
        )
    return routes


def generate_gtfs_export(gtfs_db_fp):
    """Reads the gtfs database and generates an export dictionary for GTFS"""
    # Initialize connection
    conn = sqlite3.connect(gtfs_db_fp)

    # Read database and produce the GTFS file
    # =======================================

    # Stops
    # -----
    stops = pd.read_sql_query("SELECT * FROM stops", conn)
    if "index" in stops.columns:
        stops = stops.drop("index", axis=1)

    # Drop duplicates based on stop_id
    stops = stops.drop_duplicates(subset=["stop_id"])

    # Agency
    # ------
    agency = pd.read_sql_query("SELECT * FROM agency", conn)
    if "index" in agency.columns:
        agency = agency.drop("index", axis=1)
    # Drop duplicates
    agency = agency.drop_duplicates(subset=["agency_id"])

    # Routes
    # ------
    routes = pd.read_sql_query("SELECT * FROM routes", conn)
    if "index" in routes.columns:
        routes = routes.drop("index", axis=1)
    # Drop duplicates
    routes = routes.drop_duplicates(subset=["route_id"])
    routes = make_route_names_unique(routes)

    # Trips
    # -----
    trips = pd.read_sql_query("SELECT * FROM trips", conn)
    if "index" in trips.columns:
        trips = trips.drop("index", axis=1)

    # Drop duplicates
    trips = trips.drop_duplicates(subset=["trip_id"])

    # Stop_times
    # ----------
    stop_times = pd.read_sql_query("SELECT * FROM stop_times", conn)
    if "index" in stop_times.columns:
        stop_times = stop_times.drop("index", axis=1)

    # Drop duplicates
    stop_times = stop_times.drop_duplicates()

    # Calendar
    # --------
    calendar = pd.read_sql_query("SELECT * FROM calendar", conn)
    if "index" in calendar.columns:
        calendar = calendar.drop("index", axis=1)
    # Drop duplicates
    calendar = calendar.drop_duplicates(subset=["service_id"])

    # Calendar dates
    # --------------
    if _table_exists(conn, "calendar_dates"):
        calendar_dates = pd.read_sql_query("SELECT * FROM calendar_dates", conn)
        if "index" in calendar_dates.columns:
            calendar_dates = calendar_dates.drop("index", axis=1)
        # Drop duplicates (a service has one row per exception date)
        calendar_dates = calendar_dates.drop_duplicates()
    else:
        # If data is not available pass empty DataFrame
        calendar_dates = pd.DataFrame()

    # Frequencies
    # -----------
    if _table_exists(conn, "frequencies"):
        frequencies = pd.read_sql_query("SELECT * FROM frequencies", conn)
        frequencies = frequencies.drop_duplicates(subset=["trip_id", "start_time"])
    else:
        frequencies = pd.DataFrame()

    # Create dictionary for GTFS data
    gtfs_data = dict(
        agency=agency.copy(),
        calendar=calendar.copy(),
        calendar_dates=calendar_dates.copy(),
        routes=routes.copy(),
        stops=stops.copy(),
        stop_times=stop_times.copy(),
        trips=trips.copy(),
        frequencies=frequencies.copy(),
    )

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
    log.info("Exporting GTFS\n----------------------")

    # Open stream
    with ZipFile(output_zip_fp, "w") as zf:
        for name, data in gtfs_data.items():
            fname = "{filename}.txt".format(filename=name)

            if data is not None:
                if len(data) > 0:
                    log.info("Exporting: %s", fname)
                    # Save
                    buffer = data.to_csv(
                        None,
                        sep=",",
                        index=False,
                        quotechar='"',
                        quoting=csv.QUOTE_NONNUMERIC,
                    )

                    zf.writestr(fname, buffer, compress_type=ZIP_DEFLATED)
                else:
                    log.info("Skipping. No data available for: %s", fname)
            else:
                log.info("Skipping. No data available for: %s", fname)
    log.info("Success.")
    log.info("GTFS zipfile was saved to: %s", output_zip_fp)
