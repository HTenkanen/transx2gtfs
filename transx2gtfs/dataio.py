import sqlite3
import pandas as pd
from zipfile import ZipFile, ZIP_DEFLATED
import csv
import glob
import io
import os
import untangle


def get_paths_from_zip(zip_filepath):
    """
    Extracts TransXchange xml-paths from ZipFile (also nested).
    """
    xml_contents = []
    z = ZipFile(zip_filepath)
    files_in_zip = z.namelist()

    for name in files_in_zip:
        if name.endswith('xml'):
            # Create dictionary with name as key and zip filepath value
            xml_contents.append({name: z.filename})

        # If the zip contained another zip take it's contents
        elif name.endswith('.zip'):
            # Read inner zip to memory
            inner_zip = ZipFile(io.BytesIO(z.read(name)))
            # Read files from inner zip
            for inner_name in inner_zip.namelist():
                if inner_name.endswith('xml'):
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
        xml_contents = glob.glob(os.path.join(filepath, '*.xml'))
        zip_contents = glob.glob(os.path.join(filepath, '*.zip'))

        # Parse xml references inside zip files
        if len(zip_contents) > 0:
            for zfp in zip_contents:
                xml_contents += get_paths_from_zip(zfp)

    # Input is a ZipFile
    elif filepath.endswith('.zip'):
        xml_contents = get_paths_from_zip(filepath)

    return xml_contents

def read_unpacked_xml(xml_path):
    """
    Reads an XML with untangle.
    """
    file_size = os.path.getsize(xml_path)
    parsed_xml = untangle.parse(xml_path)
    return parsed_xml, file_size, os.path.basename(xml_path)

def read_xml_inside_zip(xml_path):
    """
    Reads an XML with untangle which is inside a ZipFile.
    """
    zip_filepath = list(xml_path.values())[0]
    filename = list(xml_path.keys())[0]
    z = ZipFile(zip_filepath)
    file_size = z.getinfo(filename).file_size
    parsed_xml = untangle.parse(
        io.TextIOWrapper(
            io.BytesIO(
                z.read(filename)
            )
        )
    )
    return parsed_xml, file_size, filename


def read_xml_inside_nested_zip(xml_path):
    """
    Reads an XML with untangle which is in a ZipFile inside another ZipFile.
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
    parsed_xml = untangle.parse(
        io.TextIOWrapper(
            io.BytesIO(
                inner_zip.read(xml_name)
            )
        )
    )
    return parsed_xml, file_size, xml_name


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

            if data is not None:
                if len(data) > 0:
                    print("Exporting:", fname)
                    # Save
                    buffer = data.to_csv(None, sep=',', index=False,
                                         quotechar='"',
                                         quoting=csv.QUOTE_NONNUMERIC)

                    zf.writestr(fname, buffer, compress_type=ZIP_DEFLATED)
                else:
                    print("Skipping. No data available for:", fname)
            else:
                print("Skipping. No data available for:", fname)
    print("Success.")
    print("GTFS zipfile was saved to: %s" % output_zip_fp)
