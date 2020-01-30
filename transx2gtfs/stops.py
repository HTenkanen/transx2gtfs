import os
import pandas as pd
import pyproj
import warnings
import io
import urllib
from zipfile import ZipFile
import tempfile


def _update_naptan_data(url="http://naptan.app.dft.gov.uk/DataRequest/Naptan.ashx?format=csv",
                       filepath=None):
    if filepath is None:
        temp_dir = tempfile.gettempdir()
        target_dir = os.path.join(temp_dir, 'transx2gtfs')
        target_file = os.path.join(target_dir, "NaPTAN_data.zip")

        if not os.path.exists(target_dir):
            os.makedirs(target_dir)

        if os.path.exists(target_file):
            print("Removing old stop data")
            os.remove(target_file)
    else:
        target_file = filepath

    # Download the NaPTAN data to temp
    filepath, msg = urllib.request.urlretrieve(url, target_file)
    print("Downloaded/updated NaPTAN stop dataset to:\n'{fp}'".format(fp=filepath))


def read_naptan_stops(naptan_fp=None):
    """
    Reads NaPTAN stops from temp. If the Stops do not exist in the temp, downloads the data.
    """
    if naptan_fp is None:
        naptan_fp = os.path.join(tempfile.gettempdir(),
                                 'transx2gtfs',
                                 'NaPTAN_data.zip')

    max_attemps = 20
    i = 1
    while True:
        if not os.path.exists(naptan_fp):
            _update_naptan_data()
        else:
            break

        if i == max_attemps:
            raise ValueError("Could not update the stops data.\nMax attempts reached.")
        i += 1

    # Read the stops from the zip
    z = ZipFile(naptan_fp)

    if 'Stops.csv' not in z.namelist():
        raise ValueError("NaPTAN dataset did not contain stops!")

    stops = pd.read_csv(io.BytesIO(z.read('Stops.csv')), encoding='latin1',
                        low_memory=False)

    # Rename required columns into GTFS format
    stops = stops.rename(columns={
        'ATCOCode': 'stop_id',
        'Longitude': 'stop_lon',
        'Latitude': 'stop_lat',
        'CommonName': 'stop_name',
    })

    # Keep only required columns
    required_cols = ['stop_id', 'stop_lon', 'stop_lat', 'stop_name']
    for col in required_cols:
        if col not in stops.columns:
            raise ValueError(
                "Required column {col} could not be found from stops DataFrame.".format(
                col=col)
            )
    stops = stops[required_cols].copy()
    return stops


def _get_tfl_style_stops(data):
    """"""
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
    naptan_stops = read_naptan_stops()

    # Iterate over stop points
    for p in data.TransXChange.StopPoints.StopPoint:
        # Name of the stop
        stop_name = p.Descriptor.CommonName.cdata

        # Stop_id
        stop_id = p.AtcoCode.cdata

        # Get stop info
        stop = naptan_stops.loc[naptan_stops[_stop_id_col] == stop_id]

        # If local NAPTAN db does not contain the info,
        # try to refresh local dump or parse from the data directly

        if len(stop) == 0:
            # Try first to refresh the Stop data
            # -----------------------------------
            _update_naptan_data()
            naptan_stops = read_naptan_stops()
            stop = naptan_stops.loc[naptan_stops[_stop_id_col] == stop_id]

            if len(stop) == 0:
                # If was not found, try to read from TransXchange data directly
                # -------------------------------------------------------------
                try:
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

                    # Create row
                    stop = dict(stop_id=stop_id,
                                stop_code=None,
                                stop_name=stop_name,
                                stop_lat=y,
                                stop_lon=x,
                                stop_url=None
                                )

                except Exception:
                    warnings.warn("Did not find a NaPTAN stop for '%s'" % stop_id,
                                  UserWarning,
                                  stacklevel=2)
                    continue

        elif len(stop) > 1:
            raise ValueError("Had more than 1 stop with identical stop reference.")

        # Add to container
        stop_data = stop_data.append(stop, ignore_index=True, sort=False)

    return stop_data

def _get_txc_21_style_stops(data):
    # Attributes
    _stop_id_col = 'stop_id'

    # Container
    stop_data = pd.DataFrame()

    # Get stop database
    naptan_stops = read_naptan_stops()

    # Iterate over stop points using TransXchange version 2.1
    for p in data.TransXChange.StopPoints.AnnotatedStopPointRef:

        # Stop_id
        stop_id = p.StopPointRef.cdata

        # Get stop info
        stop = naptan_stops.loc[naptan_stops[_stop_id_col] == stop_id]

        if len(stop) == 0:
            # Try first to refresh the Stop data
            _update_naptan_data()
            naptan_stops = read_naptan_stops()
            stop = naptan_stops.loc[naptan_stops[_stop_id_col] == stop_id]

            # If it could still not be found warn and skip
            if len(stop) == 0:
                warnings.warn("Did not find a NaPTAN stop for '%s'" % stop_id,
                              UserWarning,
                              stacklevel=2)
                continue

        elif len(stop) > 1:
            raise ValueError("Had more than 1 stop with identical stop reference.")

        # Add to container
        stop_data = stop_data.append(stop, ignore_index=True, sort=False)

    return stop_data


def get_stops(data):
    """Parse stop data from TransXchange elements"""

    if 'StopPoint' in data.TransXChange.StopPoints.__dir__():
        stop_data = _get_tfl_style_stops(data)
    elif 'AnnotatedStopPointRef' in data.TransXChange.StopPoints.__dir__():
        stop_data = _get_txc_21_style_stops(data)
    else:
        raise ValueError(
            "Did not find tag for Stop data in TransXchange xml. " 
            "Could not parse Stop information from the TransXchange."
        )

    # Check that stops were found
    if len(stop_data) == 0:
        return None

    return stop_data
