import os
import pandas as pd
import pyproj

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


def _get_tfl_style_stops(data, naptan_stops_fp):
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
    naptan_stops = download_naptan_stops(local_path=naptan_stops_fp)

    # Iterate over stop points
    for p in data.TransXChange.StopPoints.StopPoint:
        # Name of the stop
        stop_name = p.Descriptor.CommonName.cdata

        # Stop_id
        stop_id = p.AtcoCode.cdata

        # Get stop info
        stop = naptan_stops.loc[naptan_stops[_stop_id_col] == stop_id]

        # If NAPTAN db does not contain the info, try to parse from the data directly

        if len(stop) == 0:
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

            # Create row
            stop = dict(stop_id=stop_id,
                        stop_code=None,
                        stop_name=stop_name,
                        stop_lat=y,
                        stop_lon=x,
                        stop_url=None
                        )

        elif len(stop) > 1:
            raise ValueError("Had more than 1 stop with identical stop reference.")

        # Add to container
        stop_data = stop_data.append(stop, ignore_index=True, sort=False)

    return stop_data

def _get_txc_21_style_stops(data, naptan_stops_fp):
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

    # Iterate over stop points using TransXchange version 2.1
    for p in data.TransXChange.StopPoints.AnnotatedStopPointRef:
        # Name of the stop
        stop_name = p.CommonName.cdata

        # Stop_id
        stop_id = p.StopPointRef.cdata

        # Get stop info
        stop = naptan_stops.loc[naptan_stops[_stop_id_col] == stop_id]

        # If NAPTAN db does not contain the info, try to parse from the data directly
        # Note: this seems not to be part of the data in TXC 2.1 schema
        # TODO: Remove?
        try:
            if len(stop) == 0:
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

                    # Create row
                stop = dict(stop_id=stop_id,
                            stop_code=None,
                            stop_name=stop_name,
                            stop_lat=y,
                            stop_lon=x,
                            stop_url=None
                            )
            elif len(stop) > 1:
                raise ValueError("Had more than 1 stop with identical stop reference.")
        # If stop location cannot be determined do not add it to the stops frame.
        except Exception as e:
            print(e)
            continue

        # Add to container
        stop_data = stop_data.append(stop, ignore_index=True, sort=False)

    return stop_data


def get_stops(data, naptan_stops_fp=None):
    """Parse stop data from TransXchange elements"""
    keep_cols = ['stop_id', 'stop_code', 'stop_name',
                 'stop_lat', 'stop_lon', 'stop_url']

    if 'StopPoint' in data.TransXChange.StopPoints.__dir__():
        stop_data = _get_tfl_style_stops(data, naptan_stops_fp)
    elif 'AnnotatedStopPointRef' in data.TransXChange.StopPoints.__dir__():
        stop_data = _get_txc_21_style_stops(data, naptan_stops_fp)
    else:
        raise ValueError("Could not parse Stop information from the TransXchange.")

    # Check that stops were found
    if len(stop_data) == 0:
        return None

    # Check that required columns exist
    for col in keep_cols:
        if col not in stop_data.columns:
            return None
    return stop_data
