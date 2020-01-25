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