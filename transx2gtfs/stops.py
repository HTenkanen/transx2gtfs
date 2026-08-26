import os
import tempfile
import urllib.request
import warnings

import pandas as pd
from pyproj import Transformer

NAPTAN_URL = "https://naptan.api.dft.gov.uk/v1/access-nodes?dataFormat=csv"
NAPTAN_PATH_ENV = "TRANSX2GTFS_NAPTAN_PATH"

_naptan_columns = {
    "ATCOCode": "stop_id",
    "CommonName": "stop_name",
    "Latitude": "stop_lat",
    "Longitude": "stop_lon",
}
_stop_columns = ["stop_id", "stop_name", "stop_lat", "stop_lon"]

# One NaPTAN frame per process, keyed by (path, mtime)
_naptan_cache = {}
_osgb36_to_wgs84 = None


def default_naptan_path():
    return os.path.join(tempfile.gettempdir(), "transx2gtfs", "naptan.csv")


def get_naptan_path():
    """Local NaPTAN CSV: TRANSX2GTFS_NAPTAN_PATH if set, else the temp cache."""
    return os.environ.get(NAPTAN_PATH_ENV) or default_naptan_path()


def download_naptan(target_file, url=NAPTAN_URL):
    """Download the national NaPTAN stop dataset (CSV) to target_file."""
    target_dir = os.path.dirname(target_file) or "."
    os.makedirs(target_dir, mode=0o700, exist_ok=True)
    if os.path.islink(target_file):
        raise OSError("Refusing to replace symlink '%s' with NaPTAN data" % target_file)
    print("Downloading NaPTAN stops from %s" % url)
    # Download into a unique partial file so concurrent runs cannot clobber
    # each other, then publish it atomically
    fd, partial = tempfile.mkstemp(prefix="naptan-", suffix=".part", dir=target_dir)
    os.close(fd)
    try:
        urllib.request.urlretrieve(url, partial)
        os.replace(partial, target_file)
    finally:
        if os.path.exists(partial):
            os.remove(partial)
    print("Saved NaPTAN stops to '%s'" % target_file)
    return target_file


def ensure_naptan_data(naptan_fp=None):
    """Return the path of a local NaPTAN CSV, downloading it once if needed."""
    if naptan_fp is None:
        naptan_fp = get_naptan_path()
    if os.path.exists(naptan_fp):
        return naptan_fp
    if naptan_fp != default_naptan_path():
        raise FileNotFoundError("NaPTAN file '%s' does not exist." % naptan_fp)
    return download_naptan(naptan_fp)


def _read_naptan_csv(naptan_fp):
    usecols = list(_naptan_columns.keys())
    try:
        try:
            stops = pd.read_csv(naptan_fp, usecols=usecols, dtype={"ATCOCode": str})
        except UnicodeDecodeError:
            stops = pd.read_csv(
                naptan_fp, usecols=usecols, dtype={"ATCOCode": str}, encoding="latin1"
            )
    except ValueError as e:
        raise ValueError(
            "NaPTAN file '%s' must contain the columns %s." % (naptan_fp, usecols)
        ) from e
    return stops.rename(columns=_naptan_columns)[_stop_columns]


def read_naptan_stops(naptan_fp=None):
    """
    Read NaPTAN stops as a DataFrame with GTFS column names.

    Uses ``naptan_fp``, else ``TRANSX2GTFS_NAPTAN_PATH``, else a copy downloaded
    once into the temp directory.
    """
    naptan_fp = ensure_naptan_data(naptan_fp)
    key = (naptan_fp, os.path.getmtime(naptan_fp))
    if key not in _naptan_cache:
        _naptan_cache.clear()
        _naptan_cache[key] = _read_naptan_csv(naptan_fp)
    return _naptan_cache[key]


def osgb36_to_wgs84(easting, northing):
    """Transform British National Grid coordinates to WGS84 (lon, lat)."""
    global _osgb36_to_wgs84
    if _osgb36_to_wgs84 is None:
        _osgb36_to_wgs84 = Transformer.from_crs(
            "EPSG:27700", "EPSG:4326", always_xy=True
        )
    return _osgb36_to_wgs84.transform(easting, northing)


def _lookup_naptan_stop(naptan_stops, stop_id):
    stop = naptan_stops.loc[naptan_stops["stop_id"] == stop_id]
    if len(stop) > 1:
        raise ValueError("Had more than 1 stop with identical stop reference.")
    if len(stop) == 1:
        return stop.iloc[0].to_dict()
    return None


def _stop_from_location(stop_point):
    """Build a stop row from the StopPoint's own lon/lat or Easting/Northing."""
    if stop_point.longitude is not None and stop_point.latitude is not None:
        x, y = float(stop_point.longitude), float(stop_point.latitude)
    else:
        x = float(stop_point.easting)
        y = float(stop_point.northing)
        # Values in metres are OSGB36 grid coordinates, otherwise assume WGS84
        if x > 180:
            x, y = osgb36_to_wgs84(x, y)
    return dict(
        stop_id=stop_point.atco_code,
        stop_name=stop_point.common_name,
        stop_lat=y,
        stop_lon=x,
    )


def _get_tfl_style_stops(doc, naptan_stops=None):
    """Parse StopPoint records (TfL style), coordinates from NaPTAN or the file."""
    if naptan_stops is None:
        naptan_stops = read_naptan_stops()

    rows = []
    for p in doc.stop_points:
        stop_id = p.atco_code

        stop = _lookup_naptan_stop(naptan_stops, stop_id)
        if stop is None:
            try:
                stop = _stop_from_location(p)
            except (TypeError, ValueError):
                warnings.warn(
                    "Did not find a NaPTAN stop for '%s'" % stop_id,
                    UserWarning,
                    stacklevel=2,
                )
                continue
        rows.append(stop)

    return pd.DataFrame(rows, columns=_stop_columns)


def _get_txc_21_style_stops(doc, naptan_stops=None):
    """Parse AnnotatedStopPointRef records, coordinates from NaPTAN."""
    if naptan_stops is None:
        naptan_stops = read_naptan_stops()

    rows = []
    for p in doc.stop_points:
        stop_id = p.atco_code

        stop = _lookup_naptan_stop(naptan_stops, stop_id)
        if stop is None:
            warnings.warn(
                "Did not find a NaPTAN stop for '%s'" % stop_id,
                UserWarning,
                stacklevel=2,
            )
            continue
        rows.append(stop)

    return pd.DataFrame(rows, columns=_stop_columns)


def get_stops(doc):
    """Parse stop data from the StopPoint records of a TxcDocument"""
    naptan_stops = read_naptan_stops()

    if doc.stop_point_style == "StopPoint":
        stop_data = _get_tfl_style_stops(doc, naptan_stops)
    elif doc.stop_point_style == "AnnotatedStopPointRef":
        stop_data = _get_txc_21_style_stops(doc, naptan_stops)
    else:
        raise ValueError(
            "Did not find tag for Stop data in TransXchange xml. "
            "Could not parse Stop information from the TransXchange."
        )

    # Check that stops were found
    if len(stop_data) == 0:
        return None

    return stop_data
