import os
import time

import pytest
import pandas as pd
from pandas import DataFrame
from pandas.testing import assert_frame_equal

from transx2gtfs.stops import (
    _get_tfl_style_stops,
    _get_txc_21_style_stops,
    get_stops,
    read_naptan_stops,
)
from transx2gtfs.txc import read_txc

REQUIRED_COLUMNS = ["stop_id", "stop_name", "stop_lat", "stop_lon"]


def test_read_naptan_stops():
    stops = read_naptan_stops()
    assert list(stops.columns) == REQUIRED_COLUMNS
    assert len(stops) == 283
    assert stops["stop_id"].is_unique
    assert pd.api.types.is_string_dtype(stops["stop_id"])


def test_reading_stops_from_txc21(txc21_file):
    doc = read_txc(txc21_file)
    stops = _get_txc_21_style_stops(doc)

    assert isinstance(stops, DataFrame)
    assert stops.shape == (3, 4)
    assert list(stops.columns) == REQUIRED_COLUMNS
    for col in REQUIRED_COLUMNS:
        assert stops[col].hasnans is False


def test_reading_stops_from_tfl(tfl_file):
    doc = read_txc(tfl_file)
    stops = _get_tfl_style_stops(doc)

    assert isinstance(stops, DataFrame)
    assert stops.shape == (43, 4)
    assert list(stops.columns) == REQUIRED_COLUMNS
    for col in REQUIRED_COLUMNS:
        assert stops[col].hasnans is False

    # Coordinates come from NaPTAN and are in London
    assert stops["stop_lat"].between(51.3, 51.7).all()
    assert stops["stop_lon"].between(-0.6, 0.3).all()


def test_get_stops_detects_style(tfl_file, txc21_file):
    tfl = read_txc(tfl_file)
    assert_frame_equal(get_stops(tfl), _get_tfl_style_stops(tfl))

    txc21 = read_txc(txc21_file)
    assert_frame_equal(get_stops(txc21), _get_txc_21_style_stops(txc21))


# Cache directory and refresh -------------------------------------------------------


def test_cache_dir_follows_the_platform_and_overrides(monkeypatch, tmp_path):
    from transx2gtfs import stops

    monkeypatch.delenv("TRANSX2GTFS_CACHE_DIR", raising=False)
    monkeypatch.delenv("XDG_CACHE_HOME", raising=False)
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setattr(stops.os.path, "expanduser", lambda p: str(tmp_path))
    monkeypatch.setattr(stops.sys, "platform", "linux")
    assert stops.cache_dir() == str(tmp_path / ".cache" / "transx2gtfs")
    monkeypatch.setenv("XDG_CACHE_HOME", str(tmp_path / "xdg"))
    assert stops.cache_dir() == str(tmp_path / "xdg" / "transx2gtfs")
    monkeypatch.setattr(stops.sys, "platform", "darwin")
    assert stops.cache_dir() == str(tmp_path / "Library" / "Caches" / "transx2gtfs")
    monkeypatch.setattr(stops.sys, "platform", "win32")
    monkeypatch.delenv("LOCALAPPDATA", raising=False)
    assert stops.cache_dir() == str(tmp_path / "AppData" / "Local" / "transx2gtfs")
    monkeypatch.setenv("LOCALAPPDATA", str(tmp_path / "local"))
    assert stops.cache_dir() == str(tmp_path / "local" / "transx2gtfs")
    monkeypatch.setenv("TRANSX2GTFS_CACHE_DIR", str(tmp_path / "mine"))
    assert stops.cache_dir() == str(tmp_path / "mine")
    assert stops.default_naptan_path() == str(tmp_path / "mine" / "naptan.csv")


@pytest.fixture
def cache(monkeypatch, tmp_path):
    """A NaPTAN cache in tmp_path, no env var, a fake downloader recording calls"""
    from transx2gtfs import stops

    monkeypatch.delenv("TRANSX2GTFS_NAPTAN_PATH", raising=False)
    monkeypatch.setenv("TRANSX2GTFS_CACHE_DIR", str(tmp_path / "cache"))
    calls = []

    def fake_download(target_file, url=stops.NAPTAN_URL):
        calls.append(target_file)
        os.makedirs(os.path.dirname(target_file), exist_ok=True)
        with open(target_file, "w") as f:
            f.write("ATCOCode,CommonName,Latitude,Longitude\n")
        return target_file

    monkeypatch.setattr(stops, "download_naptan", fake_download)
    return stops, calls, str(tmp_path / "cache" / "naptan.csv")


def age(path, days):
    stamp = time.time() - days * 24 * 3600
    os.utime(path, (stamp, stamp))


def test_cached_download_is_fetched_once_and_refreshed_when_stale(cache):
    stops, calls, cached = cache
    assert stops.ensure_naptan_data() == cached and calls == [cached]
    assert stops.ensure_naptan_data() == cached and len(calls) == 1  # fresh
    age(cached, 29)
    assert stops.ensure_naptan_data() == cached and len(calls) == 1
    age(cached, 31)
    assert stops.ensure_naptan_data() == cached and len(calls) == 2  # stale
    assert stops.ensure_naptan_data(refresh=True) == cached and len(calls) == 3


def test_failed_refresh_keeps_the_cached_copy_with_a_warning(cache, monkeypatch):
    stops, calls, cached = cache
    stops.ensure_naptan_data()
    age(cached, 40)

    def fail(target_file, url=stops.NAPTAN_URL):
        raise stops.URLError("offline")

    monkeypatch.setattr(stops, "download_naptan", fail)
    with pytest.warns(UserWarning, match="Could not download the NaPTAN stops"):
        assert stops.ensure_naptan_data() == cached
    with pytest.warns(UserWarning, match="using the copy from"):
        assert stops.ensure_naptan_data(refresh=True) == cached

    # a truncated response is a failed refresh too
    def truncated(target_file, url=stops.NAPTAN_URL):
        raise stops.http.client.IncompleteRead(b"partial")

    monkeypatch.setattr(stops, "download_naptan", truncated)
    with pytest.warns(UserWarning, match="Could not download the NaPTAN stops"):
        assert stops.ensure_naptan_data(refresh=True) == cached
    # a missing cache with no way to download it is an error, not a warning
    os.remove(cached)
    monkeypatch.setattr(stops, "download_naptan", fail)
    with pytest.raises(stops.URLError):
        stops.ensure_naptan_data()


def test_a_symlinked_cache_is_refused(cache, tmp_path):
    stops, calls, cached = cache
    os.makedirs(os.path.dirname(cached), exist_ok=True)
    target = tmp_path / "elsewhere.csv"
    target.write_text("ATCOCode,CommonName,Latitude,Longitude\n")
    os.symlink(target, cached)
    with pytest.raises(OSError, match="Refusing to use symlink"):
        stops.ensure_naptan_data()
    with pytest.raises(OSError, match="Refusing to use symlink"):
        stops.ensure_naptan_data(refresh=True)
    assert calls == []


def test_parsed_stops_follow_the_file_contents(cache):
    stops, calls, cached = cache
    stops.ensure_naptan_data()
    assert len(stops.read_naptan_stops()) == 0
    # same mtime, different contents: the parsed frame is refreshed
    stat = os.stat(cached)
    with open(cached, "a") as f:
        f.write("9400ZZLUKSX3,Kings Cross,51.53,-0.12\n")
    os.utime(cached, ns=(stat.st_atime_ns, stat.st_mtime_ns))
    assert len(stops.read_naptan_stops()) == 1


def test_a_given_file_is_used_as_it_is(cache, monkeypatch, tmp_path):
    stops, calls, cached = cache
    own = tmp_path / "own.csv"
    own.write_text("ATCOCode,CommonName,Latitude,Longitude\n")
    age(own, 400)
    assert stops.ensure_naptan_data(str(own)) == str(own)
    assert stops.ensure_naptan_data(str(own), refresh=True) == str(own)
    monkeypatch.setenv("TRANSX2GTFS_NAPTAN_PATH", str(own))
    assert stops.ensure_naptan_data() == str(own)
    assert calls == []
    with pytest.raises(FileNotFoundError, match="does not exist"):
        stops.ensure_naptan_data(str(tmp_path / "missing.csv"))
    # even a given path equal to the cache path is used as it is
    os.makedirs(os.path.dirname(cached), exist_ok=True)
    with open(cached, "w") as f:
        f.write("ATCOCode,CommonName,Latitude,Longitude\n")
    age(cached, 400)
    monkeypatch.setenv("TRANSX2GTFS_NAPTAN_PATH", cached)
    assert stops.ensure_naptan_data(refresh=True) == cached
    monkeypatch.delenv("TRANSX2GTFS_NAPTAN_PATH")
    assert stops.ensure_naptan_data(cached, refresh=True) == cached
    assert calls == []
    # without a given path the same stale file is refreshed
    assert stops.ensure_naptan_data() == cached and calls == [cached]
