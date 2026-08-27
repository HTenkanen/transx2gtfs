"""Regenerate the golden GTFS tables of every fixture (run after a deliberate
output change): python tests/regenerate_goldens.py"""

import os
import pathlib
import sys
from zipfile import ZIP_DEFLATED, ZipFile, ZipInfo

TESTS_DIR = pathlib.Path(__file__).resolve().parent
sys.path.insert(0, str(TESTS_DIR))
sys.path.insert(0, str(TESTS_DIR.parent))

from transx2gtfs.data import get_path  # noqa: E402
from transx2gtfs.txc import read_txc  # noqa: E402
from conftest import DATA_DIR, UNPACKED_DIR  # noqa: E402
from test_golden import FIXTURES, TABLES, gtfs_tables  # noqa: E402

# Tables larger than a few hundred rows are stored zipped, the others as CSV
ZIP_ROWS = 500


def write_zipped(frame, path, name):
    """Zip a CSV with fixed entry metadata and line endings, so that an unchanged
    table gives identical bytes (for one zlib version)"""
    entry = ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
    entry.compress_type = ZIP_DEFLATED
    entry.create_system = 3  # Unix
    entry.external_attr = 0o644 << 16
    data = frame.to_csv(index=False, lineterminator="\n").encode("utf-8")
    with ZipFile(path, "w") as archive:
        archive.writestr(entry, data)


def main():
    # The same offline data the test suite uses
    os.environ["TRANSX2GTFS_NAPTAN_PATH"] = str(DATA_DIR / "naptan_subset.csv")
    os.environ["TRANSX2GTFS_BANK_HOLIDAYS_PATH"] = get_path("bank_holidays")
    for fixture in sorted(FIXTURES):
        directory = DATA_DIR / "golden" / fixture
        directory.mkdir(parents=True, exist_ok=True)
        for old in list(directory.glob("*.csv")) + list(directory.glob("*.zip")):
            old.unlink()
        tables = gtfs_tables(read_txc(UNPACKED_DIR / (fixture + ".xml")))
        for table in TABLES:
            frame = tables[table]
            if frame is None:
                continue
            if len(frame) > ZIP_ROWS:
                write_zipped(frame, directory / (table + ".zip"), table + ".csv")
            else:
                frame.to_csv(directory / (table + ".csv"), index=False)
        print(
            fixture, {t: None if tables[t] is None else tables[t].shape for t in TABLES}
        )


if __name__ == "__main__":
    main()
