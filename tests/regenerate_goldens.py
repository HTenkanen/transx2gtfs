"""Regenerate the golden GTFS tables of every fixture (run after a deliberate
output change): python tests/regenerate_goldens.py"""

import os
import pathlib
import sys

sys.path.insert(0, str(pathlib.Path(__file__).parent))

from transx2gtfs.data import get_path  # noqa: E402
from conftest import DATA_DIR  # noqa: E402
from test_golden import FIXTURES, TABLES, gtfs_tables, read_fixture  # noqa: E402

# stop_times is large; it is stored zipped, the other tables as plain CSV
ZIPPED = {"stop_times"}


def main():
    # The same offline data the test suite uses
    os.environ["TRANSX2GTFS_NAPTAN_PATH"] = str(DATA_DIR / "naptan_subset.csv")
    os.environ["TRANSX2GTFS_BANK_HOLIDAYS_PATH"] = get_path("bank_holidays")
    for fixture in sorted(FIXTURES):
        directory = DATA_DIR / "golden" / fixture
        directory.mkdir(parents=True, exist_ok=True)
        for old in list(directory.glob("*.csv")) + list(directory.glob("*.zip")):
            old.unlink()
        tables = gtfs_tables(read_fixture(fixture))
        for table in TABLES:
            frame = tables[table]
            if frame is None:
                continue
            if table in ZIPPED:
                frame.to_csv(
                    directory / (table + ".zip"),
                    index=False,
                    compression={"method": "zip", "archive_name": table + ".csv"},
                )
            else:
                frame.to_csv(directory / (table + ".csv"), index=False)
        print(
            fixture, {t: None if tables[t] is None else tables[t].shape for t in TABLES}
        )


if __name__ == "__main__":
    main()
