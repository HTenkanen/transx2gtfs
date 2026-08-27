"""Command-line entry point: ``transx2gtfs`` or ``python -m transx2gtfs``."""

import argparse

from transx2gtfs import __version__
from transx2gtfs.converter import convert


def build_parser():
    parser = argparse.ArgumentParser(
        prog="transx2gtfs",
        description="Convert TransXChange XML files into a GTFS zip file.",
    )
    parser.add_argument(
        "input",
        help="directory or zip file containing TransXChange .xml files "
        "(nested zip files are supported)",
    )
    parser.add_argument("output", help="path of the GTFS zip file to write")
    parser.add_argument(
        "--append",
        action="store_true",
        help="append to the intermediate database left by a previous run "
        "next to the output file instead of starting from scratch",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="number of worker processes (default: number of CPUs minus one)",
    )
    parser.add_argument(
        "--file-size-limit",
        type=int,
        default=2000,
        help="skip XML files larger than this many megabytes (default: 2000)",
    )
    parser.add_argument(
        "--keep-superseded",
        action="store_true",
        help="convert every file, also versions superseded by a newer revision "
        "of the same service (by default only the current version is converted)",
    )
    parser.add_argument(
        "--naptan-path",
        default=None,
        help="local NaPTAN CSV to read stop coordinates from (default: "
        "TRANSX2GTFS_NAPTAN_PATH if set, else a copy downloaded into the user's "
        "cache directory and refreshed monthly)",
    )
    parser.add_argument(
        "--refresh-naptan",
        action="store_true",
        help="download the NaPTAN data anew even if the cached copy is recent",
    )
    parser.add_argument(
        "--log-file",
        default=None,
        help="append the progress messages and data warnings to this file as well",
    )
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s {__version__}"
    )
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    convert(
        args.input,
        args.output,
        append_to_existing=args.append,
        worker_cnt=args.workers,
        file_size_limit=args.file_size_limit,
        skip_superseded=not args.keep_superseded,
        naptan_path=args.naptan_path,
        refresh_naptan=args.refresh_naptan,
        log_file=args.log_file,
    )


if __name__ == "__main__":
    main()
