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
    )


if __name__ == "__main__":
    main()
