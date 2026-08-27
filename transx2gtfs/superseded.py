"""
Selection of the current version of every service among a set of TransXChange
files.

Change archives (the Bus Open Data Service bulk download, TNDS) hold several
revisions of one service. For each ServiceCode, a file version is superseded
when another version whose operating period overlaps outranks it: a higher
RevisionNumber, else a later ModificationDateTime, else the earlier file in the
input (an identical copy). Versions with disjoint periods, such as the current
timetable and a future re-registration, are all kept.
"""

from dataclasses import dataclass
from datetime import datetime, timezone

from transx2gtfs.transxchange import parse_date, service_end_date


@dataclass(slots=True)
class DroppedFile:
    """A file left out because every service in it is superseded"""

    item: object
    file_name: str
    service_code: str
    revision_number: "str | None"
    superseded_by: str
    superseded_by_revision: "str | None"


@dataclass(slots=True)
class Selection:
    kept: list
    dropped: list


# Ranks are (valid, value) pairs: every valid value outranks every invalid one
_INVALID = (0, 0)


def _revision(header):
    """RevisionNumber rank: (1, number) when numeric, else lowest"""
    try:
        return (1, int(header.revision_number))
    except (TypeError, ValueError):
        return _INVALID


def _modified(header):
    """ModificationDateTime rank: (1, UTC datetime at microsecond precision; a
    naive value counts as UTC), else lowest for a missing or unparsable value"""
    text = (header.modification_date_time or "").strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    try:
        stamp = datetime.fromisoformat(text)
        if stamp.tzinfo is None:
            stamp = stamp.replace(tzinfo=timezone.utc)
        return (1, stamp.astimezone(timezone.utc))
    except (ValueError, OverflowError):
        return _INVALID


def _period(header, service):
    """Operating period of a service as dates, with the conversion's EndDate default"""
    if not service.start_date:
        return None
    start = parse_date(service.start_date)
    return start, service_end_date(header, service, start)


def select_current_files(items):
    """
    Choose the files to convert from ``(item, header)`` pairs, where ``item``
    is whatever identifies the file to the caller and ``header`` its
    :class:`~transx2gtfs.txc.TxcHeader`.

    Returns a :class:`Selection`: ``kept`` holds the items to convert in input
    order, ``dropped`` a :class:`DroppedFile` per file whose services are all
    superseded (naming the service and the file that supersedes it).
    """
    items = list(items)
    # Every occurrence of every service code: (rank, file index, header, service)
    versions = {}
    for index, (_item, header) in enumerate(items):
        rank = (_revision(header), _modified(header), -index)
        for service in header.services:
            versions.setdefault(service.code, []).append((rank, index, header, service))

    # Per file, per service occurrence: the strongest overlapping version of the
    # same code that outranks it (None when the occurrence is current)
    winners = {index: [] for index in range(len(items))}
    for code, entries in versions.items():
        for rank, index, header, service in entries:
            period = _period(header, service)
            best = None
            for other_rank, other_index, other_header, other_service in entries:
                if other_index == index or other_rank <= rank:
                    continue
                other_period = _period(other_header, other_service)
                if period is None or other_period is None:
                    continue
                if period[0] <= other_period[1] and other_period[0] <= period[1]:
                    if best is None or other_rank > best[0]:
                        best = (other_rank, other_header)
            winners[index].append((code, best))

    kept, dropped = [], []
    for index, (item, header) in enumerate(items):
        occurrences = winners[index]
        if occurrences and all(best is not None for _, best in occurrences):
            # Report the file's first service and the file that supersedes it
            code, (_, winner) = occurrences[0]
            dropped.append(
                DroppedFile(
                    item=item,
                    file_name=header.file_name,
                    service_code=code,
                    revision_number=header.revision_number,
                    superseded_by=winner.file_name,
                    superseded_by_revision=winner.revision_number,
                )
            )
        else:
            kept.append(item)
    return Selection(kept=kept, dropped=dropped)
