"""Tests for the selection of current file versions (transx2gtfs.superseded)."""

from transx2gtfs.superseded import select_current_files
from transx2gtfs.txc import ServiceHeader, TxcHeader


def header(
    name,
    revision="1",
    modified="2026-01-01T00:00:00",
    services=(("S1", "2026-01-01", "2026-06-30"),),
    created=None,
):
    return TxcHeader(
        file_name=name,
        revision_number=revision,
        creation_date_time=created,
        modification_date_time=modified,
        services=[
            ServiceHeader(code=code, start_date=start, end_date=end)
            for code, start, end in services
        ],
    )


def select(*headers):
    return select_current_files([(h.file_name, h) for h in headers])


def test_higher_revision_supersedes_an_overlapping_version():
    old = header("old.xml", revision="3")
    new = header("new.xml", revision="16", modified="2026-02-01T00:00:00")
    result = select(old, new)
    assert result.kept == ["new.xml"]
    (dropped,) = result.dropped
    assert (dropped.item, dropped.file_name, dropped.service_code) == (
        "old.xml",
        "old.xml",
        "S1",
    )
    assert (dropped.revision_number, dropped.superseded_by) == ("3", "new.xml")
    assert dropped.superseded_by_revision == "16"
    # input order does not matter
    assert select(new, old).kept == ["new.xml"]


def test_disjoint_periods_are_all_kept():
    current = header(
        "current.xml", revision="3", services=(("S1", "2026-01-01", "2026-06-30"),)
    )
    future = header(
        "future.xml", revision="4", services=(("S1", "2026-07-01", "2026-12-31"),)
    )
    result = select(current, future)
    assert result.kept == ["current.xml", "future.xml"] and result.dropped == []
    # periods touching on one day overlap
    touching = header(
        "touch.xml", revision="4", services=(("S1", "2026-06-30", "2026-12-31"),)
    )
    assert select(current, touching).kept == ["touch.xml"]


def test_ties_are_broken_by_modification_time_then_input_order():
    a = header("a.xml", revision="2", modified="2026-01-01T00:00:00")
    b = header("b.xml", revision="2", modified="2026-03-01T00:00:00")
    assert select(a, b).kept == ["b.xml"]
    assert select(b, a).kept == ["b.xml"]
    # identical copies (nested archives): the first one is kept
    copy = header("a.xml", revision="2", modified="2026-01-01T00:00:00")
    result = select(a, copy)
    assert result.kept == ["a.xml"]
    assert result.dropped[0].superseded_by == "a.xml"


def test_modification_times_compare_as_instants():
    # 12:00 at +05:00 is 07:00 UTC, before 08:00 UTC
    east = header("east.xml", revision="2", modified="2026-01-01T12:00:00+05:00")
    utc = header("utc.xml", revision="2", modified="2026-01-01T08:00:00Z")
    assert select(east, utc).kept == ["utc.xml"]
    assert select(utc, east).kept == ["utc.xml"]
    # a naive time counts as UTC; fractional seconds are fine; unparsable is lowest
    naive = header("naive.xml", revision="2", modified="2026-01-01T07:30:00.974")
    assert select(naive, utc).kept == ["utc.xml"]
    assert select(east, naive).kept == ["naive.xml"]
    odd = header("odd.xml", revision="2", modified="yesterday")
    assert select(odd, east).kept == ["east.xml"]
    # the earliest representable instant is still a valid one: it outranks an
    # unparsable value, and an offset at the boundary is invalid, not an error
    dawn = header("dawn.xml", revision="2", modified="0001-01-01T00:00:00Z")
    assert select(odd, dawn).kept == ["dawn.xml"]
    edge = header("edge.xml", revision="2", modified="0001-01-01T00:00:00+05:00")
    assert select(edge, dawn).kept == ["dawn.xml"]
    # instants a microsecond apart are told apart, whatever the input order
    early = header("early.xml", revision="2", modified="2026-01-01T08:00:00.000001Z")
    late = header("late.xml", revision="2", modified="2026-01-01T08:00:00.000002Z")
    assert select(late, early).kept == ["late.xml"]
    assert select(early, late).kept == ["late.xml"]


def test_input_may_be_any_iterable():
    old = header("old.xml", revision="3")
    new = header("new.xml", revision="16")
    result = select_current_files((h.file_name, h) for h in (old, new))
    assert result.kept == ["new.xml"] and len(result.dropped) == 1


def test_unknown_or_non_numeric_revisions_rank_lowest():
    unknown = header("unknown.xml", revision=None)
    text = header("text.xml", revision="draft")
    numbered = header("one.xml", revision="1")
    result = select(unknown, text, numbered)
    assert result.kept == ["one.xml"]
    assert {d.file_name for d in result.dropped} == {"unknown.xml", "text.xml"}
    # any number, even a negative one, outranks an unknown revision
    negative = header("negative.xml", revision="-2")
    assert select(unknown, negative).kept == ["negative.xml"]
    # among unknowns the later modification wins
    later = header("later.xml", revision=None, modified="2026-05-01T00:00:00")
    assert select(unknown, later).kept == ["later.xml"]


def test_missing_end_date_uses_the_conversion_default():
    # no EndDate: one year after the latest of StartDate and the file's dates
    open_ended = header(
        "open.xml",
        revision="1",
        modified="2026-02-01T00:00:00",
        services=(("S1", "2026-01-01", None),),
    )
    inside = header(
        "inside.xml", revision="2", services=(("S1", "2026-12-01", "2027-01-31"),)
    )
    assert select(open_ended, inside).kept == ["inside.xml"]
    beyond = header(
        "beyond.xml", revision="2", services=(("S1", "2027-03-01", "2027-06-30"),)
    )
    assert select(open_ended, beyond).kept == ["open.xml", "beyond.xml"]
    # a service without StartDate cannot be compared and is kept
    undated = header("undated.xml", revision="0", services=(("S1", None, None),))
    assert select(undated, inside).kept == ["undated.xml", "inside.xml"]


def test_a_file_stays_while_one_of_its_services_is_current():
    both = header(
        "both.xml",
        revision="1",
        services=(
            ("S1", "2026-01-01", "2026-06-30"),
            ("S2", "2026-01-01", "2026-06-30"),
        ),
    )
    newer_s1 = header(
        "s1.xml", revision="2", services=(("S1", "2026-01-01", "2026-06-30"),)
    )
    assert select(both, newer_s1).kept == ["both.xml", "s1.xml"]
    newer_s2 = header(
        "s2.xml", revision="2", services=(("S2", "2026-01-01", "2026-06-30"),)
    )
    result = select(both, newer_s1, newer_s2)
    assert result.kept == ["s1.xml", "s2.xml"]
    assert result.dropped[0].file_name == "both.xml"


def test_a_file_repeating_a_service_code_is_judged_per_occurrence():
    # one file carries the same code twice: a superseded winter period and a
    # current summer period; it stays because the summer occurrence is current
    twice = header(
        "twice.xml",
        revision="1",
        services=(
            ("S1", "2026-01-01", "2026-03-31"),
            ("S1", "2026-07-01", "2026-09-30"),
        ),
    )
    winter = header(
        "winter.xml", revision="2", services=(("S1", "2026-01-01", "2026-03-31"),)
    )
    assert select(twice, winter).kept == ["twice.xml", "winter.xml"]
    assert select(winter, twice).kept == ["winter.xml", "twice.xml"]
    # when both occurrences are superseded the file is dropped
    summer = header(
        "summer.xml", revision="2", services=(("S1", "2026-07-01", "2026-09-30"),)
    )
    result = select(twice, winter, summer)
    assert result.kept == ["winter.xml", "summer.xml"]
    assert result.dropped[0].file_name == "twice.xml"


def test_unrelated_services_and_empty_input():
    a = header("a.xml", revision="1", services=(("S1", "2026-01-01", "2026-06-30"),))
    b = header("b.xml", revision="9", services=(("S2", "2026-01-01", "2026-06-30"),))
    assert select(a, b).kept == ["a.xml", "b.xml"]
    assert select().kept == [] and select().dropped == []
    # a file without services is kept (the conversion reports it)
    empty = header("empty.xml", services=())
    assert select(empty).kept == ["empty.xml"]
