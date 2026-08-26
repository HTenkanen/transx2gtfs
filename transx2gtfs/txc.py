"""
Streaming reader turning a TransXChange document into a plain-data model.

One ``lxml.etree.iterparse`` pass fills the dataclasses below; processed
elements are cleared as soon as their record is complete, so memory scales with
the model rather than the XML tree. Tags are matched by local name, so documents
with or without the TransXChange default namespace are handled alike.
"""

import io
import os
from dataclasses import dataclass, field

from lxml import etree

# Elements (directly below their container) that become one record each
_RECORD_TAGS = (
    "Operator",
    "LicensedOperator",
    "StopPoint",
    "AnnotatedStopPointRef",
    "RouteSection",
    "Route",
    "JourneyPatternSection",
    "ServicedOrganisation",
    "Service",
    "VehicleJourney",
)


@dataclass(slots=True)
class Operator:
    id: str
    code: str = None
    national_operator_code: str = None
    short_name: str = None
    name_on_licence: str = None
    trading_name: str = None
    licence_number: str = None


@dataclass(slots=True)
class StopPoint:
    atco_code: str
    common_name: str = None
    easting: str = None
    northing: str = None
    longitude: str = None
    latitude: str = None


@dataclass(slots=True)
class Route:
    id: str
    private_code: str = None
    description: str = None
    route_section_refs: list = field(default_factory=list)


@dataclass(slots=True)
class TimingLink:
    id: str
    from_stop: str
    to_stop: str
    from_sequence: str = None
    to_sequence: str = None
    from_activity: str = None
    to_activity: str = None
    from_timing_status: str = None
    to_timing_status: str = None
    route_link_ref: str = None
    run_time: str = None
    from_wait_time: str = None
    to_wait_time: str = None


@dataclass(slots=True)
class JourneyPatternSection:
    id: str
    timing_links: list = field(default_factory=list)


@dataclass(slots=True)
class OperatingProfile:
    """
    Name lists hold the child tag names of the respective element and are None
    when the element is absent. Date ranges are ``(start, end)`` ISO date string
    pairs; serviced organisation entries are ``(organisation code, day type)``
    pairs where the day type is ``"WorkingDays"`` or ``"Holidays"``.
    """

    days_of_week: list = None
    holidays_only: bool = False
    bank_holiday_days_of_operation: list = None
    bank_holiday_days_of_non_operation: list = None
    # OtherPublicHoliday entries: (description, date)
    other_public_holidays_of_operation: list = field(default_factory=list)
    other_public_holidays_of_non_operation: list = field(default_factory=list)
    special_days_of_operation: list = field(default_factory=list)
    special_days_of_non_operation: list = field(default_factory=list)
    serviced_organisation_days_of_operation: list = field(default_factory=list)
    serviced_organisation_days_of_non_operation: list = field(default_factory=list)


@dataclass(slots=True)
class ServicedOrganisation:
    code: str
    name: str = None
    working_days: list = field(default_factory=list)
    holidays: list = field(default_factory=list)


@dataclass(slots=True)
class Frequency:
    end_time: str = None
    interval: str = None


@dataclass(slots=True)
class VehicleJourneyTimingLink:
    journey_pattern_timing_link_ref: str
    run_time: str = None
    from_wait_time: str = None
    to_wait_time: str = None


@dataclass(slots=True)
class Line:
    id: str
    name: str = None


@dataclass(slots=True)
class JourneyPattern:
    id: str
    direction: str = None
    route_ref: str = None
    section_refs: list = field(default_factory=list)
    vehicle_type_code: str = None
    vehicle_type_description: str = None
    operating_profile: OperatingProfile = None


@dataclass(slots=True)
class Service:
    code: str
    private_code: str = None
    lines: list = field(default_factory=list)
    start_date: str = None
    end_date: str = None
    operating_profile: OperatingProfile = None
    registered_operator_ref: str = None
    mode: str = None
    description: str = None
    origin: str = None
    destination: str = None
    journey_patterns: list = field(default_factory=list)


@dataclass(slots=True)
class VehicleJourney:
    code: str
    private_code: str = None
    service_ref: str = None
    line_ref: str = None
    journey_pattern_ref: str = None
    vehicle_journey_ref: str = None
    operator_ref: str = None
    departure_time: str = None
    frequency: Frequency = None
    operating_profile: OperatingProfile = None
    vehicle_type_code: str = None
    vehicle_type_description: str = None
    timing_links: list = field(default_factory=list)
    notes: list = field(default_factory=list)


@dataclass(slots=True)
class TxcDocument:
    file_name: str = None
    schema_version: str = None
    creation_date_time: str = None
    modification_date_time: str = None
    operators: list = field(default_factory=list)
    stop_points: list = field(default_factory=list)
    # RouteLink id -> Distance text, for links that declare a distance
    route_link_distances: dict = field(default_factory=dict)
    # RouteLink id -> id of the RouteSection containing it
    route_link_sections: dict = field(default_factory=dict)
    routes: list = field(default_factory=list)
    journey_pattern_sections: list = field(default_factory=list)
    serviced_organisations: list = field(default_factory=list)
    services: list = field(default_factory=list)
    vehicle_journeys: list = field(default_factory=list)
    # "StopPoint" (coordinates in the file) or "AnnotatedStopPointRef" (refs only)
    stop_point_style: str = None

    def operator(self, operator_id):
        for operator in self.operators:
            if operator.id == operator_id:
                return operator
        raise KeyError("Operator '%s' not found" % operator_id)

    def serviced_organisation(self, code):
        for organisation in self.serviced_organisations:
            if organisation.code == code:
                return organisation
        raise KeyError("ServicedOrganisation '%s' not found" % code)

    def vehicle_journey(self, code):
        for journey in self.vehicle_journeys:
            if journey.code == code:
                return journey
        raise KeyError("VehicleJourney '%s' not found" % code)

    def journey_pattern_section(self, section_id):
        for section in self.journey_pattern_sections:
            if section.id == section_id:
                return section
        raise KeyError("JourneyPatternSection '%s' not found" % section_id)

    def journey_pattern(self, journey_pattern_id):
        for service in self.services:
            for jp in service.journey_patterns:
                if jp.id == journey_pattern_id:
                    return jp
        raise KeyError("JourneyPattern '%s' not found" % journey_pattern_id)


# Element access helpers (local-name based)
# -----------------------------------------


def _local(tag):
    return tag.rsplit("}", 1)[-1]


def _children(elem, name):
    return [c for c in elem if isinstance(c.tag, str) and _local(c.tag) == name]


def _find(elem, *names):
    for name in names:
        found = None
        for child in elem:
            if isinstance(child.tag, str) and _local(child.tag) == name:
                found = child
                break
        if found is None:
            return None
        elem = found
    return elem


def _text(elem, *names):
    """Text of a nested child, '' for an empty element, None if absent."""
    found = _find(elem, *names)
    if found is None:
        return None
    return found.text or ""


def _required_text(elem, *names):
    value = _text(elem, *names)
    if value is None:
        raise ValueError(
            "<%s> is missing required element %s" % (_local(elem.tag), "/".join(names))
        )
    return value


def _child_names(elem, *names):
    """Tag names of the children of a nested element; None if it is absent."""
    found = _find(elem, *names)
    if found is None:
        return None
    return [_local(c.tag) for c in found if isinstance(c.tag, str)]


# Record builders
# ---------------


def _date_ranges(elem, *names):
    """(StartDate, EndDate) pairs of the DateRange children of a nested element."""
    found = _find(elem, *names)
    if found is None:
        return []
    ranges = []
    for date_range in _children(found, "DateRange"):
        start = _text(date_range, "StartDate")
        end = _text(date_range, "EndDate")
        ranges.append((start, end if end is not None else start))
    return ranges


def _other_public_holidays(elem, *names):
    found = _find(elem, *names)
    if found is None:
        return []
    return [
        (_text(holiday, "Description"), _text(holiday, "Date"))
        for holiday in _children(found, "OtherPublicHoliday")
    ]


def _serviced_organisation_refs(elem, *names):
    """(organisation code, 'WorkingDays'|'Holidays') pairs of a day type element."""
    found = _find(elem, *names)
    if found is None:
        return []
    refs = []
    for day_type in found:
        if not isinstance(day_type.tag, str):
            continue
        kind = _local(day_type.tag)
        for ref in _children(day_type, "ServicedOrganisationRef"):
            refs.append((ref.text or "", kind))
    return refs


def _operating_profile(elem):
    profile = _find(elem, "OperatingProfile")
    if profile is None:
        return None
    bank_holidays = _child_names(profile, "BankHolidayOperation", "DaysOfOperation")
    bank_non = _child_names(profile, "BankHolidayOperation", "DaysOfNonOperation")
    return OperatingProfile(
        days_of_week=_child_names(profile, "RegularDayType", "DaysOfWeek"),
        holidays_only=_find(profile, "RegularDayType", "HolidaysOnly") is not None,
        bank_holiday_days_of_operation=(
            [n for n in bank_holidays if n != "OtherPublicHoliday"]
            if bank_holidays is not None
            else None
        ),
        bank_holiday_days_of_non_operation=(
            [n for n in bank_non if n != "OtherPublicHoliday"]
            if bank_non is not None
            else None
        ),
        other_public_holidays_of_operation=_other_public_holidays(
            profile, "BankHolidayOperation", "DaysOfOperation"
        ),
        other_public_holidays_of_non_operation=_other_public_holidays(
            profile, "BankHolidayOperation", "DaysOfNonOperation"
        ),
        special_days_of_operation=_date_ranges(
            profile, "SpecialDaysOperation", "DaysOfOperation"
        ),
        special_days_of_non_operation=_date_ranges(
            profile, "SpecialDaysOperation", "DaysOfNonOperation"
        ),
        serviced_organisation_days_of_operation=_serviced_organisation_refs(
            profile, "ServicedOrganisationDayType", "DaysOfOperation"
        ),
        serviced_organisation_days_of_non_operation=_serviced_organisation_refs(
            profile, "ServicedOrganisationDayType", "DaysOfNonOperation"
        ),
    )


def _serviced_organisation(elem):
    return ServicedOrganisation(
        code=_required_text(elem, "OrganisationCode"),
        name=_text(elem, "Name"),
        working_days=_date_ranges(elem, "WorkingDays"),
        holidays=_date_ranges(elem, "Holidays"),
    )


def _operator(elem):
    return Operator(
        id=elem.get("id"),
        code=_text(elem, "OperatorCode"),
        national_operator_code=_text(elem, "NationalOperatorCode"),
        short_name=_text(elem, "OperatorShortName"),
        name_on_licence=_text(elem, "OperatorNameOnLicence"),
        trading_name=_text(elem, "TradingName"),
        licence_number=_text(elem, "LicenceNumber"),
    )


def _stop_point(elem):
    location = _find(elem, "Place", "Location")
    stop = StopPoint(
        atco_code=_required_text(elem, "AtcoCode"),
        common_name=_text(elem, "Descriptor", "CommonName"),
    )
    if location is not None:
        stop.easting = _text(location, "Easting")
        stop.northing = _text(location, "Northing")
        stop.longitude = _text(location, "Translation", "Longitude")
        stop.latitude = _text(location, "Translation", "Latitude")
        if stop.longitude is None:
            stop.longitude = _text(location, "Longitude")
            stop.latitude = _text(location, "Latitude")
    return stop


def _annotated_stop_point_ref(elem):
    return StopPoint(
        atco_code=_required_text(elem, "StopPointRef"),
        common_name=_text(elem, "CommonName"),
    )


def _route(elem):
    return Route(
        id=elem.get("id"),
        private_code=_text(elem, "PrivateCode"),
        description=_text(elem, "Description"),
        route_section_refs=[r.text or "" for r in _children(elem, "RouteSectionRef")],
    )


def _timing_link(elem):
    from_elem = _find(elem, "From")
    to_elem = _find(elem, "To")
    if from_elem is None or to_elem is None:
        raise ValueError("<JourneyPatternTimingLink> is missing From/To")
    return TimingLink(
        id=elem.get("id"),
        from_stop=_required_text(from_elem, "StopPointRef"),
        to_stop=_required_text(to_elem, "StopPointRef"),
        from_sequence=from_elem.get("SequenceNumber"),
        to_sequence=to_elem.get("SequenceNumber"),
        from_activity=_text(from_elem, "Activity"),
        to_activity=_text(to_elem, "Activity"),
        from_timing_status=_text(from_elem, "TimingStatus"),
        to_timing_status=_text(to_elem, "TimingStatus"),
        route_link_ref=_text(elem, "RouteLinkRef"),
        run_time=_required_text(elem, "RunTime"),
        from_wait_time=_text(from_elem, "WaitTime"),
        to_wait_time=_text(to_elem, "WaitTime"),
    )


def _journey_pattern_section(elem):
    return JourneyPatternSection(
        id=elem.get("id"),
        timing_links=[
            _timing_link(link) for link in _children(elem, "JourneyPatternTimingLink")
        ],
    )


def _journey_pattern(elem):
    return JourneyPattern(
        id=elem.get("id"),
        direction=_text(elem, "Direction"),
        route_ref=_text(elem, "RouteRef"),
        # IDREFS: one element may list several ids separated by whitespace
        section_refs=[
            ref
            for r in _children(elem, "JourneyPatternSectionRefs")
            for ref in (r.text or "").split()
        ],
        vehicle_type_code=_text(elem, "Operational", "VehicleType", "VehicleTypeCode"),
        vehicle_type_description=_text(
            elem, "Operational", "VehicleType", "Description"
        ),
        operating_profile=_operating_profile(elem),
    )


def _service(elem):
    lines = _find(elem, "Lines")
    standard = _find(elem, "StandardService")
    return Service(
        code=_required_text(elem, "ServiceCode"),
        private_code=_text(elem, "PrivateCode"),
        lines=[
            Line(id=line.get("id"), name=_text(line, "LineName"))
            for line in (_children(lines, "Line") if lines is not None else [])
        ],
        start_date=_text(elem, "OperatingPeriod", "StartDate"),
        end_date=_text(elem, "OperatingPeriod", "EndDate"),
        operating_profile=_operating_profile(elem),
        registered_operator_ref=_text(elem, "RegisteredOperatorRef"),
        mode=_text(elem, "Mode"),
        description=_text(elem, "Description"),
        origin=_text(standard, "Origin") if standard is not None else None,
        destination=_text(standard, "Destination") if standard is not None else None,
        journey_patterns=[
            _journey_pattern(jp)
            for jp in (
                _children(standard, "JourneyPattern") if standard is not None else []
            )
        ],
    )


def _frequency(elem):
    frequency = _find(elem, "Frequency")
    if frequency is None:
        return None
    return Frequency(
        end_time=_text(frequency, "EndTime"),
        interval=_text(frequency, "Interval", "ScheduledFrequency"),
    )


def _vehicle_journey_timing_link(elem):
    from_elem = _find(elem, "From")
    to_elem = _find(elem, "To")
    return VehicleJourneyTimingLink(
        journey_pattern_timing_link_ref=_required_text(
            elem, "JourneyPatternTimingLinkRef"
        ),
        run_time=_text(elem, "RunTime"),
        from_wait_time=_text(from_elem, "WaitTime") if from_elem is not None else None,
        to_wait_time=_text(to_elem, "WaitTime") if to_elem is not None else None,
    )


def _note_text(note):
    """Text of a Note: its NoteText child (2.4), else its own text"""
    text = _text(note, "NoteText")
    if text is None:
        text = note.text or ""
    return text.strip()


def _vehicle_journey(elem):
    vehicle_journey_ref = _text(elem, "VehicleJourneyRef")
    departure_time = _text(elem, "DepartureTime")
    if departure_time is None and vehicle_journey_ref is None:
        raise ValueError(
            "<VehicleJourney> is missing required element DepartureTime "
            "(only a journey with VehicleJourneyRef may omit it)"
        )
    return VehicleJourney(
        code=_required_text(elem, "VehicleJourneyCode"),
        private_code=_text(elem, "PrivateCode"),
        service_ref=_required_text(elem, "ServiceRef"),
        line_ref=_text(elem, "LineRef"),
        journey_pattern_ref=_text(elem, "JourneyPatternRef"),
        vehicle_journey_ref=vehicle_journey_ref,
        operator_ref=_text(elem, "OperatorRef"),
        departure_time=departure_time,
        frequency=_frequency(elem),
        operating_profile=_operating_profile(elem),
        vehicle_type_code=_text(elem, "Operational", "VehicleType", "VehicleTypeCode"),
        vehicle_type_description=_text(
            elem, "Operational", "VehicleType", "Description"
        ),
        timing_links=[
            _vehicle_journey_timing_link(link)
            for link in _children(elem, "VehicleJourneyTimingLink")
        ],
        notes=[_note_text(note) for note in _children(elem, "Note")],
    )


def _add_record(doc, name, elem):
    if name in ("Operator", "LicensedOperator"):
        doc.operators.append(_operator(elem))
    elif name == "ServicedOrganisation":
        doc.serviced_organisations.append(_serviced_organisation(elem))
    elif name == "StopPoint":
        doc.stop_points.append(_stop_point(elem))
        doc.stop_point_style = doc.stop_point_style or "StopPoint"
    elif name == "AnnotatedStopPointRef":
        doc.stop_points.append(_annotated_stop_point_ref(elem))
        doc.stop_point_style = doc.stop_point_style or "AnnotatedStopPointRef"
    elif name == "Route":
        doc.routes.append(_route(elem))
    elif name == "JourneyPatternSection":
        doc.journey_pattern_sections.append(_journey_pattern_section(elem))
    elif name == "Service":
        doc.services.append(_service(elem))
    elif name == "VehicleJourney":
        doc.vehicle_journeys.append(_vehicle_journey(elem))


def _release(elem):
    """Free a finished element and everything before it in the document."""
    elem.clear()
    parent = elem.getparent()
    if parent is None:
        return
    while elem.getprevious() is not None:
        del parent[0]
    # Containers that precede this element's container are complete as well
    grandparent = parent.getparent()
    if grandparent is not None:
        while parent.getprevious() is not None:
            del grandparent[0]


def read_txc(source, file_name=None):
    """
    Read a TransXChange document into a :class:`TxcDocument`.

    ``source`` is a file path, ``bytes`` or a binary file-like object. The XML
    declaration's encoding is honoured in every case.
    """
    if isinstance(source, bytes):
        source = io.BytesIO(source)
    elif isinstance(source, (str, os.PathLike)):
        if file_name is None:
            file_name = os.path.basename(os.fspath(source))

    doc = TxcDocument(file_name=file_name)
    # Every element two levels below the root (a StopPoint, a VehicleJourney, but
    # also an AnnotatedNptgLocalityRef or a RouteSection) is handled and freed as
    # soon as it ends. Inside elements the model ignores, descendants are freed
    # as they end too, so no ignored subtree ever accumulates; inside a record the
    # subtree is kept until the record is built, so the peak is bounded by the
    # largest single record.
    depth = 0
    in_record = False
    # A RouteSection can carry track geometry for thousands of links, so its
    # RouteLinks are handled one at a time instead of with the whole section
    in_route_section = False
    route_section_id = None
    # Entities are never needed in TransXChange; not resolving them rules out
    # external-entity file access and entity-expansion blow-ups
    for event, elem in etree.iterparse(
        source, events=("start", "end"), resolve_entities=False
    ):
        if event == "start":
            depth += 1
            if depth == 1:
                doc.schema_version = elem.get("SchemaVersion")
                doc.creation_date_time = elem.get("CreationDateTime")
                doc.modification_date_time = elem.get("ModificationDateTime")
            elif depth == 3:
                name = _local(elem.tag) if isinstance(elem.tag, str) else None
                in_record = name in _RECORD_TAGS
                in_route_section = name == "RouteSection"
                route_section_id = elem.get("id") if in_route_section else None
            continue
        depth -= 1
        if depth == 2:
            if in_record and not in_route_section:
                _add_record(doc, _local(elem.tag), elem)
            _release(elem)
            in_record = False
            in_route_section = False
        elif depth == 3 and in_route_section:
            if isinstance(elem.tag, str) and _local(elem.tag) == "RouteLink":
                link_id = elem.get("id")
                if link_id is not None:
                    doc.route_link_sections[link_id] = route_section_id
                    distance = _text(elem, "Distance")
                    if distance is not None:
                        doc.route_link_distances[link_id] = distance
            _release(elem)
        elif depth > 2 and not in_record:
            _release(elem)
    return doc
