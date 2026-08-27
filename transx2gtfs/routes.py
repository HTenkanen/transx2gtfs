import warnings

import pandas as pd


def get_mode(mode):
    """Parse mode from TransXChange value; a missing mode is treated as bus"""
    if mode is None or mode == "":
        warnings.warn("Service has no Mode, assuming bus.", UserWarning, stacklevel=2)
        return 3
    key = mode.strip().lower()
    if key in ("tram",):
        return 0
    elif key in ("underground", "metro"):
        return 1
    elif key == "rail":
        return 2
    elif key in ("bus", "coach"):
        return 3
    elif key == "ferry":
        return 4
    elif key == "trolleybus":
        return 11
    raise ValueError("Unknown Mode '%s'." % mode)


def _origin_destination(doc, route_info):
    """'Origin - Destination' of the service whose journeys use the route"""
    service_ref = route_info["service_ref"].unique()[0]
    for service in doc.services:
        if service.code == service_ref:
            return "%s - %s" % (
                (service.origin or "").strip(),
                (service.destination or "").strip(),
            )
    return ""


def synthetic_route_ids(doc):
    """
    route_id per (service code, line id) for routes synthesised for journey
    patterns without a matching Route: '<service>_<line>', made unique against
    the declared Route ids and the ids generated before it (document order).
    """
    used = {route.id for route in doc.routes}
    ids = {}
    for service in doc.services:
        for line in service.lines:
            route_id = "%s_%s" % (service.code, line.id)
            while route_id in used:
                route_id += "_"
            used.add(route_id)
            ids[(service.code, line.id)] = route_id
    return ids


def get_routes(gtfs_info, doc):
    """
    Get routes from the Route records of a TxcDocument. Journey patterns
    without a RouteRef that could not be matched to a Route use a synthetic
    route per service and line, which is added here.
    """
    # Columns to use in output
    use_cols = [
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_type",
    ]

    rows = []
    for r in doc.routes:
        route_id = r.id

        # Agency and travel mode come from the journey patterns using the route
        route_info = gtfs_info.loc[gtfs_info["route_id"] == route_id]
        if len(route_info) == 0:
            warnings.warn(
                "Route '%s' is not used by any vehicle journey, skipping." % route_id,
                UserWarning,
                stacklevel=2,
            )
            continue
        agency_id = route_info["agency_id"].unique()[0]
        route_type = int(route_info["travel_mode"].unique()[0])

        # Route short name from the PrivateCode (test '-_-' separator), else the
        # line name of the journeys using the route; long name from the
        # Description, else the service's origin and destination
        if r.private_code:
            route_short_name = r.private_code.split("-_-")[0]
        else:
            route_short_name = str(route_info["line_name"].unique()[0])
        route_long_name = (r.description or "").strip()
        if not route_long_name:
            route_long_name = _origin_destination(doc, route_info)

        rows.append(
            dict(
                route_id=route_id,
                agency_id=agency_id,
                route_short_name=route_short_name,
                route_long_name=route_long_name,
                route_type=route_type,
            )
        )

    # Synthetic routes for journeys whose pattern has no (matching) Route
    synthetic = synthetic_route_ids(doc)
    for service in doc.services:
        for line in service.lines:
            route_id = synthetic[(service.code, line.id)]
            route_info = gtfs_info.loc[gtfs_info["route_id"] == route_id]
            if len(route_info) == 0:
                continue
            rows.append(
                dict(
                    route_id=route_id,
                    agency_id=route_info["agency_id"].unique()[0],
                    route_short_name=(line.name or "").strip(),
                    route_long_name="%s - %s"
                    % (
                        (service.origin or "").strip(),
                        (service.destination or "").strip(),
                    ),
                    route_type=int(route_info["travel_mode"].unique()[0]),
                )
            )

    return pd.DataFrame(rows, columns=use_cols)
