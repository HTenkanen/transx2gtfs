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


def get_routes(gtfs_info, doc):
    """Get routes from the Route records of a TxcDocument"""
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

        if r.description is None or r.private_code is None:
            raise ValueError("Route '%s' lacks Description or PrivateCode." % route_id)

        # Get route short name (test '-_-' separator)
        route_short_name = r.private_code.split("-_-")[0]

        rows.append(
            dict(
                route_id=route_id,
                agency_id=agency_id,
                route_short_name=route_short_name,
                route_long_name=r.description,
                route_type=route_type,
            )
        )

    routes = pd.DataFrame(rows, columns=use_cols)
    return routes
