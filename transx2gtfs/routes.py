import warnings

import pandas as pd

from transx2gtfs.utils import as_list


def get_mode(mode):
    """Parse mode from TransXChange value"""
    if mode in ["tram", "trolleyBus"]:
        return 0
    elif mode in ["underground", "metro"]:
        return 1
    elif mode == "rail":
        return 2
    elif mode in ["bus", "coach"]:
        return 3
    elif mode == "ferry":
        return 4


def get_routes(gtfs_info, data):
    """Get routes from TransXchange elements"""
    # Columns to use in output
    use_cols = [
        "route_id",
        "agency_id",
        "route_short_name",
        "route_long_name",
        "route_type",
    ]

    rows = []
    for r in as_list(data.TransXChange.Routes.Route):
        route_id = r.get_attribute("id")

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

        # Get route long name
        route_long_name = r.Description.cdata

        # Get route private id
        route_private_id = r.PrivateCode.cdata

        # Get route short name (test '-_-' separator)
        route_short_name = route_private_id.split("-_-")[0]

        # Route Section reference (might be needed somewhere)
        route_section_id = r.RouteSectionRef.cdata

        rows.append(
            dict(
                route_id=route_id,
                agency_id=agency_id,
                route_private_id=route_private_id,
                route_long_name=route_long_name,
                route_short_name=route_short_name,
                route_type=route_type,
                route_section_id=route_section_id,
            )
        )

    routes = pd.DataFrame(rows, columns=use_cols)
    return routes
