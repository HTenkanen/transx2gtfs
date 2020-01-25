import pandas as pd


def get_mode(mode):
    """Parse mode from TransXChange value"""
    if mode in ['tram', 'trolleyBus']:
        return 0
    elif mode in ['underground', 'metro']:
        return 1
    elif mode == 'rail':
        return 2
    elif mode in ['bus', 'coach']:
        return 3
    elif mode == 'ferry':
        return 4

def get_route_type(data):
    """Returns the route type according GTFS reference"""
    mode = data.TransXChange.Services.Service.Mode.cdata
    return get_mode(mode)

def get_routes(gtfs_info, data):
    """Get routes from TransXchange elements"""
    # Columns to use in output
    use_cols = ['route_id', 'agency_id', 'route_short_name', 'route_long_name', 'route_type']

    routes = pd.DataFrame()

    for r in data.TransXChange.Routes.Route:
        # Get route id
        route_id = r.get_attribute('id')

        # Get agency_id
        agency_id = gtfs_info.loc[gtfs_info['route_id'] == route_id, 'agency_id'].unique()[0]

        # Get route long name
        route_long_name = r.Description.cdata

        # Get route private id
        route_private_id = r.PrivateCode.cdata

        # Get route short name (test '-_-' separator)
        route_short_name = route_private_id.split('-_-')[0]

        # Route Section reference (might be needed somewhere)
        route_section_id = r.RouteSectionRef.cdata

        # Get route_type
        route_type = get_route_type(data)

        # Generate row
        route = dict(route_id=route_id,
                     agency_id=agency_id,
                     route_private_id=route_private_id,
                     route_long_name=route_long_name,
                     route_short_name=route_short_name,
                     route_type=route_type,
                     route_section_id=route_section_id
                     )
        routes = routes.append(route, ignore_index=True, sort=False)

    # Ensure that route type is integer
    routes['route_type'] = routes['route_type'].astype(int)

    # Select only required columns
    routes = routes[use_cols].copy()
    return routes
