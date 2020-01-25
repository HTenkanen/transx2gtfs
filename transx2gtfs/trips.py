
def get_trip_headsign(data, service_ref):
    """Parse trip headsign based on service reference id"""
    service = data.TransXChange.Services.Service
    if service.ServiceCode == service_ref:
        return service.Description.cdata
    else:
        raise ValueError("Could not find trip headsign for", service_ref)

def get_trips(gtfs_info):
    """Extract trips attributes from GTFS info DataFrame"""
    trip_cols = ['route_id', 'service_id', 'trip_id', 'trip_headsign', 'direction_id']

    # Extract trips from GTFS info
    trips = gtfs_info.drop_duplicates(subset=['route_id', 'service_id', 'trip_id'])
    trips = trips[trip_cols].copy()
    trips = trips.reset_index(drop=True)

    # Ensure correct data types
    trips['direction_id'] = trips['direction_id'].astype(int)

    return trips
