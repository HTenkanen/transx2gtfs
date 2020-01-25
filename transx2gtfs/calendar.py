import pandas as pd


def get_service_operative_days_info(data):
    """
    Get operating profile information from Services.Service.

    This is used if VehicleJourney does not contain the information.
    """
    try:
        reg_weekdays = data.TransXChange.Services.Service.OperatingProfile.RegularDayType.DaysOfWeek.get_elements()
        weekdays = []
        for elem in reg_weekdays:
            weekdays.append(elem._name)
        if len(weekdays) == 1:
            return weekdays[0]
        else:
            return "|".join(weekdays)
    except:
        # If service does not have OperatingProfile available, return None
        return None


def get_weekday_info(vehicle_journey_element):
    """Parses weekday info from TransXChange VehicleJourney element"""
    j = vehicle_journey_element
    try:
        reg_weekdays = j.OperatingProfile.RegularDayType.DaysOfWeek.get_elements()
        weekdays = []
        for elem in reg_weekdays:
            weekdays.append(elem._name)
        if len(weekdays) == 1:
            return weekdays[0]
        else:
            return "|".join(weekdays)
    except:
        # If journey does not have OperatingProfile available, return None
        return None

def parse_day_range(dayinfo):
    """Parse day range from TransXChange DayOfWeek element"""
    # Converters
    weekday_to_num = {'monday': 0, 'tuesday': 1, 'wednesday': 2,
                      'thursday': 3, 'friday': 4, 'saturday': 5,
                      'sunday': 6}
    num_to_weekday = {0: 'monday', 1: 'tuesday', 2: 'wednesday',
                      3: 'thursday', 4: 'friday', 5: 'saturday',
                      6: 'sunday'}

    # Containers
    active_days = []
    day_info = pd.DataFrame()

    # Process 'weekend'
    if "weekend" in dayinfo.strip().lower():
        active_days.append(5)
        active_days.append(6)

    # Check if dayinfo is specified as day-range
    elif "To" in dayinfo:
        day_range = dayinfo.split('To')
        start_i = weekday_to_num[day_range[0].lower()]
        end_i = weekday_to_num[day_range[1].lower()]

        # Get days when the service is active
        for idx in range(start_i, end_i + 1):
            # Get days
            active_days.append(idx)

    # Process a collection of individual weekdays
    elif "|" in dayinfo:
        days = dayinfo.split('|')
        for day in days:
            active_days.append(weekday_to_num[day.lower()])

    # If input is only a single day
    else:
        active_days.append(weekday_to_num[dayinfo.lower()])

    # Generate calendar row
    row = {}
    # Create columns
    for daynum in range(0, 7):
        # Get day column
        daycol = num_to_weekday[daynum]

        # Check if service is operative or not
        if daynum in active_days:
            active = 1
        else:
            active = 0
        row[daycol] = active

    # Generate DF
    day_info = day_info.append(row, ignore_index=True, sort=False)
    return day_info

def get_calendar(gtfs_info):
    """Parse calendar attributes from GTFS info DataFrame"""
    # Parse calendar
    use_cols = ['service_id', 'weekdays', 'start_date', 'end_date']
    calendar = gtfs_info.drop_duplicates(subset=use_cols)
    calendar = calendar[use_cols].copy()
    calendar = calendar.reset_index(drop=True)

    # Container for final results
    gtfs_calendar = pd.DataFrame()

    # Parse weekday columns
    for idx, row in calendar.iterrows():
        # Get dayinfo
        dayinfo = row['weekdays']

        # Parse day information
        dayrow = parse_day_range(dayinfo)

        # Add service and operation range info
        dayrow['service_id'] = row['service_id']
        dayrow['start_date'] = row['start_date']
        dayrow['end_date'] = row['end_date']

        # Add to container
        gtfs_calendar = gtfs_calendar.append(dayrow, ignore_index=True, sort=False)

    # Fix column order
    col_order = ['service_id', 'monday', 'tuesday', 'wednesday',
                 'thursday', 'friday', 'saturday', 'sunday',
                 'start_date', 'end_date']
    gtfs_calendar = gtfs_calendar[col_order].copy()

    # Ensure correct datatypes
    int_types = ['monday', 'tuesday', 'wednesday',
                 'thursday', 'friday', 'saturday', 'sunday']
    for col in int_types:
        gtfs_calendar[col] = gtfs_calendar[col].astype(int)

    return gtfs_calendar
