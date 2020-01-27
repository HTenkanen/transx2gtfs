import pandas as pd

def get_agency_url(operator_code):
    """Get url for operators"""
    operator_urls = {
        'OId_LUL': "https://tfl.gov.uk/maps/track/tube",
        'OId_DLR': "https://tfl.gov.uk/modes/dlr/",
        'OId_TRS': "https://www.thamesriverservices.co.uk/",
        'OId_CCR': "https://www.citycruises.com/",
        'OId_CV': "https://www.thamesclippers.com/",
        'OId_WFF': "https://tfl.gov.uk/modes/river/woolwich-ferry",
        'OId_TCL': "https://tfl.gov.uk/modes/trams/",
        'OId_EAL': "https://www.emiratesairline.co.uk/",
        #'OId_CRC': "https://www.crownrivercruise.co.uk/",
    }
    if operator_code in list(operator_urls.keys()):
        return operator_urls[operator_code]
    else:
        return "NA"

def get_agency(data):
    """Parse agency information from TransXchange elements"""
    # Container
    agency_data = pd.DataFrame()

    # Agency id
    agency_id = data.TransXChange.Operators.Operator.get_attribute('id')

    # Agency name
    agency_name = data.TransXChange.Operators.Operator.OperatorNameOnLicence.cdata

    # Agency url
    agency_url = get_agency_url(agency_id)

    # Agency timezone
    agency_tz = "Europe/London"

    # Agency language
    agency_lang = "en"

    # Parse row
    agency = dict(agency_id=agency_id,
                  agency_name=agency_name,
                  agency_url=agency_url,
                  agency_timezone=agency_tz,
                  agency_lang=agency_lang)

    agency_data = agency_data.append(agency, ignore_index=True, sort=False)
    return agency_data
