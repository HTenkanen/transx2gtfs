import pandas as pd


def get_agency_url(operator_code):
    """Get url for operators"""
    operator_urls = {
        "OId_LUL": "https://tfl.gov.uk/maps/track/tube",
        "OId_DLR": "https://tfl.gov.uk/modes/dlr/",
        "OId_TRS": "https://www.thamesriverservices.co.uk/",
        "OId_CCR": "https://www.citycruises.com/",
        "OId_CV": "https://www.thamesclippers.com/",
        "OId_WFF": "https://tfl.gov.uk/modes/river/woolwich-ferry",
        "OId_TCL": "https://tfl.gov.uk/modes/trams/",
        "OId_EAL": "https://www.emiratesairline.co.uk/",
        # 'OId_CRC': "https://www.crownrivercruise.co.uk/",
    }
    return operator_urls.get(operator_code, "NA")


def get_agency(doc):
    """Parse agency information from the first Operator of a TxcDocument"""
    if not doc.operators:
        raise ValueError("TransXChange document does not contain an Operator.")
    operator = doc.operators[0]
    agency_id = operator.id
    agency_name = operator.name_on_licence or operator.short_name or operator.code
    if agency_name is None:
        raise ValueError("Operator '%s' does not have a name." % agency_id)

    agency = dict(
        agency_id=agency_id,
        agency_name=agency_name,
        agency_url=get_agency_url(agency_id),
        agency_timezone="Europe/London",
        agency_lang="en",
    )
    return pd.DataFrame([agency])
