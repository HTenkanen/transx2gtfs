import pandas as pd

# Where the registered services of every operator are published; used when an
# operator has no URL of its own in the table (agency_url must be a URL)
DEFAULT_AGENCY_URL = "https://data.bus-data.dft.gov.uk/"


def get_agency_url(operator_code):
    """URL of an operator: its own for the known ones, else the Bus Open Data
    Service where its services are published"""
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
    return operator_urls.get(operator_code, DEFAULT_AGENCY_URL)


def get_agency_name(operator):
    """Operator name: trading name, else name on licence, short name or code"""
    name = (
        operator.trading_name
        or operator.name_on_licence
        or operator.short_name
        or operator.code
    )
    if name is None:
        raise ValueError("Operator '%s' does not have a name." % operator.id)
    return name


def get_agency(doc):
    """Parse agency information from the Operators of a TxcDocument"""
    if not doc.operators:
        raise ValueError("TransXChange document does not contain an Operator.")
    rows = []
    for operator in doc.operators:
        rows.append(
            dict(
                agency_id=operator.id,
                agency_name=get_agency_name(operator),
                agency_url=get_agency_url(operator.id),
                agency_timezone="Europe/London",
                agency_lang="en",
            )
        )
    return pd.DataFrame(rows)
