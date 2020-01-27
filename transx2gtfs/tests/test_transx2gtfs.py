from transx2gtfs.data import get_path
import pytest

@pytest.fixture
def test_data():
    return get_path('test_data_dir')

@pytest.fixture
def temp_output_filepath():
    import tempfile
    import os
    temp_dir = tempfile.gettempdir()
    temp_fp = os.path.join(temp_dir, 'test_gtfs.zip')
    return temp_fp

def test_data_availability_for_testing(test_data):
    import os
    import glob
    assert os.path.isdir(test_data)
    files = glob.glob(os.path.join(test_data, '*.xml'))
    for file in files:
        assert os.path.isfile(file)

def test_agency_urls():
    from transx2gtfs.agency import get_agency_url
    import requests
    operator_codes = [
            'OId_LUL',
            'OId_DLR',
            'OId_TRS',
            'OId_CCR',
            'OId_CV',
            'OId_WFF',
            'OId_TCL',
            'OId_EAL',
            #'OId_CRC'
        ]
    for code in operator_codes:
        url = get_agency_url(code)

        req = requests.get(url)
        assert req.status_code == 200, "Web site '%s' does not exist." % url

def test_converting_to_gtfs(test_data, temp_output_filepath):
    import transx2gtfs
    import os
    from zipfile import ZipFile

    # Do the conversion
    transx2gtfs.convert(test_data, temp_output_filepath)

    # Check that the zip-file was created
    assert os.path.isfile(temp_output_filepath)

    # Check the contents
    zf = ZipFile(temp_output_filepath)
    zip_contents = zf.namelist()

    required_files = ['stops.txt', 'agency.txt', 'stop_times.txt',
                      'trips.txt', 'calendar.txt', 'routes.txt']
    for file in required_files:
        assert file in zip_contents







