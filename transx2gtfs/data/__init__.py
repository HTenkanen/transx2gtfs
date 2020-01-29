import os

__all__ = ["available", "get_path"]

_module_path = os.path.dirname(__file__)
_available_files = {"naptan_stops": "Stops.txt",
                    "bank_holidays": "bank-holidays.json",
                    "test_data_dir": "test_data",
                    "test_tfl_format": ["test_data", "tfl_1-HAM-_-y05-2675925.xml"],
                    "test_txc21_format": ["test_data", "tfl_99-PIC-B-y05-4.xml"]
                    }
available = list(_available_files.keys())

def get_path(dataset):
    """
    Get the path to the data file.

    Parameters
    ----------
    dataset : str
        The name of the dataset. See ``caftes.data.available`` for
        all options.

    """
    if dataset in _available_files:
        if isinstance(_available_files[dataset], list):
            return os.path.abspath(os.path.join(_module_path, *_available_files[dataset]))
        else:
            return os.path.abspath(os.path.join(_module_path, _available_files[dataset]))
    else:
        msg = "The dataset '{data}' is not available. ".format(data=dataset)
        msg += "Available datasets are {}".format(", ".join(available))
        raise ValueError(msg)
