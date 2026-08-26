import os

__all__ = ["available", "get_path"]

_module_path = os.path.dirname(__file__)
_available_files = {"bank_holidays": "bank-holidays.json"}
available = list(_available_files.keys())


def get_path(dataset):
    """
    Get the path to a data file bundled with the package.

    Parameters
    ----------
    dataset : str
        The name of the dataset. See ``transx2gtfs.data.available`` for
        all options.
    """
    if dataset in _available_files:
        return os.path.abspath(os.path.join(_module_path, _available_files[dataset]))
    msg = "The dataset '{data}' is not available. ".format(data=dataset)
    msg += "Available datasets are {}".format(", ".join(available))
    raise ValueError(msg)
