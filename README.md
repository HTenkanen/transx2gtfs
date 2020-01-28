# transx2gtfs 
[![PyPI version](https://badge.fury.io/py/transx2gtfs.svg)](https://badge.fury.io/py/transx2gtfs) [![build status](https://travis-ci.com/HTenkanen/transx2gtfs.svg?branch=master)](https://travis-ci.com/HTenkanen/transx2gtfs) [![Coverage Status](https://codecov.io/gh/HTenkanen/transx2gtfs/branch/master/graph/badge.svg)](https://codecov.io/gh/HTenkanen/transx2gtfs) [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.3628737.svg)](https://doi.org/10.5281/zenodo.3628737) [![Join the chat at https://gitter.im/transx2gtfs/community](https://badges.gitter.im/transx2gtfs/community.svg)](https://gitter.im/transx2gtfs/community?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge&utm_content=badge)

This is a small library to convert transit data from TransXchange format into GTFS -format that
can be used with various routing engines such as OpenTripPlanner. 

## Note!

This package is still in a Beta-phase, so use it at your own risk. 
Requires more testing. If you find an issue, you can contribute and 
help solving them by [raising an issue](https://github.com/HTenkanen/transx2gtfs/issues).

## Features

 - Reads TransXchange xml-files and converts into GTFS feed with all necessary information 
 according the General Transit Feed Specification.
 - Combines multiple TransXchange files into a single GTFS feed if present in the same folder.
 - Uses multiprocessing to parallelize the conversion process.
 - Parses dates of non-operation (bank holidays etc.) which are written to calendar_dates.txt.
 - Uses NaPTAN stops.  

## Install


The package is available at PyPi and you can install it with:

`$ pip install transx2gtfs`

Library works and is being tested with Python versions 3.6, 3.7 and 3.8.  

## Basic usage

After you have installed the library you can use it in a similar manner as any Python
library:

```python
>>> import transx2gtfs
>>> data_dir_for_transxchange_files = "data/my_transxchange_files"
>>> output_path = "data/my_converted_gtfs.zip"
>>> transx2gtfs.convert(data_dir_for_transxchange_files, output_path)
```

After you have successfully converted the TransXchange into GTFS, you can start doing
multimodal routing with your favourite routing engine such as OpenTripPlanner:

![OTP_example_in_London](img/London_multimodal_route.PNG)

## Citation

If you use this tool for research purposes, we encourage you to cite this work:

 - Henrikki Tenkanen. (2020, January 27). HTenkanen/transx2gtfs: transx2gtfs v0.3.3.1 (Version v0.3.3.1). Zenodo. http://doi.org/10.5281/zenodo.3628737

## Developers

- Henrikki Tenkanen, University College London