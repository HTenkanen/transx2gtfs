# transx2gtfs 
[![build status](https://travis-ci.com/HTenkanen/transx2gtfs.svg?branch=master)](https://travis-ci.com/HTenkanen/transx2gtfs)

This is a small library to convert transit data from TransXchange format into GTFS -format that
can be used with various routing engines such as OpenTripPlanner. 

## Note!

This package is still in a Beta-phase, so use it at your own risk. Requires still testing.

## Install

The package is available at PyPi and you can install it with:

`$ pip install transx2gtfs`

## Basic usage

After you have installed the library you can use it in a similar manner as any Python
library:

```python
>>> import transx2gtfs
>>> data_dir_for_transxchange_files = "data/my_transxchange_files"
>>> output_path = "data/my_converted_gtfs.zip"
>>> transx2gtfs.convert(data_dir_for_transxchange_files, output_path)
```