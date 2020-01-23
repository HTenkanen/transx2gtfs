#!/usr/bin/env python
# -*- encoding: utf-8 -*-
from __future__ import absolute_import
from __future__ import print_function

import io
from glob import glob
import os
from os.path import basename
from os.path import dirname
from os.path import join
from os.path import splitext

from setuptools import find_packages
from setuptools import setup


def read(*names, **kwargs):
    with io.open(
        join(dirname(__file__), *names),
        encoding=kwargs.get('encoding', 'utf8')
    ) as fh:
        return fh.read()


requirements = ['pandas>=0.24.1',
                'pyproj>=2.1.3'
                'untangle>=1.1.1'
                'datetime'
                ]

# get all data dirs in the datasets module
data_files = []

data_files.append("transx2gtfs/data/Stops.txt")

setup(
    name='transx2gtfs',
    version='0.0.1',
    license='MIT',
    description='A Python tool to convert TransXchange data into GTFS.',
    author='Henrikki Tenkanen',
    author_email='h.tenkanen@ucl.ac.uk',
    url='https://github.com/htenkanen/transx2gtfs',
    package_data={"transx2gtfs": data_files},
    include_package_data=True,
    zip_safe=False,
    classifiers=[
        # complete classifier list: http://pypi.python.org/pypi?%3Aaction=list_classifiers
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Operating System :: Unix',
        'Operating System :: POSIX',
        'Operating System :: Microsoft :: Windows',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Topic :: Utilities',
    ],
    python_requires='>=3, !=3.0.*, !=3.1.*, !=3.2.*, !=3.3.*, !=3.4.*, <3.8',
    install_requires=[
        requirements
    ],
)
