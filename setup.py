#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
.. _setup

Setup script of the `pyeudatnat` package.
    
**Dependencies**

*require*:      :mod:`io`, :mod:`os`, :mod:`sys`, :mod:`shutil`, :mod:`setuptools`

**Contents**
"""

# *credits*:      `gjacopo <jacopo.grazzini@ec.europa.eu>`_ 
# *since*:        Mon Apr  6 14:21:45 2020

#%%

import io, sys, os
from shutil import rmtree

from setuptools import find_packages, setup, Command

#from pyhcs import PACKNAME # of course not...

__THISDIR = os.path.dirname(__file__)


#%%

# Package meta-data.

PACKNAME            = 'pyeudatnat' # this package...
PACKURL             = 'https://github.com/eurostat/pyEUDatNat'
DESCRIPTION         = 'Python package supporting the automated harvesting, ingestion and formatting \
    of pan-European datasets made openly available by national/regional/local authorities.'
VERSION             = None

EMAIL               = 'jacopo.grazzini@ec.europa.eu'
AUTHOR              = 'Jacopo Grazzini'

REQUIRES_PYTHON = '>=3.6.0'

# packages required for this module to be executed
REQUIRED = [
    'numpy', 'pandas', 'geopandas', 'requests', 'chardet'
]
# in Python standard library (see https://docs.python.org/3/library/):
# 'collections', 'collections.abc', 'json', 'urllib', 'zipfile',
# 'datetime', 'time', 'calendar', 'copy', 'hashlib': 'hashlib', 'shutil': 'shutil',
# 'inspect', 'operator', 'functools', 'itertools'

# optional packages
EXTRAS = {
    'geopy': 'geopy', 
    'geojson': 'geojson', 
    'pyproj': 'pyproj',
    'gtrans': 'googletrans',
    'bs4': 'bs4',
    'chardet': 'chardet',
    'gisco': 'happygisco'
}


#%%

# import the README and use it as the long-description.
try:
    with io.open(os.path.join(__THISDIR, 'README.md'), encoding='utf-8') as f:
        long_description = '\n' + f.read()
except FileNotFoundError:
    long_description = DESCRIPTION

# load the package's __version__.py module as a dictionary.
about = {}
if not VERSION:
    with open(os.path.join(__THISDIR, PACKNAME, '__version__.py')) as f:
        exec(f.read(), about)
else:
    about['__version__'] = VERSION


#%%
#==============================================================================
# Class UploadCommand
#==============================================================================

class UploadCommand(Command):
    """Support setup.py upload.
    """

    description = 'Build and publish the package.'
    user_options = []

    @staticmethod
    def status(s):
        """Prints things in bold.
        """
        print('\033[1m{0}\033[0m'.format(s))

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        try:
            self.status('Removing previous builds…')
            rmtree(os.path.join(__THISDIR, 'dist'))
        except OSError:
            pass

        self.status('Building Source and Wheel (universal) distribution…')
        os.system('{0} setup.py sdist bdist_wheel --universal'.format(sys.executable))

        self.status('Uploading the package to PyPI via Twine…')
        os.system('twine upload dist/*')

        self.status('Pushing git tags…')
        os.system('git tag v{0}'.format(about['__version__']))
        os.system('git push --tags')
        
        sys.exit()


#%%
#==============================================================================
# setup
#==============================================================================

# where the magic happens...
# note: To use the 'upload' functionality of this file, you must:
#   $ pip install twine
setup(
    name =                              PACKNAME,
    version =                           about['__version__'],
    description =                       DESCRIPTION,
    long_description =                  long_description,
    long_description_content_type =     'text/markdown',
    author =                            AUTHOR,
    author_email =                      EMAIL,
    python_requires =                   REQUIRES_PYTHON,
    url =                               PACKURL,
    packages =                          find_packages(exclude=('tests',)),
    # py_modules =                      [PACKNAME],
    # entry_points={
    #     'console_scripts': ['mycli=mymodule:cli'],
    # },
    install_requires =                  REQUIRED,
    extras_require =                    EXTRAS,
    include_package_data =              True,
    license =                           'EUPL',
    classifiers =                       [
                                        # Trove classifiers
                                        'Programming Language :: Python',
                                        'Programming Language :: Python :: 3',
                                        'Programming Language :: Python :: 3.6',
                                        'Topic :: Database'
                                        # full list: https://pypi.python.org/pypi?%3Aaction=list_classifiers
                                        ],
    # $ setup.py publish support
    cmdclass =                          {
                                        'upload': UploadCommand,
                                        },
)
