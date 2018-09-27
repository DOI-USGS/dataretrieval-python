"""
Tools for retrieving data from the National Atmospheric Deposition Program (NADP) including
the National Trends Network (NTN), the Mercury Deposition Network (MDN). 

National Trends Network
-----------------------
The  NTN provides longterm records of precipitation chemistry across the United States.
See nadp.slw.wisc.edu/ntn for more info.

Mercury Deposition Network
--------------------------
the MDN provides longterm records of total mercury (Hg) concentration and deposition in precipitation in the United States and Canada.
For more information visit nadp.slh.wisc.edu/MDN

Notes
-----
Gridded data on NADP is served as zipped tif files. Functions in this module will either download and extract the data,
when a path is specified, or open the data as a GDAL memory-mmapped file when no path is specified.


Todo list
---------
- include AIRMoN, AMNet, and AMoN
- add errorchecking
- add tests
"""

import requests
import zipfile
import io
import os
import re
import gdal

from os.path import basename
from uuid import uuid4

NADP_URL = 'https://nadp.slh.wisc.edu'
NADP_MAP_EXT = 'maplib/grids'

NTN_CONC_PARAMS = ['pH','So4','NO3','NH4','Ca','Mg','K','Na','Cl','Br']
NTN_DEP_PARAMS  = ['H','So4','NO3','NH4','Ca','Mg','K','Na','Cl','Br','N','SPlusN']

NTN_MEAS_TYPE = ['conc','dep','precip'] #concentration or deposition


class GDALMemFile():
    """Creates a GDAL memmory-mapped file

    Modeled after rasterio function of same name

    Example
    ------
    >>> with GDALMemFile(buf).open() as dataset:
            # do something

    TODO
    ----
    - could this work on url, file, or buf?
    """
    def __init__(self, buf):
        """
        Arugments
        ---------
        buf : buffer
            Buffer containing gdal formatted data.
        """
        self.buf = buf

    def open(self):
        """

        see gist.github.com/jleinonen/5781308 gdal_mmap.py
        """
        mmap_name = '/vsimem/' + uuid4().hex #vsimem is special GDAL string
        gdal.FileFromMemBuffer(mmap_name, self.buf)

        return gdal.Open(mmap_name)


class NADP_ZipFile(zipfile.ZipFile):
    """Extend zipfile.ZipFile for working on data from NADP
    """
    def tif_name(self):
        filenames = self.namelist()
        r = re.compile(".*tif$")
        tif_list = list(filter(r.match, filenames))
        return tif_list[0]

    def tif(self):
        return self.read( self.tif_name() )


def get_annual_MDN_map(measurement_type, year, path=None):
    """Download a MDN map from NDAP

    Parameters
    ----------
    measurement_type : string
        The type of measurement (concentration or deposition)

    year : string

    path : string
        download directory
    """
    url = '{}/{}/mdn/'.format(NADP_URL, NADP_MAP_EXT)

    filename = 'Hg_{}_{}.zip'.format(measurement_type,year)

    z = get_zip(url, filename)

    if path:
        z.extractall(path)
        return '{}{}{}'.format(path, os.sep, basename(filename))

    #else if no path return a buffer
    return GDALMemFile(z.tif())


def get_annual_NTN_map(measurement_type, measurement=None, year=None, path=None):
    """Download a NTN map from NDAP.

    Parameters
    ----------
    measurement : string
        The measured constituent to return.
    measurement_type : string
        The type of measurement (concentration, deposition, or precip)
    year : string

    path : string
        download directory

    Returns
    -------
    GDALMemFile containing in-memory GDAL object.
    or
    Path that data was extracted into if path was specified.

    Examples
    --------
    >>> get_annual_NTN_map(measurement='NO3', mesurement_type='conc',
                           year='1996')
    """
    url = '{}/{}/{}/'.format(NADP_URL, NADP_MAP_EXT, year)

    filename = '{}_{}.zip'.format(measurement_type, year)

    if measurement:
        filename = '{}_{}'.format(measurement, filename)

    z = get_zip(url, filename)

    if path:
        z.extractall(path)
        return '{}{}{}'.format(path, os.sep, basename(filename))

    #else if no path return a buffer
    return GDALMemFile(z.tif())


def get_zip(url, filename):
    """Gets a ZipFile at url and returns it

    Returns
    -------
    ZipFile

    TODO
    ----
    """
    req = requests.get(url + filename)
    req.raise_for_status()

    #z = zipfile.ZipFile(io.BytesIO(req.content))
    z = NADP_ZipFile(io.BytesIO(req.content))
    return z


