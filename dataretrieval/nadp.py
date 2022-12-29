"""
Tools for retrieving data from the National Atmospheric Deposition Program (NADP) including
the National Trends Network (NTN), the Mercury Deposition Network (MDN).

National Trends Network
-----------------------
The  NTN provides long-term records of precipitation chemistry across the
United States. See https://nadp.slh.wisc.edu/ntn for more info.

Mercury Deposition Network
--------------------------
The MDN provides long-term records of total mercury (Hg) concentration and
deposition in precipitation in the United States and Canada. For more
information visit https://nadp.slh.wisc.edu/networks/mercury-deposition-network/

Notes
-----
Gridded data on NADP is served as zipped tif files. Functions in this module
will either download and extract the data, when a path is specified, or open
the data as a GDAL memory-mapped file when no path is specified.

.. todo::

    - include AIRMoN, AMNet, and AMoN
    - flexible handling of strings for parameters and measurement types
    - add errorchecking
    - add tests

"""

import requests
import zipfile
import io
import os
import re
import warnings

try:
    import gdal
except:
    try:
        from osgeo import gdal
    except:
        warnings.warn('GDAL not installed. Some functions will not work.')
        import unittest.mock as mock
        gdal = mock.MagicMock()

from os.path import basename
from uuid import uuid4

NADP_URL = 'https://nadp.slh.wisc.edu'
NADP_MAP_EXT = 'filelib/maps'

NTN_CONC_PARAMS = ['pH', 'So4', 'NO3', 'NH4', 'Ca',
                   'Mg', 'K', 'Na', 'Cl', 'Br']
NTN_DEP_PARAMS  = ['H', 'So4', 'NO3', 'NH4', 'Ca', 'Mg',
                   'K', 'Na', 'Cl', 'Br', 'N', 'SPlusN']

NTN_MEAS_TYPE = ['conc', 'dep', 'precip']  # concentration or deposition


class GDALMemFile():
    """Creates a GDAL memory-mapped file

    Modeled after ``rasterio`` function of same name

    .. code::

        with GDALMemFile(buf).open() as dataset:
            # do something

    .. todo::

        could this work on url, file, or buf?

    """
    def __init__(self, buf):
        """
        Parameters
        ----------
        buf : buffer
            Buffer containing gdal formatted data.

        """
        self.buf = buf

    def open(self):
        """
        see https://gist.github.com/jleinonen/5781308

        """
        mmap_name = '/vsimem/' + uuid4().hex  # vsimem is special GDAL string
        gdal.FileFromMemBuffer(mmap_name, self.buf)

        return gdal.Open(mmap_name)


class NADP_ZipFile(zipfile.ZipFile):
    """Extend zipfile.ZipFile for working on data from NADP
    """
    def tif_name(self):
        """Get the name of the tif file in the zip file."""
        filenames = self.namelist()
        r = re.compile(".*tif$")
        tif_list = list(filter(r.match, filenames))
        return tif_list[0]

    def tif(self):
        """Read the tif file in the zip file."""
        return self.read(self.tif_name())


def get_annual_MDN_map(measurement_type, year, path=None):
    """Download a MDN map from NDAP.

    This function looks for a zip file containing gridded information at:
    https://nadp.slh.wisc.edu/maps-data/mdn-gradient-maps/.
    The function will download the zip file and extract it, exposing the tif
    file if a path is provided. If not, then a
    :obj:`dataretrieval.nadp.GDALMemFile` object is returned.

    Parameters
    ----------
    measurement_type: string
        The type of measurement (concentration or deposition) as a string,
        either 'conc' or 'dep' respectively.

    year: string
        Year as a string 'YYYY'

    path: string
        Download directory

    Returns
    -------
    path: string
        Path that zip file was extracted into if path was specified.

    GDALMemFile: :obj:`dataretrieval.nadp.GDALMemFile`
        GDALMemFile containing in-memory GDAL object.

    Examples
    --------
    .. code::

        >>> # get map of mercury concentration in 2010 and extract it to a path
        >>> data_path = dataretrieval.nadp.get_annual_MDN_map(
        ...     measurement_type='conc', year='2010', path='somepath')

        >>> # get map of mercury deposition in 2008 as a GDALMemFile object
        >>> gmem = dataretrieval.nadp.get_annual_MDN_map(
        ...     measurement_type='dep', year='2008')

    """
    url = '{}/{}/MDN/grids/'.format(NADP_URL, NADP_MAP_EXT)

    filename = 'Hg_{}_{}.zip'.format(measurement_type, year)

    z = get_zip(url, filename)

    if path:
        z.extractall(path)
        return '{}{}{}'.format(path, os.sep, basename(filename))

    # else if no path return a buffer
    return GDALMemFile(z.tif())


def get_annual_NTN_map(measurement_type, measurement=None, year=None, path=None):
    """Download a NTN map from NDAP.

    This function looks for a zip file containing gridded information at:
    https://nadp.slh.wisc.edu/maps-data/ntn-gradient-maps/.
    The function will download the zip file and extract it, exposing the tif
    file if a path is provided. If not, then a
    :obj:`dataretrieval.nadp.GDALMemFile` object is returned.

    .. note::

        Measurement type abbreviations for concentration and deposition are
        all lower-case, but for precipitation data, the first letter must be
        capitalized!

    Parameters
    ----------
    measurement : string
        The measured constituent to return.
    measurement_type : string
        The type of measurement, 'conc', 'dep', or 'Precip', which represent
        concentration, deposition, or precipitation respectively.
    year : string
        Year as a string 'YYYY'
    path : string
        Download directory

    Returns
    -------
    path: string
        Path that zip file was extracted into if path was specified.

    GDALMemFile: :obj:`dataretrieval.nadp.GDALMemFile`
        GDALMemFile containing in-memory GDAL object.

    Examples
    --------
    .. code::

        >>> # get a map of nitrate concentration in 1996
        >>> gmem = dataretrieval.nadp.get_annual_NTN_map(
        ...     measurement='NO3', measurement_type='conc', year='1996')

        >>> # get a map of precipitation in 2015 and extract it to a path
        >>> data_path = dataretrieval.nadp.get_annual_NTN_map(
        ...     measurement_type='Precip', year='2015', path='somepath')

    """
    url = '{}/{}/NTN/grids/{}/'.format(NADP_URL, NADP_MAP_EXT, year)

    filename = '{}_{}.zip'.format(measurement_type, year)

    if measurement:
        filename = '{}_{}'.format(measurement, filename)

    z = get_zip(url, filename)

    if path:
        z.extractall(path)
        return '{}{}{}'.format(path, os.sep, basename(filename))

    # else if no path return a buffer
    return GDALMemFile(z.tif())


def get_zip(url, filename):
    """Gets a ZipFile at url and returns it

    Parameters
    ----------
    url : string
        URL to zip file

    filename : string
        Name of zip file

    Returns
    -------
    ZipFile

    .. todo::

        finish docstring

    """
    req = requests.get(url + filename)
    req.raise_for_status()

    # z = zipfile.ZipFile(io.BytesIO(req.content))
    z = NADP_ZipFile(io.BytesIO(req.content))
    return z
