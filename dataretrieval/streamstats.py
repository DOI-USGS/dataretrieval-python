"""
This module is a wrapper for the streamstats API (`streamstats documentation`_).

.. _streamstats documentation: https://streamstats.usgs.gov/streamstatsservices/#/

"""

import json

import requests


def download_workspace(workspaceID, format=''):
    """Function to download streamstats workspace.

    Parameters
    ----------
    workspaceID: string
        Service workspace received from watershed result

    format: string
        Download return format. Default will return ESRI geodatabase zipfile.
        'SHAPE' will return a zip file containing shape format.

    Returns
    -------
    r: geodatabase or shapefiles
        A zip file containing the workspace contents, in either a
        geodatabase or shape files.

    """
    payload = {'workspaceID': workspaceID, 'format': format}
    url = 'https://streamstats.usgs.gov/streamstatsservices/download'

    r = requests.get(url, params=payload)

    r.raise_for_status()
    return r
    # data = r.raw.read()

    # with open(filepath, 'wb') as f:
    #    f.write(data)

    # return


def get_sample_watershed():
    """Sample function to get a watershed object for a location in NY.

    Makes the function call :obj:`dataretrieval.streamstats.get_watershed`
    with the parameters 'NY', -74.524, 43.939, and returns the watershed
    object.

    Returns
    -------
    Watershed: :obj:`dataretrieval.streamstats.Watershed`
        Custom object that contains the watershed information as extracted
        from the streamstats JSON object.

    """
    return get_watershed('NY', -74.524, 43.939)


def get_watershed(
    rcode,
    xlocation,
    ylocation,
    crs=4326,
    includeparameters=True,
    includeflowtypes=False,
    includefeatures=True,
    simplify=True,
    format='geojson',
):
    """Get watershed object based on location

    **Streamstats documentation:**
    Returns a watershed object. The request configuration will determine the
    overall request response. However all returns will return a watershed
    object with at least the workspaceid. The workspace id is the id to the
    service workspace where files are stored and can be used for further
    processing such as for downloads and flow statistic computations.

    See: https://streamstats.usgs.gov/streamstatsservices/#/ for more
    information.

    Parameters
    ----------
    rcode: string
        StreamStats 2-3 character code that identifies the Study Area --
        either a State or a Regional Study.
    xlocation: float
        X location of the most downstream point of desired study area.
    ylocation: float
        Y location of the most downstream point of desired study area.
    crs: integer, string, optional
        ESPSG spatial reference code, default is 4326
    includeparameters: bool, optional
        Boolean flag to include parameters in response.
    includeflowtypes: bool, string, optional
        Not yet implemented. Would be a comma separated list of region flow
        types to compute with the default being True
    includefeatures: list, optional
        Comma separated list of features to include in response.
    simplify: bool, optional
        Boolean flag controlling whether or not to simplify the returned
        result.

    Returns
    -------
    Watershed: :obj:`dataretrieval.streamstats.Watershed`
        Custom object that contains the watershed information as extracted
        from the streamstats JSON object.

    """
    payload = {
        'rcode': rcode,
        'xlocation': xlocation,
        'ylocation': ylocation,
        'crs': crs,
        'includeparameters': includeparameters,
        'includeflowtypes': includeflowtypes,
        'includefeatures': includefeatures,
        'simplify': simplify,
    }
    url = 'https://streamstats.usgs.gov/streamstatsservices/watershed.geojson'

    r = requests.get(url, params=payload)

    r.raise_for_status()

    if format == 'geojson':
        return r

    if format == 'shape':
        # use Fiona to return a shape object
        pass

    if format == 'object':
        # return a python object
        pass

    data = json.loads(r.text)
    return Watershed.from_streamstats_json(data)


class Watershed:
    """Class to extract information from the streamstats JSON object."""

    @classmethod
    def from_streamstats_json(cls, streamstats_json):
        """Method that creates a Watershed object from a streamstats JSON."""
        cls.watershed_point = streamstats_json['featurecollection'][0]['feature']
        cls.watershed_polygon = streamstats_json['featurecollection'][1]['feature']
        cls.parameters = streamstats_json['parameters']
        cls._workspaceID = streamstats_json['workspaceID']
        return cls

    def __init__(self, rcode, xlocation, ylocation):
        """Init method that calls the :obj:`from_streamstats_json` method."""
        self = get_watershed(rcode, xlocation, ylocation)
