"""
This module is a wrapper for the streamstats API, documentation for which is at
https://streamstats.usgs.gov/streamstatsservices/#/
"""

import json
import requests

def download_workspace(filepath, workspaceID, format=''):
    """

    Args:
        workspaceID (string): Service workspace received form watershed result
        format (string): Download return format. Default will return ESRI
                         geodatabase zipfile. 'SHAPE' will return a zip file containing
                         shape format.

    Returns:
        A zip file containing the workspace contents, in either a geodatabase or shape
        files.
    """
    payload = {'workspaceID':workspaceID, 'format':format}
    url = 'https://streamstats.usgs.gov/streamstatsservices/download'

    r = requests.get(url, params=payload)

    r.raise_for_status()
    return r
    #data = r.raw.read()

    #with open(filepath, 'wb') as f:
    #    f.write(data)

    #return

def get_sample_watershed():
    return get_watershed('NY',-74.524, 43.939) 

def get_watershed(rcode, xlocation, ylocation, crs=4326,
                  includeparameters=True, includeflowtypes=False,
                  includefeatures=True, simplify=True, format='geojson'):
    """ Get watershed object based on locationi

    Streamstats documentation
    -------------------------
    Returns a watershed object. The request configuration will determine the
    overall request response. However all returns will return a watershed object
    with at least the workspaceid. The workspace id is the id to the service
    workspace where files are stored and can be used for further processing such
    as for downloads and flow statistic computations.

    Args:
        rcode: StreamStats 2-3 character code that identifies the Study Area -- either a
               State or a Regional Study.
        xlocation: X location of the most downstream point of desired study area.
        ylocation: Y location of the most downstream point of desired study area.
        crs: ESPSG spatial reference code.
        includeparameters:
        includeflowtypes: Not yet implemented.
        includefeatures: Comma seperated list of features to include in response.
        simplify:

    Returns:
        Json watershed object describing watershed

    see: https://streamstats.usgs.gov/streamstatsservices/#/
    """

    payload = {'rcode':rcode, 'xlocation':xlocation, 'ylocation':ylocation, 'crs':crs,
               'includeparameters':includeparameters, 'includeflowtypes':includeflowtypes,
               'includefeatures':includefeatures, 'simplify':simplify}
    url = 'https://streamstats.usgs.gov/streamstatsservices/watershed.geojson'

    r   = requests.get(url, params=payload)

    r.raise_for_status()

    if format == 'geojson':
        return r

    if format == 'shape':
        # use Fiona to return a shape object
        pass

    if format == 'object':
        # return a python object
        pass

    #data = r.json() #raise error
    data = json.loads(r.text)
    return Watershed.from_streamstats_json(data)

class Watershed:

    @classmethod
    def from_streamstats_json(cls, streamstats_json):
        cls.watershed_point = streamstats_json['featurecollection'][0]['feature']
        cls.watershed_polygon = streamstats_json['featurecollection'][1]['feature']
        cls.parameters = streamstats_json['parameters']
        cls._workspaceID = streamstats_json['workspaceID']
        return cls

    def __init__(self, rcode, xlocation, ylocation):
        self = get_watershed(rcode, xlocation, ylocation)
