"""
This module is a wrapper for the streamstats API (`streamstats documentation`_).

.. _streamstats documentation: https://streamstats.usgs.gov/streamstatsservices/#/

"""

from __future__ import annotations

import json
from typing import Any, cast

import httpx

from dataretrieval.utils import HTTPX_DEFAULTS


def download_workspace(workspaceID: str, format: str = "") -> httpx.Response:
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
    payload = {"workspaceID": workspaceID, "format": format}
    url = "https://streamstats.usgs.gov/streamstatsservices/download"

    r = httpx.get(url, params=payload, **HTTPX_DEFAULTS)

    r.raise_for_status()
    return r
    # data = r.raw.read()

    # with open(filepath, 'wb') as f:
    #    f.write(data)

    # return


def get_sample_watershed() -> Watershed:
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
    return cast(
        "Watershed",
        get_watershed("NY", -74.524, 43.939, format="object"),
    )


def get_watershed(
    rcode: str,
    xlocation: float,
    ylocation: float,
    crs: int | str = 4326,
    includeparameters: bool = True,
    includeflowtypes: bool = False,
    includefeatures: bool = True,
    simplify: bool = True,
    format: str = "geojson",
) -> httpx.Response | Watershed:
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
    payload: dict[str, str | int | float | bool] = {
        "rcode": rcode,
        "xlocation": xlocation,
        "ylocation": ylocation,
        "crs": crs,
        "includeparameters": includeparameters,
        "includeflowtypes": includeflowtypes,
        "includefeatures": includefeatures,
        "simplify": simplify,
    }
    url = "https://streamstats.usgs.gov/streamstatsservices/watershed.geojson"

    r = httpx.get(url, params=payload, **HTTPX_DEFAULTS)

    r.raise_for_status()

    if format == "geojson":
        return r

    if format == "shape":
        # Returning a shapefile/Fiona object isn't implemented; fail
        # loudly instead of silently falling through to a Watershed.
        raise NotImplementedError(
            "format='shape' is not implemented. Use format='geojson' "
            "(default) for the raw response, or format='object' for a "
            "parsed Watershed."
        )

    # format == "object" (and any other value): parse into a Watershed.
    data = json.loads(r.text)
    return Watershed.from_streamstats_json(data)


class Watershed:
    """Parsed StreamStats watershed result.

    Holds the delineated watershed features, the computed basin
    parameters, and the service ``workspaceID`` extracted from a
    StreamStats watershed response. Build one from an already-fetched
    payload with :meth:`from_streamstats_json`, or construct directly
    from a location to fetch and parse in a single step.

    Attributes
    ----------
    watershed_point : dict
        GeoJSON feature for the delineation (pour) point.
    watershed_polygon : dict
        GeoJSON feature for the delineated basin polygon.
    parameters : list
        Basin characteristics returned by the service.
    _workspaceID : str
        Service workspace id, usable with
        :obj:`dataretrieval.streamstats.download_workspace`.
    """

    def __init__(self, rcode: str, xlocation: float, ylocation: float) -> None:
        """Delineate the watershed at ``(xlocation, ylocation)`` and
        parse the response onto this instance."""
        response = cast(
            httpx.Response,
            get_watershed(rcode, xlocation, ylocation, format="geojson"),
        )
        self._populate(json.loads(response.text))

    @classmethod
    def from_streamstats_json(cls, streamstats_json: dict[str, Any]) -> Watershed:
        """Create a :class:`Watershed` from an already-parsed StreamStats
        JSON payload, without issuing a new request.

        Builds a fresh instance (via ``__new__``, so the
        network-fetching ``__init__`` is bypassed) and populates it; each
        call returns an independent object rather than mutating shared
        class state.
        """
        self = cls.__new__(cls)
        self._populate(streamstats_json)
        return self

    def _populate(self, streamstats_json: dict[str, Any]) -> None:
        """Extract watershed fields from a StreamStats JSON payload onto
        this instance."""
        self.watershed_point = streamstats_json["featurecollection"][0]["feature"]
        self.watershed_polygon = streamstats_json["featurecollection"][1]["feature"]
        self.parameters = streamstats_json["parameters"]
        self._workspaceID = streamstats_json["workspaceID"]
