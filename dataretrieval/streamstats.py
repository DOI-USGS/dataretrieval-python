"""Wrapper for the StreamStats API (`streamstats documentation`_).

.. _streamstats documentation: https://streamstats.usgs.gov/streamstatsservices/#/

"""

from __future__ import annotations

import json
from typing import Any, ClassVar, cast

import httpx

from dataretrieval import settings as _settings
from dataretrieval._querying import _get_with_retry
from dataretrieval.settings import (
    AdapterSettings,
    _Redirectable,
    _register,
    _Retrying,
)
from dataretrieval.transport.http import HTTPX_DEFAULTS

__all__ = [
    "StreamstatsSettings",
    "Watershed",
    "download_workspace",
    "get_sample_watershed",
    "get_watershed",
]

STREAMSTATS_URL = "https://streamstats.usgs.gov/streamstatsservices"


def _service_base() -> str:
    """The StreamStats base this call targets: a block's redirect, or its own.

    Both endpoints below hang off this, so a
    ``StreamstatsSettings(base_url=...)`` moves the whole service rather
    than the one endpoint a caller happened to reach first. Resolved per call,
    because a ``configure`` block is scoped to a ``with`` statement.
    """
    return _settings.base_url(adapter="streamstats", default=STREAMSTATS_URL)


def download_workspace(workspaceID: str, format: str = "") -> httpx.Response:
    """Download a StreamStats workspace.

    Parameters
    ----------
    workspaceID: string
        Service workspace received from a watershed result.

    format: string
        Format of the download. The default returns an ESRI geodatabase
        zipfile; 'SHAPE' returns a zip file containing shape format.

    Returns
    -------
    r: geodatabase or shapefiles
        A zip file containing the workspace contents, in either a
        geodatabase or shape files.

    """
    payload = {"workspaceID": workspaceID, "format": format}
    url = f"{_service_base()}/download"

    r = _get_with_retry(url, params=payload, adapter="streamstats", **HTTPX_DEFAULTS)
    return r
    # data = r.raw.read()

    # with open(filepath, 'wb') as f:
    #    f.write(data)

    # return


def get_sample_watershed() -> Watershed:
    """Get a watershed object for a sample location in NY.

    Calls :obj:`dataretrieval.streamstats.get_watershed` with the parameters
    'NY', -74.524, and 43.939, and returns the resulting watershed object.

    Returns
    -------
    Watershed: :obj:`dataretrieval.streamstats.Watershed`
        Custom object that contains the watershed information as extracted
        from the StreamStats JSON object.

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
    """Get a watershed object for a location.

    **StreamStats documentation:**
    Returns a watershed object. The request configuration will determine the
    overall request response. However, all returns will return a watershed
    object with at least the workspaceid. The workspace id is the id to the
    service workspace where files are stored, and can be used for further
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
        EPSG spatial reference code. Default is 4326.
    includeparameters: bool, optional
        Whether to include parameters in the response.
    includeflowtypes: bool, string, optional
        Comma-separated list of region flow types to compute, with the default
        being True. Not yet implemented.
    includefeatures: list, optional
        Comma-separated list of features to include in the response.
    simplify: bool, optional
        Whether to simplify the returned result.
    format: string, optional
        Controls the return type, default is 'geojson'. 'geojson' returns
        the raw ``httpx.Response``; 'object' parses the response into a
        :obj:`dataretrieval.streamstats.Watershed`. 'shape' is not
        implemented and raises ``NotImplementedError``.

    Returns
    -------
    r: ``httpx.Response`` or :obj:`dataretrieval.streamstats.Watershed`
        The raw response when ``format='geojson'`` (the default), or a
        custom ``Watershed`` object containing the watershed information
        extracted from the StreamStats JSON when ``format='object'``.

    Raises
    ------
    NotImplementedError
        If ``format='shape'``, which is not yet implemented.

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
    url = f"{_service_base()}/watershed.geojson"

    r = _get_with_retry(url, params=payload, adapter="streamstats", **HTTPX_DEFAULTS)

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
        """Delineate the watershed at ``(xlocation, ylocation)``.

        Parses the response onto this instance.
        """
        response = cast(
            "httpx.Response",
            get_watershed(rcode, xlocation, ylocation, format="geojson"),
        )
        self._populate(json.loads(response.text))

    @classmethod
    def from_streamstats_json(cls, streamstats_json: dict[str, Any]) -> Watershed:
        """Create a :class:`Watershed` from a parsed StreamStats JSON payload.

        No new request is issued. Builds a fresh instance (via ``__new__``, so
        the network-fetching ``__init__`` is bypassed) and populates it; each
        call returns an independent object rather than mutating shared class
        state.
        """
        self = cls.__new__(cls)
        self._populate(streamstats_json)
        return self

    def _populate(self, streamstats_json: dict[str, Any]) -> None:
        """Extract watershed fields from ``streamstats_json`` onto this instance."""
        self.watershed_point = streamstats_json["featurecollection"][0]["feature"]
        self.watershed_polygon = streamstats_json["featurecollection"][1]["feature"]
        self.parameters = streamstats_json["parameters"]
        self._workspaceID = streamstats_json["workspaceID"]


class StreamstatsSettings(_Redirectable, _Retrying, AdapterSettings):
    """Settings for StreamStats calls alone.

    No fan-out dials: a StreamStats query is answered by a single
    request.

    Lives here rather than in :mod:`dataretrieval.settings` because
    *which* settings a service reads is the service's own knowledge (ADR
    0011); what each of them means is shared, so the fields come from the
    setting groups declared beside their grammar.

    Parameters
    ----------
    retries : int, optional
        Retries attempted after a transient failure; ``0`` disables retrying.
    stall_timeout : float, optional
        Seconds a call may go without receiving any data before retrying
        stops.
    base_url : str, optional
        Services base to send StreamStats requests to, instead of its own
        (``STREAMSTATS_URL``). Both endpoints hang off it. Code only:
        the file and the environment refuse it.
    """

    # One request per call, so this service reads the retry dials and a
    # redirectable base and no fan-out dial. Each setting is declared once,
    # in :mod:`dataretrieval.settings`, beside its grammar.
    adapter: ClassVar[str] = "streamstats"


_register(StreamstatsSettings)
