"""Retrieve hydrologic network features from the Network Linked Data Index (NLDI).

The getters below navigate the hydrologic network from an origin -- a feature
source and id, a ``comid``, or a lat/long point -- and return flowlines, basins,
or registered features as a ``geopandas.GeoDataFrame``, or as raw JSON when
``as_json=True``. This module requires geopandas.

See https://api.water.usgs.gov/nldi/linked-data for the API reference.
"""

from __future__ import annotations

from dataclasses import dataclass
from json import JSONDecodeError
from typing import Any, ClassVar, Literal, cast

from dataretrieval import configuration as _configuration
from dataretrieval._querying import _query_with_retry
from dataretrieval._validation import (
    reject_together,
    require_argument,
    require_exactly_one,
    require_one_of,
    require_together,
)
from dataretrieval.configuration import (
    BaseConfiguration,
    _Redirectable,
    _register,
    _Retrying,
)

__all__ = [
    "NldiConfiguration",
    "get_flowlines",
    "get_basin",
    "get_features",
    "get_features_by_data_source",
    "search",
]


try:
    import geopandas as gpd
except ImportError as err:
    raise ImportError(
        "The NLDI module requires geopandas, which is not installed. "
        "Install it with `pip install 'dataretrieval[nldi]'` "
        "(quoted, so the shell does not glob the brackets)."
    ) from err

NLDI_API_BASE_URL = "https://api.water.usgs.gov/nldi/linked-data"
_AVAILABLE_DATA_SOURCES = None
_CRS = "EPSG:4326"
_VALID_NAVIGATION_MODES = ("UM", "DM", "UT", "DD")
#: Built from the tuple above, so a mode added there cannot go unmentioned.
_NAVIGATION_MODES_HINT = (
    f"Pass one of {', '.join(repr(mode) for mode in _VALID_NAVIGATION_MODES)}."
)
#: Shared by the conflict check and the nothing-supplied check, so both
#: offer the same ways forward.
_ORIGIN_HINT = (
    "Navigate from a comid, e.g. comid=13294314, or from a "
    "feature_source/feature_id pair -- not both"
)


def _api_base() -> str:
    """The NLDI base this call targets: a block's redirect, or the service's.

    Every URL below is built from this rather than from
    :data:`NLDI_API_BASE_URL` directly, so a ``NldiConfiguration(base_url=...)``
    reaches every navigation, basin, and catalog request alike -- a redirect
    that covered only some of them would leave the library asking the real
    service about the mirror's data. Resolved per call, because a ``configure``
    block is scoped to a ``with`` statement rather than to the process.

    Six call sites, which is what this seam is for; choosing between the
    redirect and the service's own base is the accessor's job, not each
    service's.
    """
    return _configuration.base_url(adapter="nldi", default=NLDI_API_BASE_URL)


def _query_nldi(
    url: str,
    query_params: dict[str, str],
) -> dict[str, Any] | list[Any]:
    # A helper function to query the NLDI API. ``query()`` already raises a
    # typed ``DataRetrievalError`` for any HTTP error response, so a returned
    # response is a success that we only need to parse.
    response = _query_with_retry(url, payload=query_params, adapter="nldi")
    response_data: dict[str, Any] | list[Any] = {}
    try:
        response_data = response.json()
    except JSONDecodeError:
        # even with a 200 status code, the response sometimes does not return JSON
        # data which causes a JSONDecodeError
        pass
    return response_data


def _features_to_gdf(feature_collection: dict[str, Any]) -> gpd.GeoDataFrame:
    """Build a GeoDataFrame from an NLDI FeatureCollection, tolerating empties.

    NLDI can legitimately return no features (e.g. a feature with nothing
    upstream), and :func:`_query_nldi` returns ``{}`` when a 200 response
    carries no JSON body. ``GeoDataFrame.from_features`` raises on both cases
    (there's no geometry column to attach the CRS to), so return an empty
    GeoDataFrame with the correct CRS instead of crashing.
    """
    features = feature_collection.get("features") if feature_collection else None
    if not features:
        return gpd.GeoDataFrame(geometry=[], crs=_CRS)
    return gpd.GeoDataFrame.from_features(feature_collection, crs=_CRS)


def _query_features(
    url: str, query_params: dict[str, str], as_json: bool
) -> gpd.GeoDataFrame | dict[str, Any]:
    """Run an NLDI query and return the raw FeatureCollection or a GeoDataFrame."""
    feature_collection = cast("dict[str, Any]", _query_nldi(url, query_params))
    return feature_collection if as_json else _features_to_gdf(feature_collection)


def get_flowlines(
    navigation_mode: str,
    distance: int = 5,
    feature_source: str | None = None,
    feature_id: str | None = None,
    comid: int | None = None,
    stop_comid: int | None = None,
    trim_start: bool = False,
    as_json: bool = False,
) -> gpd.GeoDataFrame | dict[str, Any]:
    """Get the flowlines for a navigation, either by comid or by feature source.

    Flowlines are returned in WGS84 lat/long coordinates as a GeoDataFrame
    containing a polyline geometry.

    Parameters
    ----------
    navigation_mode: string, navigation mode, allowed values are 'UM', 'DM', 'UT', 'DD'
    distance: int, distance in kilometers, default is 5
    feature_source: string, optional, name of the feature source,
                    required if comid is not provided
    feature_id: string, optional, identifier of the feature,
                required if comid is not provided
    comid: integer, optional, comid, required if feature source is not provided
    stop_comid: integer, optional, stop comid
    trim_start: bool, trim start, default is False
    as_json: bool, optional, return flowlines as JSON if set to True,
             otherwise return as GeoDataFrame, default is False

    Returns
    -------
    gdf: GeoDataFrame or dict
        GeoDataFrame/dict of flowlines

    Examples
    --------
    .. doctest::

        >>> # Get flowlines for a feature source: WQP and
        >>> # feature id: USGS-01031500 in the upstream main
        >>> gdf = dataretrieval.nldi.get_flowlines(
        ...     feature_source="WQP",
        ...     feature_id="USGS-01031500",
        ...     navigation_mode="UM",
        ... )
        >>> # Get flowlines for comid: 13294314 in the upstream main
        >>> gdf = dataretrieval.nldi.get_flowlines(
        ...     comid=13294314, navigation_mode="UM"
        ... )
    """
    navigation_mode = _validate_navigation_mode(navigation_mode)
    _validate_feature_source_comid(feature_source, feature_id, comid)
    if feature_source:
        _validate_data_source(feature_source, name="feature source")
    url, query_params = _navigation_request(
        feature_source=feature_source,
        feature_id=feature_id,
        comid=comid,
        navigation_mode=navigation_mode,
        distance=distance,
        tail="flowlines",
    )
    query_params["trimStart"] = str(trim_start).lower()
    if stop_comid is not None:
        query_params["stopComid"] = str(stop_comid)

    return _query_features(url, query_params, as_json)


def get_basin(
    feature_source: str,
    feature_id: str,
    simplified: bool = True,
    split_catchment: bool = False,
    as_json: bool = False,
) -> gpd.GeoDataFrame | dict[str, Any]:
    """Get the aggregated basin for the specified feature.

    The basin is returned in WGS84 lat/lon as a GeoDataFrame or as JSON,
    containing a polygon geometry.

    Parameters
    ----------
    feature_source: string, name of the feature source
    feature_id: string, identifier of the feature
    simplified: bool, simplified, default is True
    split_catchment: bool, split catchment, default is False
    as_json: bool, return basin as JSON if set to True, otherwise return
             as GeoDataFrame, default is False

    Returns
    -------
    gdf: GeoDataFrame or dict
        GeoDataFrame/dict of basin

    Examples
    --------
    .. doctest::

        >>> # Get basin for a feature source: WQP and feature id: USGS-01031500
        >>> gdf = dataretrieval.nldi.get_basin(
        ...     feature_source="WQP", feature_id="USGS-01031500"
        ... )
    """
    # validate the feature source
    _validate_data_source(feature_source, name="feature source")
    require_argument(
        "feature_id",
        feature_id or None,
        context=f"to say which {feature_source} feature the basin drains to",
        remedy=(
            "Pass the id as its source spells it, e.g. feature_id='USGS-01031500'."
        ),
    )

    url = f"{_api_base()}/{feature_source}/{feature_id}/basin"
    simplified_str = str(simplified).lower()
    split_catchment_str = str(split_catchment).lower()
    query_params = {
        "simplified": simplified_str,
        "splitCatchment": split_catchment_str,
    }
    return _query_features(url, query_params, as_json)


def get_features(
    data_source: str | None = None,
    navigation_mode: str | None = None,
    distance: int = 50,
    feature_source: str | None = None,
    feature_id: str | None = None,
    comid: int | None = None,
    lat: float | None = None,
    long: float | None = None,
    stop_comid: int | None = None,
    as_json: bool = False,
) -> gpd.GeoDataFrame | dict[str, Any]:
    """Get all features along a navigation, either by comid or by feature source.

    Features are returned as points in WGS84 lat/long coordinates - a
    GeoDataFrame containing a point geometry.

    Parameters
    ----------
    feature_source: string, optional, name of the feature source,
                    required if comid is not provided
    feature_id: string, optional, identifier of the feature,
                required if comid is not provided
    navigation_mode: string, navigation mode, allowed values are 'UM', 'DM', 'UT', 'DD'
    data_source: string, data source
    distance: int, distance in kilometers, default is 50
    comid: integer, optional, comid, required if feature source is not provided
    lat: float, optional, latitude, if provided, long is also required
    long: float, optional, longitude, if provided, lat is also required
    stop_comid: integer, optional, stop comid
    as_json: bool, optional, return features as JSON if set to True,
             otherwise return as GeoDataFrame, default is False

    Returns
    -------
    gdf: GeoDataFrame or dict
        GeoDataFrame/dict of features

    Examples
    --------
    .. doctest::

        >>> # Get registered features for a feature source: WQP,
        >>> # feature id: USGS-01031500
        >>> gdf = dataretrieval.nldi.get_features(
        ...     feature_source="WQP", feature_id="USGS-01031500"
        ... )
        >>> # Get features for a feature source: WQP, feature id: USGS-01031500,
        >>> # and data source: nwissite in the upstream main
        >>> gdf = dataretrieval.nldi.get_features(
        ...     feature_source="WQP",
        ...     feature_id="USGS-01031500",
        ...     navigation_mode="UM",
        ...     data_source="nwissite",
        ...     distance=50,
        ... )
        >>> # Get features for a comid: 13294314, and data source: nwissite
        >>> # in the upstream main
        >>> gdf = dataretrieval.nldi.get_features(
        ...     comid=13294314,
        ...     navigation_mode="UM",
        ...     data_source="nwissite",
        ...     distance=50,
        ... )
        >>> # Get features for a latitude: 43.073051 and longitude: -89.401230
        >>> gdf = dataretrieval.nldi.get_features(lat=43.073051, long=-89.401230)
    """

    url, query_params = _get_features_request(
        data_source=data_source,
        navigation_mode=navigation_mode,
        distance=distance,
        feature_source=feature_source,
        feature_id=feature_id,
        comid=comid,
        lat=lat,
        long=long,
        stop_comid=stop_comid,
    )

    return _query_features(url, query_params, as_json)


def _navigation_request(
    *,
    feature_source: str | None,
    feature_id: str | None,
    comid: int | None,
    navigation_mode: str,
    distance: int,
    tail: str,
) -> tuple[str, dict[str, str]]:
    """URL and query params for an NLDI navigation from a validated origin.

    The single home for the navigation path grammar — ``{origin}/navigation/
    {mode}/{tail}`` — and its ``distance`` knob. Callers add the knobs specific
    to their endpoint (``trimStart``, ``stopComid``) afterwards, so the query
    string keeps its documented parameter order.
    """
    origin = f"{feature_source}/{feature_id}" if feature_source else f"comid/{comid}"
    url = f"{_api_base()}/{origin}/navigation/{navigation_mode}/{tail}"
    return url, {"distance": str(distance)}


def _validate_lat_long_origin(
    comid: int | None,
    feature_source: str | None,
    feature_id: str | None,
) -> None:
    """Raise if lat/long is combined with another origin type.

    Called with a lat/long already supplied, so the pair is passed as a
    present marker: the conflict is between origin *types*, and naming the
    type is what tells the caller which argument to drop.
    """
    reject_together(
        {
            "lat/long": True,
            "comid": comid,
            "feature_source": feature_source,
            "feature_id": feature_id,
        },
        context="each names a different origin to navigate from",
        remedy=(
            "Navigate from a point (lat and long), a comid, or a "
            "feature_source/feature_id pair -- one origin per call."
        ),
    )


def _get_features_request(
    *,
    data_source: str | None,
    navigation_mode: str | None,
    distance: int,
    feature_source: str | None,
    feature_id: str | None,
    comid: int | None,
    lat: float | None,
    long: float | None,
    stop_comid: int | None,
) -> tuple[str, dict[str, str]]:
    """Validate a feature origin and build its NLDI request parameters."""
    require_together(
        {"lat": lat, "long": long},
        context="to navigate from a point",
        remedy="Pass both, e.g. lat=43.087, long=-89.509.",
    )

    if lat is not None:
        _validate_lat_long_origin(comid, feature_source, feature_id)
        return f"{_api_base()}/comid/position", {"coords": f"POINT({long} {lat})"}

    if comid is not None or data_source is not None:
        require_argument(
            "navigation_mode",
            navigation_mode,
            context="when comid or data_source is given",
            remedy=_NAVIGATION_MODES_HINT,
        )

    _validate_feature_source_comid(feature_source, feature_id, comid)
    if data_source is not None:
        _validate_data_source(data_source)
    if feature_source is not None:
        _validate_data_source(feature_source, name="feature source")

    if not navigation_mode:
        return f"{_api_base()}/{feature_source}/{feature_id}", {}

    # Before the data_source check below: a caller who mistyped the mode should
    # hear about the mode, not be sent to fix a second argument first.
    navigation_mode = _validate_navigation_mode(navigation_mode)
    # The navigation's tail is the data source, so a missing one is spelled
    # "None" into the path and the service answers 200 with zero features.
    data_source = require_argument(
        "data_source",
        data_source,
        context=(
            "when navigation_mode is given -- it names which features to "
            "return along the navigation"
        ),
        remedy=(
            "Pass the source of the features, e.g. data_source='nwissite'. "
            "For the flowlines themselves call get_flowlines() instead."
        ),
    )
    url, query_params = _navigation_request(
        feature_source=feature_source,
        feature_id=feature_id,
        comid=comid,
        navigation_mode=navigation_mode,
        distance=distance,
        tail=data_source,
    )
    if stop_comid is not None:
        query_params["stopComid"] = str(stop_comid)
    return url, query_params


# TODO: This function can cause a timeout error for some data sources
#  - maybe we shouldn't provide this function?
def get_features_by_data_source(data_source: str) -> gpd.GeoDataFrame:
    """Get all features for the specified data source.

    Features are returned as points in WGS84 lat/long coordinates as a
    GeoDataFrame containing a point geometry.

    Parameters
    ----------
    data_source: string, data source

    Returns
    -------
    gdf: GeoDataFrame
        GeoDataFrame of features

    Examples
    --------
    .. doctest::

        >>> # "nwissite" returns every NWIS site nationwide, so this example is
        >>> # skipped in the doctest build to avoid the (very large) download.
        >>> gdf = dataretrieval.nldi.get_features_by_data_source(  # doctest: +SKIP
        ...     data_source="nwissite"
        ... )
    """
    # validate the data source
    _validate_data_source(data_source)
    url = f"{_api_base()}/{data_source}"
    feature_collection = cast("dict[str, Any]", _query_nldi(url, {}))
    gdf = _features_to_gdf(feature_collection)
    return gdf


def _search_basin(feature_source: str | None, feature_id: str | None) -> dict[str, Any]:
    """Handle ``find='basin'`` for :func:`search`."""
    remedy = (
        "Pass both, e.g. feature_source='WQP', feature_id='USGS-01031500'; "
        "a basin has no other origin."
    )
    require_together(
        {"feature_source": feature_source, "feature_id": feature_id},
        context="for find='basin'",
        remedy=remedy,
    )
    return get_basin(
        feature_source=require_argument(
            "feature_source", feature_source, context="for find='basin'", remedy=remedy
        ),
        feature_id=require_argument(
            "feature_id", feature_id, context="for find='basin'", remedy=remedy
        ),
        as_json=True,
    )


def _search_flowlines(
    *,
    navigation_mode: str | None,
    distance: int,
    feature_source: str | None,
    feature_id: str | None,
    comid: int | None,
) -> dict[str, Any]:
    """Handle ``find='flowlines'`` for :func:`search`."""
    navigation_mode = require_argument(
        "navigation_mode",
        navigation_mode,
        context="for find='flowlines'",
        remedy=_NAVIGATION_MODES_HINT,
    )
    return get_flowlines(
        navigation_mode=navigation_mode,
        distance=distance,
        feature_source=feature_source,
        feature_id=feature_id,
        comid=comid,
        as_json=True,
    )


def search(
    feature_source: str | None = None,
    feature_id: str | None = None,
    navigation_mode: str | None = None,
    data_source: str | None = None,
    find: Literal["basin", "flowlines", "features"] = "features",
    comid: int | None = None,
    lat: float | None = None,
    long: float | None = None,
    distance: int = 50,
) -> dict[str, Any]:
    """Search NLDI for the specified feature and return the results as a dict.

    Parameters
    ----------
    feature_source: string, name of the feature source
    feature_id: string, identifier of the feature
    navigation_mode: string, optional, navigation mode,
                     allowed values are 'UM', 'DM', 'UT', 'DD'
    data_source: string, optional, data source
    find: string, search for 'basin', 'flowlines', or 'features', default is 'features'
    comid: int, optional, comid, default is None
    lat: float, optional, latitude, default is None
    long: float, optional, longitude, default is None
    distance: int, optional, distance in kilometers, default is 50

    Returns
    -------
    dict: search results

    Examples
    --------
    .. doctest::

        >>> # Search for aggregated basin for feature source: WQP
        >>> # and feature id: USGS-01031500
        >>> search_results = dataretrieval.nldi.search(
        ...     feature_source="WQP", feature_id="USGS-01031500", find="basin"
        ... )
        >>> # Search for flowlines for feature source: WQP and
        >>> # feature id: USGS-01031500 in the upstream main
        >>> search_results = dataretrieval.nldi.search(
        ...     feature_source="WQP",
        ...     feature_id="USGS-01031500",
        ...     navigation_mode="UM",
        ...     find="flowlines",
        ... )
        >>> # Get registered features for a feature source: WQP,
        >>> # feature id: USGS-01031500
        >>> gdf = dataretrieval.nldi.get_features(
        ...     feature_source="WQP", feature_id="USGS-01031500"
        ... )
        >>> # Search for features for feature source: WQP, feature id: USGS-01031500,
        >>> # and data source: census2020-nhdpv2 in the upstream main
        >>> search_results = dataretrieval.nldi.search(
        ...     feature_source="WQP",
        ...     feature_id="USGS-01031500",
        ...     data_source="census2020-nhdpv2",
        ...     navigation_mode="UM",
        ...     find="features",
        ... )
        >>> # Search for features for comid: 13294314,
        >>> # and data source: census2020-nhdpv2 in the upstream main
        >>> search_results = dataretrieval.nldi.search(
        ...     comid=13294314,
        ...     data_source="census2020-nhdpv2",
        ...     navigation_mode="UM",
        ... )
        >>> # Search for flowlines for comid: 13294314 in the upstream main
        >>> search_results = dataretrieval.nldi.search(
        ...     comid=13294314, navigation_mode="UM", find="flowlines"
        ... )
        >>> # Search for features for latitude: 43.073051 and longitude: -89.401230
        >>> search_results = dataretrieval.nldi.search(
        ...     lat=43.073051, long=-89.401230
        ... )

    """
    require_together(
        {"lat": lat, "long": long},
        context="to search from a point",
        remedy="Pass both, e.g. lat=43.087, long=-89.509.",
    )

    find = cast("Literal['basin', 'flowlines', 'features']", find.lower())
    require_one_of(find, ("basin", "flowlines", "features"), name="find")
    if lat is not None and find != "features":
        raise ValueError(
            f"find={find!r} cannot be combined with lat/long -- a point origin "
            "resolves to features only. Pass find='features' to keep the "
            "point origin, or drop lat and long and pass the origin "
            f"{find} takes: feature_source and feature_id"
            f"{' or comid' if find == 'flowlines' else ''}."
        )
    if comid is not None and find == "basin":
        raise ValueError(
            "find='basin' cannot be combined with comid -- a basin is looked "
            "up by feature, not by flowline. Pass feature_source and "
            "feature_id instead, or keep comid and pass find='flowlines' "
            "or find='features'."
        )

    if lat is not None:
        return get_features(lat=lat, long=long, as_json=True)

    if find == "basin":
        return _search_basin(feature_source, feature_id)

    if find == "flowlines":
        return _search_flowlines(
            navigation_mode=navigation_mode,
            distance=distance,
            feature_source=feature_source,
            feature_id=feature_id,
            comid=comid,
        )

    # find == 'features'
    return get_features(
        data_source=data_source,
        navigation_mode=navigation_mode,
        distance=distance,
        feature_source=feature_source,
        feature_id=feature_id,
        comid=comid,
        as_json=True,
    )


def _validate_data_source(data_source: str, *, name: str = "data source") -> None:
    # A helper function to validate user specified data source/feature source

    global _AVAILABLE_DATA_SOURCES

    # get the available data/feature sources - if not already cached
    if _AVAILABLE_DATA_SOURCES is None:
        url = f"{_api_base()}/"
        available_data_sources = _query_nldi(url, {})
        if not isinstance(available_data_sources, list) or not all(
            isinstance(ds, dict) and "source" in ds for ds in available_data_sources
        ):
            raise ValueError(
                "NLDI data-source catalog returned an unexpected shape; "
                "expected a list of {'source': ..., ...} objects, got: "
                f"{available_data_sources!r}. If you set "
                "NldiConfiguration(base_url=...), point it at the linked-data "
                "root, e.g. base_url='https://api.water.usgs.gov/nldi/"
                "linked-data'; otherwise the service returned an unexpected "
                "body -- retry later."
            )
        _AVAILABLE_DATA_SOURCES = [ds["source"] for ds in available_data_sources]

    if data_source not in _AVAILABLE_DATA_SOURCES:
        err_msg = (
            f"Invalid {name} '{data_source}'."
            f" Available sources are: {_AVAILABLE_DATA_SOURCES}"
        )
        raise ValueError(err_msg)


def _validate_navigation_mode(navigation_mode: str | None) -> str:
    navigation_mode = require_argument(
        "navigation_mode",
        navigation_mode,
        remedy=_NAVIGATION_MODES_HINT,
    )
    normalized = navigation_mode.upper()
    require_one_of(normalized, _VALID_NAVIGATION_MODES, name="navigation_mode")
    return normalized


def _validate_feature_source_comid(
    feature_source: str | None, feature_id: str | None, comid: int | None
) -> None:
    if comid is not None:
        # Half a feature pair beside a comid is a conflict, not a gap: advising
        # the caller to complete the pair would only raise the conflict next.
        reject_together(
            {
                "comid": comid,
                "feature_source": feature_source,
                "feature_id": feature_id,
            },
            context="they name different origins",
            remedy=f"{_ORIGIN_HINT}.",
        )
    require_together(
        {"feature_source": feature_source, "feature_id": feature_id},
        context="to name one feature between them",
        remedy=("Pass both, e.g. feature_source='WQP', feature_id='USGS-01031500'."),
    )
    require_exactly_one(
        {"comid": comid, "feature_source": feature_source},
        context="as the origin to navigate from",
        remedy=f"{_ORIGIN_HINT}, and not neither.",
    )


@dataclass(frozen=True)
class NldiConfiguration(_Redirectable, _Retrying, BaseConfiguration):
    """Settings for NLDI calls alone.

    No fan-out dials: an NLDI query is answered by a single request.

    This adapter is imported on demand for the geopandas extra, so this
    class registers itself later than the rest -- which is exactly why
    the adapter roster lives in :data:`~dataretrieval.configuration.ADAPTERS`
    rather than being derived from what has been imported.

    Lives here rather than in :mod:`dataretrieval.configuration` because
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
        Linked-data base to send NLDI requests to, instead of the
        service's own (``NLDI_API_BASE_URL``). Every navigation, basin
        and catalog request is built on it. Code only: the file and the
        environment refuse it.
    """

    # One request per call, so this service reads the retry dials and a
    # redirectable base and no fan-out dial. Each setting is declared once,
    # in :mod:`dataretrieval.configuration`, beside its grammar.
    adapter: ClassVar[str] = "nldi"


_register(NldiConfiguration)
