from __future__ import annotations

from json import JSONDecodeError
from typing import Literal

from dataretrieval.utils import query

try:
    import geopandas as gpd
except ImportError as err:
    raise ImportError("Install geopandas to use the NLDI module.") from err

NLDI_API_BASE_URL = "https://api.water.usgs.gov/nldi/linked-data"
_AVAILABLE_DATA_SOURCES = None
_CRS = "EPSG:4326"


def _query_nldi(url, query_params, error_message):
    # A helper function to query the NLDI API
    response = query(url, payload=query_params)
    if response.status_code != 200:
        raise ValueError(f"{error_message}. Error reason: {response.reason}")

    response_data = {}
    try:
        response_data = response.json()
    except JSONDecodeError:
        # even with a 200 status code, the response sometimes does not return JSON
        # data which causes a JSONDecodeError
        pass
    return response_data


def get_flowlines(
    navigation_mode: str,
    distance: int = 5,
    feature_source: str | None = None,
    feature_id: str | None = None,
    comid: int | None = None,
    stop_comid: int | None = None,
    trim_start: bool = False,
    as_json: bool = False,
) -> gpd.GeoDataFrame | dict:
    """Gets the flowlines for the specified navigation either by comid or feature
    source in WGS84 lat/long coordinates as GeoDataFrame containing a polyline geometry.

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
        _validate_data_source(feature_source)
        url = f"{NLDI_API_BASE_URL}/{feature_source}/{feature_id}/navigation"
    else:
        url = f"{NLDI_API_BASE_URL}/comid/{comid}/navigation"
    query_params = {"distance": str(distance), "trimStart": str(trim_start).lower()}

    url += f"/{navigation_mode}/flowlines"
    if stop_comid is not None:
        query_params["stopComid"] = str(stop_comid)

    if feature_source:
        err_msg = (
            f"Error getting flowlines for feature source '{feature_source}'"
            f" and feature_id '{feature_id}'"
        )
    else:
        err_msg = f"Error getting flowlines for comid '{comid}'"

    feature_collection = _query_nldi(url, query_params, err_msg)
    if as_json:
        return feature_collection
    gdf = gpd.GeoDataFrame.from_features(feature_collection, crs=_CRS)
    return gdf


def get_basin(
    feature_source: str,
    feature_id: str,
    simplified: bool = True,
    split_catchment: bool = False,
    as_json: bool = False,
) -> gpd.GeoDataFrame | dict:
    """Gets the aggregated basin for the specified feature in WGS84 lat/lon
    as GeoDataFrame or as JSON conatining a polygon geometry.

    Parameters
    ----------
    feature_source: string, name of the feature source
    feature_id: string, identifier of the feature
    simplified: bool, simplified, default is True
    split_catchment: bool, split catchment, default is False
    as_json: bool, return basin as JSON is set to True, otherwise return
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
    _validate_data_source(feature_source)
    if not feature_id:
        raise ValueError("feature_id is required")

    url = f"{NLDI_API_BASE_URL}/{feature_source}/{feature_id}/basin"
    simplified = str(simplified).lower()
    split_catchment = str(split_catchment).lower()
    query_params = {"simplified": simplified, "splitCatchment": split_catchment}
    err_msg = (
        f"Error getting basin for feature source '{feature_source}' and "
        f"feature_id '{feature_id}'"
    )
    feature_collection = _query_nldi(url, query_params, err_msg)
    if as_json:
        return feature_collection
    gdf = gpd.GeoDataFrame.from_features(feature_collection, crs=_CRS)
    return gdf


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
) -> gpd.GeoDataFrame | dict:
    """Gets all features found along the specified navigation either by
    comid or feature source as points in WGS84 lat/long coordinates - a GeoDataFrame
    containing a point geometry.

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

    if (lat is None) != (long is None):
        raise ValueError("Both lat and long are required")

    have_latlong = lat is not None
    if have_latlong:
        if comid is not None:
            raise ValueError(
                "Provide only one origin type - comid cannot be provided"
                " with lat or long"
            )
        if feature_source is not None or feature_id is not None:
            raise ValueError(
                "Provide only one origin type - feature_source and feature_id cannot"
                " be provided with lat or long"
            )
    else:
        if (comid is not None or data_source is not None) and navigation_mode is None:
            raise ValueError(
                "navigation_mode is required if comid or data_source is provided"
            )
        _validate_feature_source_comid(feature_source, feature_id, comid)
        if data_source:
            _validate_data_source(data_source)
        if feature_source:
            _validate_data_source(feature_source)
        if navigation_mode:
            navigation_mode = _validate_navigation_mode(navigation_mode)

    if have_latlong:
        url = f"{NLDI_API_BASE_URL}/comid/position"
        query_params = {"coords": f"POINT({long} {lat})"}
        err_msg = f"Error getting features for lat '{lat}' and long '{long}'"
    elif navigation_mode:
        if feature_source:
            url = f"{NLDI_API_BASE_URL}/{feature_source}/{feature_id}/navigation"
        else:
            url = f"{NLDI_API_BASE_URL}/comid/{comid}/navigation"
        url += f"/{navigation_mode}/{data_source}"
        query_params = {"distance": str(distance)}
        if stop_comid is not None:
            query_params["stopComid"] = str(stop_comid)
        err_msg = _features_err_msg(feature_source, feature_id, comid, data_source)
    else:
        url = f"{NLDI_API_BASE_URL}/{feature_source}/{feature_id}"
        query_params = {}
        err_msg = _features_err_msg(feature_source, feature_id, comid, data_source)

    feature_collection = _query_nldi(url, query_params, err_msg)
    if as_json:
        return feature_collection
    gdf = gpd.GeoDataFrame.from_features(feature_collection, crs=_CRS)
    return gdf


# TODO: This function can cause timeout error for some data sources
#  - may be we shouldn't provide this function?
def get_features_by_data_source(data_source: str) -> gpd.GeoDataFrame:
    """Gets all features found for the specified data source as
    points in WGS84 lat/long coordinates as GeoDataFrame containing a point geometry.

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

        >>> # Get features for a feature wqp and feature_id USGS-01031500
        >>> gdf = dataretrieval.nldi.get_features_by_data_source(
        ...     data_source="nwissite"
        ... )
    """
    # validate the data source
    _validate_data_source(data_source)
    url = f"{NLDI_API_BASE_URL}/{data_source}"
    err_msg = f"Error getting features for data source '{data_source}'"
    feature_collection = _query_nldi(url, {}, err_msg)
    gdf = gpd.GeoDataFrame.from_features(feature_collection, crs=_CRS)
    return gdf


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
) -> dict:
    """Searches for the specified feature in NLDI and returns the results
    as a dictionary.

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
    if (lat is None) != (long is None):
        raise ValueError("Both lat and long are required")

    find = find.lower()
    if find not in ("basin", "flowlines", "features"):
        raise ValueError(
            f"Invalid value for find: {find} - allowed values are:"
            f" 'basin', 'flowlines', or 'features'"
        )
    if lat is not None and find != "features":
        raise ValueError(
            f"Invalid value for find: {find} - lat/long is to get features not {find}"
        )
    if comid is not None and find == "basin":
        raise ValueError(
            "Invalid value for find: basin - comid is to get features"
            " or flowlines not basin"
        )

    if lat is not None:
        return get_features(lat=lat, long=long, as_json=True)

    if find == "basin":
        return get_basin(
            feature_source=feature_source, feature_id=feature_id, as_json=True
        )

    if find == "flowlines":
        if navigation_mode is None:
            raise ValueError(
                "navigation_mode is required for find='flowlines';"
                f" allowed values are {_VALID_NAVIGATION_MODES}"
            )
        return get_flowlines(
            navigation_mode=navigation_mode,
            distance=distance,
            feature_source=feature_source,
            feature_id=feature_id,
            comid=comid,
            as_json=True,
        )
    # here find == 'features'
    return get_features(
        data_source=data_source,
        navigation_mode=navigation_mode,
        distance=distance,
        feature_source=feature_source,
        feature_id=feature_id,
        comid=comid,
        as_json=True,
    )


def _validate_data_source(data_source: str):
    # A helper function to validate user specified data source/feature source

    global _AVAILABLE_DATA_SOURCES

    # get the available data/feature sources - if not already cached
    if _AVAILABLE_DATA_SOURCES is None:
        url = f"{NLDI_API_BASE_URL}/"
        available_data_sources = _query_nldi(
            url, {}, "Error getting available data sources"
        )
        _AVAILABLE_DATA_SOURCES = [ds["source"] for ds in available_data_sources]
        if data_source not in _AVAILABLE_DATA_SOURCES:
            err_msg = (
                f"Invalid data source '{data_source}'."
                f" Available data sources are: {_AVAILABLE_DATA_SOURCES}"
            )
            raise ValueError(err_msg)


_VALID_NAVIGATION_MODES = ("UM", "DM", "UT", "DD")


def _features_err_msg(feature_source, feature_id, comid, data_source) -> str:
    if feature_source is not None:
        return (
            f"Error getting features for feature source '{feature_source}'"
            f" and feature_id '{feature_id}', and data source '{data_source}'"
        )
    return f"Error getting features for comid '{comid}' and data source '{data_source}'"


def _validate_navigation_mode(navigation_mode: str | None) -> str:
    if navigation_mode is None:
        raise ValueError(
            f"navigation_mode is required; allowed values are {_VALID_NAVIGATION_MODES}"
        )
    normalized = navigation_mode.upper()
    if normalized not in _VALID_NAVIGATION_MODES:
        raise ValueError(
            f"Invalid navigation mode '{navigation_mode}';"
            f" allowed values are {_VALID_NAVIGATION_MODES}"
        )
    return normalized


def _validate_feature_source_comid(
    feature_source: str | None, feature_id: str | None, comid: int | None
):
    if feature_source is not None and feature_id is None:
        raise ValueError("feature_id is required if feature_source is provided")
    if feature_id is not None and feature_source is None:
        raise ValueError("feature_source is required if feature_id is provided")
    if comid is not None and feature_source is not None:
        raise ValueError(
            "Specify only one origin type - comid and feature_source"
            " cannot be provided together"
        )
    if comid is None and feature_source is None:
        raise ValueError(
            "Specify one origin type - comid or feature_source is required"
        )
