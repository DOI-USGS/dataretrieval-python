from typing import Optional

import geopandas as gpd

from dataretrieval.utils import query

NLDI_API_BASE_URL = 'https://labs.waterdata.usgs.gov/api/nldi/linked-data'


def _query_nldi(url, query_params, error_message):
    # A helper function to query the NLDI API
    response = query(url, query_params)
    if response.status_code != 200:
        raise ValueError(f'{error_message}. Error reason: {response.reason}')
    return response.json()


def get_flowlines(
    feature_source: str,
    feature_id: str,
    navigation_mode: str,
    distance: int = 5,
    stop_comid: Optional[int] = None,
    trim_start: bool = False,
) -> gpd.GeoDataFrame:
    """Gets the flowlines for the specified navigation in WGS84 lat/long coordinates
     as GeoDataFrame containing a polyline geometry.

    Parameters
    ----------
    feature_source: string, name of the feature source
    feature_id: string, identifier of the feature
    navigation_mode: string, navigation mode, allowed values are 'UM', 'DM', 'UT', 'DD'
    distance: int, distance in kilometers, default is 5
    stop_comid: integer, optional, stop comid
    trim_start: bool, trim start, default is False

    Returns
    -------
    gdf: GeoDataFrame
        GeoDataFrame of flowlines

    Examples
    --------
    .. doctest::

        >>> # Get flowlines for a feature wqp and feature_id USGS-01031500
        >>> gdf = dataretrieval.nldi.get_flowlines(
        ...     feature_source="wqp", feature_id="USGS-01031500", navigation_mode="UM"
        ... )
    """
    navigation_mode = navigation_mode.upper()
    if navigation_mode not in ('UM', 'DM', 'UT', 'DD'):
        raise TypeError(f"Invalid navigation mode '{navigation_mode}'")

    url = f'{NLDI_API_BASE_URL}/{feature_source}/{feature_id}/navigation'
    url += f'/{navigation_mode}/flowlines'
    query_params = {'distance': str(distance), 'trimStart': str(trim_start).lower()}
    if stop_comid is not None:
        query_params['stopComid'] = str(stop_comid)

    err_msg = (
        f"Error getting flowlines for feature source '{feature_source}'"
        f" and feature_id '{feature_id}'"
    )
    feature_collection = _query_nldi(url, query_params, err_msg)
    gdf = gpd.GeoDataFrame.from_features(feature_collection)
    return gdf


def get_basin(
    feature_source: str,
    feature_id: str,
    simplified: bool = True,
    split_catchment: bool = False,
) -> gpd.GeoDataFrame:
    """Gets the aggregated basin for the specified feature in WGS84 lat/lon
    as GeoDataFrame conatining a polygon geometry.

    Parameters
    ----------
    feature_source: string, name of the feature source
    feature_id: string, identifier of the feature
    simplified: bool, simplified, default is True
    split_catchment: bool, split catchment, default is False

    Returns
    -------
    gdf: GeoDataFrame
        GeoDataFrame of basin

    Examples
    --------
    .. doctest::

        >>> # Get basin for a feature wqp and feature_id USGS-01031500
        >>> gdf = dataretrieval.nldi.get_basin(
        ...     feature_source="wqp", feature_id="USGS-01031500"
        ... )
    """
    feature_source = feature_source.upper()
    url = f'{NLDI_API_BASE_URL}/{feature_source}/{feature_id}/basin'
    query_params = {'simplified': simplified, 'splitCatchment': split_catchment}
    err_msg = (
        f"Error getting basin for feature source '{feature_source}' and "
        f"feature_id '{feature_id}'"
    )
    feature_collection = _query_nldi(url, query_params, err_msg)
    gdf = gpd.GeoDataFrame.from_features(feature_collection)
    return gdf
