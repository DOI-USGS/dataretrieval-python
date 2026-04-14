from __future__ import annotations

import json
import logging
import os
import re
from collections.abc import Iterable, Mapping
from datetime import datetime
from typing import Any, get_args
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from dataretrieval import __version__
from dataretrieval.utils import BaseMetadata
from dataretrieval.waterdata import filters
from dataretrieval.waterdata.types import (
    PROFILE_LOOKUP,
    PROFILES,
    SERVICES,
)

try:
    import geopandas as gpd

    GEOPANDAS = True
except ImportError:
    GEOPANDAS = False

# Set up logger for this module
logger = logging.getLogger(__name__)

BASE_URL = "https://api.waterdata.usgs.gov"
OGC_API_VERSION = "v0"
OGC_API_URL = f"{BASE_URL}/ogcapi/{OGC_API_VERSION}"
SAMPLES_URL = f"{BASE_URL}/samples-data"
STATISTICS_API_VERSION = "v0"
STATISTICS_API_URL = f"{BASE_URL}/statistics/{STATISTICS_API_VERSION}"


def _switch_arg_id(ls: dict[str, Any], id_name: str, service: str):
    """
    Switch argument id from its package-specific identifier to the standardized "id" key
    that the API recognizes.

    Sets the "id" key in the provided dictionary `ls`
    with the value from either the service name or the expected id column name.
    If neither key exists, "id" will be set to None.

    Parameters
    ----------
    ls : Dict[str, Any]
        The dictionary containing identifier keys to be standardized.
    id_name : str
        The name of the specific identifier key to look for.
    service : str
        The service name.

    Returns
    -------
    Dict[str, Any]
        The modified dictionary with the "id" key set appropriately.

    Examples
    --------
    For service "time-series-metadata", the function will look for either
    "time_series_metadata_id" or "time_series_id" and change the key to simply
    "id".
    """

    service_id = service.replace("-", "_") + "_id"

    if "id" not in ls:
        if service_id in ls:
            ls["id"] = ls[service_id]
        elif id_name in ls:
            ls["id"] = ls[id_name]

    # Remove the original keys regardless of whether they were used
    ls.pop(service_id, None)
    ls.pop(id_name, None)

    return ls


def _switch_properties_id(properties: list[str] | None, id_name: str, service: str):
    """
    Switch properties id from its package-specific identifier to the
    standardized "id" key that the API recognizes.

    Sets the "id" key in the provided dictionary `ls` with the value from either
    the service name or the expected id column name. If neither key exists, "id"
    will be set to None.

    Parameters
    ----------
    properties : Optional[List[str]]
        A list containing the properties or column names to be pulled from the
        service, or None.
    id_name : str
        The name of the specific identifier key to look for.
    service : str
        The service name.

    Returns
    -------
    List[str]
        The modified list with the "id" key set appropriately.

    Examples
    --------
    For service "monitoring-locations", it will look for
    "monitoring_location_id" and change
    it to "id".
    """
    if not properties:
        return []
    service_id = service.replace("-", "_") + "_id"
    last_letter = service[-1]
    service_id_singular = ""
    if last_letter == "s":
        service_singular = service[:-1]
        service_id_singular = service_singular.replace("-", "_") + "_id"
    # Replace id fields with "id"
    id_fields = [service_id, service_id_singular, id_name]
    properties = ["id" if p in id_fields else p.replace("-", "_") for p in properties]
    # Remove unwanted fields
    return [p for p in properties if p not in ["geometry", service_id]]


_DATETIME_FORMATS = (
    "%Y-%m-%dT%H:%M:%S.%f%z",
    "%Y-%m-%dT%H:%M:%S%z",
    "%Y-%m-%dT%H:%M:%S.%f",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%d %H:%M:%S.%f",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
)

# Anchored to ``[Pp]\d`` so a normal word containing ``p`` (e.g. ``"Apr"``)
# doesn't get mis-classified as an ISO 8601 duration; the optional ``T``
# admits time-only forms like ``PT36H``.
_DURATION_RE = re.compile(r"^[Pp]T?\d")

# OGC API parameters that carry a date/datetime value (single string,
# two-element range, or interval/duration string) rather than a multi-value
# string list. Used by ``_construct_api_requests`` to keep them out of the
# POST/CQL2 multi-value path and to route them through ``_format_api_dates``,
# and by ``_NO_NORMALIZE_PARAMS`` to bypass string-iterable normalization.
_DATE_RANGE_PARAMS = frozenset(
    {"datetime", "last_modified", "begin", "begin_utc", "end", "end_utc", "time"}
)


def _parse_datetime(value: str) -> datetime | None:
    """Parse a single datetime string against the supported formats.

    Returns a ``datetime`` (tz-aware iff the input carried a UTC offset),
    or ``None`` if no format matched.
    """
    # ``datetime.strptime`` accepts a numeric offset like ``+00:00`` but not
    # the ``Z`` shorthand, so normalize trailing ``Z`` first.
    candidate = value[:-1] + "+00:00" if value.endswith("Z") else value
    for fmt in _DATETIME_FORMATS:
        try:
            return datetime.strptime(candidate, fmt)
        except ValueError:
            continue
    return None


def _format_one(dt, *, date: bool, local_tz) -> str | None:
    """Format a single datetime element for inclusion in the API time arg."""
    if pd.isna(dt) or dt == "" or dt is None:
        return ".."
    parsed = _parse_datetime(dt)
    if parsed is None:
        return None
    if date:
        return parsed.strftime("%Y-%m-%d")
    aware = parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=local_tz)
    return aware.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")


def _format_api_dates(
    datetime_input: str | list[str | None] | None, date: bool = False
) -> str | None:
    """
    Formats date or datetime input(s) for use with an API.

    Handles single values or ranges, and converting to ISO 8601 or date-only
    formats as needed.

    Parameters
    ----------
    datetime_input : Union[str, List[Optional[str]], None]
        A single date/datetime string or a list of one or two date/datetime
        strings. Accepts formats like "%Y-%m-%d %H:%M:%S", ISO 8601 (with or
        without ``Z``/numeric offset), or relative periods (e.g., "P7D" /
        "PT36H"). Range endpoints may be ``None``/``NaN``/empty to denote a
        half-bounded range.
    date : bool, optional
        If True, uses only the date portion ("YYYY-MM-DD"). If False (default),
        returns full datetime in UTC ISO 8601 format ("YYYY-MM-DDTHH:MM:SSZ").

    Returns
    -------
    Union[str, None]
        - If input is a single value, returns the formatted date/datetime string
        or None if parsing fails.
        - If input is a list of two values, returns a date/datetime range string
        separated by "/" (e.g., "YYYY-MM-DD/YYYY-MM-DD" or
        "YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ").
        - Returns None if input is empty, all NA, or cannot be parsed.

    Raises
    ------
    ValueError
        If `datetime_input` contains more than two values.

    Notes
    -----
    - A single blank/NA value returns None. In a two-value range, a blank/NA
    endpoint is rendered as ``".."`` to denote an open bound (e.g.
    ``"2024-01-01/.."``); the range is only None when *every* element is
    blank/NA or any non-NA element fails to parse.
    - Supports ISO 8601 durations such as "P7D" and "PT36H" and pre-formatted
    intervals containing ``"/"``; both are passed through unchanged.
    - Converts datetimes to UTC and formats as ISO 8601 with 'Z' suffix when
    `date` is False. Inputs with an explicit offset (``Z`` or ``+HH:MM``) are
    converted from that offset to UTC; naive inputs are interpreted in the
    local time zone for backwards compatibility.
    """
    if datetime_input is None:
        return None
    # Get timezone
    local_timezone = datetime.now().astimezone().tzinfo

    # Convert single string to list for uniform processing
    if isinstance(datetime_input, str):
        datetime_input = [datetime_input]
    elif isinstance(datetime_input, Mapping):
        # `list(mapping)` returns keys, which silently accepts the wrong shape.
        raise TypeError(
            f"date input must be a string or sequence of strings, "
            f"not {type(datetime_input).__name__}."
        )
    elif not isinstance(datetime_input, (list, tuple)):
        # Materialize any other iterable (pandas.Series, numpy.ndarray,
        # generator, ...) so the len()/subscript operations below work.
        datetime_input = list(datetime_input)

    # Check for null or all NA and return None
    if all(pd.isna(dt) or dt == "" or dt is None for dt in datetime_input):
        return None

    if len(datetime_input) > 2:
        raise ValueError("datetime_input should only include 1-2 values")

    # Pass through duration ("P7D", "PT36H") and pre-formatted interval ("a/b")
    # strings untouched.
    if len(datetime_input) == 1 and isinstance(datetime_input[0], str):
        single = datetime_input[0]
        if _DURATION_RE.match(single) or "/" in single:
            return single

    # Half-bounded ranges: NA endpoints render as ".."; any unparseable non-NA
    # element invalidates the range.
    formatted = [
        _format_one(dt, date=date, local_tz=local_timezone) for dt in datetime_input
    ]
    if any(f is None for f in formatted):
        return None
    return "/".join(formatted)


def _cql2_param(args: dict[str, Any]) -> str:
    """
    Convert query parameters to CQL2 JSON format for POST requests.

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of query parameters to convert to CQL2 format.

    Returns
    -------
    str
        JSON string representation of the CQL2 query.
    """
    filters = []
    for key, values in args.items():
        filters.append({"op": "in", "args": [{"property": key}, values]})

    query = {"op": "and", "args": filters}

    return json.dumps(query, indent=4)


def _default_headers():
    """
    Generate default HTTP headers for API requests.

    Returns
    -------
    dict
        A dictionary containing default headers including 'Accept-Encoding',
        'Accept', 'User-Agent', and 'lang'. If the environment variable
        'API_USGS_PAT' is set, its value is included as the 'X-Api-Key' header.
    """
    headers = {
        "Accept-Encoding": "compress, gzip",
        "Accept": "application/json",
        "User-Agent": f"python-dataretrieval/{__version__}",
        "lang": "en-US",
    }
    token = os.getenv("API_USGS_PAT")
    if token:
        headers["X-Api-Key"] = token
    return headers


def _check_ogc_requests(endpoint: str = "daily", req_type: str = "queryables"):
    """
    Sends an HTTP GET request to the specified OGC endpoint and request type,
    returning the JSON response.

    Parameters
    ----------
    endpoint : str, optional
        The OGC collection endpoint to query (default is "daily").
    req_type : str, optional
        The type of request to make. Must be either "queryables" or "schema"
        (default is "queryables").

    Returns
    -------
    dict
        The JSON response from the OGC endpoint.

    Raises
    ------
    ValueError
        If req_type is not "queryables" or "schema".
    requests.HTTPError
        If the HTTP request returns an unsuccessful status code.
    """
    if req_type not in ("queryables", "schema"):
        raise ValueError(f"req_type must be 'queryables' or 'schema', got {req_type!r}")
    url = f"{OGC_API_URL}/collections/{endpoint}/{req_type}"
    resp = requests.get(url, headers=_default_headers())
    resp.raise_for_status()
    return resp.json()


def _error_body(resp: requests.Response):
    """
    Build an informative error message from an HTTP response.

    Parameters
    ----------
    resp : requests.Response
        The HTTP response object to extract the error message from.

    Returns
    -------
    str
        An error message string assembled per status code:

        * **429** — predefined message describing the rate-limit and pointing
          at the API-token path; the response body is not consulted.
        * **403** — predefined message describing the most common cause
          (query exceeding server limits); the response body is not
          consulted.
        * **other statuses** — attempts ``resp.json()`` and renders
          ``"<status>: <code>. <description>."`` from the JSON error
          envelope. If the body is not JSON (e.g. an HTML 502 from a
          gateway), falls back to ``"<status>: <reason>. <snippet>"`` with
          the first 200 characters of ``resp.text``; an empty body
          degrades to ``"<status>: <reason>."``.
    """
    status = resp.status_code
    if status == 429:
        return (
            "429: Too many requests made. Please obtain an API token "
            "or try again later."
        )
    elif status == 403:
        return (
            "403: Query request denied. Possible reasons include "
            "query exceeding server limits."
        )
    try:
        j_txt = resp.json()
    except ValueError:
        snippet = (resp.text or "").strip()[:200]
        reason = resp.reason or "Error"
        if snippet:
            return f"{status}: {reason}. {snippet}"
        return f"{status}: {reason}."
    return (
        f"{status}: {j_txt.get('code', 'Unknown type')}. "
        f"{j_txt.get('description', 'No description provided')}."
    )


def _construct_api_requests(
    service: str,
    properties: list[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    skip_geometry: bool = False,
    **kwargs,
):
    """
    Constructs an HTTP request object for the specified water data API service.

    For most services, list parameters are comma-joined and sent as a single
    GET request (e.g. ``parameter_code=["00060","00010"]`` becomes
    ``parameter_code=00060,00010`` in the URL). For services that do not
    support comma-separated values (currently only ``monitoring-locations``),
    a POST request with CQL2 JSON is used instead.

    Parameters
    ----------
    service : str
        The name of the API service to query (e.g., "daily").
    properties : Optional[List[str]], optional
        List of property names to include in the request.
    bbox : Optional[List[float]], optional
        Bounding box coordinates as a list of floats.
    limit : Optional[int], optional
        Maximum number of results to return per request.
    skip_geometry : bool, optional
        Whether to exclude geometry from the response (default is False).
    **kwargs
        Additional query parameters, including date/time filters and other
        API-specific options.

    Returns
    -------
    requests.PreparedRequest
        The constructed HTTP request object ready to be sent.

    Notes
    -----
    - Date/time parameters are automatically formatted to ISO8601.
    """
    service_url = f"{OGC_API_URL}/collections/{service}/items"

    # The monitoring-locations endpoint does not support comma-separated values
    # for multi-value GET parameters; CQL2 POST is required for that service.
    _cql2_required_services = {"monitoring-locations"}

    # Format date/time parameters to ISO8601 first — both routing paths need it.
    for key in _DATE_RANGE_PARAMS:
        if key in kwargs:
            kwargs[key] = _format_api_dates(
                kwargs[key],
                date=(service == "daily" and key != "last_modified"),
            )

    if service in _cql2_required_services:
        # Legacy path: POST with CQL2 for multi-value params
        post_params = {
            k: v
            for k, v in kwargs.items()
            if k not in _DATE_RANGE_PARAMS
            and isinstance(v, (list, tuple))
            and len(v) > 1
        }
        params = {k: v for k, v in kwargs.items() if k not in post_params}
    else:
        # Join list/tuple values with commas for multi-value GET parameters.
        post_params = {}
        params = {
            k: ",".join(str(x) for x in v) if isinstance(v, (list, tuple)) else v
            for k, v in kwargs.items()
        }

    params["skipGeometry"] = skip_geometry
    params["limit"] = 50000 if limit is None or limit > 50000 else limit

    # `len()` instead of truthiness: a numpy ndarray would raise on `if bbox:`.
    if bbox is not None and len(bbox) > 0:
        params["bbox"] = ",".join(map(str, bbox))
    if properties:
        params["properties"] = ",".join(properties)

    # Translate CQL filter Python names to the hyphenated URL parameter that
    # the OGC API expects. The Python kwarg is `filter_lang` because hyphens
    # aren't valid in Python identifiers.
    if "filter_lang" in params:
        params["filter-lang"] = params.pop("filter_lang")

    headers = _default_headers()

    if post_params:
        headers["Content-Type"] = "application/query-cql-json"
        request = requests.Request(
            method="POST",
            url=service_url,
            headers=headers,
            data=_cql2_param(post_params),
            params=params,
        )
    else:
        request = requests.Request(
            method="GET",
            url=service_url,
            headers=headers,
            params=params,
        )
    return request.prepare()


def _next_req_url(resp: requests.Response) -> str | None:
    """
    Extracts the URL for the next page of results from an HTTP response from a
    water data endpoint.

    Parameters
    ----------
    resp : requests.Response
        The HTTP response object containing JSON data and headers.

    Returns
    -------
    Optional[str]
        The URL for the next page of results if available, otherwise None.

    Notes
    -----
    - If the environment variable "API_USGS_PAT" is set, logs the remaining
    requests for the current hour.
    - Logs the next URL if found at info level.
    - Expects the response JSON to contain a "links" list with objects having
    "rel" and "href" keys.
    - Checks for the "next" relation in the "links" to determine the next URL.
    """
    body = resp.json()
    if not body.get("numberReturned"):
        return None
    header_info = resp.headers
    if os.getenv("API_USGS_PAT", ""):
        logger.info(
            "Remaining requests this hour: %s",
            header_info.get("x-ratelimit-remaining", ""),
        )
    for link in body.get("links", []):
        if link.get("rel") == "next":
            next_url = link.get("href")
            logger.info("Next URL: %s", next_url)
            return next_url
    return None


def _get_resp_data(resp: requests.Response, geopd: bool) -> pd.DataFrame:
    """
    Extracts and normalizes data from an HTTP response containing GeoJSON features.

    Parameters
    ----------
    resp : requests.Response
        The HTTP response object expected to contain a JSON body
        with a "features" key.
    geopd : bool
        Indicates whether geopandas is installed and should be used to
        handle geometries.

    Returns
    -------
    gpd.GeoDataFrame or pd.DataFrame
        A geopandas GeoDataFrame if geometry is included, or a pandas DataFrame
        containing the feature properties and each row's service-specific id.
        Returns an empty pandas DataFrame if no features are returned.
    """
    # Check if it's an empty response
    body = resp.json()
    if not body.get("numberReturned"):
        return pd.DataFrame()

    # If geopandas not installed, return a pandas dataframe
    if not geopd:
        df = pd.json_normalize(body["features"], sep="_")
        df = df.drop(
            columns=["type", "geometry", "AsGeoJSON(geometry)"], errors="ignore"
        )
        df.columns = [col.replace("properties_", "") for col in df.columns]
        df.rename(columns={"geometry_coordinates": "geometry"}, inplace=True)
        df = df.loc[:, ~df.columns.duplicated()]
        return df

    # Organize json into geodataframe and make sure id column comes along.
    df = gpd.GeoDataFrame.from_features(body["features"])
    df["id"] = pd.json_normalize(body["features"])["id"].values
    df = df[["id"] + [col for col in df.columns if col != "id"]]

    # If no geometry present, then return pandas dataframe. A geodataframe
    # is not needed.
    if df["geometry"].isnull().all():
        df = pd.DataFrame(df.drop(columns="geometry"))

    return df


def _walk_pages(
    geopd: bool,
    req: requests.PreparedRequest,
    client: requests.Session | None = None,
) -> tuple[pd.DataFrame, requests.Response]:
    """
    Iterates through paginated API responses and aggregates the results
    into a single DataFrame.

    Parameters
    ----------
    geopd : bool
        Indicates whether geopandas is installed and should be used for handling
        geometries.
    req : requests.PreparedRequest
        The initial HTTP request to send.
    client : Optional[requests.Session], default None
        An optional HTTP client to use for requests. If not provided, a new
        client is created.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the aggregated results from all pages.
    requests.Response
        The initial response object containing metadata about the first request.

    Raises
    ------
    Exception
        If a request fails/returns a non-200 status code.
    """
    logger.info("Requesting: %s", req.url)

    if not geopd:
        logger.warning(
            "Geopandas not installed. Geometries will be flattened "
            "into pandas DataFrames."
        )

    # Get first response from client
    # using GET or POST call
    close_client = client is None
    client = client or requests.Session()
    try:
        resp = client.send(req)
        if resp.status_code != 200:
            raise RuntimeError(_error_body(resp))

        # Store the initial response for metadata
        initial_response = resp

        # Grab some aspects of the original request: headers and the
        # request type (GET or POST)
        method = req.method.upper()
        headers = dict(req.headers)
        content = req.body if method == "POST" else None

        # List to collect dataframes from each page
        dfs = [_get_resp_data(resp, geopd=geopd)]
        curr_url = _next_req_url(resp)
        while curr_url:
            try:
                resp = client.request(
                    method,
                    curr_url,
                    headers=headers,
                    data=content if method == "POST" else None,
                )
                dfs.append(_get_resp_data(resp, geopd=geopd))
                curr_url = _next_req_url(resp)
            except Exception:  # noqa: BLE001
                error_text = _error_body(resp)
                logger.error("Request incomplete. %s", error_text)
                logger.warning(
                    "Request failed for URL: %s. Data download interrupted.", curr_url
                )
                curr_url = None

        # Concatenate all pages at once for efficiency
        return pd.concat(dfs, ignore_index=True), initial_response
    finally:
        if close_client:
            client.close()


def _deal_with_empty(
    return_list: pd.DataFrame, properties: list[str] | None, service: str
) -> pd.DataFrame:
    """
    Handles empty DataFrame results by returning a DataFrame with appropriate columns.

    If `return_list` is empty, determines the column names to use:
        - If `properties` is not provided or contains only NaN values,
            retrieves schema properties from the specified service.
    - Otherwise, uses the provided `properties` list as column names.

    Parameters
    ----------
    return_list : pd.DataFrame
        The DataFrame to check for emptiness.
    properties : Optional[List[str]]
        List of property names to use as columns, or None.
    service : str
        The service endpoint to query for schema properties if needed.

    Returns
    -------
    pd.DataFrame
        The original DataFrame if not empty, otherwise an empty
        DataFrame with the appropriate columns.
    """
    if return_list.empty:
        if not properties or all(pd.isna(properties)):
            schema = _check_ogc_requests(endpoint=service, req_type="schema")
            properties = list(schema.get("properties", {}).keys())
        return pd.DataFrame(columns=properties)
    return return_list


def _arrange_cols(
    df: pd.DataFrame, properties: list[str] | None, output_id: str
) -> pd.DataFrame:
    """
    Rearranges and renames columns in a DataFrame based on provided
    properties and the service output id.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame whose columns are to be rearranged or renamed.
    properties : Optional[List[str]]
        A list of column names to possibly rename. If None or contains
        only NaN, the function renames 'id' to output_id.
    output_id : str
        The name to which the 'id' column should be renamed if applicable.

    Returns
    -------
    pd.DataFrame or gpd.GeoDataFrame
        The DataFrame with columns rearranged and/or renamed according
        to the specified properties and output_id.
    """

    # Rename id column to output_id
    df = df.rename(columns={"id": output_id})

    if properties and not all(pd.isna(properties)):
        # Don't alias the caller's list — we mutate below.
        local_properties = list(properties)
        if "geometry" in df.columns and "geometry" not in local_properties:
            local_properties.append("geometry")
        # 'id' is a valid service column, but expose it under the
        # service-specific output_id name instead.
        if "id" in local_properties:
            local_properties[local_properties.index("id")] = output_id
        df = df.loc[:, [col for col in local_properties if col in df.columns]]

    # Move meaningless-to-user, extra id columns to the end
    # of the dataframe, if they exist
    extra_id_col = set(df.columns).intersection(
        {
            "latest_continuous_id",
            "latest_daily_id",
            "daily_id",
            "continuous_id",
            "field_measurement_id",
        }
    )

    # If the arbitrary id column is returned (either due to properties
    # being none or NaN), then move it to the end of the dataframe, but
    # if part of properties, keep in requested order
    if extra_id_col and (properties is None or all(pd.isna(properties))):
        id_col_order = [col for col in df.columns if col not in extra_id_col] + list(
            extra_id_col
        )
        df = df.loc[:, id_col_order]

    return df


def _type_cols(df: pd.DataFrame) -> pd.DataFrame:
    """
    Casts columns into appropriate types.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing water data.

    Returns
    -------
    pd.DataFrame
        The DataFrame with columns cast to appropriate types.

    """
    cols = set(df.columns)
    numerical_cols = [
        "altitude",
        "altitude_accuracy",
        "contributing_drainage_area",
        "drainage_area",
        "hole_constructed_depth",
        "value",
        "well_constructed_depth",
    ]
    time_cols = [
        "begin",
        "begin_utc",
        "construction_date",
        "end",
        "end_utc",
        "datetime",  # unused
        "last_modified",
        "time",
    ]

    for col in cols.intersection(time_cols):
        df[col] = pd.to_datetime(df[col], errors="coerce")

    for col in cols.intersection(numerical_cols):
        df[col] = pd.to_numeric(df[col], errors="coerce")

    return df


def _sort_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Sorts rows by 'time' and 'monitoring_location_id' columns if they
    exist.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing water data.

    Returns
    -------
    pd.DataFrame
        The DataFrame with rows ordered by time and site.

    """
    if "time" in df.columns and "monitoring_location_id" in df.columns:
        df = df.sort_values(by=["time", "monitoring_location_id"], ignore_index=True)
    elif "time" in df.columns:
        df = df.sort_values(by="time", ignore_index=True)

    return df


def get_ogc_data(
    args: dict[str, Any], output_id: str, service: str
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Retrieves OGC (Open Geospatial Consortium) data from a specified
    endpoint and returns it as a pandas DataFrame with metadata.

    This function prepares request arguments, constructs API requests,
    handles pagination, processes the results, and formats output
    according to the specified parameters.

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of request arguments for the OGC service.
    output_id : str
        The name of the output identifier to use in the request.
    service : str
        The OGC service type (e.g., "wfs", "wms").

    Returns
    -------
    pd.DataFrame or gpd.GeoDataFrame
        A DataFrame containing the retrieved and processed OGC data.
    BaseMetadata
        A metadata object containing request information including URL and query time.

    Notes
    -----
    - The function does not mutate the input `args` dictionary.
    - Handles optional arguments such as `convert_type`.
    - Applies column cleanup and reordering based on service and properties.
    """
    args = args.copy()
    args["service"] = service
    args = _switch_arg_id(args, id_name=output_id, service=service)
    # Capture `properties` before the id-switch so post-processing sees
    # the user-facing names, not the wire-format ones.
    properties = args.get("properties")
    args["properties"] = _switch_properties_id(
        properties, id_name=output_id, service=service
    )
    convert_type = args.pop("convert_type", False)
    args = {k: v for k, v in args.items() if v is not None}

    return_list, response = _fetch_once(args)
    return_list = _deal_with_empty(return_list, properties, service)
    if convert_type:
        return_list = _type_cols(return_list)
    return_list = _arrange_cols(return_list, properties, output_id)
    return_list = _sort_rows(return_list)

    return return_list, BaseMetadata(response)


@filters.chunked(build_request=_construct_api_requests)
def _fetch_once(
    args: dict[str, Any],
) -> tuple[pd.DataFrame, requests.Response]:
    """Send one prepared-args OGC request; return the frame + response.

    Filter chunking is added orthogonally by the ``@filters.chunked``
    decorator: with no filter (or an un-chunkable one) the decorator
    passes ``args`` through to this body; with a chunkable filter it
    fans out and calls this body once per sub-filter, then combines.
    Either way the return shape is ``(frame, response)``.
    """
    req = _construct_api_requests(**args)
    return _walk_pages(geopd=GEOPANDAS, req=req)


def _handle_stats_nesting(
    body: dict[str, Any],
    geopd: bool = False,
) -> pd.DataFrame:
    """
    Takes nested json from stats service and flattens into a dataframe with
    one row per monitoring location, parameter, and statistic.

    Parameters
    ----------
    body : Dict[str, Any]
        The JSON response body from the statistics service containing nested data.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the flattened statistical data.
    """
    if body is None:
        return pd.DataFrame()

    if not geopd:
        logger.info(
            "Geopandas not installed. Geometries will be flattened "
            "into pandas DataFrames."
        )

    # If geopandas not installed, return a pandas dataframe
    # otherwise return a geodataframe
    if not geopd:
        df = pd.json_normalize(body["features"]).drop(
            columns=["type", "properties.data"], errors="ignore"
        )
        df.columns = df.columns.str.split(".").str[-1]
    else:
        df = gpd.GeoDataFrame.from_features(body["features"]).drop(
            columns=["data"], errors="ignore"
        )

    # Unnest json features, properties, data, and values while retaining necessary
    # metadata to merge with main dataframe.
    dat = pd.json_normalize(
        body,
        record_path=["features", "properties", "data", "values"],
        meta=[
            ["features", "properties", "monitoring_location_id"],
            ["features", "properties", "data", "parameter_code"],
            ["features", "properties", "data", "unit_of_measure"],
            ["features", "properties", "data", "parent_time_series_id"],
            # ["features", "geometry", "coordinates"],
        ],
        meta_prefix="",
        errors="ignore",
    )
    dat.columns = dat.columns.str.split(".").str[-1]

    return df.merge(dat, on="monitoring_location_id", how="left")


def _expand_percentiles(df: pd.DataFrame) -> pd.DataFrame:
    """
    Takes percentile value and thresholds columns containing lists
    of values and turns each list element into its own row in the
    original dataframe. 'nan's are removed from the dataframe. If
    no percentile data exist, it adds a percentile column and
    populates column with percentile assigned to min, max, and
    median.

    Parameters
    ----------
    df : pd.DataFrame
        The dataframe returned from using one of the statistics services.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the flattened percentile data.
    """
    if len(df) > 0:
        if "percentile" in df["computation"].unique():
            # Explode percentile lists into rows called "value" and "percentile"
            percentiles = df.loc[df["computation"] == "percentile"]
            percentiles_explode = percentiles[
                ["computation_id", "values", "percentiles"]
            ].explode(["values", "percentiles"], ignore_index=True)
            percentiles_explode = percentiles_explode.loc[
                percentiles_explode["values"] != "nan"
            ]
            percentiles_explode["value"] = pd.to_numeric(percentiles_explode["values"])
            percentiles_explode["percentile"] = pd.to_numeric(
                percentiles_explode["percentiles"]
            )
            percentiles_explode = percentiles_explode.drop(
                columns=["values", "percentiles"]
            )

            # Merge exploded values back to other metadata/geometry
            percentiles = percentiles.drop(
                columns=["values", "percentiles", "value"], errors="ignore"
            ).merge(percentiles_explode, on="computation_id", how="left")

            # Concatenate back to original
            dfs = pd.concat(
                [df.loc[df["computation"] != "percentile"], percentiles]
            ).drop(columns=["values", "percentiles"])
        else:
            dfs = df
            dfs["percentile"] = pd.NA

        # Give min, max, median a percentile value
        dfs.loc[dfs["computation"] == "maximum", "percentile"] = 100
        dfs.loc[dfs["computation"] == "minimum", "percentile"] = 0
        dfs.loc[dfs["computation"] == "median", "percentile"] = 50

        # Make sure numeric
        dfs["percentile"] = pd.to_numeric(dfs["percentile"])

        # Move percentile column
        cols = dfs.columns.tolist()
        cols.remove("percentile")
        col_index = cols.index("value") + 1
        cols.insert(col_index, "percentile")

        return dfs[cols]

    else:
        return df


def get_stats_data(
    args: dict[str, Any],
    service: str,
    expand_percentiles: bool,
    client: requests.Session | None = None,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Retrieves statistical data from a specified endpoint and returns it
    as a pandas DataFrame with metadata.

    This function prepares request arguments, constructs API requests,
    handles pagination, processes results, and formats output according
    to the specified parameters.

    Parameters
    ----------
    args : Dict[str, Any]
        Dictionary of request arguments for the statistics service.
    service : str
        The statistics service type (for example,
        "observationNormals" or "observationIntervals").
    expand_percentiles : bool
        Determines whether the percentiles column is expanded so that
        each percentile gets its own row in the returned dataframe. If
        True and user requests a computation_type other than
        percentiles, a percentile column is still returned.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the retrieved and processed statistical data.
    BaseMetadata
        A metadata object containing request information including URL and query time.
    """

    url = f"{STATISTICS_API_URL}/{service}"

    headers = _default_headers()

    request = requests.Request(
        method="GET",
        url=url,
        headers=headers,
        params=args,
    )
    req = request.prepare()
    logger.info("Request: %s", req.url)

    # create temp client if not provided
    # and close it after the request is done
    close_client = client is None
    client = client or requests.Session()

    try:
        resp = client.send(req)
        if resp.status_code != 200:
            raise RuntimeError(_error_body(resp))

        # Store the initial response for metadata
        initial_response = resp

        # Grab some aspects of the original request: headers and the
        # request type (GET or POST)
        method = req.method.upper()
        headers = dict(req.headers)

        body = resp.json()
        all_dfs = [_handle_stats_nesting(body, geopd=GEOPANDAS)]

        # Look for a next code in the response body
        next_token = body["next"]

        while next_token:
            args["next_token"] = next_token

            try:
                resp = client.request(
                    method,
                    url=url,
                    params=args,
                    headers=headers,
                )
                body = resp.json()
                all_dfs.append(_handle_stats_nesting(body, geopd=GEOPANDAS))
                next_token = body["next"]
            except Exception:  # noqa: BLE001
                error_text = _error_body(resp)
                logger.error("Request incomplete. %s", error_text)
                logger.warning(
                    "Request failed for URL: %s. Data download interrupted.", resp.url
                )
                next_token = None

        dfs = pd.concat(all_dfs, ignore_index=True) if len(all_dfs) > 1 else all_dfs[0]

        # . If expand percentiles is True, make each percentile
        # its own row in the returned dataset.
        if expand_percentiles:
            dfs = _expand_percentiles(dfs)

        return dfs, BaseMetadata(initial_response)
    finally:
        if close_client:
            client.close()


def _check_profiles(
    service: SERVICES,
    profile: PROFILES,
) -> None:
    """Check whether a service profile is valid.

    Parameters
    ----------
    service : string
        One of the service names from the "services" list.
    profile : string
        One of the profile names from "results_profiles",
        "locations_profiles", "activities_profiles",
        "projects_profiles" or "organizations_profiles".
    """
    valid_services = get_args(SERVICES)
    if service not in valid_services:
        raise ValueError(
            f"Invalid service: '{service}'. Valid options are: {valid_services}."
        )

    valid_profiles = PROFILE_LOOKUP[service]
    if profile not in valid_profiles:
        raise ValueError(
            f"Invalid profile: '{profile}' for service '{service}'. "
            f"Valid options are: {valid_profiles}."
        )


_MONITORING_LOCATION_ID_RE = re.compile(r"[^-\s]+-[^-\s]+")


# Iterable-shaped params that ``_get_args`` must NOT push through
# ``_normalize_str_iterable`` (scalar non-string knobs are caught by runtime
# type, so only iterables with special handling need to be named here):
#   - date-range params may contain ``pd.NaT``/None or interval strings
#   - ``bbox``/``boundingBox`` are ``list[float]``, sometimes ``numpy.ndarray``
#   - ``get_peaks``'s int-valued filters (``water_year`` etc.) are ``list[int]``
#   - ``get_combined_metadata``'s ``thresholds`` is ``list[float]``
_NO_NORMALIZE_PARAMS = _DATE_RANGE_PARAMS | {
    "bbox",
    "boundingBox",
    "water_year",
    "year",
    "month",
    "day",
    "peak_since",
    "thresholds",
}


def _normalize_str_iterable(
    value: str | Iterable[str] | None,
    param_name: str = "value",
) -> str | list[str] | None:
    """Validate that ``value`` is None, a string, or an iterable of strings.

    Non-string iterables (``list``, ``tuple``, ``pandas.Series``,
    ``pandas.Index``, ``numpy.ndarray``, generators) are materialized to a
    ``list`` so downstream code that branches on ``isinstance(v, (list,
    tuple))`` keeps working. ``Mapping`` types are rejected because
    iterating a mapping yields keys, not values.

    Parameters
    ----------
    value : None, str, or iterable of str
    param_name : str, optional
        Used in error messages. Defaults to ``"value"``.

    Returns
    -------
    None, str, or list of str

    Raises
    ------
    TypeError
        If the input isn't ``None``, ``str``, or a non-``Mapping``
        iterable; or if any iterable element isn't a string.
    """
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, Mapping) or not isinstance(value, Iterable):
        raise TypeError(
            f"{param_name} must be a string or iterable of strings, "
            f"not {type(value).__name__} (got {value!r})."
        )
    values: list[str] = []
    for v in value:
        if not isinstance(v, str):
            raise TypeError(
                f"{param_name} elements must be strings, "
                f"not {type(v).__name__} (got {v!r})."
            )
        values.append(v)
    return values


def _check_monitoring_location_id(
    monitoring_location_id: str | Iterable[str] | None,
) -> str | list[str] | None:
    """Validate and normalize a ``monitoring_location_id`` value.

    Combines :func:`_normalize_str_iterable` with the AGENCY-ID format
    check that is unique to ``monitoring_location_id`` (the OGC spec
    requires a hyphen separator, e.g. ``USGS-01646500``).

    Parameters
    ----------
    monitoring_location_id : None, str, or iterable of str
        See :func:`_normalize_str_iterable`. Each string is additionally
        required to match the AGENCY-ID hyphen-separated format.

    Returns
    -------
    None, str, or list of str

    Raises
    ------
    TypeError
        If the input isn't ``None``, ``str``, or a non-``Mapping``
        iterable; or if any iterable element isn't a string.
    ValueError
        If any identifier doesn't contain a hyphen separator
        (per the OGC API spec: AGENCY-ID format, e.g. ``USGS-01646500``).
    """
    try:
        value = _normalize_str_iterable(
            monitoring_location_id, "monitoring_location_id"
        )
    except TypeError as exc:
        # Re-raise with the AGENCY-ID hint the generic helper doesn't carry.
        raise TypeError(
            f"{exc} Expected 'AGENCY-ID' format, e.g., 'USGS-01646500'."
        ) from None
    if value is None:
        return None
    for item in (value,) if isinstance(value, str) else value:
        _check_id_format(item)
    return value


def _check_id_format(value: str) -> None:
    """Raise ``ValueError`` if ``value`` is not in ``AGENCY-ID`` format."""
    if not _MONITORING_LOCATION_ID_RE.fullmatch(value):
        raise ValueError(
            f"Invalid monitoring_location_id: {value!r}. "
            f"Expected 'AGENCY-ID' format, e.g., 'USGS-01646500'."
        )


def _get_args(
    local_vars: dict[str, Any], exclude: set[str] | None = None
) -> dict[str, Any]:
    """
    Standardize parameter filtering for WaterData API functions.

    Filters out internal function arguments ('service', 'output_id')
    and None values from the provided local variables dictionary.
    Additional variables can be excluded via the 'exclude' parameter.

    Parameters
    ----------
    local_vars : dict[str, Any]
        Dictionary of local variables, typically from `locals()`.
    exclude : set[str], optional
        Additional keys to exclude from the resulting dictionary.

    Returns
    -------
    dict[str, Any]
        Filtered dictionary of arguments for API requests.
    """
    to_exclude = {"service", "output_id"}
    if exclude:
        to_exclude.update(exclude)

    args: dict[str, Any] = {}
    for k, v in local_vars.items():
        if k in to_exclude or v is None:
            continue
        if k == "monitoring_location_id":
            args[k] = _check_monitoring_location_id(v)
        elif k == "properties":
            # `",".join(properties)` would iterate a bare string as characters.
            args[k] = [v] if isinstance(v, str) else _normalize_str_iterable(v, k)
        elif (
            k in _NO_NORMALIZE_PARAMS
            or isinstance(v, str)
            or not isinstance(v, Iterable)
        ):
            args[k] = v
        else:
            args[k] = _normalize_str_iterable(v, k)
    return args
