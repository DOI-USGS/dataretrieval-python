import json
import logging
import warnings
import os
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union, get_args

import pandas as pd
import requests
from zoneinfo import ZoneInfo

from dataretrieval.utils import BaseMetadata
from dataretrieval import __version__

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


def _switch_arg_id(ls: Dict[str, Any], id_name: str, service: str):
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


def _switch_properties_id(properties: Optional[List[str]], id_name: str, service: str):
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


def _format_api_dates(
    datetime_input: Union[str, List[str]], date: bool = False
) -> Union[str, None]:
    """
    Formats date or datetime input(s) for use with an API.

    Handles single values or ranges, and converting to ISO 8601 or date-only
    formats as needed.

    Parameters
    ----------
    datetime_input : Union[str, List[str]]
        A single date/datetime string or a list of one or two date/datetime
        strings. Accepts formats like "%Y-%m-%d %H:%M:%S", ISO 8601, or relative
        periods (e.g., "P7D").
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
    - Handles blank or NA values by returning None.
    - Supports relative period strings (e.g., "P7D") and passes them through
    unchanged.
    - Converts datetimes to UTC and formats as ISO 8601 with 'Z' suffix when
    `date` is False.
    - For date ranges, replaces "nan" with ".." in the output.
    """
    # Get timezone
    local_timezone = datetime.now().astimezone().tzinfo

    # Convert single string to list for uniform processing
    if isinstance(datetime_input, str):
        datetime_input = [datetime_input]

    # Check for null or all NA and return None
    if all(pd.isna(dt) or dt == "" or dt is None for dt in datetime_input):
        return None

    if len(datetime_input) <= 2:
        # If the list is of length 1, first look for things like "P7D" or dates
        # already formatted in ISO08601. Otherwise, try to coerce to datetime
        if (
            len(datetime_input) == 1
            and re.search(r"P", datetime_input[0], re.IGNORECASE)
            or "/" in datetime_input[0]
        ):
            return datetime_input[0]
        # Otherwise, use list comprehension to parse dates
        else:
            try:
                # Parse to naive datetime
                parsed_dates = [
                    datetime.strptime(dt, "%Y-%m-%d %H:%M:%S") for dt in datetime_input
                ]
            except Exception:
                # Parse to date only
                try:
                    parsed_dates = [
                        datetime.strptime(dt, "%Y-%m-%d") for dt in datetime_input
                    ]
                except Exception:
                    return None
                # If the service only accepts dates for this input, not
                # datetimes (e.g. "daily"), return just the dates separated by a
                # "/", otherwise, return the datetime in UTC format.
            if date:
                return "/".join(dt.strftime("%Y-%m-%d") for dt in parsed_dates)
            else:
                parsed_locals = [
                    dt.replace(tzinfo=local_timezone) for dt in parsed_dates
                ]
                formatted = "/".join(
                    dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
                    for dt in parsed_locals
                )
                return formatted
    else:
        raise ValueError("datetime_input should only include 1-2 values")


def _cql2_param(args: Dict[str, Any]) -> str:
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
    AssertionError
        If req_type is not "queryables" or "schema".
    requests.HTTPError
        If the HTTP request returns an unsuccessful status code.
    """
    assert req_type in ["queryables", "schema"]
    url = f"{OGC_API_URL}/collections/{endpoint}/{req_type}"
    resp = requests.get(url, headers=_default_headers())
    resp.raise_for_status()
    return resp.json()


def _error_body(resp: requests.Response):
    """
    Provide more informative error messages based on the response status.

    Parameters
    ----------
    resp : requests.Response
        The HTTP response object to extract the error message from.

    Returns
    -------
    str
        The extracted error message. For status code 429, returns the 'message'
        field from the JSON error object. For status code 403, returns a
        predefined message indicating possible reasons for denial. For other
        status codes, returns the raw response text.
    """
    status = resp.status_code
    if status == 429:
        return "429: Too many requests made. Please obtain an API token or try again later."
    elif status == 403:
        return "403: Query request denied. Possible reasons include query exceeding server limits."
    j_txt = resp.json()
    return (
        f"{status}: {j_txt.get('code', 'Unknown type')}. " 
        f"{j_txt.get('description', 'No description provided')}."
    )


def _construct_api_requests(
    service: str,
    properties: Optional[List[str]] = None,
    bbox: Optional[List[float]] = None,
    limit: Optional[int] = None,
    skip_geometry: bool = False,
    **kwargs,
):
    """
    Constructs an HTTP request object for the specified water data API service.

    Depending on the input parameters (whether there's lists of multiple
    argument values), the function determines whether to use a GET or POST
    request, formats parameters appropriately, and sets required headers.

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
    - If multiple values are provided for non-single parameters, a POST request
    is constructed.
    - The function sets appropriate headers for GET and POST requests.
    """
    service_url = f"{OGC_API_URL}/collections/{service}/items"
    
    # Single parameters can only have one value
    single_params = {"datetime", "last_modified", "begin", "end", "time"}

    # Identify which parameters should be included in the POST content body
    post_params = {
        k: v
        for k, v in kwargs.items()
        if k not in single_params and isinstance(v, (list, tuple)) and len(v) > 1
    }

    # Everything else goes into the params dictionary for the URL
    params = {k: v for k, v in kwargs.items() if k not in post_params}
    # Set skipGeometry parameter (API expects camelCase)
    params["skipGeometry"] = skip_geometry
    
    # If limit is none or greater than 50000, then set limit to max results. Otherwise,
    # use the limit
    params["limit"] = (
        50000 if limit is None or limit > 50000 else limit
        )

    # Indicate if function needs to perform POST conversion
    POST = bool(post_params)

    # Convert dates to ISO08601 format
    time_periods = {"last_modified", "datetime", "time", "begin", "end"}
    for i in time_periods:
        if i in params:
            dates = service == "daily" and i != "last_modified"
            params[i] = _format_api_dates(params[i], date=dates)

    # String together bbox elements from a list to a comma-separated string,
    # and string together properties if provided
    if bbox:
        params["bbox"] = ",".join(map(str, bbox))
    if properties:
        params["properties"] = ",".join(properties)

    headers = _default_headers()

    if POST:
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


def _next_req_url(resp: requests.Response) -> Optional[str]:
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
        The HTTP response object expected to contain a JSON body with a "features" key.
    geopd : bool
        Indicates whether geopandas is installed and should be used to handle geometries.

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
    client: Optional[requests.Session] = None,
) -> Tuple[pd.DataFrame, requests.Response]:
    """
    Iterates through paginated API responses and aggregates the results into a single DataFrame.

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
        If a request fails or returns a non-200 status code.
    """
    logger.info("Requesting: %s", req.url)

    if not geopd:
        logger.warning(
            "Geopandas not installed. Geometries will be flattened into pandas DataFrames."
        )

    # Get first response from client
    # using GET or POST call
    close_client = client is None
    client = client or requests.Session()
    try:
        resp = client.send(req)
        if resp.status_code != 200:
            raise Exception(_error_body(resp))

        # Store the initial response for metadata
        initial_response = resp

        # Grab some aspects of the original request: headers and the
        # request type (GET or POST)
        method = req.method.upper()
        headers = dict(req.headers)
        content = req.body if method == "POST" else None

        dfs = _get_resp_data(resp, geopd=geopd)
        curr_url = _next_req_url(resp)
        while curr_url:
            try:
                resp = client.request(
                    method,
                    curr_url,
                    headers=headers,
                    data=content if method == "POST" else None,
                    )
                if resp.status_code != 200:
                    error_text = _error_body(resp)
                    raise Exception(error_text)
                df1 = _get_resp_data(resp, geopd=geopd)
                dfs = pd.concat([dfs, df1], ignore_index=True)
                curr_url = _next_req_url(resp)
            except Exception:
                warnings.warn(f"{error_text}. Data request incomplete.")
                logger.error("Request incomplete. %s", error_text)
                logger.warning("Request failed for URL: %s. Data download interrupted.", curr_url)
                curr_url = None
        return dfs, initial_response
    finally:
        if close_client:
            client.close()


def _deal_with_empty(
    return_list: pd.DataFrame, properties: Optional[List[str]], service: str
) -> pd.DataFrame:
    """
    Handles empty DataFrame results by returning a DataFrame with appropriate columns.

    If `return_list` is empty, determines the column names to use:
    - If `properties` is not provided or contains only NaN values, retrieves the schema properties from the specified service.
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
        The original DataFrame if not empty, otherwise an empty DataFrame with the appropriate columns.
    """
    if return_list.empty:
        if not properties or all(pd.isna(properties)):
            schema = _check_ogc_requests(endpoint=service, req_type="schema")
            properties = list(schema.get("properties", {}).keys())
        return pd.DataFrame(columns=properties)
    return return_list


def _arrange_cols(
    df: pd.DataFrame, properties: Optional[List[str]], output_id: str
) -> pd.DataFrame:
    """
    Rearranges and renames columns in a DataFrame based on provided properties and service's output id.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame whose columns are to be rearranged or renamed.
    properties : Optional[List[str]]
        A list of column names to possibly rename. If None or contains only NaN, the function will rename 'id' to output_id.
    output_id : str
        The name to which the 'id' column should be renamed if applicable.

    Returns
    -------
    pd.DataFrame or gpd.GeoDataFrame
        The DataFrame with columns rearranged and/or renamed according to the specified properties and output_id.
    """

    # Rename id column to output_id
    df = df.rename(columns={"id": output_id})

    # If properties are provided, filter to only those columns
    # plus geometry if skip_geometry is False
    if properties and not all(pd.isna(properties)):
        # Make sure geometry stays in the dataframe if skip_geometry is False
        if 'geometry' in df.columns and 'geometry' not in properties:
            properties.append('geometry')
        # id is technically a valid column from the service, but these
        # functions make the name more specific. So, if someone requests
        # 'id', give them the output_id column
        if 'id' in properties:
            properties[properties.index('id')] = output_id
        df = df.loc[:, [col for col in properties if col in df.columns]]

    # Move meaningless-to-user, extra id columns to the end
    # of the dataframe, if they exist
    extra_id_col = set(df.columns).intersection({
        "latest_continuous_id",
        "latest_daily_id",
        "daily_id",
        "continuous_id",
        "field_measurement_id"
        })

    # If the arbitrary id column is returned (either due to properties
    # being none or NaN), then move it to the end of the dataframe, but
    # if part of properties, keep in requested order
    if extra_id_col and (properties is None or all(pd.isna(properties))):
        id_col_order = [col for col in df.columns if col not in extra_id_col] + list(extra_id_col)
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
        "datetime", # unused
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
        df = df.sort_values(
            by=["time", "monitoring_location_id"],
            ignore_index=True
            )
    elif "time" in df.columns:
        df = df.sort_values(
            by="time",
            ignore_index=True
            )
    
    return df


def get_ogc_data(
    args: Dict[str, Any], output_id: str, service: str
) -> Tuple[pd.DataFrame, BaseMetadata]:
    """
    Retrieves OGC (Open Geospatial Consortium) data from a specified water data endpoint and returns it as a pandas DataFrame with metadata.

    This function prepares request arguments, constructs API requests, handles pagination, processes the results,
    and formats the output DataFrame according to the specified parameters.

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
    # Add service as an argument
    args["service"] = service
    # Switch the input id to "id" if needed
    args = _switch_arg_id(args, id_name=output_id, service=service)
    properties = args.get("properties")
    # Switch properties id to "id" if needed
    args["properties"] = _switch_properties_id(
        properties, id_name=output_id, service=service
    )
    convert_type = args.pop("convert_type", False)
    # Create fresh dictionary of args without any None values
    args = {k: v for k, v in args.items() if v is not None}
    # Build API request
    req = _construct_api_requests(**args)
    # Run API request and iterate through pages if needed
    return_list, response = _walk_pages(
        geopd=GEOPANDAS, req=req
    )
    # Manage some aspects of the returned dataset
    return_list = _deal_with_empty(return_list, properties, service)

    if convert_type:
        return_list = _type_cols(return_list)

    return_list = _arrange_cols(return_list, properties, output_id)

    return_list = _sort_rows(return_list)
    # Create metadata object from response
    metadata = BaseMetadata(response)
    return return_list, metadata


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

