import httpx
import os
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import pandas as pd
import json
import geopandas as gpd
from datetime import datetime
from zoneinfo import ZoneInfo
import re

BASE_API = "https://api.waterdata.usgs.gov/ogcapi/"
API_VERSION = "v0"

# --- Caching for repeated calls ---
_cached_base_url = None
def _base_url():
    """
    Returns the base URL for the USGS Water Data OGC API.

    Uses a cached value to avoid repeated string formatting. If the cached value
    is not set, it constructs the base URL using the BASE_API and API_VERSION constants.

    Returns:
        str: The base URL for the API (e.g., "https://api.waterdata.usgs.gov/ogcapi/v0/").
    """
    global _cached_base_url
    if _cached_base_url is None:
        _cached_base_url = f"{BASE_API}{API_VERSION}/"
    return _cached_base_url

def _setup_api(service: str):
    """
    Constructs and returns the API endpoint URL for a specified service.

    Args:
        service (str): The name of the service to be used in the API endpoint.

    Returns:
        str: The full URL for the API endpoint corresponding to the given service.

    Example:
        >>> _setup_api("daily")
        'https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items'
    """
    return f"{_base_url()}collections/{service}/items"

def _switch_arg_id(ls: Dict[str, Any], id_name: str, service: str):
    """
    Switch argument id from its package-specific identifier to the standardized "id" key
    that the API recognizes.

    Sets the "id" key in the provided dictionary `ls`
    with the value from either the service name or the expected id column name.
    If neither key exists, "id" will be set to None.

    Example: for service "time-series-metadata", the function will look for either "time_series_metadata_id"
    or "time_series_id" and change the key to simply "id".

    Args:
        ls (Dict[str, Any]): The dictionary containing identifier keys to be standardized.
        id_name (str): The name of the specific identifier key to look for.
        service (str): The service name.

    Returns:
        Dict[str, Any]: The modified dictionary with the "id" key set appropriately.
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
    Switch properties id from its package-specific identifier to the standardized "id" key
    that the API recognizes.

    Sets the "id" key in the provided dictionary `ls` with the value from either the service name
    or the expected id column name. If neither key exists, "id" will be set to None.
    
    Example: for service "monitoring-locations", it will look for "monitoring_location_id" and change
    it to "id".

    Args:
        properties (List[str]): A list containing the properties or column names to be pulled from the service.
        id_name (str): The name of the specific identifier key to look for.
        service (str): The service name.

    Returns:
        List[str]: The modified list with the "id" key set appropriately.
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

def _format_api_dates(datetime_input: Union[str, List[str]], date: bool = False) -> Union[str, None]:
    """
    Formats date or datetime input(s) for use with an API, handling single values or ranges, and converting to ISO 8601 or date-only formats as needed.
    Parameters
    ----------
    datetime_input : Union[str, List[str]]
        A single date/datetime string or a list of one or two date/datetime strings. Accepts formats like "%Y-%m-%d %H:%M:%S", ISO 8601, or relative periods (e.g., "P7D").
    date : bool, optional
        If True, uses only the date portion ("YYYY-MM-DD"). If False (default), returns full datetime in UTC ISO 8601 format ("YYYY-MM-DDTHH:MM:SSZ").
    Returns
    -------
    Union[str, None]
        - If input is a single value, returns the formatted date/datetime string or None if parsing fails.
        - If input is a list of two values, returns a date/datetime range string separated by "/" (e.g., "YYYY-MM-DD/YYYY-MM-DD" or "YYYY-MM-DDTHH:MM:SSZ/YYYY-MM-DDTHH:MM:SSZ").
        - Returns None if input is empty, all NA, or cannot be parsed.
    Raises
    ------
    ValueError
        If `datetime_input` contains more than two values.
    Notes
    -----
    - Handles blank or NA values by returning None.
    - Supports relative period strings (e.g., "P7D") and passes them through unchanged.
    - Converts datetimes to UTC and formats as ISO 8601 with 'Z' suffix when `date` is False.
    - For date ranges, replaces "nan" with ".." in the output.
    """
    # Get timezone
    local_timezone = datetime.now().astimezone().tzinfo
    
    # Convert single string to list for uniform processing
    if isinstance(datetime_input, str):
        datetime_input = [datetime_input]
    
    # Check for null or all NA and return None
    if all(pd.isna(dt) or dt == "" or dt == None for dt in datetime_input):
        return None

    # Replace all blanks with "nan"
    datetime_input = ["nan" if x == "" else x for x in datetime_input]

    if len(datetime_input) <=2:
        # If the list is of length 1, first look for things like "P7D" or dates
        # already formatted in ISO08601. Otherwise, try to coerce to datetime
        if len(datetime_input) == 1 and re.search(r"P", datetime_input[0], re.IGNORECASE) or "/" in datetime_input[0]:
            return datetime_input[0]
        # Otherwise, use list comprehension to parse dates
        else:
            try:
                # Parse to naive datetime
                parsed_dates = [datetime.strptime(dt, "%Y-%m-%d %H:%M:%S") for dt in datetime_input]
            except Exception:
                # Parse to date only
                try:
                    parsed_dates = [datetime.strptime(dt, "%Y-%m-%d") for dt in datetime_input]
                except Exception:
                    return None
                # If the service only accepts dates for this input, not datetimes (e.g. "daily"),
                # return just the dates separated by a "/", otherwise, return the datetime in UTC
                # format.
            if date:
                return "/".join(dt.strftime("%Y-%m-%d") for dt in parsed_dates)
            else:
                parsed_locals = [dt.replace(tzinfo=local_timezone) for dt in parsed_dates]
                formatted = "/".join(dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ") for dt in parsed_locals)
                return formatted.replace("nan", "..")
    else:
        raise ValueError("datetime_input should only include 1-2 values")

def _cql2_param(args):
    filters = []
    for key, values in args.items():
        filters.append({
            "op": "in",
            "args": [
                {"property": key},
                values
            ]
        })

    query = {
        "op": "and",
        "args": filters
    }

    return json.dumps(query, indent=4)

def _default_headers():
    """
    Generate default HTTP headers for API requests.

    Returns:
        dict: A dictionary containing default headers including 'Accept-Encoding',
        'Accept', 'User-Agent', and 'lang'. If the environment variable 'API_USGS_PAT'
        is set, its value is included as the 'X-Api-Key' header.
    """
    headers = {
        "Accept-Encoding": "compress, gzip",
        "Accept": "application/json",
        "User-Agent": "python-dataretrieval/1.0",
        "lang": "en-US"
    }
    token = os.getenv("API_USGS_PAT")
    if token:
        headers["X-Api-Key"] = token
    return headers

def _check_OGC_requests(endpoint: str = "daily", req_type: str = "queryables"):
    """
    Sends an HTTP GET request to the specified OGC endpoint and request type, returning the JSON response.

    Args:
        endpoint (str): The OGC collection endpoint to query. Defaults to "daily".
        req_type (str): The type of request to make. Must be either "queryables" or "schema". Defaults to "queryables".

    Returns:
        dict: The JSON response from the OGC endpoint.

    Raises:
        AssertionError: If req_type is not "queryables" or "schema".
        httpx.HTTPStatusError: If the HTTP request returns an unsuccessful status code.
    """
    assert req_type in ["queryables", "schema"]
    url = f"{_base_url()}collections/{endpoint}/{req_type}"
    resp = httpx.get(url, headers=_default_headers())
    resp.raise_for_status()
    return resp.json()

def _error_body(resp: httpx.Response):
    """
    Provide more informative error messages based on the response status.

    Args:
        resp (httpx.Response): The HTTP response object to extract the error message from.

    Returns:
        str: The extracted error message. For status code 429, returns the 'message' field from the JSON error object.
             For status code 403, returns a predefined message indicating possible reasons for denial.
             For other status codes, returns the raw response text.
    """
    if resp.status_code == 429:
        return resp.json().get('error', {}).get('message')
    elif resp.status_code == 403:
        return "Query request denied. Possible reasons include query exceeding server limits."
    return resp.text

def _construct_api_requests(
    service: str,
    properties: Optional[List[str]] = None,
    bbox: Optional[List[float]] = None,
    limit: Optional[int] = None,
    max_results: Optional[int] = None,
    skipGeometry: bool = False,
    **kwargs
):
    """
    Constructs an HTTP request object for the specified water data API service.
    Depending on the input parameters (whether there's lists of multiple argument values),
    the function determines whether to use a GET or POST request, formats parameters
    appropriately, and sets required headers.
    
    Args:
        service (str): The name of the API service to query (e.g., "daily").
        properties (Optional[List[str]], optional): List of property names to include in the request.
        bbox (Optional[List[float]], optional): Bounding box coordinates as a list of floats.
        limit (Optional[int], optional): Maximum number of results to return per request.
        max_results (Optional[int], optional): Maximum number of results allowed by the API.
        skipGeometry (bool, optional): Whether to exclude geometry from the response.
        **kwargs: Additional query parameters, including date/time filters and other API-specific options.
    Returns:
        httpx.Request: The constructed HTTP request object ready to be sent.
    Raises:
        ValueError: If `limit` is greater than `max_results`.
    Notes:
        - Date/time parameters are automatically formatted to ISO8601.
        - If multiple values are provided for non-single parameters, a POST request is constructed.
        - The function sets appropriate headers for GET and POST requests.
    """
    baseURL = _setup_api(service)
    # Single parameters can only have one value
    single_params = {"datetime", "last_modified", "begin", "end", "time"}
    # params = {k: v for k, v in kwargs.items() if k in single_params}
    # # Set skipGeometry parameter
    # params["skipGeometry"] = skipGeometry
    # # If limit is none and max_results is not none, then set limit to max results. Otherwise,
    # # if max_results is none, set it to 10000 (the API max).
    # params["limit"] = max_results if limit is None and max_results is not None else limit or 10000
    # if max_results is not None and limit is not None and limit > max_results:
    #     raise ValueError("limit cannot be greater than max_result")
    
    # Identify which parameters should be included in the POST content body
    post_params = {
         k: v for k, v in kwargs.items()
         if k not in single_params and isinstance(v, (list, tuple)) and len(v) > 1
         }
    
    # Everything else goes into the params dictionary for the URL
    params = {k: v for k, v in kwargs.items() if k not in post_params}
    # Set skipGeometry parameter
    params["skipGeometry"] = skipGeometry
    # If limit is none and max_results is not none, then set limit to max results. Otherwise,
    # if max_results is none, set it to 10000 (the API max).
    params["limit"] = max_results if limit is None and max_results is not None else limit or 10000
    if max_results is not None and limit is not None and limit > max_results:
        raise ValueError("limit cannot be greater than max_result")

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
        req = httpx.Request(method="POST", url=baseURL, headers=headers, content=_cql2_param(post_params), params=params)
    else:
        req = httpx.Request(method="GET", url=baseURL, headers=headers, params=params)
    return req

def _next_req_url(resp: httpx.Response) -> Optional[str]:
    """
    Extracts the URL for the next page of results from an HTTP response from a water data endpoint.

    Parameters:
        resp (httpx.Response): The HTTP response object containing JSON data and headers.

    Returns:
        Optional[str]: The URL for the next page of results if available, otherwise None.

    Side Effects:
        If the environment variable "API_USGS_PAT" is set, prints the remaining requests for the current hour.
        Prints the next URL if found.

    Notes:
        - Expects the response JSON to contain a "links" list with objects having "rel" and "href" keys.
        - Checks for the "next" relation in the "links" to determine the next URL.
    """
    body = resp.json()
    if not body.get("numberReturned"):
        return None
    header_info = resp.headers
    if os.getenv("API_USGS_PAT", ""):
        print("Remaining requests this hour:", header_info.get("x-ratelimit-remaining", ""))
    for link in body.get("links", []):
        if link.get("rel") == "next":
            next_url = link.get("href")
            print(f"Next URL: {next_url}")
            return next_url
    return None

def _get_resp_data(resp: httpx.Response) -> pd.DataFrame:
    """
    Extracts and normalizes data from an httpx.Response object containing GeoJSON features.

    Parameters:
        resp (httpx.Response): The HTTP response object expected to contain a JSON body with a "features" key.

    Returns:
        gpd.GeoDataFrame or pd.DataFrame: A geopandas GeoDataFrame if geometry is included, or a 
        pandas DataFrame containing the feature properties and each row's service-specific id. 
        Returns an empty pandas DataFrame if no features are returned.
    """
    body = resp.json()
    if not body.get("numberReturned"):
        return pd.DataFrame()
    #df = pd.json_normalize(
    #    resp.json()["features"],
    #    sep="_")
    #df = df.drop(columns=["type", "geometry", "AsGeoJSON(geometry)"], errors="ignore")
    #df.columns = [col.replace("properties_", "") for col in df.columns]
    
    df = gpd.GeoDataFrame.from_features(body["features"])
    df["id"] = pd.json_normalize(body["features"])["id"].values

    if df["geometry"].isnull().all():
        df = pd.DataFrame(df.drop(columns="geometry"))

    return df

def _walk_pages(req: httpx.Request, max_results: Optional[int], client: Optional[httpx.Client] = None) -> pd.DataFrame:
    """
    Iterates through paginated API responses and aggregates the results into a single DataFrame.

    Parameters
    ----------
    req : httpx.Request
        The initial HTTP request to send.
    max_results : Optional[int]
        The maximum number of results to retrieve. If None or NaN, retrieves all available pages.
    client : Optional[httpx.Client], default None
        An optional HTTP client to use for requests. If not provided, a new client is created.

    Returns
    -------
    pd.DataFrame
        A DataFrame containing the aggregated results from all pages.

    Raises
    ------
    Exception
        If a request fails or returns a non-200 status code.

    Notes
    -----
    - If `max_results` is None or NaN, the function will continue to request subsequent pages until no more pages are available.
    - Failed requests are tracked and reported, but do not halt the entire process unless the initial request fails.
    """
    print(f"Requesting:\n{req.url}")

    # Get first response from client
    # using GET or POST call
    client = client or httpx.Client()
    resp = client.send(req)
    if resp.status_code != 200: raise Exception(_error_body(resp))

    # Grab some aspects of the original request: headers and the
    # request type (GET or POST)
    method = req.method.upper()
    headers = req.headers
    content = req.content if method == "POST" else None

    if max_results is None or pd.isna(max_results):
        dfs = _get_resp_data(resp)
        curr_url = _next_req_url(resp)
        failures = []
        while curr_url:
            try:
                resp = client.request(method, curr_url, headers=headers, content=content if method == "POST" else None)
                if resp.status_code != 200: raise Exception(_error_body(resp))
                df1 = _get_resp_data(resp)
                dfs = pd.concat([dfs, df1], ignore_index=True)
                curr_url = _next_req_url(resp)
            except Exception:
                failures.append(curr_url)
                curr_url = None
        if failures:
            print(f"There were {len(failures)} failed requests.")
        return dfs
    else:
        resp.raise_for_status()
        return _get_resp_data(resp)

def _deal_with_empty(return_list: pd.DataFrame, properties: Optional[List[str]], service: str) -> pd.DataFrame:
    """
    Handles empty DataFrame results by returning a DataFrame with appropriate columns.

    If `return_list` is empty, determines the column names to use:
    - If `properties` is not provided or contains only NaN values, retrieves the schema properties from the specified service.
    - Otherwise, uses the provided `properties` list as column names.

    Args:
        return_list (pd.DataFrame): The DataFrame to check for emptiness.
        properties (Optional[List[str]]): List of property names to use as columns, or None.
        service (str): The service endpoint to query for schema properties if needed.

    Returns:
        pd.DataFrame: The original DataFrame if not empty, otherwise an empty DataFrame with the appropriate columns.
    """
    if return_list.empty:
        if not properties or all(pd.isna(properties)):
            schema = _check_OGC_requests(endpoint=service, req_type="schema")
            properties = list(schema.get("properties", {}).keys())
        return pd.DataFrame(columns=properties)
    return return_list

def _rejigger_cols(df: pd.DataFrame, properties: Optional[List[str]], output_id: str) -> pd.DataFrame:
    """
    Rearranges and renames columns in a DataFrame based on provided properties and output identifier.

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
    pd.DataFrame
        The DataFrame with columns rearranged and/or renamed according to the specified properties and output_id.
    """
    if properties and not all(pd.isna(properties)):
        if "id" not in properties:
            if output_id in properties:
                df = df.rename(columns={"id": output_id})
            else:
                plural = output_id.replace("_id", "s_id")
                if plural in properties:
                    df = df.rename(columns={"id": plural})
        return df.loc[:, [col for col in properties if col in df.columns]]
    else:
        return df.rename(columns={"id": output_id})

def _cleanup_cols(df: pd.DataFrame, service: str = "daily") -> pd.DataFrame:
    """
    Cleans and standardizes columns in a pandas DataFrame for water data endpoints.

    Parameters
    ----------
    df : pd.DataFrame
        The input DataFrame containing water data.
    service : str, optional
        The type of water data service (default is "daily").

    Returns
    -------
    pd.DataFrame
        The cleaned DataFrame with standardized columns.

    Notes
    -----
    - If the 'time' column exists and service is "daily", it is converted to date objects.
    - The 'value' and 'contributing_drainage_area' columns are coerced to numeric types.
    """
    if "time" in df.columns and service == "daily":
        df["time"] = pd.to_datetime(df["time"]).dt.date
    for col in ["value", "contributing_drainage_area"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def get_ogc_data(args: Dict[str, Any], output_id: str, service: str) -> pd.DataFrame:
    """
    Retrieves OGC (Open Geospatial Consortium) data from a specified water data endpoint and returns it as a pandas DataFrame.

    This function prepares request arguments, constructs API requests, handles pagination, processes the results,
    and formats the output DataFrame according to the specified parameters.

    Args:
        args (Dict[str, Any]): Dictionary of request arguments for the OGC service.
        output_id (str): The name of the output identifier to use in the request.
        service (str): The OGC service type (e.g., "wfs", "wms").

    Returns:
        pd.DataFrame: A DataFrame containing the retrieved and processed OGC data, with metadata attributes
        including the request URL and query timestamp.

    Notes:
        - The function does not mutate the input `args` dictionary.
        - Handles optional arguments such as `max_results` and `convertType`.
        - Applies column cleanup and reordering based on service and properties.
        - Metadata is attached to the DataFrame via the `.attrs` attribute.
    """
    args = args.copy()
    # Add service as an argument
    args["service"] = service
    # Pull out a max results input if exists
    max_results = args.pop("max_results", None)
    # Switch the input id to "id" if needed
    args = _switch_arg_id(args, id_name=output_id, service=service)
    properties = args.get("properties")
    # Switch properties id to "id" if needed
    args["properties"] = _switch_properties_id(properties, id_name=output_id, service=service)
    convertType = args.pop("convertType", False)
    # Create fresh dictionary of args without any None values
    args = {k: v for k, v in args.items() if v is not None}
    # Build API request
    req = _construct_api_requests(**args)
    # Run API request and iterate through pages if needed
    return_list = _walk_pages(req, max_results)
    # Manage some aspects of the returned dataset
    return_list = _deal_with_empty(return_list, properties, service)
    if convertType:
        return_list = _cleanup_cols(return_list, service=service)
    return_list = _rejigger_cols(return_list, properties, output_id)
    # Add metadata
    return_list.attrs.update(request=req.url, queryTime=pd.Timestamp.now())
    return return_list


# def _get_description(service: str):
#     tags = _get_collection().get("tags", [])
#     for tag in tags:
#         if tag.get("name") == service:
#             return tag.get("description")
#     return None

# def _get_params(service: str):
#     url = f"{_base_url()}collections/{service}/schema"
#     resp = httpx.get(url, headers=_default_headers())
#     resp.raise_for_status()
#     properties = resp.json().get("properties", {})
#     return {k: v.get("description") for k, v in properties.items()}

# def _get_collection():
#     url = f"{_base_url()}openapi?f=json"
#     resp = httpx.get(url, headers=_default_headers())
#     resp.raise_for_status()
#     return resp.json()

# def _explode_post(ls: Dict[str, Any]):
#     return {k: _cql2_param({k: v if isinstance(v, list) else [v]}) for k, v in ls.items() if v is not None}