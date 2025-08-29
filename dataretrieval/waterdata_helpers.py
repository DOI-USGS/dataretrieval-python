import httpx
import os
import warnings
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import pytz
import pandas as pd
import numpy as np
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
        If True, returns only the date portion ("YYYY-MM-DD"). If False (default), returns full datetime in UTC ISO 8601 format ("YYYY-MM-DDTHH:MM:SSZ").
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

    # If the list is of length 1, first look for things like "P7D" or dates
    # already formatted in ISO08601. Otherwise, try to coerce to datetime
    if len(datetime_input) == 1:
        dt = datetime_input[0]
        if re.search(r"P", dt, re.IGNORECASE) or "/" in dt:
            return dt
        else:
            try:
                # Parse to naive datetime
                parsed_dt = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
                # If the service only accepts dates for this input, not datetimes (e.g. "daily"),
                # return just the date, otherwise, return the datetime in UTC format.
                if date:
                    return parsed_dt.strftime("%Y-%m-%d")
                else:
                    dt_local = parsed_dt.replace(tzinfo=local_timezone)
                    # Convert to UTC and format as ISO 8601 with 'Z'
                    return dt_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                return None
    # If the list is of length 2, parse the dates and if necessary, combine them together into
    # the date range format accepted by the API
    elif len(datetime_input) == 2:
        try:
            parsed_dates = [datetime.strptime(dt, "%Y-%m-%d %H:%M:%S") for dt in datetime_input]
            if date:
                formatted = "/".join(dt.strftime("%Y-%m-%d") for dt in parsed_dates)
            else:
                formatted = "/".join(dt.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%SZ") for dt in parsed_dates)
            return formatted.replace("nan", "..")
        except Exception:
            return None
    else:
        raise ValueError("datetime_input should only include 1-2 values")

def _explode_post(ls: Dict[str, Any]):
    return {k: _cql2_param({k: v if isinstance(v, list) else [v]}) for k, v in ls.items() if v is not None}

def _cql2_param(parameter: Dict[str, List[str]]):
    property_name = next(iter(parameter))
    parameters = [str(x) for x in parameter[property_name]]
    return {"property": property_name, "parameter": parameters}

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
    baseURL = _setup_api(service)
    single_params = {"datetime", "last_modified", "begin", "end", "time"}
    params = {k: v for k, v in kwargs.items() if k in single_params}
    params["skipGeometry"] = skipGeometry
    # Limit logic
    params["limit"] = max_results if limit is None and max_results is not None else limit or 10000
    if max_results is not None and limit is not None and limit > max_results:
        raise ValueError("limit cannot be greater than max_result")
    
    # Create post calls for any input parameters that are not in the single_params list
    # and have more than one element associated with the list or tuple.
    post_params = _explode_post({
        k: v for k, v in kwargs.items()
        if k not in single_params and isinstance(v, (list, tuple)) and len(v) > 1
        })

    # Indicate if function needs to perform POST conversion
    POST = bool(post_params)

    # Convert dates to ISO08601 format
    time_periods = {"last_modified", "datetime", "time", "begin", "end"}
    for i in time_periods:
        if i in params:
            dates = service == "daily" and i != "last_modified"
            params[i] = _format_api_dates(params[i], date=dates)
            kwargs[i] = _format_api_dates(kwargs[i], date=dates)

    # String together bbox elements from a list to a comma-separated string,
    # and string together properties if provided
    if bbox:
        params["bbox"] = ",".join(map(str, bbox))
    if properties:
        params["properties"] = ",".join(properties)

    headers = _default_headers()

    if POST:
        headers["Content-Type"] = "application/query-cql-json"
        resp = httpx.post(baseURL, headers=headers, json={"params": list(post_params.values())}, params=params)
    else:
        resp = httpx.get(baseURL, headers=headers, params={**params, **{k: v for k, v in kwargs.items() if k not in single_params}})
        print(resp.url)
    if resp.status_code != 200:
        raise Exception(_error_body(resp))
    return resp.json()

def _deal_with_empty(return_list: pd.DataFrame, properties: Optional[List[str]], service: str) -> pd.DataFrame:
    if return_list.empty:
        if not properties or all(pd.isna(properties)):
            schema = _check_OGC_requests(endpoint=service, req_type="schema")
            properties = list(schema.get("properties", {}).keys())
        return pd.DataFrame(columns=properties)
    return return_list

def _rejigger_cols(df: pd.DataFrame, properties: Optional[List[str]], output_id: str) -> pd.DataFrame:
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
    if "qualifier" in df.columns:
        df["qualifier"] = df["qualifier"].apply(lambda x: ", ".join(x) if isinstance(x, list) else x)
    if "time" in df.columns and service == "daily":
        df["time"] = pd.to_datetime(df["time"]).dt.date
    for col in ["value", "contributing_drainage_area"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df

def _next_req_url(resp: httpx.Response, req_url: str) -> Optional[str]:
    body = resp.json()
    if not body.get("numberReturned"):
        return None
    header_info = resp.headers
    if os.getenv("API_USGS_PAT", ""):
        print("Remaining requests this hour:", header_info.get("x-ratelimit-remaining", ""))
    for link in body.get("links", []):
        if link.get("rel") == "next":
            return link.get("href")
    return None

def _get_resp_data(resp: httpx.Response) -> pd.DataFrame:
    body = resp.json()
    if not body.get("numberReturned"):
        return pd.DataFrame()
    df = pd.DataFrame(body.get("features", []))
    for col in ["geometry", "AsGeoJSON(geometry)"]:
        if col in df.columns:
            df = df.drop(columns=[col])
    return df

def _walk_pages(req_url: str, max_results: Optional[int], client: Optional[httpx.Client] = None) -> pd.DataFrame:
    print(f"Requesting:\n{req_url}")
    client = client or httpx.Client()
    if max_results is None or pd.isna(max_results):
        dfs = []
        curr_url = req_url
        failures = []
        while curr_url:
            try:
                resp = client.get(curr_url)
                resp.raise_for_status()
                df1 = _get_resp_data(resp)
                dfs.append(df1)
                curr_url = _next_req_url(resp, curr_url)
            except Exception:
                failures.append(curr_url)
                curr_url = None
        if failures:
            print(f"There were {len(failures)} failed requests.")
        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()
    else:
        resp = client.get(req_url)
        resp.raise_for_status()
        return _get_resp_data(resp)

def get_ogc_data(args: Dict[str, Any], output_id: str, service: str) -> pd.DataFrame:
    args = args.copy()  # Don't mutate input
    args["service"] = service
    max_results = args.pop("max_results", None)
    args = _switch_arg_id(args, id_name=output_id, service=service)
    properties = args.get("properties")
    args["properties"] = _switch_properties_id(properties, id_name=output_id, service=service)
    convertType = args.pop("convertType", False)
    args = {k: v for k, v in args.items() if v is not None}
    req_url = _construct_api_requests(**args)
    return_list = _walk_pages(req_url, max_results)
    return_list = _deal_with_empty(return_list, properties, service)
    if convertType:
        return_list = _cleanup_cols(return_list, service=service)
    return_list = _rejigger_cols(return_list, properties, output_id)
    # Metadata
    return_list.attrs.update(request=req_url, queryTime=pd.Timestamp.now())
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