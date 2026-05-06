"""Functions for downloading data from the `National Water Information System (NWIS)`_.

.. _National Water Information System (NWIS): https://waterdata.usgs.gov/nwis

"""

from __future__ import annotations

import warnings
from json import JSONDecodeError

import pandas as pd
import requests

from dataretrieval.rdb import read_rdb as _read_rdb_text
from dataretrieval.utils import BaseMetadata

from .utils import query

try:
    import geopandas as gpd
except ImportError:
    gpd = None

# Issue deprecation warning upon import
warnings.warn(
    "The 'nwis' services are deprecated and being decommissioned. "
    "Please use the 'waterdata' module to access the new services.",
    DeprecationWarning,
    stacklevel=2,
)

WATERDATA_BASE_URL = "https://nwis.waterdata.usgs.gov/"
WATERDATA_URL = WATERDATA_BASE_URL + "nwis/"
WATERSERVICE_URL = "https://waterservices.usgs.gov/nwis/"
PARAMCODES_URL = "https://help.waterdata.usgs.gov/code/parameter_cd_nm_query?"
ALLPARAMCODES_URL = "https://help.waterdata.usgs.gov/code/parameter_cd_query?"

WATERSERVICES_SERVICES = ["dv", "iv", "site", "stat"]
WATERDATA_SERVICES = [
    "peaks",
    "ratings",
]
# NAD83
_CRS = "EPSG:4269"


def _parse_json_or_raise(response: requests.Response) -> pd.DataFrame:
    """Parse a JSON NWIS response, raising a helpful error on HTML responses."""
    try:
        return _read_json(response.json())
    except (ValueError, JSONDecodeError) as e:
        text_lower = response.text.lower()
        content_type = response.headers.get("Content-Type", "").lower()
        if (
            "<html>" in text_lower
            or "<!doctype" in text_lower
            or "text/html" in content_type
        ):
            raise ValueError(
                f"Received HTML response instead of JSON from {response.url} "
                f"(Status: {response.status_code}). This often indicates "
                "that the service is currently unavailable."
            ) from e
        raise


def format_response(
    df: pd.DataFrame, service: str | None = None, **kwargs
) -> pd.DataFrame:
    """Setup index for response from query.

    This function formats the response from the NWIS web services, in
    particular it sets the index of the data frame. This function tries to
    convert the NWIS response into pandas datetime values localized to UTC,
    and if possible, uses these timestamps to define the data frame index.

    Parameters
    ----------
    df: ``pandas.DataFrame``
        The data frame to format
    service: string, optional, default is None
        The NWIS service that was queried, important because the 'peaks'
        service returns a different format than the other services.
    **kwargs: optional
        Additional keyword arguments, e.g., 'multi_index'

    Returns
    -------
    df: ``pandas.DataFrame``
        The formatted data frame

    """
    mi = kwargs.pop("multi_index", True)

    if service == "peaks":
        df = preformat_peaks_response(df)

    if gpd is not None and "dec_lat_va" in df.columns:
        geoms = gpd.points_from_xy(df.dec_long_va.values, df.dec_lat_va.values)
        df = gpd.GeoDataFrame(df, geometry=geoms, crs=_CRS)

    # check for multiple sites:
    if "datetime" not in df.columns:
        # XXX: consider making site_no index
        return df

    elif len(df["site_no"].unique()) > 1 and mi:
        # setup multi-index
        df.set_index(["site_no", "datetime"], inplace=True)
        if hasattr(df.index.levels[1], "tzinfo") and df.index.levels[1].tzinfo is None:
            df = df.tz_localize("UTC", level=1)

    else:
        df.set_index(["datetime"], inplace=True)
        if hasattr(df.index, "tzinfo") and df.index.tzinfo is None:
            df = df.tz_localize("UTC")

    return df.sort_index()


def preformat_peaks_response(df: pd.DataFrame) -> pd.DataFrame:
    """Datetime formatting for the 'peaks' service response.

    Function to format the datetime column of the 'peaks' service response.

    Parameters
    ----------
    df: ``pandas.DataFrame``
        The data frame to format

    Returns
    -------
    df: ``pandas.DataFrame``
        The formatted data frame

    """
    df["datetime"] = pd.to_datetime(df.pop("peak_dt"), errors="coerce")
    df.dropna(subset=["datetime"], inplace=True)
    return df


def get_qwdata(**kwargs):
    """Defunct: use ``waterdata.get_samples()``."""
    raise NameError(
        "`nwis.get_qwdata` has been replaced with `waterdata.get_samples()`."
    )


def get_discharge_measurements(**kwargs):
    """Defunct: use ``waterdata.get_field_measurements()``."""
    raise NameError(
        "`nwis.get_discharge_measurements` has been replaced "
        "with `waterdata.get_field_measurements`."
    )


def get_discharge_peaks(
    sites: list[str] | str | None = None,
    start: str | None = None,
    end: str | None = None,
    multi_index: bool = True,
    ssl_check: bool = True,
    **kwargs,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Get discharge peaks from the waterdata service.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        If the waterdata parameter site_no is supplied, it will overwrite the
        sites parameter
    start: string, optional, default is None
        If the waterdata parameter begin_date is supplied, it will overwrite
        the start parameter (YYYY-MM-DD)
    end: string, optional, default is None
        If the waterdata parameter end_date is supplied, it will overwrite
        the end parameter (YYYY-MM-DD)
    multi_index: bool, optional
        If False, a dataframe with a single-level index (datetime) is returned,
        default is True
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
    df: ``pandas.DataFrame``
        Times series data from the NWIS JSON
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get discharge peaks for site 01491000
        >>> df, md = dataretrieval.nwis.get_discharge_peaks(
        ...     sites="01491000", start="1980-01-01", end="1990-01-01"
        ... )

        >>> # Get discharge peaks for sites in Hawaii
        >>> df, md = dataretrieval.nwis.get_discharge_peaks(
        ...     start="1980-01-01", end="1980-01-02", stateCd="HI"
        ... )

    """
    _check_sites_value_types(sites)

    kwargs["site_no"] = kwargs.pop("site_no", sites)
    kwargs["begin_date"] = kwargs.pop("begin_date", start)
    kwargs["end_date"] = kwargs.pop("end_date", end)
    kwargs["multi_index"] = multi_index

    response = query_waterdata("peaks", format="rdb", ssl_check=ssl_check, **kwargs)

    df = _read_rdb(response.text)

    return format_response(df, service="peaks", **kwargs), NWIS_Metadata(
        response, **kwargs
    )


def get_gwlevels(**kwargs):
    """Defunct: use ``waterdata.get_field_measurements()``."""
    raise NameError(
        "`nwis.get_gwlevels` has been replaced "
        "with `waterdata.get_field_measurements()`."
    )


def get_stats(
    sites: list[str] | str | None = None, ssl_check: bool = True, **kwargs
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Queries water services statistics information.

    For more information about the water services statistics service, visit
    https://waterservices.usgs.gov/docs/statistics/statistics-details/

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        USGS site number (or list of site numbers)
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Keyword Arguments
    ---------------------
    statReportType: string
        daily (default), monthly, or annual
    statTypeCd: string
        all, mean, max, min, median

    Returns
    -------
    df: ``pandas.DataFrame``
        Statistics data from the statistics service
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    .. todo::

        fix date parsing

    Examples
    --------
    .. doctest::

        >>> # Get annual water statistics for a site
        >>> df, md = dataretrieval.nwis.get_stats(
        ...     sites="01646500", statReportType="annual", statYearType="water"
        ... )

        >>> # Get monthly statistics for a site
        >>> df, md = dataretrieval.nwis.get_stats(
        ...     sites="01646500", statReportType="monthly"
        ... )

    """
    _check_sites_value_types(sites)

    response = query_waterservices(
        service="stat", sites=sites, ssl_check=ssl_check, **kwargs
    )

    return _read_rdb(response.text), NWIS_Metadata(response, **kwargs)


def query_waterdata(
    service: str, ssl_check: bool = True, **kwargs
) -> requests.models.Response:
    """
    Queries waterdata.

    Parameters
    ----------
    service: string
        Name of the service to query: 'site', 'stats', etc.
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
    request: ``requests.models.Response``
        The response object from the API request to the web service
    """
    major_params = ["site_no", "state_cd"]
    bbox_params = [
        "nw_longitude_va",
        "nw_latitude_va",
        "se_longitude_va",
        "se_latitude_va",
    ]

    if not any(key in kwargs for key in major_params + bbox_params):
        raise TypeError("Query must specify a major filter: site_no, stateCd, bBox")

    elif any(key in kwargs for key in bbox_params) and not all(
        key in kwargs for key in bbox_params
    ):
        raise TypeError("One or more lat/long coordinates missing or invalid.")

    if service not in WATERDATA_SERVICES:
        raise TypeError("Service not recognized")

    url = WATERDATA_URL + service

    return query(url, payload=kwargs, ssl_check=ssl_check)


def query_waterservices(
    service: str, ssl_check: bool = True, **kwargs
) -> requests.models.Response:
    """
    Queries waterservices.usgs.gov

    For more documentation see https://waterservices.usgs.gov/docs/

    .. note::

        User must specify one major filter: sites, stateCd, or bBox

    Parameters
    ----------
    service: string
        Name of the service to query: 'site', 'stats', etc.
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Keyword Arguments
    ----------------
    bBox: string
        7-digit Hydrologic Unit Code (HUC)
    startDT: string
        Start date (e.g., '2017-12-31')
    endDT: string
        End date (e.g., '2018-01-01')
    modifiedSince: string
        Used to return only sites where attributes or period of record data
        have changed during the request period. String expected to be formatted
        in ISO-8601 duration format (e.g., 'P1D' for one day,
        'P1Y' for one year)

    Returns
    -------
    request: ``requests.models.Response``
        The response object from the API request to the web service

    """
    if not any(
        key in kwargs for key in ["sites", "stateCd", "bBox", "huc", "countyCd"]
    ):
        raise TypeError(
            "Query must specify a major filter: sites, stateCd, bBox, huc, or countyCd"
        )

    if service not in WATERSERVICES_SERVICES:
        raise TypeError("Service not recognized")

    if "format" not in kwargs:
        kwargs["format"] = "rdb"

    url = WATERSERVICE_URL + service

    return query(url, payload=kwargs, ssl_check=ssl_check)


def get_dv(
    sites: list[str] | str | None = None,
    start: str | None = None,
    end: str | None = None,
    multi_index: bool = True,
    ssl_check: bool = True,
    **kwargs,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Get daily values data from NWIS and return it as a ``pandas.DataFrame``.

    .. note:

        If no start or end date are provided, only the most recent record
        is returned.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        USGS site number (or list of site numbers)
    start: string, optional, default is None
        If the waterdata parameter startDT is supplied, it will overwrite the
        start parameter (YYYY-MM-DD)
    end: string, optional, default is None
        If the waterdata parameter endDT is supplied, it will overwrite the
        end parameter (YYYY-MM-DD)
    multi_index: bool, optional
        If True, return a multi-index dataframe, if False, return a
        single-index dataframe, default is True
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
    df: ``pandas.DataFrame``
        Times series data from the NWIS JSON
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get mean statistic daily values for site 04085427
        >>> df, md = dataretrieval.nwis.get_dv(
        ...     sites="04085427",
        ...     start="2012-01-01",
        ...     end="2012-06-30",
        ...     statCd="00003",
        ... )

        >>> # Get the latest daily values for site 01646500
        >>> df, md = dataretrieval.nwis.get_dv(sites="01646500")

    """
    _check_sites_value_types(sites)

    kwargs["startDT"] = kwargs.pop("startDT", start)
    kwargs["endDT"] = kwargs.pop("endDT", end)
    kwargs["sites"] = kwargs.pop("sites", sites)
    kwargs["multi_index"] = multi_index

    response = query_waterservices("dv", format="json", ssl_check=ssl_check, **kwargs)
    df = _parse_json_or_raise(response)

    return format_response(df, **kwargs), NWIS_Metadata(response, **kwargs)


def get_info(ssl_check: bool = True, **kwargs) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Get site description information from NWIS.

    **Note:** *Must specify one major parameter.*

    For additional parameter options see
    https://waterservices.usgs.gov/docs/site-service/site-service-details/

    Parameters
    ----------
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Keyword Arguments
    ----------------
    sites: string or list of strings
        A list of site numbers. Sites may be prefixed with an optional agency
        code followed by a colon.
    stateCd: string
        U.S. postal service (2-digit) state code. Only 1 state can be specified
        per request.
    huc: string or list of strings
        A list of hydrologic unit codes (HUC) or aggregated watersheds. Only 1
        major HUC can be specified per request, or up to 10 minor HUCs. A major
        HUC has two digits.
    bBox: string or list of strings
        A contiguous range of decimal latitude and longitude, starting with the
        west longitude, then the south latitude, then the east longitude, and
        then the north latitude with each value separated by a comma. The
        product of the range of latitude range and longitude cannot exceed 25
        degrees. Whole or decimal degrees must be specified, up to six digits
        of precision. Minutes and seconds are not allowed.
    countyCd: string or list of strings
        A list of county numbers, in a 5 digit numeric format. The first two
        digits of a county's code are the FIPS State Code.
        (url: https://help.waterdata.usgs.gov/code/county_query?fmt=html)
    startDt: string
        Selects sites based on whether data was collected at a point in time
        beginning after startDt (start date). Dates must be in ISO-8601
        Calendar Date format (for example: 1990-01-01).
    endDt: string
        The end date for the period of record. Dates must be in ISO-8601
        Calendar Date format (for example: 1990-01-01).
    period: string
        Selects sites based on whether they were active between now
        and a time in the past. For example, period=P10W will select sites
        active in the last ten weeks.
    modifiedSince: string
        Returns only sites where site attributes or period of record data have
        changed during the request period.
    parameterCd: string or list of strings
        Returns only site data for those sites containing the requested USGS
        parameter codes.
    siteType: string or list of strings
        Restricts sites to those having one or more major and/or minor site
        types, such as stream, spring or well. For a list of all valid site
        types see https://help.waterdata.usgs.gov/site_tp_cd
        For example, siteType='ST' returns streams only.
    siteOutput: string ('basic' or 'expanded')
        Indicates the richness of metadata you want for site attributes. Note
        that for visually oriented formats like Google Map format, this
        argument has no meaning. Note: for performance reasons,
        siteOutput=expanded cannot be used if seriesCatalogOutput=true or with
        any values for outputDataTypeCd.
    seriesCatalogOutput: bool
        A switch that provides detailed period of record information for
        certain output formats. The period of record indicates date ranges for
        a certain kind of information about a site, for example the start and
        end dates for a site's daily mean streamflow.

    Returns
    -------
    df: ``pandas.DataFrame``
        Site data from the NWIS web service
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get site information for a single site
        >>> df, md = dataretrieval.nwis.get_info(sites="05114000")

        >>> # Get site information for multiple sites
        >>> df, md = dataretrieval.nwis.get_info(sites=["05114000", "09423350"])

    """
    seriesCatalogOutput = kwargs.pop("seriesCatalogOutput", None)
    if seriesCatalogOutput in ["True", "TRUE", "true", True]:
        warnings.warn(
            (
                "WARNING: Starting in March 2024, the NWIS qw data endpoint is "
                "retiring and no longer receives updates. For more information, "
                "refer to https://waterdata.usgs.gov.nwis/qwdata and "
                "https://doi-usgs.github.io/dataRetrieval/articles/Status.html "
                "or email CompTools@usgs.gov."
            ),
            stacklevel=2,
        )
        # convert bool to string if necessary
        kwargs["seriesCatalogOutput"] = "True"
    else:
        # cannot have both seriesCatalogOutput and the expanded format
        kwargs["siteOutput"] = "Expanded"

    response = query_waterservices("site", ssl_check=ssl_check, **kwargs)

    return _read_rdb(response.text), NWIS_Metadata(response, **kwargs)


def get_iv(
    sites: list[str] | str | None = None,
    start: str | None = None,
    end: str | None = None,
    multi_index: bool = True,
    ssl_check: bool = True,
    **kwargs,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get instantaneous values data from NWIS and return it as a DataFrame.

    .. note::

        If no start or end date are provided, only the most recent record
        is returned.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        If the waterdata parameter site_no is supplied, it will overwrite the
        sites parameter
    start: string, optional, default is None
        If the waterdata parameter startDT is supplied, it will overwrite the
        start parameter (YYYY-MM-DD)
    end: string, optional, default is None
        If the waterdata parameter endDT is supplied, it will overwrite the
        end parameter (YYYY-MM-DD)
    multi_index: bool, optional
        If False, a dataframe with a single-level index (datetime) is returned,
        default is True
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
    df: ``pandas.DataFrame``
        Times series data from the NWIS JSON
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get instantaneous discharge data for site 05114000
        >>> df, md = dataretrieval.nwis.get_iv(
        ...     sites="05114000",
        ...     start="2013-11-03",
        ...     end="2013-11-03",
        ...     parameterCd="00060",
        ... )

    """
    _check_sites_value_types(sites)

    kwargs["startDT"] = kwargs.pop("startDT", start)
    kwargs["endDT"] = kwargs.pop("endDT", end)
    kwargs["sites"] = kwargs.pop("sites", sites)
    kwargs["multi_index"] = multi_index

    response = query_waterservices(
        service="iv", format="json", ssl_check=ssl_check, **kwargs
    )

    df = _parse_json_or_raise(response)
    return format_response(df, **kwargs), NWIS_Metadata(response, **kwargs)


def get_pmcodes(**kwargs):
    """Defunct: use ``get_reference_table(collection='parameter-codes')``."""
    raise NameError(
        "`nwis.get_pmcodes` has been replaced "
        "with `get_reference_table(collection='parameter-codes')`."
    )


def get_water_use(**kwargs):
    """Defunct: no current replacement."""
    raise NameError("`nwis.get_water_use` is defunct.")


def get_ratings(
    site: str | None = None,
    file_type: str = "base",
    ssl_check: bool = True,
    **kwargs,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Rating table for an active USGS streamgage retrieval.

    Reads current rating table for an active USGS streamgage from NWISweb.
    Data is retrieved from https://waterdata.usgs.gov/nwis.

    Parameters
    ----------
    site: string, optional, default is None
        USGS site number.  This is usually an 8 digit number as a string.
        If the nwis parameter site_no is supplied, it will overwrite the site
        parameter
    file_type: string, default is "base"
        can be "base", "corr", or "exsa"
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Return
    ------
    df: ``pandas.DataFrame``
        Formatted requested data
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # Get the rating table for USGS streamgage 01594440
        >>> df, md = dataretrieval.nwis.get_ratings(site="01594440")

    """
    site = kwargs.pop("site_no", site)

    payload = {}
    url = WATERDATA_BASE_URL + "nwisweb/get_ratings/"
    if site is not None:
        payload.update({"site_no": site})
    if file_type is not None:
        if file_type not in ["base", "corr", "exsa"]:
            raise ValueError(
                f'Unrecognized file_type: {file_type}, must be "base", "corr" or "exsa"'
            )
        payload.update({"file_type": file_type})
    response = query(url, payload, ssl_check=ssl_check)
    return _read_rdb(response.text), NWIS_Metadata(response, site_no=site)


def what_sites(ssl_check: bool = True, **kwargs) -> tuple[pd.DataFrame, BaseMetadata]:
    """
    Search NWIS for sites within a region with specific data.

    Parameters
    ----------
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.nwis.get_info`

    Return
    ------
    df: ``pandas.DataFrame``
        Formatted requested data
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    Examples
    --------
    .. doctest::

        >>> # get information about a single site
        >>> df, md = dataretrieval.nwis.what_sites(sites="05114000")

        >>> # get information about sites with phosphorus in Ohio
        >>> df, md = dataretrieval.nwis.what_sites(
        ...     stateCd="OH", parameterCd="00665"
        ... )

    """

    response = query_waterservices(service="site", ssl_check=ssl_check, **kwargs)

    df = _read_rdb(response.text)

    return df, NWIS_Metadata(response, **kwargs)


def get_record(
    sites: list[str] | str | None = None,
    start: str | None = None,
    end: str | None = None,
    multi_index: bool = True,
    wide_format: bool = True,
    datetime_index: bool = True,
    state: str | None = None,
    service: str = "iv",
    ssl_check: bool = True,
    **kwargs,
) -> pd.DataFrame:
    """
    Get data from NWIS and return it as a ``pandas.DataFrame``.

    .. note::

        If no start or end date are provided, only the most recent record is
        returned.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        List or comma delimited string of site.
    start: string, optional, default is None
        Starting date of record (YYYY-MM-DD)
    end: string, optional, default is None
        Ending date of record. (YYYY-MM-DD)
    multi_index: bool, optional
        If False, a dataframe with a single-level index (datetime) is returned,
        default is True
    wide_format : bool, optional
        If True, return data in wide format with multiple samples per row and
        one row per time, default is True
    datetime_index : bool, optional
        If True, create a datetime index. default is True
    state: string, optional, default is None
        full name, abbreviation or id
    service: string, default is 'iv'
        - 'iv' : instantaneous data
        - 'dv' : daily mean data
        - 'site' : site description
        - 'measurements' : (defunct) use `waterdata.get_field_measurements`
        - 'peaks': discharge peaks
        - 'gwlevels': (defunct) use `waterdata.get_field_measurements`
        - 'pmcodes': (defunct) use `get_reference_table`
        - 'water_use': (defunct) no replacement available
        - 'ratings': get rating table
        - 'stat': get statistics
    ssl_check: bool, optional
        If True, check SSL certificates, if False, do not check SSL,
        default is True
    **kwargs: optional
        If supplied, will be used as query parameters

    Returns
    -------
        ``pandas.DataFrame`` containing requested data

    Examples
    --------
    .. doctest::

        >>> # Get latest instantaneous data from site 01585200
        >>> df = dataretrieval.nwis.get_record(sites="01585200", service="iv")

        >>> # Get latest daily mean data from site 01585200
        >>> df = dataretrieval.nwis.get_record(sites="01585200", service="dv")

        >>> # Get site description for site 01585200
        >>> df = dataretrieval.nwis.get_record(sites="01585200", service="site")


        >>> # Get discharge peaks for site 01585200
        >>> df = dataretrieval.nwis.get_record(sites="01585200", service="peaks")

        >>> # Get rating table for USGS streamgage 01585200
        >>> df = dataretrieval.nwis.get_record(sites="01585200", service="ratings")

        >>> # Get annual statistics for USGS station 01646500
        >>> df = dataretrieval.nwis.get_record(
        ...     sites="01646500",
        ...     service="stat",
        ...     statReportType="annual",
        ...     statYearType="water",
        ... )

    """
    _check_sites_value_types(sites)

    defunct_replacements = {
        "measurements": "`waterdata.get_field_measurements`",
        "gwlevels": "`waterdata.get_field_measurements`",
        "pmcodes": "`waterdata.get_reference_table`",
        "water_use": "no replacement available",
    }
    if service in defunct_replacements:
        raise NameError(
            f"The NWIS service '{service}' is no longer supported by "
            f"get_record. Use {defunct_replacements[service]} instead."
        )

    if service not in WATERSERVICES_SERVICES + WATERDATA_SERVICES:
        raise TypeError(f"Unrecognized service: {service}")

    if service == "iv":
        df, _ = get_iv(
            sites=sites,
            startDT=start,
            endDT=end,
            multi_index=multi_index,
            ssl_check=ssl_check,
            **kwargs,
        )
        return df

    elif service == "dv":
        df, _ = get_dv(
            sites=sites,
            startDT=start,
            endDT=end,
            multi_index=multi_index,
            ssl_check=ssl_check,
            **kwargs,
        )
        return df

    elif service == "site":
        df, _ = get_info(sites=sites, ssl_check=ssl_check, **kwargs)
        return df

    elif service == "peaks":
        df, _ = get_discharge_peaks(
            sites=sites,
            start=start,
            end=end,
            multi_index=multi_index,
            ssl_check=ssl_check,
            **kwargs,
        )
        return df

    elif service == "ratings":
        df, _ = get_ratings(site=sites, ssl_check=ssl_check, **kwargs)
        return df

    elif service == "stat":
        df, _ = get_stats(sites=sites, ssl_check=ssl_check, **kwargs)
        return df

    else:
        raise TypeError(f"{service} service not yet implemented")


def _read_json(json):
    """
    Reads a NWIS Water Services formatted JSON into a ``pandas.DataFrame``.

    Parameters
    ----------
    json: dict
        A JSON dictionary response to be parsed into a ``pandas.DataFrame``

    Returns
    -------
    df: ``pandas.DataFrame``
        Times series data from the NWIS JSON
    md: :obj:`dataretrieval.utils.Metadata`
        A custom metadata object

    """
    all_site_dfs = []

    site_list = [
        ts["sourceInfo"]["siteCode"][0]["value"] for ts in json["value"]["timeSeries"]
    ]

    # create a list of indexes for each change in site no
    # for example, [0, 21, 22] would be the first and last indeces
    index_list = [0]
    index_list.extend(
        [i + 1 for i, (a, b) in enumerate(zip(site_list[:-1], site_list[1:])) if a != b]
    )
    index_list.append(len(site_list))

    for start, end in zip(index_list[:-1], index_list[1:]):
        # grab a block containing timeseries 0:21,
        # which are all from the same site
        site_block = json["value"]["timeSeries"][start:end]
        if not site_block:
            continue

        site_no = site_block[0]["sourceInfo"]["siteCode"][0]["value"]
        site_df = pd.DataFrame(columns=["datetime"])

        for timeseries in site_block:
            param_cd = timeseries["variable"]["variableCode"][0]["value"]
            # check whether min, max, mean record XXX
            option = timeseries["variable"]["options"]["option"][0].get("value")

            for parameter in timeseries["values"]:
                col_name = param_cd
                method = parameter["method"][0]["methodDescription"]

                if method:
                    method = method.strip("[]()").lower()
                    col_name = f"{col_name}_{method}"

                if option:
                    col_name = f"{col_name}_{option}"

                record_json = parameter["value"]

                if not record_json:
                    continue

                record_df = pd.DataFrame(record_json)
                record_df["value"] = pd.to_numeric(record_df["value"], errors="coerce")
                record_df["qualifiers"] = (
                    record_df["qualifiers"]
                    .astype(str)
                    .str.strip("[]")
                    .str.replace("'", "")
                )

                record_df.rename(
                    columns={
                        "value": col_name,
                        "dateTime": "datetime",
                        "qualifiers": col_name + "_cd",
                    },
                    inplace=True,
                )

                site_df = site_df.merge(record_df, how="outer", on="datetime")

        site_df["site_no"] = site_no
        all_site_dfs.append(site_df)

    if not all_site_dfs:
        return pd.DataFrame(columns=["site_no", "datetime"])

    merged_df = pd.concat(all_site_dfs, ignore_index=True)

    if "datetime" in merged_df.columns:
        merged_df["datetime"] = pd.to_datetime(merged_df["datetime"], utc=True)

    return merged_df


# NWIS-specific column dtype hints; pandas silently ignores unknown
# names, so passing the dict to read_rdb is safe even on responses
# whose columns don't include any of these.
_NWIS_RDB_DTYPES = {
    "site_no": str,
    "dec_long_va": float,
    "dec_lat_va": float,
    "parm_cd": str,
    "parameter_cd": str,
}


def _read_rdb(rdb):
    """Parse an NWIS RDB response and apply NWIS-specific post-processing.

    Thin wrapper around :func:`dataretrieval.rdb.read_rdb` that adds the
    NWIS column-dtype hints and runs :func:`format_response` (datetime
    index, multi-site MultiIndex, optional GeoDataFrame).
    """
    df = _read_rdb_text(rdb, dtypes=_NWIS_RDB_DTYPES)
    if df.empty:
        return df
    return format_response(df)


def _check_sites_value_types(sites):
    if sites and not isinstance(sites, list) and not isinstance(sites, str):
        raise TypeError("sites must be a string or a list of strings")


class NWIS_Metadata(BaseMetadata):
    """Metadata class for NWIS service, derived from BaseMetadata.

    Attributes
    ----------
    url : str
        Response url
    query_time: datetme.timedelta
        Response elapsed time
    header: requests.structures.CaseInsensitiveDict
        Response headers
    comments: str | None
        Metadata comments, if any
    site_info: tuple[pd.DataFrame, NWIS_Metadata] | None
        Site information if the query included `site_no`, `sites`, `stateCd`,
        `huc`, `countyCd` or `bBox`. `site_no` is preferred over `sites` if
        both are present.
    variable_info: None
        Deprecated. Accessing variable_info via NWIS_Metadata is deprecated.

    """

    def __init__(self, response, **parameters) -> None:
        """Generates a standard set of metadata informed by the response with specific
        metadata for NWIS data.

        Parameters
        ----------
        response: Response
            Response object from requests module
        parameters: unpacked dictionary
            Unpacked dictionary of the parameters supplied in the request

        Returns
        -------
        md: :obj:`dataretrieval.nwis.NWIS_Metadata`
            A ``dataretrieval`` custom :obj:`dataretrieval.nwis.NWIS_Metadata` object.

        """
        super().__init__(response)

        comments = ""
        for line in response.text.splitlines():
            if line.startswith("#"):
                comments += line.lstrip("#") + "\n"
        if comments:
            self.comment = comments

        self._parameters = parameters

    @property
    def site_info(self) -> tuple[pd.DataFrame, BaseMetadata] | None:
        """
        Return
        ------
        df: ``pandas.DataFrame``
            Formatted requested data from calling `nwis.what_sites`
        md: :obj:`dataretrieval.nwis.NWIS_Metadata`
            A NWIS_Metadata object
        """
        if "site_no" in self._parameters:
            return what_sites(sites=self._parameters["site_no"])

        elif "sites" in self._parameters:
            return what_sites(sites=self._parameters["sites"])

        elif "stateCd" in self._parameters:
            return what_sites(stateCd=self._parameters["stateCd"])

        elif "huc" in self._parameters:
            return what_sites(huc=self._parameters["huc"])

        elif "countyCd" in self._parameters:
            return what_sites(countyCd=self._parameters["countyCd"])

        elif "bBox" in self._parameters:
            return what_sites(bBox=self._parameters["bBox"])

        else:
            return None  # don't set metadata site_info attribute

    @property
    def variable_info(self) -> None:
        """
        Deprecated. Accessing variable_info via NWIS_Metadata is deprecated.
        Returns None.
        """
        warnings.warn(
            "Accessing variable_info via NWIS_Metadata is deprecated as "
            "it relies on the defunct get_pmcodes function.",
            DeprecationWarning,
            stacklevel=2,
        )
        return None
