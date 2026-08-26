"""Functions for downloading data from the `National Water Information System (NWIS)`_.

.. _National Water Information System (NWIS): https://waterdata.usgs.gov/nwis

"""

from __future__ import annotations

import functools
import threading
import warnings
from collections.abc import Callable
from json import JSONDecodeError
from typing import Any, NoReturn, TypeVar, cast

import httpx
import pandas as pd

from dataretrieval._deprecation import REMOVALS, warn_deprecated
from dataretrieval._response_metadata import BaseMetadata
from dataretrieval._validation import (
    require_any_of,
    require_one_of,
    require_together,
)
from dataretrieval.exceptions import DataCurrencyWarning
from dataretrieval.rdb import read_rdb

from ._querying import query

try:
    import geopandas as gpd
except ImportError:
    gpd = None

F = TypeVar("F", bound=Callable[..., Any])

WATERDATA_BASE_URL = "https://nwis.waterdata.usgs.gov/"
WATERDATA_URL = WATERDATA_BASE_URL + "nwis/"
WATERSERVICE_URL = "https://waterservices.usgs.gov/nwis/"
PARAMCODES_URL = "https://help.waterdata.usgs.gov/code/parameter_cd_nm_query?"
ALLPARAMCODES_URL = "https://help.waterdata.usgs.gov/code/parameter_cd_query?"

WATERSERVICES_SERVICES = ["dv", "iv", "site", "stat"]
# What ``get_record`` routes, which is wider than what ``query_waterdata``
# reaches: 'ratings' is served by ``get_ratings`` from a different endpoint.
WATERDATA_SERVICES = [
    "peaks",
    "ratings",
]
# The major filters each query function accepts, hoisted beside the service
# lists so the checks and their remedies read from one roster.
_NWIS_WEB_MAJOR_FILTERS = ("site_no", "stateCd")
_NWIS_WEB_BBOX_CORNERS = (
    "nw_longitude_va",
    "nw_latitude_va",
    "se_longitude_va",
    "se_latitude_va",
)
_WATERSERVICES_MAJOR_FILTERS = ("sites", "stateCd", "bBox", "huc", "countyCd")
# NAD83
_CRS = "EPSG:4269"

_NWIS_RDB_DTYPES = {
    "site_no": str,
    "dec_long_va": float,
    "dec_lat_va": float,
    "parm_cd": str,
    "parameter_cd": str,
}


_NWIS_REMOVAL_DATE = REMOVALS["nwis"]
_REPLACEMENTS = {
    "get_dv": "`waterdata.get_daily()`",
    "get_iv": "`waterdata.get_continuous()`",
    "get_info": "`waterdata.get_monitoring_locations()`",
    "what_sites": "`waterdata.get_monitoring_locations()`",
    "get_stats": "`waterdata.get_stats_por()` or `waterdata.get_stats_date_range()`",
    "get_discharge_peaks": "`waterdata.get_peaks()`",
    "get_ratings": "`waterdata.get_ratings()`",
    "get_record": "the appropriate `waterdata.get_*()` for the service you need",
    "query_waterdata": "a high-level `waterdata.get_*()` helper",
    "query_waterservices": "a high-level `waterdata.get_*()` helper",
}

_deprecation_state = threading.local()


def _warn_deprecated(func_name: str) -> None:
    """Emit a per-function DeprecationWarning pointing at the waterdata replacement."""
    warn_deprecated(
        f"`nwis.{func_name}`",
        replacement=_REPLACEMENTS[func_name],
        removal=_NWIS_REMOVAL_DATE,
        stacklevel=3,
    )


def _deprecated(func: F) -> F:
    """Mark an nwis function as deprecated.

    Wrappers like ``get_record`` -> ``get_iv`` -> ``query_waterservices`` would
    otherwise emit one warning per layer; the thread-local sentinel ensures the
    user sees only the outermost call's warning.
    """
    if func.__name__ not in _REPLACEMENTS:
        raise RuntimeError(
            f"_REPLACEMENTS missing entry for {func.__name__!r}; "
            "add a `waterdata` replacement before applying @_deprecated."
        )

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        if getattr(_deprecation_state, "active", False):
            return func(*args, **kwargs)
        _deprecation_state.active = True
        try:
            _warn_deprecated(func.__name__)
            return func(*args, **kwargs)
        finally:
            _deprecation_state.active = False

    return cast("F", wrapper)


def _parse_json_or_raise(response: httpx.Response) -> pd.DataFrame:
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
                f"(Status: {response.status_code}). This usually means the "
                "service is down or rate-limiting. Wait and retry; if it "
                "persists, check https://waterservices.usgs.gov/ or switch to "
                "the dataretrieval.waterdata getters."
            ) from e
        raise


def _localize_datetime_index(df: pd.DataFrame) -> pd.DataFrame:
    """Localize a naive datetime index (or multi-index level) to UTC."""
    if hasattr(df.index, "levels"):
        # Multi-index: localize the datetime level (level 1)
        if hasattr(df.index.levels[1], "tzinfo") and df.index.levels[1].tzinfo is None:
            df = df.tz_localize("UTC", level=1)
    elif hasattr(df.index, "tzinfo") and df.index.tzinfo is None:
        df = df.tz_localize("UTC")
    return df


def format_response(
    df: pd.DataFrame, service: str | None = None, **kwargs: Any
) -> pd.DataFrame:
    """Set up the index for a query response.

    Formats the response from the NWIS web services; in particular, it sets
    the index of the data frame. It converts the NWIS response into pandas
    datetime values localized to UTC and, where possible, uses those
    timestamps to define the data frame index.

    Parameters
    ----------
    df: ``pandas.DataFrame``
        The data frame to format.
    service: string, optional, default is None
        The NWIS service that was queried. This matters because the 'peaks'
        service returns a different format from the other services.
    **kwargs: optional
        Additional keyword arguments, e.g. 'multi_index'.

    Returns
    -------
    df: ``pandas.DataFrame``
        The formatted data frame.

    """
    mi = kwargs.pop("multi_index", True)

    if service == "peaks":
        df = preformat_peaks_response(df)

    if gpd is not None and "dec_lat_va" in df.columns:
        geoms = gpd.points_from_xy(df.dec_long_va.values, df.dec_lat_va.values)
        df = gpd.GeoDataFrame(df, geometry=geoms, crs=_CRS)

    if "datetime" not in df.columns:
        return df

    if len(df["site_no"].unique()) > 1 and mi:
        df.set_index(["site_no", "datetime"], inplace=True)
    else:
        df.set_index(["datetime"], inplace=True)

    df = _localize_datetime_index(df)
    return df.sort_index()


def _peak_datetimes(peak_dt: pd.Series) -> pd.Series:
    """Parse a ``peak_dt`` column, keeping peaks whose date is partly unknown.

    NWIS writes ``YYYY-MM-00`` when the day of a historical peak is not known
    and ``YYYY-00-00`` when the month is not either -- the ``Bd`` and ``Bm``
    ``peak_cd`` qualifiers. Neither parses as a date, so each is pinned to the
    start of the period that *is* known. ``waterdata.get_peaks()`` resolves the
    same records the same way, dating a year-only peak to 1 January and
    flagging it ``[MONTHUNKNOWN]``.

    A ``peak_dt`` that is blank or absent stays ``NaT``: there is no period to
    pin it to, and :func:`preformat_peaks_response` drops it.
    """
    text = peak_dt.astype("string").str.strip()
    text = text.str.replace(r"^(\d{4})-00-", r"\1-01-", regex=True)
    text = text.str.replace(r"^(\d{4}-\d{2})-00$", r"\1-01", regex=True)
    return pd.to_datetime(text, errors="coerce")


def preformat_peaks_response(df: pd.DataFrame) -> pd.DataFrame:
    """Format the datetime column of the 'peaks' service response.

    Parameters
    ----------
    df: ``pandas.DataFrame``
        The data frame to format.

    Returns
    -------
    df: ``pandas.DataFrame``
        The formatted data frame.

    Notes
    -----
    Peaks whose day or month is unknown are dated to the start of the known
    period rather than discarded; see :func:`_peak_datetimes`. Rows with no
    ``peak_dt`` at all are dropped, since they cannot be placed on the
    datetime index :func:`format_response` builds.

    """
    df["datetime"] = _peak_datetimes(df.pop("peak_dt"))
    df.dropna(subset=["datetime"], inplace=True)
    return df


def get_qwdata(**kwargs: Any) -> NoReturn:
    """Defunct: use ``waterdata.get_samples()``."""
    raise NameError(
        "`nwis.get_qwdata` has been replaced with `waterdata.get_samples()`."
    )


def get_discharge_measurements(**kwargs: Any) -> NoReturn:
    """Defunct: use ``waterdata.get_field_measurements()``."""
    raise NameError(
        "`nwis.get_discharge_measurements` has been replaced "
        "with `waterdata.get_field_measurements`."
    )


@_deprecated
def get_discharge_peaks(
    sites: list[str] | str | None = None,
    start: str | None = None,
    end: str | None = None,
    multi_index: bool = True,
    ssl_check: bool = True,
    **kwargs: Any,
) -> tuple[pd.DataFrame, NWIS_Metadata]:
    """Get discharge peaks from the waterdata service.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        USGS site number (or list of site numbers). If the waterdata parameter
        site_no is supplied, it overwrites the sites parameter.
    start: string, optional, default is None
        Starting date of record (YYYY-MM-DD). If the waterdata parameter
        begin_date is supplied, it overwrites the start parameter.
    end: string, optional, default is None
        Ending date of record (YYYY-MM-DD). If the waterdata parameter
        end_date is supplied, it overwrites the end parameter.
    multi_index: bool, optional
        If False, return a dataframe with a single-level index (datetime).
        Default is True.
    ssl_check: bool, optional
        Whether to check SSL certificates. Default is True.
    **kwargs: optional
        Additional query parameters, if supplied.

    Returns
    -------
    df: ``pandas.DataFrame``
        Time series data from the NWIS JSON.
    md: :obj:`dataretrieval.nwis.NWIS_Metadata`
        A custom metadata object.

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

    # Parse raw (read_rdb), not _read_rdb — the latter already runs
    # format_response, and the explicit format_response(service="peaks") below
    # does the peaks-specific formatting, so _read_rdb here was a redundant pass.
    df = read_rdb(response.text, dtypes=_NWIS_RDB_DTYPES)

    return format_response(df, service="peaks", **kwargs), NWIS_Metadata(
        response, **kwargs
    )


def get_gwlevels(**kwargs: Any) -> NoReturn:
    """Defunct: use ``waterdata.get_continuous()``, ``waterdata.get_daily()``,
    or ``waterdata.get_field_measurements()``."""
    raise NameError(
        "`nwis.get_gwlevels` has been replaced. Use "
        "`waterdata.get_continuous()` for continuous (typically 15-minute) "
        "values, `waterdata.get_daily()` for daily values, or "
        "`waterdata.get_field_measurements()` for discrete/manual readings."
    )


@_deprecated
def get_stats(
    sites: list[str] | str | None = None, ssl_check: bool = True, **kwargs: Any
) -> tuple[pd.DataFrame, NWIS_Metadata]:
    """Query the water services statistics service.

    For more information about the water services statistics service, visit
    https://waterservices.usgs.gov/docs/statistics/statistics-details/

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        USGS site number (or list of site numbers).
    ssl_check: bool, optional
        Whether to check SSL certificates. Default is True.
    **kwargs: optional
        Additional query parameters, if supplied.

    Keyword Arguments
    -----------------
    statReportType: string
        daily (default), monthly, or annual.
    statTypeCd: string
        all, mean, max, min, median.

    Returns
    -------
    df: ``pandas.DataFrame``
        Statistics data from the statistics service.
    md: :obj:`dataretrieval.nwis.NWIS_Metadata`
        A custom metadata object.

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


@_deprecated
def query_waterdata(
    service: str, ssl_check: bool = True, **kwargs: Any
) -> httpx.Response:
    """Query the waterdata service.

    Parameters
    ----------
    service: string
        Name of the service to query. Only ``'peaks'`` is served here; rating
        tables come from :func:`get_ratings`, which uses a different
        endpoint.
    ssl_check: bool, optional
        Whether to check SSL certificates. Default is True.
    **kwargs: optional
        Additional query parameters, if supplied.

    Returns
    -------
    request: ``httpx.Response``
        The response object from the API request to the web service.
    """
    require_any_of(
        {
            name: kwargs.get(name)
            for name in _NWIS_WEB_MAJOR_FILTERS + _NWIS_WEB_BBOX_CORNERS
        },
        context="as a major filter",
        remedy=(
            "Pass one, e.g. site_no='01491000' or stateCd='WI', or all four "
            "bounding-box corners together with "
            "coordinate_format='decimal_degrees'."
        ),
    )
    require_together(
        {name: kwargs.get(name) for name in _NWIS_WEB_BBOX_CORNERS},
        context="to describe a bounding box",
        remedy=(
            "Pass them along with coordinate_format='decimal_degrees', or "
            "drop the bounding box and filter with "
            f"{' or '.join(_NWIS_WEB_MAJOR_FILTERS)} instead."
        ),
    )
    require_one_of(
        service,
        ("peaks",),
        name="service",
        remedy=(
            "Rating tables come from waterdata.get_ratings("
            "monitoring_location_id='USGS-01646500'), served from a different "
            "endpoint and keyed by the AGENCY-ID form of the site number. It "
            "returns {'USGS-01646500.exsa.rdb': DataFrame} -- a dict per file, "
            "not a (frame, metadata) pair."
        ),
    )

    url = WATERDATA_URL + service

    return query(url, payload=kwargs, ssl_check=ssl_check)


@_deprecated
def query_waterservices(
    service: str, ssl_check: bool = True, **kwargs: Any
) -> httpx.Response:
    """Query waterservices.usgs.gov.

    For more documentation see https://waterservices.usgs.gov/docs/

    .. note::

        User must specify one major filter: sites, stateCd, or bBox

    Parameters
    ----------
    service: string
        Name of the service to query: 'dv', 'iv', 'site', or 'stat'.
    ssl_check: bool, optional
        Whether to check SSL certificates. Default is True.
    **kwargs: optional
        Additional query parameters, if supplied.

    Keyword Arguments
    -----------------
    bBox: string
        Bounding box of decimal latitude and longitude values, given as
        west longitude, south latitude, east longitude, north latitude,
        separated by commas.
    startDT: string
        Start date (e.g. '2017-12-31').
    endDT: string
        End date (e.g. '2018-01-01').
    modifiedSince: string
        Period during which site attributes or period-of-record data must have
        changed for a site to be returned. Expected to be a string in ISO-8601
        duration format (e.g. 'P1D' for one day, 'P1Y' for one year).

    Returns
    -------
    request: ``httpx.Response``
        The response object from the API request to the web service.

    """
    require_any_of(
        {name: kwargs.get(name) for name in _WATERSERVICES_MAJOR_FILTERS},
        context="as a major filter",
        remedy=("Pass one, e.g. sites='01491000', stateCd='WI', or countyCd='55025'."),
    )
    require_one_of(service, WATERSERVICES_SERVICES, name="service")

    if "format" not in kwargs:
        kwargs["format"] = "rdb"

    url = WATERSERVICE_URL + service

    return query(url, payload=kwargs, ssl_check=ssl_check)


def _get_json_values(
    service: str,
    sites: list[str] | str | None,
    start: str | None,
    end: str | None,
    multi_index: bool,
    ssl_check: bool,
    kwargs: dict[str, Any],
) -> tuple[pd.DataFrame, NWIS_Metadata]:
    """Shared body of the JSON waterservices time-series getters (dv / iv).

    The caller-facing ``sites`` / ``start`` / ``end`` arguments are aliases: an
    explicit waterservices keyword of the same meaning wins over them. Note that
    ``multi_index`` travels through ``kwargs`` so that :func:`format_response`
    sees it.
    """
    _check_sites_value_types(sites)

    kwargs["startDT"] = kwargs.pop("startDT", start)
    kwargs["endDT"] = kwargs.pop("endDT", end)
    kwargs["sites"] = kwargs.pop("sites", sites)
    kwargs["multi_index"] = multi_index

    response = query_waterservices(
        service, format="json", ssl_check=ssl_check, **kwargs
    )
    df = _parse_json_or_raise(response)

    return format_response(df, **kwargs), NWIS_Metadata(response, **kwargs)


@_deprecated
def get_dv(
    sites: list[str] | str | None = None,
    start: str | None = None,
    end: str | None = None,
    multi_index: bool = True,
    ssl_check: bool = True,
    **kwargs: Any,
) -> tuple[pd.DataFrame, NWIS_Metadata]:
    """Get daily values data from NWIS and return it as a ``pandas.DataFrame``.

    .. note::

        If no start or end date are provided, only the most recent record
        is returned.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        USGS site number (or list of site numbers).
    start: string, optional, default is None
        Starting date of record (YYYY-MM-DD). If the waterdata parameter
        startDT is supplied, it overwrites the start parameter.
    end: string, optional, default is None
        Ending date of record (YYYY-MM-DD). If the waterdata parameter endDT
        is supplied, it overwrites the end parameter.
    multi_index: bool, optional
        If True, return a multi-index dataframe; if False, return a
        single-index dataframe. Default is True.
    ssl_check: bool, optional
        Whether to check SSL certificates. Default is True.
    **kwargs: optional
        Additional query parameters, if supplied.

    Returns
    -------
    df: ``pandas.DataFrame``
        Time series data from the NWIS JSON.
    md: :obj:`dataretrieval.nwis.NWIS_Metadata`
        A custom metadata object.

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
    return _get_json_values("dv", sites, start, end, multi_index, ssl_check, kwargs)


@_deprecated
def get_info(
    ssl_check: bool = True, **kwargs: Any
) -> tuple[pd.DataFrame, NWIS_Metadata]:
    """Get site description information from NWIS.

    **Note:** *Must specify one major parameter.*

    For additional parameter options see
    https://waterservices.usgs.gov/docs/site-service/site-service-details/

    Parameters
    ----------
    ssl_check: bool, optional
        Whether to check SSL certificates. Default is True.
    **kwargs: optional
        Additional query parameters, if supplied.

    Keyword Arguments
    -----------------
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
        then the north latitude, with each value separated by a comma. The
        product of the range of latitude and longitude cannot exceed 25
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
        argument has no meaning. For performance reasons, siteOutput=expanded
        cannot be used if seriesCatalogOutput=true or with any values for
        outputDataTypeCd.
    seriesCatalogOutput: bool
        A switch that provides detailed period of record information for
        certain output formats. The period of record indicates date ranges for
        a certain kind of information about a site, for example the start and
        end dates for a site's daily mean streamflow.

    Returns
    -------
    df: ``pandas.DataFrame``
        Site data from the NWIS web service.
    md: :obj:`dataretrieval.nwis.NWIS_Metadata`
        A custom metadata object.

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
                "Starting in March 2024, the NWIS qw data endpoint is "
                "retiring and no longer receives updates. For more information, "
                "refer to https://waterdata.usgs.gov/nwis/qwdata and "
                "https://doi-usgs.github.io/dataRetrieval/articles/Status.html "
                "or email CompTools@usgs.gov."
            ),
            DataCurrencyWarning,
            stacklevel=2,
        )
        # convert bool to string if necessary
        kwargs["seriesCatalogOutput"] = "True"
    else:
        # cannot have both seriesCatalogOutput and the expanded format
        kwargs["siteOutput"] = "Expanded"

    response = query_waterservices("site", ssl_check=ssl_check, **kwargs)

    return _read_rdb(response.text), NWIS_Metadata(response, **kwargs)


@_deprecated
def get_iv(
    sites: list[str] | str | None = None,
    start: str | None = None,
    end: str | None = None,
    multi_index: bool = True,
    ssl_check: bool = True,
    **kwargs: Any,
) -> tuple[pd.DataFrame, NWIS_Metadata]:
    """Get instantaneous values data from NWIS and return it as a DataFrame.

    .. note::

        If no start or end date are provided, only the most recent record
        is returned.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        USGS site number (or list of site numbers). If the waterdata parameter
        site_no is supplied, it overwrites the sites parameter.
    start: string, optional, default is None
        Starting date of record (YYYY-MM-DD). If the waterdata parameter
        startDT is supplied, it overwrites the start parameter.
    end: string, optional, default is None
        Ending date of record (YYYY-MM-DD). If the waterdata parameter endDT
        is supplied, it overwrites the end parameter.
    multi_index: bool, optional
        If False, return a dataframe with a single-level index (datetime).
        Default is True.
    ssl_check: bool, optional
        Whether to check SSL certificates. Default is True.
    **kwargs: optional
        Additional query parameters, if supplied.

    Returns
    -------
    df: ``pandas.DataFrame``
        Time series data from the NWIS JSON.
    md: :obj:`dataretrieval.nwis.NWIS_Metadata`
        A custom metadata object.

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
    return _get_json_values("iv", sites, start, end, multi_index, ssl_check, kwargs)


def get_pmcodes(**kwargs: Any) -> NoReturn:
    """Defunct: use ``waterdata.get_reference_table(collection='parameter-codes')``."""
    raise NameError(
        "`nwis.get_pmcodes` has been replaced with "
        "`waterdata.get_reference_table(collection='parameter-codes')`."
    )


def get_water_use(**kwargs: Any) -> NoReturn:
    """Defunct: use ``dataretrieval.nwdc.get_wateruse`` instead.

    The legacy NWIS water-use service has been retired. Modeled water-use
    estimates are now served by the National Water Availability Assessment Data
    Companion (NWDC); retrieve them with
    :func:`dataretrieval.nwdc.get_wateruse`.
    """
    raise NameError(
        "`nwis.get_water_use` is defunct; use "
        "`dataretrieval.nwdc.get_wateruse` instead."
    )


@_deprecated
def get_ratings(
    site: str | None = None,
    file_type: str = "base",
    ssl_check: bool = True,
    **kwargs: Any,
) -> tuple[pd.DataFrame, NWIS_Metadata]:
    """Get the rating table for an active USGS streamgage.

    Reads the current rating table for an active USGS streamgage from NWISweb.
    Data is retrieved from https://waterdata.usgs.gov/nwis.

    Parameters
    ----------
    site: string, optional, default is None
        USGS site number, usually an 8 digit number as a string. If the nwis
        parameter site_no is supplied, it overwrites the site parameter.
    file_type: string, default is "base"
        One of "base", "corr", or "exsa".
    ssl_check: bool, optional
        Whether to check SSL certificates. Default is True.
    **kwargs: optional
        Additional query parameters, if supplied.

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted requested data.
    md: :obj:`dataretrieval.nwis.NWIS_Metadata`
        A custom metadata object.

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
        require_one_of(file_type, ("base", "corr", "exsa"), name="file_type")
        payload.update({"file_type": file_type})
    response = query(url, payload, ssl_check=ssl_check)
    return _read_rdb(response.text), NWIS_Metadata(response, site_no=site)


@_deprecated
def what_sites(
    ssl_check: bool = True, **kwargs: Any
) -> tuple[pd.DataFrame, NWIS_Metadata]:
    """Search NWIS for sites within a region with specific data.

    Parameters
    ----------
    ssl_check: bool, optional
        Whether to check SSL certificates. Default is True.
    **kwargs: optional
        Accepts the same parameters as :obj:`dataretrieval.nwis.get_info`.

    Returns
    -------
    df: ``pandas.DataFrame``
        Formatted requested data.
    md: :obj:`dataretrieval.nwis.NWIS_Metadata`
        A custom metadata object.

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


@_deprecated
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
    **kwargs: Any,
) -> pd.DataFrame:
    """Get data from NWIS and return it as a ``pandas.DataFrame``.

    .. note::

        If no start or end date are provided, only the most recent record is
        returned.

    Parameters
    ----------
    sites: string or list of strings, optional, default is None
        List of sites, or a comma-delimited string of sites.
    start: string, optional, default is None
        Starting date of record (YYYY-MM-DD).
    end: string, optional, default is None
        Ending date of record (YYYY-MM-DD).
    multi_index: bool, optional
        If False, return a dataframe with a single-level index (datetime).
        Default is True.
    wide_format : bool, optional
        If True, return data in wide format, with multiple samples per row and
        one row per time. Default is True.
    datetime_index : bool, optional
        If True, create a datetime index. Default is True.
    state: string, optional, default is None
        State full name, abbreviation, or id.
    service: string, default is 'iv'
        - 'iv' : instantaneous data
        - 'dv' : daily mean data
        - 'site' : site description
        - 'measurements' : (defunct) use `waterdata.get_field_measurements`
        - 'peaks': discharge peaks
        - 'gwlevels': (defunct) use `waterdata.get_continuous`,
          `waterdata.get_daily`, or `waterdata.get_field_measurements`
        - 'pmcodes': (defunct) use `waterdata.get_reference_table`
        - 'water_use': (defunct) use `nwdc.get_wateruse`
        - 'ratings': get rating table
        - 'stat': get statistics
    ssl_check: bool, optional
        Whether to check SSL certificates. Default is True.
    **kwargs: optional
        Additional query parameters, if supplied.

    Returns
    -------
        ``pandas.DataFrame`` containing the requested data.

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
        "gwlevels": (
            "`waterdata.get_continuous` (continuous), "
            "`waterdata.get_daily`, or `waterdata.get_field_measurements` "
            "(discrete)"
        ),
        "pmcodes": "`waterdata.get_reference_table`",
        "water_use": "`nwdc.get_wateruse`",
    }
    if service in defunct_replacements:
        raise NameError(
            f"The NWIS service '{service}' is no longer supported by "
            f"get_record. Use {defunct_replacements[service]} instead."
        )

    require_one_of(
        service,
        WATERSERVICES_SERVICES + WATERDATA_SERVICES,
        name="service",
        remedy=(
            "New work should use the dataretrieval.waterdata getters instead; "
            "NWIS is deprecated."
        ),
    )

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
        # the ratings service is single-site; get_ratings takes a scalar site
        df, _ = get_ratings(
            site=cast("str | None", sites), ssl_check=ssl_check, **kwargs
        )
        return df

    elif service == "stat":
        df, _ = get_stats(sites=sites, ssl_check=ssl_check, **kwargs)
        return df

    else:  # pragma: no cover - every recognized service has a branch above
        raise AssertionError(f"get_record has no handler for service {service!r}")


def _site_block_boundaries(site_list: list[str]) -> list[int]:
    """Return indices where the site number changes, bookended by 0 and len.

    For example, given ``['A', 'A', 'B']`` returns ``[0, 2, 3]``.
    """
    boundaries = [0]
    boundaries.extend(
        i + 1
        for i, (a, b) in enumerate(zip(site_list[:-1], site_list[1:], strict=False))
        if a != b
    )
    boundaries.append(len(site_list))
    return boundaries


def _build_column_name(param_cd: str, method: str, option: str | None) -> str:
    """Derive the DataFrame column name for a parameter record."""
    col_name = param_cd
    if method:
        col_name = f"{col_name}_{method.strip('[]()').lower()}"
    if option:
        col_name = f"{col_name}_{option}"
    return col_name


def _parse_parameter_record(
    record_json: list[dict[str, Any]], col_name: str
) -> pd.DataFrame:
    """Parse a single parameter's value list into a renamed DataFrame."""
    record_df = pd.DataFrame(record_json)
    record_df["value"] = pd.to_numeric(record_df["value"], errors="coerce")
    record_df["qualifiers"] = (
        record_df["qualifiers"].astype(str).str.strip("[]").str.replace("'", "")
    )
    record_df.rename(
        columns={
            "value": col_name,
            "dateTime": "datetime",
            "qualifiers": col_name + "_cd",
        },
        inplace=True,
    )
    return record_df


def _parse_site_block(site_block: list[dict[str, Any]]) -> pd.DataFrame:
    """Parse all timeseries in one site's block into a single DataFrame."""
    site_no = site_block[0]["sourceInfo"]["siteCode"][0]["value"]
    site_df = pd.DataFrame(columns=["datetime"])

    for timeseries in site_block:
        param_cd = timeseries["variable"]["variableCode"][0]["value"]
        option = timeseries["variable"]["options"]["option"][0].get("value")

        for parameter in timeseries["values"]:
            method = parameter["method"][0]["methodDescription"]
            col_name = _build_column_name(param_cd, method, option)
            record_json = parameter["value"]
            if not record_json:
                continue
            record_df = _parse_parameter_record(record_json, col_name)
            site_df = site_df.merge(record_df, how="outer", on="datetime")

    site_df["site_no"] = site_no
    return site_df


def _read_json(json: dict[str, Any]) -> pd.DataFrame:
    """Read a NWIS Water Services formatted JSON into a ``pandas.DataFrame``.

    Parameters
    ----------
    json: dict
        A JSON dictionary response to be parsed into a ``pandas.DataFrame``.

    Returns
    -------
    df: ``pandas.DataFrame``
        Time series data from the NWIS JSON.

    """
    time_series = json["value"]["timeSeries"]
    site_list = [ts["sourceInfo"]["siteCode"][0]["value"] for ts in time_series]
    boundaries = _site_block_boundaries(site_list)

    all_site_dfs = []
    for start, end in zip(boundaries[:-1], boundaries[1:], strict=False):
        site_block = time_series[start:end]
        if not site_block:
            continue
        all_site_dfs.append(_parse_site_block(site_block))

    if not all_site_dfs:
        return pd.DataFrame(columns=["site_no", "datetime"])

    merged_df = pd.concat(all_site_dfs, ignore_index=True)

    if "datetime" in merged_df.columns:
        merged_df["datetime"] = pd.to_datetime(merged_df["datetime"], utc=True)

    return merged_df


def _read_rdb(rdb: str) -> pd.DataFrame:
    """Parse an NWIS RDB response and apply NWIS-specific post-processing.

    Thin wrapper around :func:`dataretrieval.rdb.read_rdb` that adds the
    NWIS column-dtype hints and runs :func:`format_response` (datetime
    index, multi-site MultiIndex, optional GeoDataFrame).
    """
    return format_response(read_rdb(rdb, dtypes=_NWIS_RDB_DTYPES))


def _check_sites_value_types(sites: list[str] | str | None) -> None:
    if sites and not isinstance(sites, list) and not isinstance(sites, str):
        raise TypeError(
            "sites must be a site number as a string, or a list of them, not "
            f"{type(sites).__name__}. Pass sites='01491000' for one site, or "
            "sites=['01491000', '01645000'] for several."
        )


class NWIS_Metadata(BaseMetadata):
    """Metadata class for NWIS service, derived from BaseMetadata.

    Attributes
    ----------
    url : str
        Response url.
    query_time: datetime.timedelta
        Response elapsed time.
    header: httpx.Headers
        Response headers.
    comments: str | None
        Metadata comments, if any.

    Notes
    -----
    ``site_info`` is exposed as a property (documented below) rather than a
    plain attribute.

    """

    def __init__(self, response: httpx.Response, **parameters: Any) -> None:
        """Generate the standard metadata set, plus NWIS-specific metadata.

        Parameters
        ----------
        response: Response
            Response object from the ``httpx`` module.
        parameters: unpacked dictionary
            Unpacked dictionary of the parameters supplied in the request.

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
        """Site information for the query.

        Populated when the query included ``site_no``, ``sites``, ``stateCd``,
        ``huc``, ``countyCd`` or ``bBox`` (``site_no`` is preferred over
        ``sites`` if both are present); ``None`` otherwise.

        Returns
        -------
        df: ``pandas.DataFrame``
            Formatted requested data from calling `nwis.what_sites`.
        md: :obj:`dataretrieval.nwis.NWIS_Metadata`
            A NWIS_Metadata object.
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
