"""Getters for values measured in person rather than by a sensor.

Field measurements, annual peaks, and channel geometry. These are collected
during site visits, at low frequency and with delivery lag, which is why they
are grouped apart from the continuous record they help calibrate.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

import pandas as pd

from dataretrieval.ogc.filters import FILTER_LANG
from dataretrieval.response_metadata import BaseMetadata
from dataretrieval.waterdata.utils import (
    _get_args,
    get_ogc_data,
)


def get_field_measurements(
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    observing_procedure_code: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    field_visit_id: str | Iterable[str] | None = None,
    approval_status: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    qualifier: str | Iterable[str] | None = None,
    value: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    observing_procedure: str | Iterable[str] | None = None,
    vertical_datum: str | Iterable[str] | None = None,
    measuring_agency: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    time: str | Iterable[str] | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    max_rows: int | None = None,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get discrete measurements collected in person during a site visit.

    Field measurements consist of measurements of gage height and discharge, and
    readings of groundwater levels. They are used primarily as calibration
    readings for the automated sensors that collect continuous data. Field
    measurements are collected at a low frequency, and their delivery in WDFN
    may be delayed by data processing time.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location,
        corresponding to the id field in the monitoring-locations endpoint. IDs
        combine the agency code of the agency responsible for the monitoring
        location (e.g. USGS) with the location's ID number (e.g. 02238500),
        separated by a hyphen (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        A 5-digit code identifying the constituent measured and the units of
        measure. A complete list of parameter codes and associated groupings is
        available at https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    observing_procedure_code : string or iterable of strings, optional
        A short code corresponding to the observing procedure for the field
        measurement.
    properties : string or iterable of strings, optional
        The columns to return from the query. See the
        field-measurements schema in the OpenAPI reference for the available
        columns (e.g. geometry, id, monitoring_location_id, parameter_code,
        value, unit_of_measure, approval_status, qualifier, last_modified):
        https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/field-measurements
    field_visit_id : string or iterable of strings, optional
        A universally unique identifier (UUID) for the field visit.
        Multiple measurements may be made during a single field visit.
    approval_status : string or iterable of strings, optional
        The approval status of each record: either "Approved", meaning
        processing review has been completed and the data are approved for
        publication, or "Provisional", meaning the data are subject to revision.
        Some of the data you obtain from this U.S. Geological Survey database
        may not have received Director's approval. Any such data values are
        qualified as provisional and are subject to revision. Provisional data
        are released on the condition that neither the USGS nor the United
        States Government may be held liable for any damages resulting from
        their use. For more information about provisional data, see
        https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or iterable of strings, optional
        Any qualifiers associated with an observation, for instance whether a
        sensor may have been impacted by ice or whether values were estimated.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in
        the JSON response format to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. A refresh may
        happen due to regular operational processes and does not necessarily
        indicate that anything about the measurement has changed. You can query
        this field using date-times or intervals, adhering to RFC 3339, or using
        ISO 8601 duration objects. Intervals may be bounded or half-bounded
        (double-dots at start or end). Only features whose last_modified
        intersects the requested value are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    observing_procedure : string or iterable of strings, optional
        Water measurement or water-quality observing procedure descriptions.
    vertical_datum : string or iterable of strings, optional
        The datum used to determine altitude and vertical position at the
        monitoring location.
    measuring_agency : string or iterable of strings, optional
        The agency performing the measurement.
    skip_geometry : boolean, optional
        If True, the response omits the geometry of each feature and the
        returned object is a data frame with no spatial information. The USGS
        Water Data APIs use camelCase "skipGeometry" in CQL2 queries.
    time : string, optional
        The date an observation represents. You can query this field using
        date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features whose time intersects the requested
        value are selected. If a feature has multiple temporal properties, the
        server decides whether to use a single property or all relevant ones to
        determine the extent.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    bbox : list of numbers, optional
        Only features whose geometry intersects the bounding box are selected.
        The bounding box is provided as four or six numbers, depending on
        whether the coordinate reference system includes a vertical axis (height
        or depth). Coordinates are assumed to be in crs 4326. The expected
        format is ``[xmin, ymin, xmax, ymax]``, i.e. ``[Western-most longitude,
        Southern-most latitude, Eastern-most longitude, Northern-most
        latitude]``.
    limit : int, optional
        The number of features returned in each page. The maximum allowable
        limit is 50000; the default (None) requests that maximum. Set a lower
        number if your internet connection is spotty. This is a per-page size,
        not a cap on the total result: a query matching more rows than ``limit``
        still returns every matching row across multiple pages. Use ``max_rows``
        to cap the total instead.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    max_rows : int, optional
        Cap the total number of rows returned, stopping pagination early
        instead of downloading the whole result. Unlike ``limit`` (the
        per-page size), this bounds the total result across every page.
        The default (None) follows pagination to completion.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Call :func:`get_queryables` to see the queryables a
        collection supports.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get field measurements from a single groundwater site
        >>> # and parameter code, and do not return geometry
        >>> df, md = dataretrieval.waterdata.get_field_measurements(
        ...     monitoring_location_id="USGS-375907091432201",
        ...     parameter_code="72019",
        ...     skip_geometry=True,
        ... )

        >>> # Half-bounded time range: every measurement at this site since
        >>> # 1980 (open-ended end). Use ``"../<date>"`` for the inverse
        >>> # (everything up to a date).
        >>> df, md = dataretrieval.waterdata.get_field_measurements(
        ...     monitoring_location_id="USGS-425957088141001",
        ...     time="1980-01-01/..",
        ... )

        >>> # Get field measurements from multiple sites and
        >>> # parameter codes from the last 20 years
        >>> df, md = dataretrieval.waterdata.get_field_measurements(
        ...     monitoring_location_id=[
        ...         "USGS-451605097071701",
        ...         "USGS-263819081585801",
        ...     ],
        ...     parameter_code=["62611", "72019"],
        ...     time="P20Y",
        ... )
    """
    service = "field-measurements"

    # Build argument dictionary, omitting None values
    args = _get_args(locals(), exclude={"max_rows"})

    return get_ogc_data(args, service, max_rows=max_rows)


def get_peaks(
    monitoring_location_id: str | Iterable[str] | None = None,
    parameter_code: str | Iterable[str] | None = None,
    time_series_id: str | Iterable[str] | None = None,
    unit_of_measure: str | Iterable[str] | None = None,
    time: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    water_year: int | list[int] | None = None,
    year: int | list[int] | None = None,
    month: int | list[int] | None = None,
    day: int | list[int] | None = None,
    peak_since: int | list[int] | None = None,
    properties: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    max_rows: int | None = None,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get the annual peak streamflow / stage record for a monitoring location.

    Peaks are the largest values observed at a site each water year and are
    the standard input to flood-frequency analysis (e.g. log-Pearson Type III
    fits). The endpoint returns one row per (monitoring location, parameter,
    water year), with the peak ``value`` and the ``time`` it occurred.

    The collection covers both stage (parameter ``"00065"``, ``ft``) and
    discharge (parameter ``"00060"``, ``ft^3/s``); a typical streamgage has a
    series for each. Reference docs:
    https://api.waterdata.usgs.gov/ogcapi/v0/openapi?f=html#/peaks

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location, in
        ``AGENCY-ID`` form (e.g. ``"USGS-02238500"``).
    parameter_code : string or iterable of strings, optional
        5-digit parameter code. Most peaks records are ``"00060"`` (discharge)
        or ``"00065"`` (stage / gage height). Full list at
        https://help.waterdata.usgs.gov/codes-and-parameters/parameters.
    time_series_id : string or iterable of strings, optional
        ID of the time series the peak belongs to.
    unit_of_measure : string or iterable of strings, optional
        Human-readable units (e.g. ``"ft^3/s"``, ``"ft"``).
    time : string, optional
        Datetime, interval, or duration filter on the peak's date.
        See :func:`get_time_series_metadata` for the full grammar.
    last_modified : string, optional
        Same datetime grammar as ``time``; filters on the database
        last-modified timestamp (useful for incremental ETL polling).
    water_year, year, month, day : int or list of ints, optional
        Calendar / water-year filters on the peak event. The water year ends
        September 30 (e.g. WY2024 = Oct 1, 2023 – Sep 30, 2024).
    peak_since : int or list of ints, optional
        Filter on the year since which the peak value has stood as the
        record (the API serves this field as an integer; many rows are
        ``null``).
    properties : string or iterable of strings, optional
        Subset of columns to return. Defaults to every available property.
    skip_geometry : boolean, optional
        Skip per-feature geometries; the returned object will be a plain
        ``DataFrame`` with no spatial information.
    bbox : list of numbers, optional
        Only features whose geometry intersects the bounding box are
        selected. Format: ``[xmin, ymin, xmax, ymax]`` in CRS 4326
        (longitude / latitude, west-south-east-north).
    limit : int, optional
        Page size; the maximum allowable value is 50000. Default
        (``None``) requests the maximum allowable limit. This is a
        per-page size, not a cap on the total result: a query matching more
        rows than ``limit`` still returns every matching row across
        multiple pages. Use ``max_rows`` to cap the total instead.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    max_rows : int, optional
        Cap the total number of rows returned, stopping pagination early
        instead of downloading the whole result. Unlike ``limit`` (the
        per-page size), this bounds the total result across every page.
        The default (None) follows pagination to completion.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Call :func:`get_queryables` to see the queryables a
        collection supports.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md : :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object pertaining to the query.

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Full annual peak record at one site (both stage and discharge)
        >>> df, md = dataretrieval.waterdata.get_peaks(
        ...     monitoring_location_id="USGS-02238500"
        ... )

        >>> # Discharge peaks only
        >>> df, md = dataretrieval.waterdata.get_peaks(
        ...     monitoring_location_id="USGS-02238500",
        ...     parameter_code="00060",
        ... )

        >>> # Multi-site peaks for a parameter, narrowed to a water-year range
        >>> df, md = dataretrieval.waterdata.get_peaks(
        ...     monitoring_location_id=[
        ...         "USGS-07069000",
        ...         "USGS-07064000",
        ...         "USGS-07068000",
        ...     ],
        ...     parameter_code="00060",
        ...     water_year=[2020, 2021, 2022, 2023],
        ... )

    """
    service = "peaks"

    args = _get_args(locals(), exclude={"max_rows"})

    return get_ogc_data(args, service, max_rows=max_rows)


def get_channel(
    monitoring_location_id: str | Iterable[str] | None = None,
    field_visit_id: str | Iterable[str] | None = None,
    measurement_number: str | Iterable[str] | None = None,
    time: str | Iterable[str] | None = None,
    channel_name: str | Iterable[str] | None = None,
    channel_flow: str | Iterable[str] | None = None,
    channel_flow_unit: str | Iterable[str] | None = None,
    channel_width: str | Iterable[str] | None = None,
    channel_width_unit: str | Iterable[str] | None = None,
    channel_area: str | Iterable[str] | None = None,
    channel_area_unit: str | Iterable[str] | None = None,
    channel_velocity: str | Iterable[str] | None = None,
    channel_velocity_unit: str | Iterable[str] | None = None,
    channel_location_distance: str | Iterable[str] | None = None,
    channel_location_distance_unit: str | Iterable[str] | None = None,
    channel_stability: str | Iterable[str] | None = None,
    channel_material: str | Iterable[str] | None = None,
    channel_evenness: str | Iterable[str] | None = None,
    horizontal_velocity_description: str | Iterable[str] | None = None,
    vertical_velocity_description: str | Iterable[str] | None = None,
    longitudinal_velocity_description: str | Iterable[str] | None = None,
    measurement_type: str | Iterable[str] | None = None,
    last_modified: str | Iterable[str] | None = None,
    channel_measurement_type: str | Iterable[str] | None = None,
    properties: str | Iterable[str] | None = None,
    skip_geometry: bool | None = None,
    bbox: list[float] | None = None,
    limit: int | None = None,
    filter: str | None = None,
    filter_lang: FILTER_LANG | None = None,
    convert_type: bool = True,
    max_rows: int | None = None,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get channel-geometry measurements recorded during streamflow field visits.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location,
        corresponding to the id field in the monitoring-locations endpoint. IDs
        combine the agency code of the agency responsible for the monitoring
        location (e.g. USGS) with the location's ID number (e.g. 02238500),
        separated by a hyphen (e.g. USGS-02238500).
    field_visit_id : string or iterable of strings, optional
        A universally unique identifier (UUID) for the field visit. Multiple
        measurements may be made during a single field visit.
    measurement_number : string or iterable of strings, optional
        Measurement number.
    time : string or iterable of strings, optional
        The date an observation represents. You can query this field using
        date-times or intervals, adhering to RFC 3339, or using ISO 8601
        duration objects. Intervals may be bounded or half-bounded (double-dots
        at start or end). Only features whose time intersects the requested
        value are selected. If a feature has multiple temporal properties, the
        server decides whether to use a single property or all relevant ones to
        determine the extent.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
              "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H"
              for the last 36 hours

    channel_name : string or iterable of strings, optional
        The channel name.
    channel_flow : string or iterable of strings, optional
        The channel discharge (flow).
    channel_flow_unit : string or iterable of strings, optional
        The units for channel discharge.
    channel_width : string or iterable of strings, optional
        The channel width.
    channel_width_unit : string or iterable of strings, optional
        The units for channel width.
    channel_area : string or iterable of strings, optional
        The channel area.
    channel_area_unit : string or iterable of strings, optional
        The units for channel area.
    channel_velocity :  string or iterable of strings, optional
        The mean channel velocity.
    channel_velocity_unit : string or iterable of strings, optional
        The units for channel velocity.
    channel_location_distance : string or iterable of strings, optional
        The channel location distance.
    channel_location_distance_unit : string or iterable of strings, optional
        The units for channel location distance.
    channel_stability : string or iterable of strings, optional
        The stability of the channel material.
    channel_material : string or iterable of strings, optional
        The channel material.
    channel_evenness : string or iterable of strings, optional
        The channel evenness from bank to bank.
    horizontal_velocity_description : string or iterable of strings, optional
        The horizontal velocity description.
    vertical_velocity_description : string or iterable of strings, optional
        The vertical velocity description.
    longitudinal_velocity_description : string or iterable of strings, optional
        The longitudinal velocity description.
    measurement_type : string or iterable of strings, optional
        The type of channel measurement.
    last_modified : string, optional
        The last time a record was refreshed in our database. A refresh may
        happen due to regular operational processes and does not necessarily
        indicate that anything about the measurement has changed. You can query
        this field using date-times or intervals, adhering to RFC 3339, or using
        ISO 8601 duration objects. Intervals may be bounded or half-bounded
        (double-dots at start or end). Only features whose last_modified
        intersects the requested value are selected.
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
                "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or
                "PT36H" for the last 36 hours

    skip_geometry : boolean, optional
        If True, the response omits the geometry of each feature and the
        returned object is a data frame with no spatial information. The USGS
        Water Data APIs use camelCase "skipGeometry" in CQL2 queries.
    channel_measurement_type : string or iterable of strings, optional
        The channel measurement type.
    properties : string or iterable of strings, optional
        The columns to return from the query. Available
        options are: geometry, channel_measurements_id, monitoring_location_id,
        field_visit_id, measurement_number, time, channel_name, channel_flow,
        channel_flow_unit, channel_width, channel_width_unit, channel_area,
        channel_area_unit, channel_velocity, channel_velocity_unit,
        channel_location_distance, channel_location_distance_unit, channel_stability,
        channel_material, channel_evenness, horizontal_velocity_description,
        vertical_velocity_description, longitudinal_velocity_description,
        measurement_type, last_modified, channel_measurement_type. The default
        (None) returns all columns.
    bbox : list of numbers, optional
        Only features whose geometry intersects the bounding box are selected.
        The bounding box is provided as four or six numbers, depending on
        whether the coordinate reference system includes a vertical axis (height
        or depth). Coordinates are assumed to be in crs 4326. The expected
        format is ``[xmin, ymin, xmax, ymax]``, i.e. ``[Western-most longitude,
        Southern-most latitude, Eastern-most longitude, Northern-most
        latitude]``.
    limit : int, optional
        The number of features returned in each page. The maximum allowable
        limit is 50000; the default (None) requests that maximum. Set a lower
        number if your internet connection is spotty. This is a per-page size,
        not a cap on the total result: a query matching more rows than ``limit``
        still returns every matching row across multiple pages. Use ``max_rows``
        to cap the total instead.
    filter, filter_lang : optional
        Server-side CQL filter passed through as the OGC ``filter`` /
        ``filter-lang`` query parameters. See
        :mod:`dataretrieval.ogc.filters` for syntax, auto-chunking,
        and the lexicographic-comparison pitfall.
    convert_type : boolean, optional
        If True, converts columns to appropriate types.
    max_rows : int, optional
        Cap the total number of rows returned, stopping pagination early
        instead of downloading the whole result. Unlike ``limit`` (the
        per-page size), this bounds the total result across every page.
        The default (None) follows pagination to completion.
    **queryables : string or iterable of strings, optional
        Any other queryable property of this collection, passed through as a
        server-side filter. Call :func:`get_queryables` to see the queryables a
        collection supports.

    Returns
    -------
    df : ``pandas.DataFrame`` or ``geopandas.GeoDataFrame``
        Formatted data returned from the API query.
    md: :obj:`dataretrieval.utils.BaseMetadata`
        A custom metadata object

    Raises
    ------
    ChunkInterrupted
        A transient failure (429 / 5xx / timeout) interrupted the request
        after the built-in retries. Completed work is preserved; resume
        with ``exc.call.resume()`` (see :doc:`/userguide/errors`).

    Examples
    --------
    .. code::

        >>> # Get channel data from a
        >>> # single site from a single year
        >>> df, md = dataretrieval.waterdata.get_channel(
        ...     monitoring_location_id="USGS-02238500",
        ... )
    """
    service = "channel-measurements"

    args = _get_args(locals(), exclude={"max_rows"})

    return get_ogc_data(args, service, max_rows=max_rows)


__all__ = ["get_field_measurements", "get_peaks", "get_channel"]
