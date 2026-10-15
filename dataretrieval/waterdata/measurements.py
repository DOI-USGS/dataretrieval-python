"""Getters for values measured in person rather than by a sensor.

Field measurements, annual peaks, and channel geometry. These are collected
during site visits, at low frequency and with delivery lag, which is why they
are grouped apart from the continuous record they help calibrate.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import TYPE_CHECKING, Any

import pandas as pd

from dataretrieval.waterdata.utils import (
    _get_args,
    get_ogc_data,
)

if TYPE_CHECKING:
    from dataretrieval._response_metadata import BaseMetadata
    from dataretrieval.ogc.filters import FILTER_LANG


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
    *,
    control_condition: str | Iterable[str] | None = None,
    day: int | list[int] | None = None,
    field_measurements_series_id: str | Iterable[str] | None = None,
    measurement_rated: str | Iterable[str] | None = None,
    month: int | list[int] | None = None,
    reading_type: str | Iterable[str] | None = None,
    time_of_day: str | Iterable[str] | None = None,
    year: int | list[int] | None = None,
    sublocation_identifier: str | Iterable[str] | None = None,
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
        A unique identifier representing a single monitoring location. This corresponds
        to the ``id`` field in the ``monitoring-locations`` endpoint. Monitoring
        location IDs are created by combining the agency code of the agency responsible
        for the monitoring location (e.g. USGS) with the ID number of the monitoring
        location (e.g. 02238500), separated by a hyphen (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent measured and
        the units of measure. A complete list of parameter codes and associated
        groupings can be found at
        https://api.waterdata.usgs.gov/ogcapi/v1/collections/parameter-codes/items.
    observing_procedure_code : string or iterable of strings, optional
        A short code corresponding to the observing procedure for the field
        measurement.
    control_condition : string or iterable of strings, optional
        The state of the control feature at the time of observation.
    day : integer or list of integers, optional
        The day of the month the field measurement was taken. If null, the day is
        unknown.
    field_measurements_series_id : string or iterable of strings, optional
        A unique identifier representing a single collection series. This corresponds to
        the ``id`` field in the ``field-measurements-metadata`` endpoint. Collection
        series are defined as the set of field measurements at a given monitoring
        location for a single parameter code using a single reading type.
    measurement_rated : string or iterable of strings, optional
        A qualitative estimate of the quality of a measurement.
    month : integer or list of integers, optional
        The calendar month the field measurement was taken. If null, the month is
        unknown.
    reading_type : string or iterable of strings, optional
        Distinguishes field-measurement readings from measurements. Readings have a
        value of ReferencePrimary; measurements are Discharge or MeanGageHeight.
    time_of_day : string or iterable of strings, optional
        The time of day the field measurement was taken. If null, the time of day is
        unknown. The time column holds only the date.
    year : integer or list of integers, optional
        The calendar year the field measurement was taken.
    properties : string or iterable of strings, optional
        The columns to return from the query. See the
        field-measurements schema in the OpenAPI reference for the available
        columns (e.g. geometry, id, monitoring_location_id, parameter_code,
        value, unit_of_measure, approval_status, qualifier, last_modified):
        https://api.waterdata.usgs.gov/ogcapi/v1/openapi?f=html#/field-measurements
    field_visit_id : string or iterable of strings, optional
        A universally unique identifier (UUID) for the field visit.
        Multiple measurements may be made during a single field visit.
    approval_status : string or iterable of strings, optional
        Some of the data that you have obtained from this U.S. Geological Survey
        database may not have received Director's approval. Any such data values are
        qualified as provisional and are subject to revision. Provisional data are
        released on the condition that neither the USGS nor the United States Government
        may be held liable for any damages resulting from its use. This field reflects
        the approval status of each record, and is either "Approved", meaining
        processing review has been completed and the data is approved for publication,
        or "Provisional" and subject to revision. For more information about provisional
        data, go to https://waterdata.usgs.gov/provisional-data-statement/.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated
        with an observation.
    qualifier : string or iterable of strings, optional
        This field indicates any qualifiers associated with an observation, for instance
        if a sensor may have been impacted by ice or if values were estimated.

        Matches observations whose qualifiers are exactly these, in this order:
        ``"ICE"`` matches ``["ICE"]`` but not ``["ESTIMATED", "ICE"]``.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in the JSON
        response format in order to preserve precision.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen due to
        regular operational processes and does not necessarily indicate anything about
        the measurement has changed. You can query this field using date-times or
        intervals, adhering to RFC 3339, or using ISO 8601 duration objects. Intervals
        may be bounded or half-bounded (double-dots at start or end).
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
              "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the
              last 36 hours

        Only features that have a ``last_modified`` that intersects the value of
        datetime are selected.

        A list of two values is also accepted: ["2018-02-12", "2018-03-18"], with None
        for an open end: ["2018-02-12", None].

    observing_procedure : string or iterable of strings, optional
        Water measurement or water-quality observing procedure descriptions.
    vertical_datum : string or iterable of strings, optional
        The datum used to determine altitude and vertical position at the monitoring
        location. A list of codes is available at
        https://api.waterdata.usgs.gov/ogcapi/v1/collections/altitude-datums/items.
    measuring_agency : string or iterable of strings, optional
        The agency performing the measurement.
    skip_geometry : boolean, optional
        If True, the response omits the geometry of each feature and the
        returned object is a data frame with no spatial information. The USGS
        Water Data APIs use camelCase "skipGeometry" in CQL2 queries.
    time : string, optional
        The date an observation represents. You can query this field using date-times or
        intervals, adhering to RFC 3339, or using ISO 8601 duration objects. Intervals
        may be bounded or half-bounded (double-dots at start or end).
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
              "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the
              last 36 hours

        Only features that have a ``time`` that intersects the value of datetime are
        selected. If a feature has multiple temporal properties, it is the decision of
        the server whether only a single temporal property is used to determine the
        extent or all relevant temporal properties.

        A list of two values is also accepted: ["2018-02-12", "2018-03-18"], with None
        for an open end: ["2018-02-12", None].

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
        number if your internet connection is unreliable. This is a per-page size,
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
    sublocation_identifier : string or iterable of strings, optional
        An optional human-readable identifier used to specify where measurements are
        recorded at a monitoring location.

        For example "Primary", "Backup", or "HWM"; null when none is recorded.
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
    collection = "field-measurements"

    # Build argument dictionary, omitting None values
    args = _get_args(locals(), exclude={"max_rows"})

    return get_ogc_data(args, collection, max_rows=max_rows)


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
    *,
    qualifier: str | Iterable[str] | None = None,
    time_of_day: str | Iterable[str] | None = None,
    value: str | Iterable[str] | None = None,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get the annual peak streamflow / stage record for a monitoring location.

    Peaks are the largest values observed at a monitoring location each water
    year and are the standard input to flood-frequency analysis (e.g.
    log-Pearson Type III fits). The endpoint returns one row per (monitoring
    location, parameter, water year), with the peak ``value`` and the ``time``
    it occurred.

    The collection covers both stage (parameter ``"00065"``, ``ft``) and
    discharge (parameter ``"00060"``, ``ft^3/s``); a typical streamgage has a
    series for each. Reference docs:
    https://api.waterdata.usgs.gov/ogcapi/v1/openapi?f=html#/peaks

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This corresponds
        to the ``id`` field in the ``monitoring-locations`` endpoint. Monitoring
        location IDs are created by combining the agency code of the agency responsible
        for the monitoring location (e.g. USGS) with the ID number of the monitoring
        location (e.g. 02238500), separated by a hyphen (e.g. USGS-02238500).
    parameter_code : string or iterable of strings, optional
        Parameter codes are 5-digit codes used to identify the constituent measured and
        the units of measure. A complete list of parameter codes and associated
        groupings can be found at
        https://api.waterdata.usgs.gov/ogcapi/v1/collections/parameter-codes/items.

        Most peaks records are ``"00060"`` (discharge) or ``"00065"`` (stage / gage
        height).
    time_series_id : string or iterable of strings, optional
        A unique identifier representing a single time series. This corresponds to the
        ``id`` field in the ``time-series-metadata`` endpoint.
    unit_of_measure : string or iterable of strings, optional
        A human-readable description of the units of measurement associated with an
        observation.

        For example ``"ft^3/s"`` or ``"ft"``.
    time : string, optional
        The date an observation represents. You can query this field using date-times or
        intervals, adhering to RFC 3339, or using ISO 8601 duration objects. Intervals
        may be bounded or half-bounded (double-dots at start or end).
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
              "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the
              last 36 hours

        Only features that have a ``time`` that intersects the value of datetime are
        selected. If a feature has multiple temporal properties, it is the decision of
        the server whether only a single temporal property is used to determine the
        extent or all relevant temporal properties.

        A list of two values is also accepted: ["2018-02-12", "2018-03-18"], with None
        for an open end: ["2018-02-12", None].
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen due to
        regular operational processes and does not necessarily indicate anything about
        the measurement has changed. You can query this field using date-times or
        intervals, adhering to RFC 3339, or using ISO 8601 duration objects. Intervals
        may be bounded or half-bounded (double-dots at start or end).
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
              "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the
              last 36 hours

        Only features that have a ``last_modified`` that intersects the value of
        datetime are selected.

        Useful for incremental ETL polling. A list of two values is also accepted:
        ["2018-02-12", "2018-03-18"], with None for an open end: ["2018-02-12", None].
    water_year : int or list of ints, optional
        The water year (running from October 1st to September 30th) a peak occurred.

        For example, water year 2024 runs from October 1, 2023 to September 30, 2024.
    year : int or list of ints, optional
        The calendar year a peak occurred.
    month : int or list of ints, optional
        The calendar month a peak occurred. If null, the month a peak occurred is
        unknown.
    day : int or list of ints, optional
        The day of the month a peak occurred. If null, the day a peak occurred is
        unknown.
    peak_since : int or list of ints, optional
        If not null, this record represents the peak value for the parameter code since
        the year contained in "peak_since".

        The API serves this field as an integer; many rows are ``null``.
    qualifier : string or iterable of strings, optional
        This field indicates any qualifiers associated with an observation, for instance
        if a sensor may have been impacted by ice or if values were estimated.

        Matches observations whose qualifiers are exactly these, in this order:
        ``"ICE"`` matches ``["ICE"]`` but not ``["ESTIMATED", "ICE"]``.
    time_of_day : string or iterable of strings, optional
        The time of day a peak occurred. If null, the time of day a peak occurred is
        unknown.

        This is common for historical peaks recorded only to the day.
    value : string or iterable of strings, optional
        The value of the observation. Values are transmitted as strings in the JSON
        response format in order to preserve precision.
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

        >>> # Full annual peak record at one location (stage and discharge)
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
    collection = "peaks"

    args = _get_args(locals(), exclude={"max_rows"})

    return get_ogc_data(args, collection, max_rows=max_rows)


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
    *,
    channel_location_direction: str | Iterable[str] | None = None,
    **queryables: Any,
) -> tuple[pd.DataFrame, BaseMetadata]:
    """Get channel-geometry measurements recorded during streamflow field visits.

    Parameters
    ----------
    monitoring_location_id : string or iterable of strings, optional
        A unique identifier representing a single monitoring location. This corresponds
        to the ``id`` field in the ``monitoring-locations`` endpoint. Monitoring
        location IDs are created by combining the agency code of the agency responsible
        for the monitoring location (e.g. USGS) with the ID number of the monitoring
        location (e.g. 02238500), separated by a hyphen (e.g. USGS-02238500).
    field_visit_id : string or iterable of strings, optional
        A universally unique identifier (UUID) for the field visit. Multiple
        measurements may be made during a single field visit.
    measurement_number : string or iterable of strings, optional
        Measurement number.
    time : string or iterable of strings, optional
        The date an observation represents. You can query this field using date-times or
        intervals, adhering to RFC 3339, or using ISO 8601 duration objects. Intervals
        may be bounded or half-bounded (double-dots at start or end).
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
              "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the
              last 36 hours

        Only features that have a ``time`` that intersects the value of datetime are
        selected. If a feature has multiple temporal properties, it is the decision of
        the server whether only a single temporal property is used to determine the
        extent or all relevant temporal properties.

        A list of two values is also accepted: ["2018-02-12", "2018-03-18"], with None
        for an open end: ["2018-02-12", None].

    channel_name : string or iterable of strings, optional
        The channel name.
    channel_flow : string or iterable of strings, optional
        Channel discharge.
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
        The measurement type.
    last_modified : string, optional
        The last time a record was refreshed in our database. This may happen due to
        regular operational processes and does not necessarily indicate anything about
        the measurement has changed. You can query this field using date-times or
        intervals, adhering to RFC 3339, or using ISO 8601 duration objects. Intervals
        may be bounded or half-bounded (double-dots at start or end).
        Examples:

            * A date-time: "2018-02-12T23:20:50Z"
            * A bounded interval: "2018-02-12T00:00:00Z/2018-03-18T12:31:12Z"
            * Half-bounded intervals: "2018-02-12T00:00:00Z/.." or
              "../2018-03-18T12:31:12Z"
            * Duration objects: "P1M" for data from the past month or "PT36H" for the
              last 36 hours

        Only features that have a ``last_modified`` that intersects the value of
        datetime are selected.

        A list of two values is also accepted: ["2018-02-12", "2018-03-18"], with None
        for an open end: ["2018-02-12", None].

    skip_geometry : boolean, optional
        If True, the response omits the geometry of each feature and the
        returned object is a data frame with no spatial information. The USGS
        Water Data APIs use camelCase "skipGeometry" in CQL2 queries.
    channel_measurement_type : string or iterable of strings, optional
        The channel measurement type.
    channel_location_direction : string or iterable of strings, optional
        Location of the measurement from the gage.
    properties : string or iterable of strings, optional
        The columns to return from the query. Available
        options are: geometry, channel_measurements_id, monitoring_location_id,
        field_visit_id, measurement_number, time, channel_name, channel_flow,
        channel_flow_unit, channel_width, channel_width_unit, channel_area,
        channel_area_unit, channel_velocity, channel_velocity_unit,
        channel_location_distance, channel_location_distance_unit, channel_stability,
        channel_material, channel_evenness, horizontal_velocity_description,
        vertical_velocity_description, longitudinal_velocity_description,
        measurement_type, last_modified, channel_measurement_type,
        channel_location_direction. The default (None) returns all columns.
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
        number if your internet connection is unreliable. This is a per-page size,
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
    collection = "channel-measurements"

    args = _get_args(locals(), exclude={"max_rows"})

    return get_ogc_data(args, collection, max_rows=max_rows)


__all__ = ["get_field_measurements", "get_peaks", "get_channel"]
