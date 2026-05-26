
Retrieving site information
---------------------------

The ``waterdata`` module distinguishes a monitoring location's *descriptive*
metadata from the *catalog* of data available at it.

Use ``get_monitoring_locations`` for descriptive metadata — name, location,
site type, drainage area, hydrologic unit, and so on.

.. code:: python

    >>> from dataretrieval import waterdata

    >>> info, md = waterdata.get_monitoring_locations(
    ...     monitoring_location_id="USGS-05427718",
    ...     skip_geometry=True,
    ... )

    >>> info[["monitoring_location_name", "site_type", "drainage_area", "hydrologic_unit_code"]].T
                                                        0
    monitoring_location_name  YAHARA RIVER AT WINDSOR, WI
    site_type                                      Stream
    drainage_area                                    73.6
    hydrologic_unit_code                     070900020504

To discover *what data are available* at a location — the period-of-record
catalog that the legacy ``seriesCatalogOutput`` switch used to provide — use
``get_time_series_metadata``. Each row is one time series; the ``begin`` and
``end`` columns give its period of record.

.. code:: python

    >>> series, md = waterdata.get_time_series_metadata(
    ...     monitoring_location_id="USGS-05427718",
    ...     skip_geometry=True,
    ... )

    >>> len(series)  # number of available time series
    22

    >>> series[["parameter_code", "parameter_name", "computation_period_identifier"]].head()
      parameter_code        parameter_name computation_period_identifier
    0          00045         Precipitation                        Points
    1          91060  Orthophosphate, diss                         Daily
    2          91057     NH3+orgN, wu as N                         Daily
    3          00060             Discharge                        Points
    4          80155   Suspnd sedmnt disch                         Daily
