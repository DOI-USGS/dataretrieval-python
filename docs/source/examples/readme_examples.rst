
Retrieving USGS water data with the ``waterdata`` module
--------------------------------------------------------

.. note::

    The ``waterdata`` module accesses the USGS `Water Data API`_ and is the
    recommended way to retrieve USGS water data. The legacy ``nwis`` module
    remains available but is deprecated.

.. _Water Data API: https://api.waterdata.usgs.gov/

.. code:: python

    >>> # import the waterdata module
    >>> from dataretrieval import waterdata

    >>> # a USGS monitoring location id joins the agency code and the site
    >>> # number with a hyphen
    >>> site = "USGS-05427718"

    >>> # get continuous (instantaneous) streamflow — parameter code 00060 —
    >>> # over a one-day window
    >>> df, md = waterdata.get_continuous(
    ...     monitoring_location_id=site,
    ...     parameter_code="00060",
    ...     time="2024-03-01/2024-03-02",
    ... )

    >>> df[["time", "value", "unit_of_measure", "approval_status"]].head()
                            time  value unit_of_measure approval_status
    0 2024-03-01 00:00:00+00:00   18.7          ft^3/s        Approved
    1 2024-03-01 00:15:00+00:00   18.5          ft^3/s        Approved
    2 2024-03-01 00:30:00+00:00   18.5          ft^3/s        Approved
    3 2024-03-01 00:45:00+00:00   18.5          ft^3/s        Approved
    4 2024-03-01 01:00:00+00:00   18.3          ft^3/s        Approved

    >>> # get descriptive metadata about the monitoring location itself
    >>> info, md = waterdata.get_monitoring_locations(
    ...     monitoring_location_id=site,
    ...     skip_geometry=True,
    ... )

    >>> info[["monitoring_location_name", "state_name", "site_type", "drainage_area"]].T
                                                        0
    monitoring_location_name  YAHARA RIVER AT WINDSOR, WI
    state_name                                  Wisconsin
    site_type                                      Stream
    drainage_area                                    73.6
