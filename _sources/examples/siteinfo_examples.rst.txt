
Retrieving site information
---------------------------

By default ``dataretrieval`` fetches the so-called "expanded" site date from
the NWIS web service. However there is an optional keyword parameter called
``seriesCatalogOutput`` that can be set to "True" if you wish to retrieve the
detailed period of record information for a site instead. Refer to the
`NWIS water services documentation`_ for additional information. The below
example illustrates the use of the ``seriesCatalogOutput`` switch and displays
the resulting column names for the output dataframes (example prompted by
`GitHub Issue #34`_).

.. _NWIS water services documentation: https://waterservices.usgs.gov/docs/site-service/site-service-details/

.. _GitHub Issue #34: https://github.com/DOI-USGS/dataretrieval-python/issues/34

.. doctest::

    # first import the functions for downloading data from NWIS
    >>> import dataretrieval.nwis as nwis

    # fetch data from a major HUC basin with seriesCatalogOutput set to True
    >>> df = nwis.get_record(huc='20', parameterCd='00060',
    ...                      service='site', seriesCatalogOutput='True')

    >>> print(df.columns)
    Index(['agency_cd', 'site_no', 'station_nm', 'site_tp_cd', 'dec_lat_va',
           'dec_long_va', 'coord_acy_cd', 'dec_coord_datum_cd', 'alt_va',
           'alt_acy_va', 'alt_datum_cd', 'huc_cd', 'data_type_cd', 'parm_cd',
           'stat_cd', 'ts_id', 'loc_web_ds', 'medium_grp_cd', 'parm_grp_cd',
           'srs_id', 'access_cd', 'begin_date', 'end_date', 'count_nu'],
          dtype='object')

    # repeat the same query with seriesCatalogOutput set as False
    >>> df = nwis.get_record(huc='20', parameterCd='00060',
    ...                      service='site', seriesCatalogOutput='False')

    >>> print(df.columns)
    Index(['agency_cd', 'site_no', 'station_nm', 'site_tp_cd', 'lat_va', 'long_va',
           'dec_lat_va', 'dec_long_va', 'coord_meth_cd', 'coord_acy_cd',
           'coord_datum_cd', 'dec_coord_datum_cd', 'district_cd', 'state_cd',
           'county_cd', 'country_cd', 'land_net_ds', 'map_nm', 'map_scale_fc',
           'alt_va', 'alt_meth_cd', 'alt_acy_va', 'alt_datum_cd', 'huc_cd',
           'basin_cd', 'topo_cd', 'instruments_cd', 'construction_dt',
           'inventory_dt', 'drain_area_va', 'contrib_drain_area_va', 'tz_cd',
           'local_time_fg', 'reliability_cd', 'gw_file_cd', 'nat_aqfr_cd',
           'aqfr_cd', 'aqfr_type_cd', 'well_depth_va', 'hole_depth_va',
           'depth_src_cd', 'project_no'],
          dtype='object')
