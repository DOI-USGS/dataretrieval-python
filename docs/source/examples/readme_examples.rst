
Examples from the Readme file on retrieving NWIS data
-----------------------------------------------------

.. note::

    NWIS stands for the National Water Information System


.. doctest::

    >>> # first import the functions for downloading data from NWIS
    >>> import dataretrieval.nwis as nwis

    >>> # specify the USGS site code for which we want data.
    >>> site = '03339000'

    >>> # get instantaneous values (iv)
    >>> df = nwis.get_record(sites=site, service='iv', start='2017-12-31', end='2018-01-01')

    >>> df.head()
                               00010 00010_cd   site_no  00060 00060_cd  ...  63680_ysi), [discontinued 10/5/21_cd 63680_hach  63680_hach_cd 99133  99133_cd
    datetime                                                             ...
    2017-12-31 06:00:00+00:00    1.0        A  03339000  140.0        A  ...                                     A        3.6              A  4.61         A
    2017-12-31 06:15:00+00:00    1.0        A  03339000  138.0        A  ...                                     A        3.6              A  4.61         A
    2017-12-31 06:30:00+00:00    1.0        A  03339000  139.0        A  ...                                     A        3.4              A  4.61         A
    2017-12-31 06:45:00+00:00    1.0        A  03339000  139.0        A  ...                                     A        3.4              A  4.61         A
    2017-12-31 07:00:00+00:00    1.0        A  03339000  139.0        A  ...                                     A        3.5              A  4.61         A
    <BLANKLINE>
    [5 rows x 21 columns]


    >>> # get basic info about the site
    >>> df3 = nwis.get_record(sites=site, service='site')

    >>> print(df3)
      agency_cd   site_no                         station_nm site_tp_cd  lat_va  long_va  ...  aqfr_cd  aqfr_type_cd well_depth_va hole_depth_va depth_src_cd project_no
    0      USGS  03339000  VERMILION RIVER NEAR DANVILLE, IL         ST  400603   873550  ...      NaN           NaN           NaN           NaN          NaN        100
    <BLANKLINE>
    [1 rows x 42 columns]