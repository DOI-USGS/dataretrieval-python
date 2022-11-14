
Examples from the Readme file on retrieving NWIS data
-----------------------------------------------------

.. note::

    NWIS stands for the National Water Information System


.. doctest::

    # first import the functions for downloading data from NWIS
    import dataretrieval.nwis as nwis

    # specify the USGS site code for which we want data.
    site = '03339000'


    # get instantaneous values (iv)
    df = nwis.get_record(sites=site, service='iv', start='2017-12-31', end='2018-01-01')

    # get water quality samples (qwdata)
    df2 = nwis.get_record(sites=site, service='qwdata', start='2017-12-31', end='2018-01-01')

    # get basic info about the site
    df3 = nwis.get_record(sites=site, service='site')