.. examples:

========
Examples
========

Introduction to the ``waterdata`` module of ``dataretrieval``
-------------------------------------------------------------
The ``waterdata`` module will replace the ``nwis`` module as the primary
set of data download functions for USGS water data. This Jupyter notebook
covers a basic introduction to module functions and usage.

.. toctree::
    :maxdepth: 1

    WaterData_demo

USGS Water Data API vignettes
-----------------------------
These notebooks are Python ports of the new USGS Water Data API vignettes from
the R `dataRetrieval`_ package. Each introduces a family of Water Data API
functions and is executed against the live USGS Water Data API.

.. _dataRetrieval: https://doi-usgs.github.io/dataRetrieval/

.. toctree::
    :maxdepth: 1

    USGS_WaterData_Introduction_Examples
    USGS_WaterData_DiscreteSamples_Examples
    USGS_WaterData_DailyStatistics_Examples
    USGS_WaterData_ContinuousData_Examples
    USGS_WaterData_ReferenceLists_Examples
    USGS_NGWMN_Examples

Simple uses of the ``dataretrieval`` package
--------------------------------------------

.. toctree::
    :maxdepth: 2

    readme_examples
    siteinfo_examples


Example Notebooks from Hydroshare
---------------------------------
A set of Jupyter Notebooks with Python code examples on how to use the
``dataretrieval`` package are available on the `Hydroshare`_ platform.
We provide executed versions of these notebooks below; to download the
``.ipynb`` files for your own use, either visit the `Hydroshare`_ repository,
or navigate to the `demos/hydroshare`_ subdirectory of the ``dataretrieval``
project repository.

.. _Hydroshare: https://www.hydroshare.org/resource/c97c32ecf59b4dff90ef013030c54264/

.. _demos/hydroshare: https://github.com/DOI-USGS/dataretrieval-python/tree/main/demos/hydroshare

.. toctree::
    :maxdepth: 1

    USGS_WaterData_DailyValues_Examples
    USGS_WaterData_GroundwaterLevels_Examples
    USGS_WaterData_Measurements_Examples
    USGS_WaterData_ParameterCodes_Examples
    USGS_WaterData_Peaks_Examples
    USGS_WaterData_Ratings_Examples
    USGS_WaterData_SiteInfo_Examples
    USGS_WaterData_SiteInventory_Examples
    USGS_WaterData_Statistics_Examples
    USGS_WaterData_UnitValues_Examples
    USGS_WaterData_Samples_Examples
    USGS_NWIS_WaterUse_Examples


Using ``dataretrieval`` to obtain nation trends in peak annual streamflow
-------------------------------------------------------------------------

.. toctree::
    :maxdepth: 2

    peak_streamflow_trends


Duplicating the R ``dataRetrieval`` vignettes functionality
-----------------------------------------------------------

.. note::

    Some of the larger (e.g., state-wide) examples have been commented out
    in the interest of run-time for the notebook.

.. toctree::
    :maxdepth: 2

    rvignettes
