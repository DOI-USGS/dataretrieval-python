"""Discover and retrieve water data from U.S. federal hydrologic web services.

Access each service through its submodule::

    from dataretrieval import waterdata  # modern USGS Water Data API

    df, meta = waterdata.get_daily(monitoring_location_id="USGS-05427718")

    from dataretrieval import nwis  # legacy NWIS services

    df, meta = nwis.get_dv(sites="05427718")

Available service modules: ``waterdata``, ``wqp`` (Water Quality Portal),
``nldi``, ``samples``, ``streamstats``, and the deprecated ``nwis`` and
``nadp``.

``nldi`` requires geopandas (``pip install dataretrieval[nldi]``) and is
imported on demand: ``from dataretrieval import nldi``.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dataretrieval")
except PackageNotFoundError:
    __version__ = "version-unknown"

from . import (
    nadp,
    nwis,
    samples,
    streamstats,
    utils,
    waterdata,
    wqp,
)

__all__ = [
    "nadp",
    "nwis",
    "samples",
    "streamstats",
    "utils",
    "waterdata",
    "wqp",
    "__version__",
]
