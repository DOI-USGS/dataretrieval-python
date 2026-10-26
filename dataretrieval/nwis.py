"""Removed: ``dataretrieval.nwis`` is retired.

USGS is decommissioning the legacy waterservices endpoints this module called
(https://waterdata.usgs.gov/blog/api-waterservices-decom/). The module is kept
only so that every spelling of the import -- ``import dataretrieval.nwis``,
``from dataretrieval import nwis``, ``from dataretrieval.nwis import get_dv`` --
fails with a message that names the replacement, rather than a bare
``ModuleNotFoundError``.
"""

raise ImportError(
    "dataretrieval.nwis has been removed, because USGS is retiring the legacy "
    "waterservices endpoints it called. Use dataretrieval.waterdata instead, or "
    "dataretrieval.nwdc for water use. To keep using nwis while the endpoints "
    "remain online, pin the last release that includes it: "
    'pip install "dataretrieval<=1.4.0"'
)
