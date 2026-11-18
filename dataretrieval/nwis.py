"""Removed: ``dataretrieval.nwis`` is retired.

USGS is decommissioning the legacy waterservices endpoints this module called
(https://waterdata.usgs.gov/blog/api-waterservices-decom/). The module is kept
only so that every spelling of the import -- ``import dataretrieval.nwis``,
``from dataretrieval import nwis``, ``from dataretrieval.nwis import get_dv`` --
fails with a message that names the replacement, rather than a bare
``ModuleNotFoundError``. Attribute access after ``import dataretrieval``
(``dataretrieval.nwis.get_dv()``) never loads this file; the package
``__getattr__`` raises the same message for that case.
"""

from dataretrieval import _NWIS_REMOVED_MESSAGE

raise ImportError(_NWIS_REMOVED_MESSAGE)
