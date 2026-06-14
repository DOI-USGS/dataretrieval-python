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

A failed request raises a subclass of :class:`dataretrieval.DataRetrievalError`
(the taxonomy lives in ``dataretrieval.exceptions``); connection-level failures
(timeouts, DNS) are wrapped as :class:`dataretrieval.NetworkError`. A large
request interrupted mid-stream raises :class:`dataretrieval.ChunkInterrupted`,
whose ``.call.resume()`` continues from the work already completed.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dataretrieval")
except PackageNotFoundError:
    __version__ = "version-unknown"

from dataretrieval.exceptions import (
    DataRetrievalError,
    HTTPError,
    NetworkError,
    NoSitesError,
    RateLimited,
    RequestTooLarge,
    ServiceUnavailable,
    TransientError,
    Unchunkable,
    URLTooLong,
)

# Resumable chunk-interruption exceptions. They are defined in
# ``dataretrieval.ogc.chunking`` rather than ``dataretrieval.exceptions``
# because they carry pandas/httpx state and a resumable ``ChunkedCall`` handle,
# which would pull heavy dependencies into the lightweight exceptions module.
# Surfaced here so callers get a stable public path:
# ``from dataretrieval import ChunkInterrupted``.
from dataretrieval.ogc.chunking import (
    ChunkInterrupted,
    QuotaExhausted,
    ServiceInterrupted,
)

from . import (
    exceptions,
    nadp,
    ngwmn,
    nwis,
    samples,
    streamstats,
    utils,
    waterdata,
    wqp,
)

__all__ = [
    # service modules
    "nadp",
    "ngwmn",
    "nwis",
    "samples",
    "streamstats",
    "utils",
    "waterdata",
    "wqp",
    # error taxonomy (canonical home: ``dataretrieval.exceptions``), re-exported
    # so callers can ``except dataretrieval.DataRetrievalError``
    "exceptions",
    "DataRetrievalError",
    "HTTPError",
    "NetworkError",
    "NoSitesError",
    "RateLimited",
    "RequestTooLarge",
    "ServiceUnavailable",
    "TransientError",
    "URLTooLong",
    "Unchunkable",
    # resumable chunk-interruption exceptions (defined in ogc.chunking)
    "ChunkInterrupted",
    "QuotaExhausted",
    "ServiceInterrupted",
    "__version__",
]
