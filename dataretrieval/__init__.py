"""Discover and retrieve water data from U.S. federal hydrologic web services.

Access each service through its submodule::

    from dataretrieval import waterdata  # modern USGS Water Data API

    df, meta = waterdata.get_daily(monitoring_location_id="USGS-05427718")

    from dataretrieval import nwis  # legacy NWIS services

    df, meta = nwis.get_dv(sites="05427718")

Available service modules: ``waterdata``, ``wqp`` (Water Quality Portal),
``wateruse`` (NWDC water-use data), ``nldi``, ``streamstats``, and the
deprecated ``nwis``.

``nldi`` requires geopandas (``pip install dataretrieval[nldi]``) and is
imported on demand: ``from dataretrieval import nldi``.

A failed request raises a subclass of :class:`dataretrieval.DataRetrievalError`
(the taxonomy lives in ``dataretrieval.exceptions``); connection-level failures
(timeouts, DNS) are wrapped as :class:`dataretrieval.NetworkError`. A fanned-out
request interrupted mid-stream raises :class:`dataretrieval.FanOutInterrupted`
(also available under its original ``ChunkInterrupted`` name), whose
``.call.resume()`` continues from the work already completed.
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dataretrieval")
except PackageNotFoundError:
    __version__ = "version-unknown"

from dataretrieval.exceptions import (
    ConfigurationError,
    DataRetrievalError,
    HTTPError,
    NetworkError,
    NoSitesError,
    RateLimited,
    RequestTooLarge,
    ServiceUnavailable,
    SkippedRatingWarning,
    TransientError,
    Unchunkable,
    URLTooLong,
)

# Resumable fan-out interruption exceptions. They are defined in
# ``dataretrieval.interruptions`` rather than ``dataretrieval.exceptions``
# because they carry pandas/httpx state and a resumable ``FanOut`` handle,
# which would pull heavy dependencies into the lightweight exceptions module.
# They are not under ``ogc`` because Water Use raises them too. Surfaced here so
# callers get a stable public path: ``from dataretrieval import ChunkInterrupted``.
from dataretrieval.interruptions import (
    ChunkInterrupted,
    FanOutInterrupted,
    QuotaExhausted,
    ServiceInterrupted,
)

# Parallel-chunks control (a context manager). Defined with the chunker in
# ``dataretrieval.ogc.chunking``; surfaced here for a stable public path
# ``from dataretrieval import parallel_chunks``.
from dataretrieval.ogc.chunking import parallel_chunks

from . import (
    exceptions,
    ngwmn,
    nwis,
    streamstats,
    utils,
    waterdata,
    wateruse,
    wqp,
)

__all__ = [
    # service modules
    "ngwmn",
    "nwis",
    "streamstats",
    "utils",
    "waterdata",
    "wateruse",
    "wqp",
    # error taxonomy (canonical home: ``dataretrieval.exceptions``), re-exported
    # so callers can ``except dataretrieval.DataRetrievalError``
    "exceptions",
    "ConfigurationError",
    "DataRetrievalError",
    "HTTPError",
    "NetworkError",
    "NoSitesError",
    "RateLimited",
    "RequestTooLarge",
    "ServiceUnavailable",
    "SkippedRatingWarning",
    "TransientError",
    "URLTooLong",
    "Unchunkable",
    # resumable fan-out interruption exceptions (defined in interruptions)
    "ChunkInterrupted",
    "FanOutInterrupted",
    "QuotaExhausted",
    "ServiceInterrupted",
    # parallel-chunks control (defined in ogc.chunking)
    "parallel_chunks",
    "__version__",
]
