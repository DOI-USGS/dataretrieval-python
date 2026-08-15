"""Discover and retrieve water data from U.S. federal hydrologic web services.

Access each service through its submodule::

    from dataretrieval import waterdata  # modern USGS Water Data API

    df, meta = waterdata.get_daily(monitoring_location_id="USGS-05427718")

    from dataretrieval import nwis  # legacy NWIS services

    df, meta = nwis.get_dv(sites="05427718")

Available service modules: ``waterdata``, ``wqp`` (Water Quality Portal),
``nwdc`` (National Water Availability Assessment Data Companion, incl.
water use), ``nldi``, ``streamstats``, and the
deprecated ``nwis``.

``nldi`` requires geopandas (``pip install dataretrieval[nldi]``) and is
imported on demand: ``from dataretrieval import nldi``.

Settings -- the Water Data API key, fan-out concurrency, retries, the progress
line -- resolve through :mod:`dataretrieval.configuration`: a
``with dataretrieval.configure(Configuration(...))`` block, then the
``API_USGS_*`` environment variables, then ``~/.dataretrieval/config.toml``.
A setting for one service goes on that adapter's own configuration, such as
``waterdata.WaterdataConfiguration``. ``dataretrieval.show_configuration()``
reports what is in effect and where each value came from.

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

# Layered configuration: a ``with configure(...)`` block, the environment, then
# the config file. The canonical home is ``dataretrieval.configuration``;
# the callable is named ``configure`` so it doesn't shadow that module.
#
# The module itself is deliberately absent from ``__all__`` below: it and the
# ``Configuration`` class differ only by case, and keeping the module out of the
# package's exports means ``from dataretrieval import configuration,
# Configuration`` never arises (ADR 0011).
from dataretrieval.configuration import Configuration, configure, show_configuration
from dataretrieval.exceptions import (
    ConfigurationError,
    DataCurrencyWarning,
    DataRetrievalError,
    HTTPError,
    NetworkError,
    NoSitesError,
    RateLimited,
    RequestTooLarge,
    ServiceUnavailable,
    SkippedItemWarning,
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
    nwdc,
    nwis,
    streamstats,
    utils,
    waterdata,
    wqp,
)

__all__ = [
    # layered configuration (canonical home: ``dataretrieval.configuration``)
    "Configuration",
    "configure",
    "show_configuration",
    "ConfigurationError",
    # service modules
    "ngwmn",
    "nwdc",
    "nwis",
    "streamstats",
    "utils",
    "waterdata",
    "wqp",
    # error taxonomy (canonical home: ``dataretrieval.exceptions``), re-exported
    # so callers can ``except dataretrieval.DataRetrievalError``
    "exceptions",
    "DataCurrencyWarning",
    "DataRetrievalError",
    "HTTPError",
    "NetworkError",
    "NoSitesError",
    "RateLimited",
    "RequestTooLarge",
    "ServiceUnavailable",
    "SkippedItemWarning",
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
