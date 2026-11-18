"""Discover and retrieve water data from U.S. federal hydrologic web services.

Access each service through its submodule::

    from dataretrieval import waterdata  # modern USGS Water Data API

    df, meta = waterdata.get_daily(monitoring_location_id="USGS-05427718")

Available service modules: ``waterdata``, ``wqp`` (Water Quality Portal),
``nwdc`` (National Water Availability Assessment Data Companion, incl.
water use), ``nldi``, and ``streamstats``.

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
(the taxonomy is defined in ``dataretrieval.exceptions``); connection-level failures
(timeouts, DNS) are wrapped as :class:`dataretrieval.NetworkError`. A fanned-out
request interrupted partway raises :class:`dataretrieval.FanOutInterrupted`
(also available under its original ``ChunkInterrupted`` name), whose
``.call.resume()`` continues from the work already completed.
"""

from importlib.metadata import PackageNotFoundError, version
from typing import TYPE_CHECKING

try:
    __version__ = version("dataretrieval")
except PackageNotFoundError:
    __version__ = "version-unknown"

# Layered configuration: a ``with configure(...)`` block, the environment, then
# the config file. It is defined in ``dataretrieval.configuration``;
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
# which would make the exceptions module depend on pandas and httpx.
# They are not under ``ogc`` because Water Use raises them too. Re-exported here so
# callers get a stable public path: ``from dataretrieval import ChunkInterrupted``.
from dataretrieval.interruptions import (
    ChunkInterrupted,
    FanOutInterrupted,
    QuotaExhausted,
    ServiceInterrupted,
)

# Parallel-chunks control (a context manager). Defined with the chunker in
# ``dataretrieval.ogc.chunking``; re-exported here for a stable public path
# ``from dataretrieval import parallel_chunks``.
from dataretrieval.ogc.chunking import parallel_chunks

from . import (
    exceptions,
    ngwmn,
    nwdc,
    streamstats,
    utils,
    waterdata,
    wqp,
)

# Shared by the ``nwis`` stub and ``__getattr__`` below (ADR 0005). It lives
# here because the stub raises on import, so its text cannot be imported from it.
_NWIS_REMOVED_MESSAGE = (
    "The dataretrieval.nwis module has been removed because USGS is retiring the "
    "legacy waterservices endpoints. Use dataretrieval.waterdata instead, or "
    "dataretrieval.nwdc for water use. To keep using nwis while the endpoints "
    "remain online, pin the last release that includes it: "
    'pip install "dataretrieval<=1.4.0"'
)

# Hidden from mypy: a module-level ``__getattr__`` would make mypy accept any
# misspelled ``dataretrieval.<name>`` as valid.
if not TYPE_CHECKING:

    def __getattr__(name):
        """Raise ``AttributeError`` for a name the package does not define.

        Called only when normal attribute lookup fails (PEP 562). For
        ``nwis`` the error carries the removal message, so
        ``dataretrieval.nwis`` after ``import dataretrieval`` names the
        replacement; ``hasattr(dataretrieval, "nwis")`` still returns ``False``.
        """
        if name == "nwis":
            raise AttributeError(_NWIS_REMOVED_MESSAGE)
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


__all__ = [
    # layered configuration (defined in ``dataretrieval.configuration``)
    "Configuration",
    "configure",
    "show_configuration",
    "ConfigurationError",
    # service modules
    "ngwmn",
    "nwdc",
    "streamstats",
    "utils",
    "waterdata",
    "wqp",
    # error taxonomy (defined in ``dataretrieval.exceptions``), re-exported
    # so callers can ``except dataretrieval.DataRetrievalError``
    "exceptions",
    "DataCurrencyWarning",
    "DataRetrievalError",
    "HTTPError",
    "NetworkError",
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
