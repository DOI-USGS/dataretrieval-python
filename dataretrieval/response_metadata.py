"""The metadata object every getter returns alongside its DataFrame.

A dependency-free leaf on purpose. This class is the second half of the
``(DataFrame, metadata)`` return contract, so nearly every service module needs
it -- and while it lived in :mod:`dataretrieval.utils` beside the legacy query
machinery, needing it meant inheriting that module's whole HTTP stack
(transport, credentials, error policy) transitively. Here it costs its
consumers nothing but ``httpx``.

``dataretrieval.utils.BaseMetadata`` remains a working import.
"""

from __future__ import annotations

from typing import Any

import httpx

__all__ = ["BaseMetadata"]


class BaseMetadata:
    """Base class for metadata.

    Attributes
    ----------
    url : str
        Response url.
    query_time: datetime.timedelta
        Response elapsed time.
    header: httpx.Headers
        Response headers.

    """

    def __init__(self, response: httpx.Response) -> None:
        """Generate a standard set of metadata informed by the response.

        Parameters
        ----------
        response: ``httpx.Response``
            Response object from the ``httpx`` module.

        """

        # Coerce httpx.URL -> str: BaseMetadata.url has always been str.
        self.url = str(response.url)
        self.query_time = response.elapsed
        self.header = response.headers
        self.comment: str | None = None

        # # not sure what statistic_info is
        # self.statistic_info = None

        # # disclaimer seems to be only part of importWaterML1
        # self.disclaimer = None

    # ``site_info`` is set by ``nwis`` / ``wqp``-specific metadata classes; the
    # modern ``waterdata`` metadata leaves it unimplemented (use
    # ``waterdata.get_monitoring_locations`` to retrieve site descriptions).
    @property
    def site_info(self) -> Any:
        raise NotImplementedError(
            "site_info must be implemented by BaseMetadata children"
        )

    def __repr__(self) -> str:
        return f"{type(self).__name__}(url={self.url})"
