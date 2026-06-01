"""Functions for downloading data from the USGS Aquarius Samples database
(https://waterdata.usgs.gov/download-samples/).

See https://api.waterdata.usgs.gov/samples-data/docs#/ for API reference
"""

from __future__ import annotations

import warnings
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

    from dataretrieval.utils import BaseMetadata


def get_usgs_samples(**kwargs: Any) -> tuple[pd.DataFrame, BaseMetadata]:
    """Deprecated: use ``waterdata.get_samples()`` instead.

    All keyword arguments are forwarded directly to
    :func:`dataretrieval.waterdata.get_samples`.
    """
    warnings.warn(
        "`get_usgs_samples` is deprecated and will be removed. "
        "Use `waterdata.get_samples` instead.",
        DeprecationWarning,
        stacklevel=2,
    )

    from dataretrieval.waterdata import get_samples

    return get_samples(**kwargs)
