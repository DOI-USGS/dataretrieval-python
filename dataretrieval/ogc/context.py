"""Ambient per-call OGC request context."""

from dataretrieval.ogc.policy import DEFAULT_DIALECT, OGC_API_URL, OgcDialect
from dataretrieval.utils import Ambient

# Optional cap on rows accumulated by one paginated request.
_row_cap: Ambient[int | None] = Ambient("ogc_row_cap", None)

# OGC base URL targeted by request construction and schema lookup.
_ogc_base_url: Ambient[str] = Ambient("ogc_base_url", OGC_API_URL)

# Per-call request and response dialect.
_dialect: Ambient[OgcDialect] = Ambient("ogc_dialect", DEFAULT_DIALECT)
