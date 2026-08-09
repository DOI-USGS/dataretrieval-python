"""Per-call OGC request state, scoped rather than passed.

The base URL, dialect, and row cap apply to a whole call but are read deep
inside request construction, several frames below whoever set them. Threading
them through every signature would put protocol plumbing in the getters, so they
travel as context variables -- which also makes them safe under the concurrent
fan-out, where a thread-global would not be.
"""

from dataretrieval._ambient import Ambient
from dataretrieval.ogc.policy import DEFAULT_DIALECT, OgcDialect

# Optional cap on rows accumulated by one paginated request.
_row_cap: Ambient[int | None] = Ambient("ogc_row_cap", None)

# OGC base URL targeted by request construction and schema lookup. Empty by
# default *on purpose*: this package is API-neutral, so the adapter naming the
# service is the one that sets it (``get_ogc_data(base_url=...)`` does, and a
# hand-built request path such as ``waterdata.get_cql`` enters this context
# itself). A default endpoint here would silently send a caller that forgot to
# set it -- e.g. an NGWMN path -- to whichever API happened to be the default;
# an unset value instead fails loudly on the malformed URL.
_ogc_base_url: Ambient[str] = Ambient("ogc_base_url", "")

# Per-call request and response dialect.
_dialect: Ambient[OgcDialect] = Ambient("ogc_dialect", DEFAULT_DIALECT)
