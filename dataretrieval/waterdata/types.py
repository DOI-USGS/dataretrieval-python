from collections.abc import Iterable
from typing import Literal, Optional, Union

# Multi-value string filter: accepts a single string (single value),
# or any iterable of strings (list, tuple, ``pandas.Series``,
# ``numpy.ndarray``, generator). Iterables are materialized to a list
# internally; the OGC API receives the value(s) comma-joined in the URL
# — or, if the list is long enough to overflow the URL, the request
# switches to POST/CQL2.
StringFilter = Optional[Union[str, Iterable[str]]]

# A list of string property/column names, comma-joined into the URL.
# Unlike ``StringFilter``, a single string passed here would be iterated
# as characters by ``",".join(...)`` and produce a malformed URL — so
# the type explicitly excludes ``str``. ``_get_args`` does wrap a stray
# single-string input into a one-element list at runtime as a
# convenience, but users are encouraged to pass a list.
StringList = Optional[Iterable[str]]

CODE_SERVICES = Literal[
    "characteristicgroup",
    "characteristics",
    "counties",
    "countries",
    "observedproperty",
    "samplemedia",
    "sitetype",
    "states",
]

METADATA_COLLECTIONS = Literal[
    "agency-codes",
    "altitude-datums",
    "aquifer-codes",
    "aquifer-types",
    "coordinate-accuracy-codes",
    "coordinate-datum-codes",
    "coordinate-method-codes",
    "counties",
    "hydrologic-unit-codes",
    "medium-codes",
    "national-aquifer-codes",
    "parameter-codes",
    "reliability-codes",
    "site-types",
    "states",
    "statistic-codes",
    "topographic-codes",
    "time-zone-codes",
]

SERVICES = Literal[
    "activities",
    "locations",
    "organizations",
    "projects",
    "results",
]

PROFILES = Literal[
    "actgroup",
    "actmetric",
    "basicbio",
    "basicphyschem",
    "count",
    "fullbio",
    "fullphyschem",
    "labsampleprep",
    "narrow",
    "organization",
    "project",
    "projectmonitoringlocationweight",
    "resultdetectionquantitationlimit",
    "sampact",
    "site",
]

PROFILE_LOOKUP = {
    "activities": ["sampact", "actmetric", "actgroup", "count"],
    "locations": ["site", "count"],
    "organizations": ["organization", "count"],
    "projects": ["project", "projectmonitoringlocationweight"],
    "results": [
        "fullphyschem",
        "basicphyschem",
        "fullbio",
        "basicbio",
        "narrow",
        "resultdetectionquantitationlimit",
        "labsampleprep",
        "count",
    ],
}
