"""Accepted argument values for the Water Data getters.

Each ``Literal`` type alias lists the values an argument accepts:
``CODE_SERVICES`` for the Samples code services,
``METADATA_COLLECTIONS`` for the reference-table collections,
``SERVICES`` and ``PROFILES`` for the Samples resources and output profiles,
and ``WATERDATA_COLLECTIONS`` for the collections ``get_cql`` queries.
``PROFILE_LOOKUP`` maps each Samples resource to its valid output profiles.

``WATERDATA_SERVICES`` is the previous name for ``WATERDATA_COLLECTIONS``.
Both names refer to the same type alias.
The previous name remains supported for compatibility and is not scheduled
for removal.

Callers can use these aliases in type annotations.
The getters use the same definitions to validate argument values at runtime.
"""

from typing import Literal, get_args

from dataretrieval._validation import require_one_of

__all__ = [
    "CODE_SERVICES",
    "METADATA_COLLECTIONS",
    "SERVICES",
    "WATERDATA_COLLECTIONS",
    "WATERDATA_SERVICES",
    "PROFILES",
    "PROFILE_LOOKUP",
]


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
    "countries",
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

# OGC API collections queryable via ``get_cql``. Keep in sync with the keys of
# ``utils._OUTPUT_ID_BY_COLLECTION``, which maps each collection to its ``id``
# column and is used by ``get_cql`` to validate the collection argument.
WATERDATA_COLLECTIONS = Literal[
    "channel-measurements",
    "combined-metadata",
    "continuous",
    "daily",
    "field-measurements",
    "field-measurements-metadata",
    "latest-continuous",
    "latest-daily",
    "monitoring-locations",
    "peaks",
    "time-series-metadata",
]

#: Previous name for ``WATERDATA_COLLECTIONS``, retained for compatibility.
#: Both names refer to the same object; neither is scheduled for removal.
WATERDATA_SERVICES = WATERDATA_COLLECTIONS

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


def _check_profiles(
    service: SERVICES,
    profile: PROFILES,
) -> None:
    """Check whether an output profile is valid for a Samples resource.

    Parameters
    ----------
    service : string
        A Samples resource name from ``SERVICES``.
    profile : string
        An output profile name from ``PROFILE_LOOKUP[service]``.
    """
    require_one_of(service, get_args(SERVICES), name="service")
    require_one_of(
        profile,
        PROFILE_LOOKUP[service],
        name="profile",
        context=f"service {service!r}",
    )
