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

# OGC API time-series/monitoring collections queryable via ``get_cql``.
# Keep in sync with ``utils._OUTPUT_ID_BY_COLLECTION`` (same keys): that dict maps
# each service to its user-facing ``id`` column and is the runtime source of
# truth ``get_cql`` validates against.
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

#: Permanent alias. OGC API - Features calls these collections -- the value is
#: the ``collectionId`` in ``/collections/{id}/items`` -- but this name is the one
#: the package published first, so it keeps resolving.
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
    """Check whether a service profile is valid.

    Parameters
    ----------
    service : string
        One of the service names from the "services" list.
    profile : string
        One of the profile names from "results_profiles",
        "locations_profiles", "activities_profiles",
        "projects_profiles" or "organizations_profiles".
    """
    require_one_of(service, get_args(SERVICES), name="service")
    require_one_of(
        profile,
        PROFILE_LOOKUP[service],
        name="profile",
        context=f"service {service!r}",
    )
