from typing import Literal, get_args

__all__ = [
    "CODE_SERVICES",
    "METADATA_COLLECTIONS",
    "SERVICES",
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
# Keep in sync with ``utils._OUTPUT_ID_BY_SERVICE`` (same keys): that dict maps
# each service to its user-facing ``id`` column and is the runtime source of
# truth ``get_cql`` validates against.
WATERDATA_SERVICES = Literal[
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
    valid_services = get_args(SERVICES)
    if service not in valid_services:
        raise ValueError(
            f"Invalid service: '{service}'. Valid options are: {valid_services}."
        )

    valid_profiles = PROFILE_LOOKUP[service]
    if profile not in valid_profiles:
        raise ValueError(
            f"Invalid profile: '{profile}' for service '{service}'. "
            f"Valid options are: {valid_profiles}."
        )
