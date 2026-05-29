from typing import Literal

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


# --- CF / xarray vocabulary mappings ---------------------------------------
# Lookup tables used by :mod:`dataretrieval.waterdata.xarray` to translate
# USGS terms into CF-conventions metadata. Each is intentionally partial:
# anything not listed falls back to a sensible default (raw unit string kept
# verbatim; no standard_name emitted) rather than guessing a wrong CF term.
# They are plain data, so they live here rather than in the (xarray-optional)
# converter module and can be extended without importing xarray.

# USGS unit strings -> UDUNITS / CF-canonical form.
CF_UNIT_MAP = {
    "ft^3/s": "ft3 s-1",
    "ft3/s": "ft3 s-1",
    "ft": "ft",
    "in": "in",
    "degC": "degC",
    "deg C": "degC",
    "uS/cm": "uS/cm",
    "mg/l": "mg L-1",
    "mg/L": "mg L-1",
    # UDUNITS 'ton' is the US short ton; 'short_ton' is not a valid UDUNITS name.
    "tons/day": "ton day-1",
    "%": "percent",
}

# USGS statistic_id -> the operator in a CF ``cell_methods`` string.
CF_CELL_METHODS = {
    "00001": "maximum",
    "00002": "minimum",
    "00003": "mean",
    "00006": "sum",
    "00008": "median",
    "00011": "point",  # instantaneous
}

# USGS 5-digit parameter code -> CF standard_name. Deliberately conservative;
# codes without a confident match are left without a standard_name.
CF_STANDARD_NAMES = {
    "00060": "water_volume_transport_in_river_channel",
    # 00010 (water temperature) is intentionally omitted: ``water_temperature``
    # is NOT a CF standard name, and the only valid CF water-temperature name,
    # ``sea_water_temperature``, is wrong-domain for USGS freshwater/groundwater.
    # Leaving it unmapped keeps the variable's ``long_name`` without emitting an
    # invalid or misleading ``standard_name``.
    "00065": "water_surface_height_above_reference_datum",
    "63160": "water_surface_height_above_reference_datum",
    "00045": "lwe_thickness_of_precipitation_amount",
}

# USGS parameter code -> vertical reference datum, attached as a
# ``vertical_datum`` attribute. The two water-surface-height parameters share
# the CF standard_name water_surface_height_above_reference_datum, so the datum
# distinguishes them: gage height (00065) is measured from a local site (gage)
# datum, while stream water level (63160) is referenced to NAVD88.
CF_VERTICAL_DATUM = {
    "00065": "local site datum",
    "63160": "NAVD88",
}
