"""Enumerations for constrained waterdata API parameters.

Using these enums is optional — plain strings are accepted everywhere — but
they enable IDE autocompletion and make valid values discoverable without
consulting the documentation.

Examples
--------
>>> from dataretrieval.waterdata import SiteTypeCode, StatisticCode
>>> df, _ = get_monitoring_locations(site_type_code=SiteTypeCode.STREAM)
>>> df, _ = get_daily(statistic_id=StatisticCode.MEAN)
"""

from enum import Enum


class SiteTypeCode(str, Enum):
    """Site type codes for monitoring locations.

    Codes correspond to the ``site_type_code`` parameter accepted by
    :func:`~dataretrieval.waterdata.get_monitoring_locations` and related
    functions.  The full reference table is available via
    ``get_reference_table("site-types")``.
    """

    # Primary site types
    AGGREGATE_GROUNDWATER_USE = "AG"
    AGGREGATE_SURFACE_WATER_USE = "AS"
    ATMOSPHERE = "AT"
    AGGREGATE_WATER_USE_ESTABLISHMENT = "AW"
    ESTUARY = "ES"
    GLACIER = "GL"
    WELL = "GW"
    LAND = "LA"
    LAKE = "LK"
    OCEAN = "OC"
    SUBSURFACE = "SB"
    SPRING = "SP"
    STREAM = "ST"
    WETLAND = "WE"

    # Facility secondary types
    FA_ANIMAL_WASTE_LAGOON = "FA-AWL"
    FA_CISTERN = "FA-CI"
    FA_COMBINED_SEWER = "FA-CS"
    FA_DIVERSION = "FA-DV"
    FA_FIELD_PASTURE_ORCHARD_NURSERY = "FA-FON"
    FA_GOLF_COURSE = "FA-GC"
    FA_HYDROELECTRIC_PLANT = "FA-HP"
    FA_LANDFILL = "FA-LF"
    FA_OUTFALL = "FA-OF"
    FA_PAVEMENT = "FA-PV"
    FA_LABORATORY = "FA-QC"
    FA_WASTEWATER_SEWER = "FA-SEW"
    FA_SEPTIC_SYSTEM = "FA-SPS"
    FA_STORM_SEWER = "FA-STS"
    FA_THERMOELECTRIC_PLANT = "FA-TEP"
    FA_WATER_DISTRIBUTION_SYSTEM = "FA-WDS"
    FA_WASTE_INJECTION_WELL = "FA-WIW"
    FA_WATER_SUPPLY_TREATMENT_PLANT = "FA-WTP"
    FA_WATER_USE_ESTABLISHMENT = "FA-WU"
    FA_WASTEWATER_LAND_APPLICATION = "FA-WWD"
    FA_WASTEWATER_TREATMENT_PLANT = "FA-WWTP"

    # Groundwater secondary types
    GW_COLLECTOR_WELL = "GW-CR"
    GW_EXTENSOMETER_WELL = "GW-EX"
    GW_HYPORHEIC_ZONE_WELL = "GW-HZ"
    GW_INTERCONNECTED_WELLS = "GW-IW"
    GW_MULTIPLE_WELLS = "GW-MW"
    GW_TEST_HOLE = "GW-TH"

    # Land secondary types
    LA_EXCAVATION = "LA-EX"
    LA_OUTCROP = "LA-OU"
    LA_PLAYA = "LA-PLY"
    LA_SOIL_HOLE = "LA-SH"
    LA_SINKHOLE = "LA-SNK"
    LA_SHORE = "LA-SR"
    LA_VOLCANIC_VENT = "LA-VOL"

    # Ocean secondary type
    OC_COASTAL = "OC-CO"

    # Subsurface secondary types
    SB_CAVE = "SB-CV"
    SB_GROUNDWATER_DRAIN = "SB-GWD"
    SB_TUNNEL_SHAFT_MINE = "SB-TSM"
    SB_UNSATURATED_ZONE = "SB-UZ"

    # Stream secondary types
    ST_CANAL = "ST-CA"
    ST_DITCH = "ST-DCH"
    ST_TIDAL = "ST-TS"


class StatisticCode(str, Enum):
    """Common statistic codes used by the waterdata time-series endpoints.

    Used in the ``statistic_id`` parameter of
    :func:`~dataretrieval.waterdata.get_daily`,
    :func:`~dataretrieval.waterdata.get_continuous`, and similar functions.
    The full reference table is available via
    ``get_reference_table("statistic-codes")``.
    """

    MAXIMUM = "00001"
    MINIMUM = "00002"
    MEAN = "00003"
    AM = "00004"
    PM = "00005"
    SUM = "00006"
    MODE = "00007"
    MEDIAN = "00008"
    STD = "00009"
    VARIANCE = "00010"
    INSTANTANEOUS = "00011"
    EQUIVALENT_MEAN = "00012"
    SKEWNESS = "00013"
    TIDAL_HIGH_HIGH = "00021"
    TIDAL_LOW_HIGH = "00022"
    TIDAL_HIGH_LOW = "00023"
    TIDAL_LOW_LOW = "00024"
