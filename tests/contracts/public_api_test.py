"""Public import, export, and signature contracts for Water Data."""
# ruff: noqa: E501

from __future__ import annotations

import inspect

from dataretrieval import waterdata
from dataretrieval.waterdata import api

_EXPECTED_WATERDATA_ALL = [
    "CODE_SERVICES",
    "FILTER_LANG",
    "PROFILES",
    "PROFILE_LOOKUP",
    "SERVICES",
    "WATERDATA_SERVICES",
    "parallel_chunks",
    "get_channel",
    "get_codes",
    "get_combined_metadata",
    "get_continuous",
    "get_cql",
    "get_daily",
    "get_field_measurements",
    "get_field_measurements_metadata",
    "get_latest_continuous",
    "get_latest_daily",
    "get_monitoring_locations",
    "get_nearest_continuous",
    "get_peaks",
    "get_queryables",
    "get_ratings",
    "get_reference_table",
    "get_samples",
    "get_samples_summary",
    "get_stats_date_range",
    "get_stats_por",
    "get_time_series_metadata",
]

_EXPECTED_API_NAMES = [
    "get_channel",
    "get_codes",
    "get_combined_metadata",
    "get_continuous",
    "get_cql",
    "get_daily",
    "get_field_measurements",
    "get_field_measurements_metadata",
    "get_latest_continuous",
    "get_latest_daily",
    "get_monitoring_locations",
    "get_peaks",
    "get_queryables",
    "get_reference_table",
    "get_samples",
    "get_samples_summary",
    "get_stats_date_range",
    "get_stats_por",
    "get_time_series_metadata",
]

_EXPECTED_SIGNATURES = {
    "get_channel": "(monitoring_location_id: 'str | Iterable[str] | None' = None, field_visit_id: 'str | Iterable[str] | "
    "None' = None, measurement_number: 'str | Iterable[str] | None' = None, time: 'str | Iterable[str] | "
    "None' = None, channel_name: 'str | Iterable[str] | None' = None, channel_flow: 'str | Iterable[str] | "
    "None' = None, channel_flow_unit: 'str | Iterable[str] | None' = None, channel_width: 'str | "
    "Iterable[str] | None' = None, channel_width_unit: 'str | Iterable[str] | None' = None, channel_area: "
    "'str | Iterable[str] | None' = None, channel_area_unit: 'str | Iterable[str] | None' = None, "
    "channel_velocity: 'str | Iterable[str] | None' = None, channel_velocity_unit: 'str | Iterable[str] | "
    "None' = None, channel_location_distance: 'str | Iterable[str] | None' = None, "
    "channel_location_distance_unit: 'str | Iterable[str] | None' = None, channel_stability: 'str | "
    "Iterable[str] | None' = None, channel_material: 'str | Iterable[str] | None' = None, "
    "channel_evenness: 'str | Iterable[str] | None' = None, horizontal_velocity_description: 'str | "
    "Iterable[str] | None' = None, vertical_velocity_description: 'str | Iterable[str] | None' = None, "
    "longitudinal_velocity_description: 'str | Iterable[str] | None' = None, measurement_type: 'str | "
    "Iterable[str] | None' = None, last_modified: 'str | Iterable[str] | None' = None, "
    "channel_measurement_type: 'str | Iterable[str] | None' = None, properties: 'str | Iterable[str] | "
    "None' = None, skip_geometry: 'bool | None' = None, bbox: 'list[float] | None' = None, limit: 'int | "
    "None' = None, filter: 'str | None' = None, filter_lang: 'FILTER_LANG | None' = None, convert_type: "
    "'bool' = True, max_rows: 'int | None' = None, **queryables: 'Any') -> 'tuple[pd.DataFrame, "
    "BaseMetadata]'",
    "get_codes": "(code_service: 'CODE_SERVICES') -> 'tuple[pd.DataFrame, BaseMetadata]'",
    "get_combined_metadata": "(monitoring_location_id: 'str | Iterable[str] | None' = None, parameter_code: 'str | "
    "Iterable[str] | None' = None, parameter_name: 'str | Iterable[str] | None' = None, "
    "parameter_description: 'str | Iterable[str] | None' = None, unit_of_measure: 'str | "
    "Iterable[str] | None' = None, statistic_id: 'str | Iterable[str] | None' = None, data_type: "
    "'str | Iterable[str] | None' = None, computation_identifier: 'str | Iterable[str] | None' = "
    "None, thresholds: 'float | list[float] | None' = None, sublocation_identifier: 'str | "
    "Iterable[str] | None' = None, primary: 'str | Iterable[str] | None' = None, "
    "parent_time_series_id: 'str | Iterable[str] | None' = None, web_description: 'str | "
    "Iterable[str] | None' = None, last_modified: 'str | Iterable[str] | None' = None, begin: "
    "'str | Iterable[str] | None' = None, end: 'str | Iterable[str] | None' = None, agency_code: "
    "'str | Iterable[str] | None' = None, agency_name: 'str | Iterable[str] | None' = None, "
    "monitoring_location_number: 'str | Iterable[str] | None' = None, monitoring_location_name: "
    "'str | Iterable[str] | None' = None, district_code: 'str | Iterable[str] | None' = None, "
    "country_code: 'str | Iterable[str] | None' = None, country_name: 'str | Iterable[str] | "
    "None' = None, state: 'str | Iterable[str] | None' = None, state_code: 'str | Iterable[str] "
    "| None' = None, state_name: 'str | Iterable[str] | None' = None, county_code: 'str | "
    "Iterable[str] | None' = None, county_name: 'str | Iterable[str] | None' = None, "
    "minor_civil_division_code: 'str | Iterable[str] | None' = None, site_type_code: 'str | "
    "Iterable[str] | None' = None, site_type: 'str | Iterable[str] | None' = None, "
    "hydrologic_unit_code: 'str | Iterable[str] | None' = None, basin_code: 'str | Iterable[str] "
    "| None' = None, altitude: 'str | Iterable[str] | None' = None, altitude_accuracy: 'str | "
    "Iterable[str] | None' = None, altitude_method_code: 'str | Iterable[str] | None' = None, "
    "altitude_method_name: 'str | Iterable[str] | None' = None, vertical_datum: 'str | "
    "Iterable[str] | None' = None, vertical_datum_name: 'str | Iterable[str] | None' = None, "
    "horizontal_positional_accuracy_code: 'str | Iterable[str] | None' = None, "
    "horizontal_positional_accuracy: 'str | Iterable[str] | None' = None, "
    "horizontal_position_method_code: 'str | Iterable[str] | None' = None, "
    "horizontal_position_method_name: 'str | Iterable[str] | None' = None, "
    "original_horizontal_datum: 'str | Iterable[str] | None' = None, "
    "original_horizontal_datum_name: 'str | Iterable[str] | None' = None, drainage_area: 'str | "
    "Iterable[str] | None' = None, contributing_drainage_area: 'str | Iterable[str] | None' = "
    "None, time_zone_abbreviation: 'str | Iterable[str] | None' = None, uses_daylight_savings: "
    "'str | Iterable[str] | None' = None, construction_date: 'str | Iterable[str] | None' = "
    "None, aquifer_code: 'str | Iterable[str] | None' = None, national_aquifer_code: 'str | "
    "Iterable[str] | None' = None, aquifer_type_code: 'str | Iterable[str] | None' = None, "
    "well_constructed_depth: 'str | Iterable[str] | None' = None, hole_constructed_depth: 'str | "
    "Iterable[str] | None' = None, depth_source_code: 'str | Iterable[str] | None' = None, "
    "properties: 'str | Iterable[str] | None' = None, skip_geometry: 'bool | None' = None, bbox: "
    "'list[float] | None' = None, limit: 'int | None' = None, filter: 'str | None' = None, "
    "filter_lang: 'FILTER_LANG | None' = None, convert_type: 'bool' = True, max_rows: 'int | "
    "None' = None, **queryables: 'Any') -> 'tuple[pd.DataFrame, BaseMetadata]'",
    "get_continuous": "(monitoring_location_id: 'str | Iterable[str] | None' = None, parameter_code: 'str | Iterable[str] "
    "| None' = None, statistic_id: 'str | Iterable[str] | None' = None, properties: 'str | "
    "Iterable[str] | None' = None, time_series_id: 'str | Iterable[str] | None' = None, continuous_id: "
    "'str | Iterable[str] | None' = None, approval_status: 'str | Iterable[str] | None' = None, "
    "unit_of_measure: 'str | Iterable[str] | None' = None, qualifier: 'str | Iterable[str] | None' = "
    "None, value: 'str | Iterable[str] | None' = None, last_modified: 'str | Iterable[str] | None' = "
    "None, time: 'str | Iterable[str] | None' = None, limit: 'int | None' = None, filter: 'str | None' "
    "= None, filter_lang: 'FILTER_LANG | None' = None, convert_type: 'bool' = True, max_rows: 'int | "
    "None' = None, **queryables: 'Any') -> 'tuple[pd.DataFrame, BaseMetadata]'",
    "get_cql": "(service: 'WATERDATA_SERVICES', cql: 'str | dict[str, Any]', *, properties: 'str | Iterable[str] | None' "
    "= None, bbox: 'list[float] | None' = None, limit: 'int | None' = None, skip_geometry: 'bool | None' = "
    "None, convert_type: 'bool' = True) -> 'tuple[pd.DataFrame, BaseMetadata]'",
    "get_daily": "(monitoring_location_id: 'str | Iterable[str] | None' = None, parameter_code: 'str | Iterable[str] | "
    "None' = None, statistic_id: 'str | Iterable[str] | None' = None, properties: 'str | Iterable[str] | "
    "None' = None, time_series_id: 'str | Iterable[str] | None' = None, daily_id: 'str | Iterable[str] | "
    "None' = None, approval_status: 'str | Iterable[str] | None' = None, unit_of_measure: 'str | "
    "Iterable[str] | None' = None, qualifier: 'str | Iterable[str] | None' = None, value: 'str | "
    "Iterable[str] | None' = None, last_modified: 'str | Iterable[str] | None' = None, skip_geometry: 'bool "
    "| None' = None, time: 'str | Iterable[str] | None' = None, bbox: 'list[float] | None' = None, limit: "
    "'int | None' = None, filter: 'str | None' = None, filter_lang: 'FILTER_LANG | None' = None, "
    "convert_type: 'bool' = True, max_rows: 'int | None' = None, **queryables: 'Any') -> "
    "'tuple[pd.DataFrame, BaseMetadata]'",
    "get_field_measurements": "(monitoring_location_id: 'str | Iterable[str] | None' = None, parameter_code: 'str | "
    "Iterable[str] | None' = None, observing_procedure_code: 'str | Iterable[str] | None' = "
    "None, properties: 'str | Iterable[str] | None' = None, field_visit_id: 'str | "
    "Iterable[str] | None' = None, approval_status: 'str | Iterable[str] | None' = None, "
    "unit_of_measure: 'str | Iterable[str] | None' = None, qualifier: 'str | Iterable[str] | "
    "None' = None, value: 'str | Iterable[str] | None' = None, last_modified: 'str | "
    "Iterable[str] | None' = None, observing_procedure: 'str | Iterable[str] | None' = None, "
    "vertical_datum: 'str | Iterable[str] | None' = None, measuring_agency: 'str | "
    "Iterable[str] | None' = None, skip_geometry: 'bool | None' = None, time: 'str | "
    "Iterable[str] | None' = None, bbox: 'list[float] | None' = None, limit: 'int | None' = "
    "None, filter: 'str | None' = None, filter_lang: 'FILTER_LANG | None' = None, convert_type: "
    "'bool' = True, max_rows: 'int | None' = None, **queryables: 'Any') -> 'tuple[pd.DataFrame, "
    "BaseMetadata]'",
    "get_field_measurements_metadata": "(monitoring_location_id: 'str | Iterable[str] | None' = None, parameter_code: "
    "'str | Iterable[str] | None' = None, parameter_name: 'str | Iterable[str] | None' "
    "= None, parameter_description: 'str | Iterable[str] | None' = None, begin: 'str | "
    "Iterable[str] | None' = None, end: 'str | Iterable[str] | None' = None, "
    "last_modified: 'str | Iterable[str] | None' = None, properties: 'str | "
    "Iterable[str] | None' = None, skip_geometry: 'bool | None' = None, bbox: "
    "'list[float] | None' = None, limit: 'int | None' = None, filter: 'str | None' = "
    "None, filter_lang: 'FILTER_LANG | None' = None, convert_type: 'bool' = True, "
    "max_rows: 'int | None' = None, **queryables: 'Any') -> 'tuple[pd.DataFrame, "
    "BaseMetadata]'",
    "get_latest_continuous": "(monitoring_location_id: 'str | Iterable[str] | None' = None, parameter_code: 'str | "
    "Iterable[str] | None' = None, statistic_id: 'str | Iterable[str] | None' = None, "
    "properties: 'str | Iterable[str] | None' = None, time_series_id: 'str | Iterable[str] | "
    "None' = None, latest_continuous_id: 'str | Iterable[str] | None' = None, approval_status: "
    "'str | Iterable[str] | None' = None, unit_of_measure: 'str | Iterable[str] | None' = None, "
    "qualifier: 'str | Iterable[str] | None' = None, value: 'str | Iterable[str] | None' = None, "
    "last_modified: 'str | Iterable[str] | None' = None, skip_geometry: 'bool | None' = None, "
    "time: 'str | Iterable[str] | None' = None, bbox: 'list[float] | None' = None, limit: 'int | "
    "None' = None, filter: 'str | None' = None, filter_lang: 'FILTER_LANG | None' = None, "
    "convert_type: 'bool' = True, max_rows: 'int | None' = None, **queryables: 'Any') -> "
    "'tuple[pd.DataFrame, BaseMetadata]'",
    "get_latest_daily": "(monitoring_location_id: 'str | Iterable[str] | None' = None, parameter_code: 'str | "
    "Iterable[str] | None' = None, statistic_id: 'str | Iterable[str] | None' = None, properties: "
    "'str | Iterable[str] | None' = None, time_series_id: 'str | Iterable[str] | None' = None, "
    "latest_daily_id: 'str | Iterable[str] | None' = None, approval_status: 'str | Iterable[str] | "
    "None' = None, unit_of_measure: 'str | Iterable[str] | None' = None, qualifier: 'str | "
    "Iterable[str] | None' = None, value: 'str | Iterable[str] | None' = None, last_modified: 'str | "
    "Iterable[str] | None' = None, skip_geometry: 'bool | None' = None, time: 'str | Iterable[str] | "
    "None' = None, bbox: 'list[float] | None' = None, limit: 'int | None' = None, filter: 'str | "
    "None' = None, filter_lang: 'FILTER_LANG | None' = None, convert_type: 'bool' = True, max_rows: "
    "'int | None' = None, **queryables: 'Any') -> 'tuple[pd.DataFrame, BaseMetadata]'",
    "get_monitoring_locations": "(monitoring_location_id: 'str | Iterable[str] | None' = None, agency_code: 'str | "
    "Iterable[str] | None' = None, agency_name: 'str | Iterable[str] | None' = None, "
    "monitoring_location_number: 'str | Iterable[str] | None' = None, "
    "monitoring_location_name: 'str | Iterable[str] | None' = None, district_code: 'str | "
    "Iterable[str] | None' = None, country_code: 'str | Iterable[str] | None' = None, "
    "country_name: 'str | Iterable[str] | None' = None, state: 'str | Iterable[str] | None' = "
    "None, state_code: 'str | Iterable[str] | None' = None, state_name: 'str | Iterable[str] "
    "| None' = None, county_code: 'str | Iterable[str] | None' = None, county_name: 'str | "
    "Iterable[str] | None' = None, minor_civil_division_code: 'str | Iterable[str] | None' = "
    "None, site_type_code: 'str | Iterable[str] | None' = None, site_type: 'str | "
    "Iterable[str] | None' = None, hydrologic_unit_code: 'str | Iterable[str] | None' = None, "
    "basin_code: 'str | Iterable[str] | None' = None, altitude: 'str | Iterable[str] | None' "
    "= None, altitude_accuracy: 'str | Iterable[str] | None' = None, altitude_method_code: "
    "'str | Iterable[str] | None' = None, altitude_method_name: 'str | Iterable[str] | None' "
    "= None, vertical_datum: 'str | Iterable[str] | None' = None, vertical_datum_name: 'str | "
    "Iterable[str] | None' = None, horizontal_positional_accuracy_code: 'str | Iterable[str] "
    "| None' = None, horizontal_positional_accuracy: 'str | Iterable[str] | None' = None, "
    "horizontal_position_method_code: 'str | Iterable[str] | None' = None, "
    "horizontal_position_method_name: 'str | Iterable[str] | None' = None, "
    "original_horizontal_datum: 'str | Iterable[str] | None' = None, "
    "original_horizontal_datum_name: 'str | Iterable[str] | None' = None, drainage_area: 'str "
    "| Iterable[str] | None' = None, contributing_drainage_area: 'str | Iterable[str] | None' "
    "= None, time_zone_abbreviation: 'str | Iterable[str] | None' = None, "
    "uses_daylight_savings: 'str | Iterable[str] | None' = None, construction_date: 'str | "
    "Iterable[str] | None' = None, aquifer_code: 'str | Iterable[str] | None' = None, "
    "national_aquifer_code: 'str | Iterable[str] | None' = None, aquifer_type_code: 'str | "
    "Iterable[str] | None' = None, well_constructed_depth: 'str | Iterable[str] | None' = "
    "None, hole_constructed_depth: 'str | Iterable[str] | None' = None, depth_source_code: "
    "'str | Iterable[str] | None' = None, properties: 'str | Iterable[str] | None' = None, "
    "skip_geometry: 'bool | None' = None, bbox: 'list[float] | None' = None, limit: 'int | "
    "None' = None, filter: 'str | None' = None, filter_lang: 'FILTER_LANG | None' = None, "
    "convert_type: 'bool' = True, max_rows: 'int | None' = None, **queryables: 'Any') -> "
    "'tuple[pd.DataFrame, BaseMetadata]'",
    "get_peaks": "(monitoring_location_id: 'str | Iterable[str] | None' = None, parameter_code: 'str | Iterable[str] | "
    "None' = None, time_series_id: 'str | Iterable[str] | None' = None, unit_of_measure: 'str | "
    "Iterable[str] | None' = None, time: 'str | Iterable[str] | None' = None, last_modified: 'str | "
    "Iterable[str] | None' = None, water_year: 'int | list[int] | None' = None, year: 'int | list[int] | "
    "None' = None, month: 'int | list[int] | None' = None, day: 'int | list[int] | None' = None, peak_since: "
    "'int | list[int] | None' = None, properties: 'str | Iterable[str] | None' = None, skip_geometry: 'bool "
    "| None' = None, bbox: 'list[float] | None' = None, limit: 'int | None' = None, filter: 'str | None' = "
    "None, filter_lang: 'FILTER_LANG | None' = None, convert_type: 'bool' = True, max_rows: 'int | None' = "
    "None, **queryables: 'Any') -> 'tuple[pd.DataFrame, BaseMetadata]'",
    "get_queryables": "(collection: 'str') -> 'tuple[pd.DataFrame, BaseMetadata]'",
    "get_reference_table": "(collection: 'str', limit: 'int | None' = None, query: 'dict[str, Any] | None' = None, "
    "max_rows: 'int | None' = None) -> 'tuple[pd.DataFrame, BaseMetadata]'",
    "get_samples": "(ssl_check: 'bool' = True, service: 'SERVICES' = 'results', profile: 'PROFILES' = 'fullphyschem', "
    "activity_media_name: 'str | Iterable[str] | None' = None, activity_start_date_lower: 'str | None' = "
    "None, activity_start_date_upper: 'str | None' = None, activity_type_code: 'str | Iterable[str] | "
    "None' = None, characteristic_group: 'str | Iterable[str] | None' = None, characteristic: 'str | "
    "Iterable[str] | None' = None, characteristic_user_supplied: 'str | Iterable[str] | None' = None, "
    "bbox: 'list[float] | None' = None, country_code: 'str | Iterable[str] | None' = None, state_code: "
    "'str | Iterable[str] | None' = None, county_code: 'str | Iterable[str] | None' = None, "
    "site_type_code: 'str | Iterable[str] | None' = None, site_type_name: 'str | Iterable[str] | None' = "
    "None, usgs_pcode: 'str | Iterable[str] | None' = None, hydrologic_unit: 'str | Iterable[str] | None' "
    "= None, monitoring_location_id: 'str | Iterable[str] | None' = None, organization_id: 'str | "
    "Iterable[str] | None' = None, point_location_latitude: 'float | None' = None, "
    "point_location_longitude: 'float | None' = None, point_location_within_miles: 'float | None' = None, "
    "project_id: 'str | Iterable[str] | None' = None, record_identifier_user_supplied: 'str | "
    "Iterable[str] | None' = None) -> 'tuple[pd.DataFrame, BaseMetadata]'",
    "get_samples_summary": "(monitoring_location_id: 'str', ssl_check: 'bool' = True) -> 'tuple[pd.DataFrame, "
    "BaseMetadata]'",
    "get_stats_date_range": "(approval_status: 'str | None' = None, computation_type: 'str | Iterable[str] | None' = "
    "None, country_code: 'str | Iterable[str] | None' = None, state: 'str | Iterable[str] | None' "
    "= None, state_code: 'str | Iterable[str] | None' = None, county_code: 'str | Iterable[str] | "
    "None' = None, start_date: 'str | None' = None, end_date: 'str | None' = None, "
    "monitoring_location_id: 'str | Iterable[str] | None' = None, page_size: 'int' = 1000, "
    "parent_time_series_id: 'str | Iterable[str] | None' = None, site_type_code: 'str | "
    "Iterable[str] | None' = None, site_type_name: 'str | Iterable[str] | None' = None, "
    "parameter_code: 'str | Iterable[str] | None' = None, interval_type: 'str | Iterable[str] | "
    "None' = None, expand_percentiles: 'bool' = True) -> 'tuple[pd.DataFrame, BaseMetadata]'",
    "get_stats_por": "(approval_status: 'str | None' = None, computation_type: 'str | Iterable[str] | None' = None, "
    "country_code: 'str | Iterable[str] | None' = None, state: 'str | Iterable[str] | None' = None, "
    "state_code: 'str | Iterable[str] | None' = None, county_code: 'str | Iterable[str] | None' = None, "
    "start_date: 'str | None' = None, end_date: 'str | None' = None, monitoring_location_id: 'str | "
    "Iterable[str] | None' = None, page_size: 'int' = 1000, parent_time_series_id: 'str | Iterable[str] "
    "| None' = None, site_type_code: 'str | Iterable[str] | None' = None, site_type_name: 'str | "
    "Iterable[str] | None' = None, parameter_code: 'str | Iterable[str] | None' = None, normal_type: "
    "'str | None' = None, expand_percentiles: 'bool' = True) -> 'tuple[pd.DataFrame, BaseMetadata]'",
    "get_time_series_metadata": "(monitoring_location_id: 'str | Iterable[str] | None' = None, parameter_code: 'str | "
    "Iterable[str] | None' = None, parameter_name: 'str | Iterable[str] | None' = None, "
    "properties: 'str | Iterable[str] | None' = None, statistic_id: 'str | Iterable[str] | "
    "None' = None, hydrologic_unit_code: 'str | Iterable[str] | None' = None, state: 'str | "
    "Iterable[str] | None' = None, state_name: 'str | Iterable[str] | None' = None, "
    "last_modified: 'str | Iterable[str] | None' = None, begin: 'str | Iterable[str] | None' "
    "= None, end: 'str | Iterable[str] | None' = None, begin_utc: 'str | Iterable[str] | "
    "None' = None, end_utc: 'str | Iterable[str] | None' = None, unit_of_measure: 'str | "
    "Iterable[str] | None' = None, computation_period_identifier: 'str | Iterable[str] | "
    "None' = None, computation_identifier: 'str | Iterable[str] | None' = None, thresholds: "
    "'float | list[float] | None' = None, sublocation_identifier: 'str | Iterable[str] | "
    "None' = None, primary: 'str | Iterable[str] | None' = None, parent_time_series_id: 'str "
    "| Iterable[str] | None' = None, time_series_id: 'str | Iterable[str] | None' = None, "
    "web_description: 'str | Iterable[str] | None' = None, skip_geometry: 'bool | None' = "
    "None, bbox: 'list[float] | None' = None, limit: 'int | None' = None, filter: 'str | "
    "None' = None, filter_lang: 'FILTER_LANG | None' = None, convert_type: 'bool' = True, "
    "max_rows: 'int | None' = None, **queryables: 'Any') -> 'tuple[pd.DataFrame, "
    "BaseMetadata]'",
}


def test_waterdata_exports_are_stable() -> None:
    assert waterdata.__all__ == _EXPECTED_WATERDATA_ALL
    assert api.__all__ == _EXPECTED_API_NAMES
    assert all(hasattr(waterdata, name) for name in waterdata.__all__)


def test_api_facade_preserves_function_contracts() -> None:
    for name, expected_signature in _EXPECTED_SIGNATURES.items():
        package_function = getattr(waterdata, name)
        facade_function = getattr(api, name)
        assert package_function is facade_function
        assert str(inspect.signature(facade_function)) == expected_signature
        assert facade_function.__module__ == "dataretrieval.waterdata.api"


def test_api_private_samples_compatibility_names_remain() -> None:
    assert isinstance(api._SAMPLES_PARAM_TO_API, dict)
    assert isinstance(api._SAMPLES_LEGACY_KWARGS, dict)
    assert callable(api.get_ogc_data)
