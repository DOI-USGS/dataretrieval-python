# dataretrieval: Download hydrologic data

![PyPI - Version](https://img.shields.io/pypi/v/dataretrieval)
![Conda Version](https://img.shields.io/conda/v/conda-forge/dataretrieval)
![Downloads](https://static.pepy.tech/badge/dataretrieval)

## What is dataretrieval?

`dataretrieval` simplifies the process of loading hydrologic data into Python.
Like the original R version
[`dataRetrieval`](https://github.com/DOI-USGS/dataRetrieval), it retrieves major
U.S. Geological Survey (USGS) hydrology data types available on the Web, as well
as data from the Water Quality Portal (WQP), the National Ground-Water
Monitoring Network (NGWMN), and the Network Linked Data Index (NLDI).

Check the [NEWS](NEWS.md) for all updates and announcements.

## Installation

Install dataretrieval using pip:

```bash
pip install dataretrieval
```

Or conda:

```bash
conda install -c conda-forge dataretrieval
```

Or directly from GitHub:

```bash
pip install git+https://github.com/DOI-USGS/dataretrieval-python.git
```

## Usage Examples

### Water Data API (Recommended - Modern USGS Data)

Access USGS water-monitoring data.

**Important:** Users are strongly encouraged to obtain an API key for higher
rate limits. [Register for an API key](https://api.waterdata.usgs.gov/signup/)
and set it as an environment variable:

```python
import os
os.environ["API_USGS_PAT"] = "your_api_key_here"
```

The following example retrieves daily streamflow data for a specific
monitoring location. The `/` in the `time` argument separates the start and
end of the desired range:

```python
from dataretrieval import waterdata

# Get daily streamflow data (returns DataFrame and metadata)
df, metadata = waterdata.get_daily(
    monitoring_location_id='USGS-01646500',
    parameter_code='00060',  # Discharge
    time='2024-10-01/2025-09-30'
)

print(f"Retrieved {len(df)} records")
print(f"Site: {df['monitoring_location_id'].iloc[0]}")
print(f"Mean discharge: {df['value'].mean():.2f} {df['unit_of_measure'].iloc[0]}")
```
Retrieve streamflow at multiple locations from October 1, 2024 to the present:

```python
df, metadata = waterdata.get_daily(
    monitoring_location_id=["USGS-13018750","USGS-13013650"],
    parameter_code='00060',
    time='2024-10-01/..'
)

print(f"Retrieved {len(df)} records")
```
Retrieve location information for all monitoring locations categorized as
stream sites in Maryland:

```python
# Get monitoring location information
df, metadata = waterdata.get_monitoring_locations(
    state='Maryland',  # full name, postal code ('MD'), or FIPS ('24')
    site_type_code='ST'  # Stream sites
)

print(f"Found {len(df)} stream monitoring locations in Maryland")
```
Finally, retrieve continuous (a.k.a. "instantaneous") data for one location.
We *strongly advise* breaking continuous data requests into smaller time
windows to avoid timeouts and other issues:

```python
# Get continuous data for a single monitoring location and water year
df, metadata = waterdata.get_continuous(
    monitoring_location_id='USGS-01646500',
    parameter_code='00065',  # Gage height
    time='2024-10-01/2025-09-30'
)
print(f"Retrieved {len(df)} continuous gage height measurements")
```

Visit the
[API Reference](https://doi-usgs.github.io/dataretrieval-python/reference/waterdata.html)
for more information and examples on available services and input parameters.

For verbose troubleshooting and support — including the request URL sent to the
API — enable debug-level
[logging](https://docs.python.org/3/howto/logging.html#logging-basic-tutorial):

```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### National Ground-Water Monitoring Network (NGWMN)

Access groundwater data aggregated from many state, federal, and local
agencies. NGWMN uses the same OGC engine as the Water Data API,
so chunking and pagination behave the same way:

```python
from dataretrieval import ngwmn

# Find the groundwater monitoring sites in a state
# (state accepts a full name, a postal code like 'WI', or a FIPS code like '55')
sites, metadata = ngwmn.get_sites(state='Wisconsin')

print(f"Found {len(sites)} NGWMN sites in Wisconsin")

# Pull water levels from the first twenty sites over a time window.
water_levels, metadata = ngwmn.get_water_level(
    monitoring_location_id=sites['monitoring_location_id'][:20],
    datetime=['2022-01-01', '2024-01-01']
)

print(f"Retrieved {len(water_levels)} water-level observations")
```

### Water Quality Portal (WQP)

Access water quality data from multiple agencies:

```python
from dataretrieval import wqp

# Find water quality monitoring sites (returns a DataFrame and metadata)
sites, metadata = wqp.what_sites(
    statecode='US:55',  # Wisconsin
    siteType='Stream'
)

print(f"Found {len(sites)} stream monitoring sites in Wisconsin")

# Get water quality results
results, metadata = wqp.get_results(
    siteid='USGS-05427718',
    characteristicName='Temperature, water'
)

print(f"Retrieved {len(results)} temperature measurements")
```

### Network Linked Data Index (NLDI)

Discover and navigate hydrologic networks:

```python
from dataretrieval import nldi

# Get watershed basin for a stream reach
basin = nldi.get_basin(
    feature_source='comid',
    feature_id='13293474'  # NHD reach identifier
)

print(f"Basin contains {len(basin)} feature(s)")

# Find upstream flowlines
flowlines = nldi.get_flowlines(
    feature_source='comid',
    feature_id='13293474',
    navigation_mode='UT',  # Upstream tributaries
    distance=50  # km
)

print(f"Found {len(flowlines)} upstream tributaries within 50km")
```

### Water Use (NWDC)

Retrieve modeled water-use estimates from the National Water Availability
Assessment Data Companion:

```python
from dataretrieval import wateruse

# Monthly public-supply withdrawals for Rhode Island, split into
# groundwater and surface-water sources (returns a DataFrame and metadata).
df, metadata = wateruse.get_wateruse(
    model='wu-public-supply-wd',
    variable=['pswdtot', 'pswdgw', 'pswdsw'],
    state='RI',  # name/postal/FIPS; pass a list to fan out over several areas
    start_date='2020-01',
    time_resolution='monthly',
)

print(f"Retrieved {len(df)} records across {df['huc12_id'].nunique()} watersheds")

# Aggregate the HUC12 grid to a statewide monthly total (million gallons/day)
statewide = df.groupby('year_month')['pswdtot_mgd'].sum()
print(statewide.head())
```

## Available Data Services

### Modern USGS Water Data APIs (Recommended) — `dataretrieval.waterdata`
- `get_daily`: Daily statistical summaries (mean, min, max)
- `get_continuous`: High-frequency continuous (instantaneous) values
- `get_field_measurements`: Discrete measurements from field visits
- `get_monitoring_locations`: Site information and metadata
- `get_time_series_metadata`: A location's available data parameters
- `get_latest_daily`: Most recent daily statistical summary
- `get_latest_continuous`: Most recent high-frequency value
- `get_stats_por` / `get_stats_date_range`: Daily, monthly, and annual statistics
- `get_samples`: Discrete USGS water-quality samples
- `get_ratings`: Stage-discharge rating curves

### National Ground-Water Monitoring Network (NGWMN) — `dataretrieval.ngwmn`
- `get_sites`: Groundwater monitoring-location metadata across many agencies
- `get_water_level`: Depth-to-water and water-level observations
- `get_lithology`: Geologic-material logs by depth interval
- `get_well_construction`: Casing, screen, and build-out records
- `get_providers`: Contributing data-provider organizations

### Legacy NWIS Services (Deprecated) — `dataretrieval.nwis`
- `get_dv`: Legacy daily statistical data
- `get_iv`: Legacy continuous (instantaneous) data
- `get_info`: Basic site information
- `get_stats`: Statistical summaries
- `get_discharge_peaks`: Annual peak discharge events

### Water Quality Portal — `dataretrieval.wqp`
- `get_results`: Water-quality analytical results from USGS, EPA, and other agencies
- `what_sites`: Monitoring-location information
- `what_organizations`: Data-provider information
- `what_projects`: Sampling-project details

### Network Linked Data Index (NLDI) — `dataretrieval.nldi`
- `get_basin`: Watershed boundary for a point or feature
- `get_flowlines`: Upstream/downstream flowline navigation
- `get_features`: Find monitoring sites, dams, and other features along the network
- `get_features_by_data_source`: Features from a specific data source

### Water Use (NWDC)
- **Public supply**: Modeled public-supply withdrawals and consumptive use
- **Irrigation**: Modeled irrigation withdrawals and consumptive use
- **Thermoelectric**: Modeled thermoelectric-power water use
- **HUC12 estimates**: National coverage on a 12-digit hydrologic-unit grid,
  summarizable to counties, states, or coarser hydrologic units

## More Examples

Explore additional examples in the
[`demos`](https://github.com/DOI-USGS/dataretrieval-python/tree/main/demos)
directory, including Jupyter notebooks demonstrating advanced usage patterns.

## Getting Help

- **Issue tracker**: Report bugs and request features at https://github.com/DOI-USGS/dataretrieval-python/issues
- **Documentation**: https://doi-usgs.github.io/dataretrieval-python/

## Contributing

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for
development guidelines.

## Acknowledgments

This material is partially based upon work supported by the National Science
Foundation (NSF) under award 1931297. Any opinions, findings, conclusions, or
recommendations expressed in this material are those of the authors and do not
necessarily reflect the views of the NSF.

## Disclaimer

This software is preliminary or provisional and is subject to revision. It is
being provided to meet the need for timely best science. The software has not
received final approval by the U.S. Geological Survey (USGS). No warranty,
expressed or implied, is made by the USGS or the U.S. Government as to the
functionality of the software and related material nor shall the fact of release
constitute any such warranty. The software is provided on the condition that
neither the USGS nor the U.S. Government shall be held liable for any damages
resulting from the authorized or unauthorized use of the software.

## Citation

Hodson, T.O., Hariharan, J.A., Black, S., and Horsburgh, J.S., 2023,
dataretrieval (Python): a Python package for discovering and retrieving water
data available from U.S. federal hydrologic web services: U.S. Geological Survey
software release, https://doi.org/10.5066/P94I5TX3.
