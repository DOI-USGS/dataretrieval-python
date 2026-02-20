# dataretrieval: Download hydrologic data

![PyPI - Version](https://img.shields.io/pypi/v/dataretrieval)
![Conda Version](https://img.shields.io/conda/v/conda-forge/dataretrieval)
![Downloads](https://static.pepy.tech/badge/dataretrieval)

## Latest Announcements

:mega: **01/16/2025:** `dataretrieval` now features the `waterdata` module,
which provides access to USGS's modernized [Water Data
APIs](https://api.waterdata.usgs.gov/). The Water Data API endpoints include
daily values, **instantaneous values**, field measurements, time series metadata,
and discrete water quality data from the [Samples database](https://waterdata.usgs.gov/download-samples/#dataProfile=site). This new module replaces the `nwis` module, which provides access to the legacy [NWIS
Water Services](https://waterservices.usgs.gov/). Take a look at the new [`waterdata` module demo notebook](demos/WaterData_demo.ipynb), which walks through an extended example using a majority of the available `waterdata` functions.

Check out the [NEWS](NEWS.md) file for all updates and announcements.

**Important:** Users of the Water Data APIs are strongly encouraged to obtain an
API key for higher rate limits and greater access to USGS data. [Register for
an API key](https://api.waterdata.usgs.gov/signup/) and set it as an
environment variable:

```python
import os
os.environ["API_USGS_PAT"] = "your_api_key_here"
```

## What is dataretrieval?

`dataretrieval` simplifies the process of loading hydrologic data into Python.
Like the original R version
[`dataRetrieval`](https://github.com/DOI-USGS/dataRetrieval), it retrieves major
U.S. Geological Survey (USGS) hydrology data types available on the Web, as well
as data from the Water Quality Portal (WQP) and Network Linked Data Index
(NLDI).

## Installation

Install dataretrieval using pip:

```bash
pip install dataretrieval
```

Or using conda:

```bash
conda install -c conda-forge dataretrieval
```

To install the "main" branch directly from GitHub, use:

```bash
pip install git+https://github.com/DOI-USGS/dataretrieval-python.git
```

## Usage Examples

### Water Data API (Recommended - Modern USGS Data)

The `waterdata` module provides access to modern USGS Water Data APIs.

Some basic usage examples include retrieving daily streamflow data for a
specific monitoring location, where the `/` in the `time` argument indicates
the desired range:

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
Retrieving streamflow at multiple locations from October 1, 2024 to the present:

```python
df, metadata = waterdata.get_daily(
    monitoring_location_id=["USGS-13018750","USGS-13013650"],
    parameter_code='00060',
    time='2024-10-01/..'
)

print(f"Retrieved {len(df)} records")
```
Retrieving location information for all monitoring locations categorized as
stream sites in the state of Maryland:

```python
# Get monitoring location information
df, metadata = waterdata.get_monitoring_locations(
    state_name='Maryland',
    site_type_code='ST'  # Stream sites
)

print(f"Found {len(df)} stream monitoring locations in Maryland")
```
Finally, retrieving continuous (a.k.a. "instantaneous") data
for one location. We *strongly advise* breaking up continuous data requests into smaller time periods and collections to avoid timeouts and other issues:

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

**NEW:** This new module implements
[logging](https://docs.python.org/3/howto/logging.html#logging-basic-tutorial)
in which users can view the URL requests sent to the USGS Water Data APIs
and the number of requests they have remaining each hour. These messages can
be helpful for troubleshooting and support. To enable logging in your python
console or notebook:

```python
import logging
logging.basicConfig(level=logging.INFO)
``` 
To log messages to a file, you can specify a filename in the
`basicConfig` call:

```python
logging.basicConfig(filename='waterdata.log', level=logging.INFO)
```

### NWIS Legacy Services (Deprecated but still functional)

The `nwis` module accesses legacy NWIS Water Services:

```python
from dataretrieval import nwis

# Get site information
info, metadata = nwis.get_info(sites='01646500')
    
print(f"Site name: {info['station_nm'].iloc[0]}")

# Get daily values
dv, metadata = nwis.get_dv(
  sites='01646500',
  start='2024-10-01',
  end='2024-10-02',
  parameterCd='00060',
)
    
print(f"Retrieved {len(dv)} daily values")
```

### Water Quality Portal (WQP)

Access water quality data from multiple agencies:

```python
from dataretrieval import wqp

# Find water quality monitoring sites
sites = wqp.what_sites(
    statecode='US:55',  # Wisconsin
    siteType='Stream'
)

print(f"Found {len(sites)} stream monitoring sites in Wisconsin")

# Get water quality results
results = wqp.get_results(
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

## Available Data Services

### Modern USGS Water Data APIs (Recommended)
- **Daily values**: Daily statistical summaries (mean, min, max)
- **Instantaneous values**: High-frequency continuous data
- **Field measurements**: Discrete measurements from field visits
- **Monitoring locations**: Site information and metadata
- **Time series metadata**: Information about available data parameters
- **Latest daily values**: Most recent daily statistical summary data
- **Latest instantaneous values**: Most recent high-frequency continuous data
- **Samples data**: Discrete USGS water quality data

### Legacy NWIS Services (Deprecated)
- **Daily values (dv)**: Legacy daily statistical data
- **Instantaneous values (iv)**: Legacy continuous data
- **Site info (site)**: Basic site information  
- **Statistics (stat)**: Statistical summaries
- **Discharge peaks (peaks)**: Annual peak discharge events
- **Discharge measurements (measurements)**: Direct flow measurements

### Water Quality Portal
- **Results**: Water quality analytical results from USGS, EPA, and other agencies
- **Sites**: Monitoring location information
- **Organizations**: Data provider information
- **Projects**: Sampling project details

### Network Linked Data Index (NLDI)
- **Basin delineation**: Watershed boundaries for any point
- **Flow navigation**: Upstream/downstream network traversal
- **Feature discovery**: Find monitoring sites, dams, and other features
- **Hydrologic connectivity**: Link data across the stream network

## More Examples

Explore additional examples in the
[`demos`](https://github.com/USGS-python/dataretrieval/tree/main/demos)
directory, including Jupyter notebooks demonstrating advanced usage patterns.

## Getting Help

- **Issue tracker**: Report bugs and request features at https://github.com/USGS-python/dataretrieval/issues
- **Documentation**: Full API documentation available in the source code docstrings

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
