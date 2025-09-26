# dataretrieval: Download hydrologic data

![PyPI - Version](https://img.shields.io/pypi/v/dataretrieval)
![Conda Version](https://img.shields.io/conda/v/conda-forge/dataretrieval)
![Downloads](https://static.pepy.tech/badge/dataretrieval)

## Latest Announcements

:mega: **10/01/2025:** `dataretrieval` is pleased to offer a new, *in-development* module, `waterdata`, which gives users access USGS's modernized [Water Data APIs](https://api.waterdata.usgs.gov/). The Water Data API endpoints include daily values, instantaneous values, field measurements (modernized groundwater levels service), time series metadata, and discrete water quality data from the Samples database. Though there will be a period of overlap, the functions within `waterdata` will eventually replace the `nwis` module, which currently provides access to the legacy [NWIS Water Services](https://waterservices.usgs.gov/). More example workflows and functions coming soon. Check `help(waterdata)` for more information.

**Important:** Users of the Water Data APIs are strongly encouraged to obtain an API key, which gives users higher rate limits and thus greater access to USGS data. [Register for an API key](https://api.waterdata.usgs.gov/signup/) and then place that API key in your python environment as an environment variable named "API_USGS_PAT". One option is to set the variable as follows:

```python
import os
os.environ["API_USGS_PAT"] = "your_api_key_here"
```
Note that you may need to restart your python session for the environment variable to be recognized.

Check out the [NEWS](NEWS.md) file for all updates and announcements, or track updates to the package via the GitHub releases.

## What is dataretrieval?
`dataretrieval` was created to simplify the process of loading hydrologic data into the Python environment.
Like the original R version [`dataRetrieval`](https://github.com/DOI-USGS/dataRetrieval),
it is designed to retrieve the major data types of U.S. Geological Survey (USGS) hydrology
data that are available on the Web, as well as data from the Water
Quality Portal (WQP), which currently houses water quality data from the
Environmental Protection Agency (EPA), U.S. Department of Agriculture
(USDA), and USGS. Direct USGS data is obtained from a service called the
National Water Information System (NWIS).

Note that the python version is not a direct port of the original: it attempts to reproduce the functionality of the R package, though its organization and interface often differ.

If there's a hydrologic or environmental data portal that you'd like dataretrieval to 
work with, raise it as an [issue](https://github.com/USGS-python/dataretrieval/issues).

Here's an example using `dataretrieval` to retrieve data from the National Water Information System (NWIS).

```python
# first import the functions for downloading data from NWIS
import dataretrieval.nwis as nwis

# specify the USGS site code for which we want data.
site = '03339000'

# get instantaneous values (iv)
df = nwis.get_record(sites=site, service='iv', start='2017-12-31', end='2018-01-01')

# get basic info about the site
df2 = nwis.get_record(sites=site, service='site')
```
Services available from NWIS include:
- instantaneous values (iv)
- daily values (dv)
- statistics (stat)
- site info (site)
- discharge peaks (peaks)
- discharge measurements (measurements)

Water quality data are available from:
- [Samples](https://waterdata.usgs.gov/download-samples/#dataProfile=site) - Discrete USGS water quality data only
- [Water Quality Portal](https://www.waterqualitydata.us/) - Discrete water quality data from USGS and EPA. Older data are available in the legacy WQX version 2 format; all data are available in the beta WQX3.0 format.

To access the full functionality available from NWIS web services, `nwis.get_record()` appends any additional kwargs into the REST request. For example, this function call:
```python
nwis.get_record(sites='03339000', service='dv', start='2017-12-31', parameterCd='00060')
```
...will download daily data with the parameter code 00060 (discharge).

## Accessing the "Internal" NWIS
If you're connected to the USGS network, dataretrieval call pull from the internal (non-public) NWIS interface.
Most dataretrieval functions pass kwargs directly to NWIS's REST API, which provides simple access to internal data; simply specify "access='3'".
For example
```python
nwis.get_record(sites='05404147',service='iv', start='2021-01-01', end='2021-3-01', access='3')
```

## Quick start

dataretrieval can be installed using pip:
	
    $ python3 -m pip install -U dataretrieval

or conda:

    $ conda install -c conda-forge dataretrieval

More examples of use are include in [`demos`](https://github.com/USGS-python/dataretrieval/tree/main/demos).

## Issue tracker

Please report any bugs and enhancement ideas using the dataretrieval issue
tracker:

  https://github.com/USGS-python/dataretrieval/issues

Feel free to also ask questions on the tracker.


## Contributing

Any help in testing, development, documentation and other tasks is welcome.
For more details, see the file [CONTRIBUTING.md](CONTRIBUTING.md).


## Need help?

The Water Mission Area of the USGS supports the development and maintenance of `dataretrieval`. Any questions can be directed to the Computational Tools team at comptools@usgs.gov. 

Resources are available primarily for maintenance and responding to user questions.
Priorities on the development of new features are determined by the `dataretrieval` development team.

## Acknowledgments
This material is partially based upon work supported by the National Science Foundation (NSF) under award 1931297.
Any opinions, findings, conclusions, or recommendations expressed in this material are those of the authors and do not necessarily reflect the views of the NSF.

## Disclaimer

This software is preliminary or provisional and is subject to revision. 
It is being provided to meet the need for timely best science.
The software has not received final approval by the U.S. Geological Survey (USGS).
No warranty, expressed or implied, is made by the USGS or the U.S. Government as to the functionality of the software and related material nor shall the fact of release constitute any such warranty. 
The software is provided on the condition that neither the USGS nor the U.S. Government shall be held liable for any damages resulting from the authorized or unauthorized use of the software.

## Citation

Hodson, T.O., Hariharan, J.A., Black, S., and Horsburgh, J.S., 2023, dataretrieval (Python): a Python package for discovering
and retrieving water data available from U.S. federal hydrologic web services:
U.S. Geological Survey software release,
https://doi.org/10.5066/P94I5TX3.
