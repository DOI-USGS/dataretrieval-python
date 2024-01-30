# dataretrieval: Download hydrologic data

:warning: USGS data availability and format are changing on Water Quality Portal (WQP). Beginning in February 2024 data obtained from WQP legacy profiles will not include new USGS data or recent updates to existing data. 
To view the status of changes in data availability and code functionality, visit: https://doi-usgs.github.io/dataRetrieval/articles/Status.html

## What is dataretrieval?
`dataretrieval` was created to simplify the process of loading hydrologic data into the Python environment.
Like the original R version [`dataRetrieval`](https://github.com/DOI-USGS/dataRetrieval),
it is designed to retrieve the major data types of U.S. Geological Survey (USGS) hydrology
data that are available on the Web, as well as data from the Water
Quality Portal (WQP), which currently houses water quality data from the
Environmental Protection Agency (EPA), U.S. Department of Agriculture
(USDA), and USGS. Direct USGS data is obtained from a service called the
National Water Information System (NWIS).

Note that the python version is not a direct port of the original: it attempts to reproduce the functionality of the R package,
though its organization and interface often differ.

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

# get water quality samples (qwdata)
df2 = nwis.get_record(sites=site, service='qwdata', start='2017-12-31', end='2018-01-01')

# get basic info about the site
df3 = nwis.get_record(sites=site, service='site')
```
Services available from NWIS include:
- instantaneous values (iv)
- daily values (dv)
- statistics (stat)
- site info (site)
- discharge peaks (peaks)
- discharge measurements (measurements)
* water quality samples (qwdata)

To access the full functionality available from NWIS web services, nwis.get record appends any additional kwargs into the REST request. For example
```python
nwis.get_record(sites='03339000', service='dv', start='2017-12-31', parameterCd='00060')
```
will download daily data with the parameter code 00060 (discharge).

## Accessing the "Internal" NWIS
If you're connected to the USGS network, dataretrieval call pull from the internal (non-public) NWIS interface.
Most dataretrieval functions pass kwargs directly to NWIS's REST API, which provides simple access to internal data; simply specify "access='3'".
For example
```python
nwis.get_record(sites='05404147',service='iv', start='2021-01-01', end='2021-3-01', access='3')
```

More services and documentation to come!

## Quick start

dataretrieval can be installed using pip:
	
    $ python3 -m pip install -U dataretrieval

or conda:

    $ conda install -c conda-forge dataretrieval

More examples of use are include in [`demos`](https://github.com/USGS-python/dataretrieval/tree/master/demos).

## Issue tracker

Please report any bugs and enhancement ideas using the dataretrieval issue
tracker:

  https://github.com/USGS-python/dataretrieval/issues

Feel free to also ask questions on the tracker.


## Contributing

Any help in testing, development, documentation and other tasks is welcome.
For more details, see the file [CONTRIBUTING.md](CONTRIBUTING.md).


[![Coverage Status](https://coveralls.io/repos/github/thodson-usgs/data_retrieval/badge.svg?branch=master)](https://coveralls.io/github/thodson-usgs/data_retrieval?branch=master)

## Package Support
The Water Mission Area of the USGS supports the development and maintenance of `dataretrieval`
and most likely further into the future.
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
