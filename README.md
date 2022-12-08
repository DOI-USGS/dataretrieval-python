# dataretrieval: Download hydrologic data

## What is dataretrieval?

dataretrieval is a Python alternative to the R [dataRetrieval](https://github.com/DOI-USGS/dataRetrieval)
package for obtaining USGS or EPA water quality data, streamflow data, and metadata
directly from web services. Note that dataretrieval is an **alternative** to the
R package, not a port, in that it attempts to reproduce the functionality of the R package,
though its organization and functionality often differ.

If there's a hydrologic or environmental data portal that you'd like dataretrieval to 
work with, raise it as an [issue](https://github.com/USGS-python/dataretrieval/issues).

Here's an example of how to use dataRetrieval to retrieve data from the National Water Information System (NWIS).

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


## Issue tracker

Please report any bugs and enhancement ideas using the dataretrieval issue
tracker:

  https://github.com/USGS-python/dataretrieval/issues

Feel free to also ask questions on the tracker.


## Help wanted

Any help in testing, development, documentation and other tasks is
highly appreciated and useful to the project. 

For more details, see the file [CONTRIBUTING.md](CONTRIBUTING.md).


[![Coverage Status](https://coveralls.io/repos/github/thodson-usgs/data_retrieval/badge.svg?branch=master)](https://coveralls.io/github/thodson-usgs/data_retrieval?branch=master)

## Disclaimer

This software is in the public domain because it contains materials that originally came from the U.S. Geological Survey,
an agency of the United States Department of Interior. For more information, see the
[official USGS copyright policy](https://www2.usgs.gov/visual-id/credit_usgs.html#copyright)

Although this software program has been used by the U.S. Geological Survey (USGS),
no warranty, expressed or implied, is made by the USGS or the U.S. Government
as to the accuracy and functioning of the program and related program material nor shall the fact of distribution
constitute any such warranty, and no responsibility is assumed by the USGS in connection therewith.

This software is provided “AS IS.”

[![CC0](http://i.creativecommons.org/p/zero/1.0/88x31.png)](http://creativecommons.org/publicdomain/zero/1.0/)

## Citation

Hodson, T.O. and contributors, 2022, dataretrieval (Python): a Python package for discovering and retrieving water data
available from U.S. federal hydrologic web services. https://doi.org/10.5066/P94I5TX3
