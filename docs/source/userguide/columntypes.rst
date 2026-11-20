.. _columntypes:

Column Types and Memory
-----------------------

The Water Data getters return measurement columns such as ``value`` as
``float64``, and time columns as detailed in :ref:`timeconventions`. With
pandas 3, text columns that have values, such as ``monitoring_location_id``,
use the pandas default string type. Text columns take most of a result's
memory. The examples below show how the pyarrow package reduces the memory a
pandas DataFrame uses.


Installing pyarrow
******************

With pandas 3, pandas stores text columns in Arrow memory whenever pyarrow is
installed. Installing pyarrow alone can save considerable memory, and no
``dataretrieval`` call changes.

.. code:: bash

    pip install pyarrow

In a daily-values query for one monitoring location (12,784 rows), the
columns other than ``geometry`` used 6.2 MiB without pyarrow and 2.7 MiB with
it, as reported by ``memory_usage(deep=True)``. To check which storage a text
column uses with pandas 3:

.. code:: python

    >>> from dataretrieval import waterdata
    >>> df, md = waterdata.get_daily(
    ...     monitoring_location_id="USGS-05427718",
    ...     parameter_code="00060",
    ...     time="1990-01-01/2024-12-31",
    ... )
    >>> df["monitoring_location_id"].dtype.storage
    'pyarrow'

pandas 2 stores text as ``object`` whether or not pyarrow is installed. Python
3.10 gets pandas 2, because pandas 3 requires Python 3.11 or later.


Converting to Arrow dtypes
**************************

With pandas 3, installing pyarrow changes only the storage of text columns.
They keep the pandas default string type, and the other columns keep their
NumPy types. To reduce memory further, convert the columns other than
``geometry`` to Arrow dtypes:

.. code:: python

    >>> def to_arrow(frame):
    ...     cols = frame.columns.drop("geometry", errors="ignore")
    ...     floats = frame.select_dtypes("float").columns
    ...     frame[cols] = frame[cols].convert_dtypes(dtype_backend="pyarrow")
    ...     frame[floats] = frame[floats].astype("double[pyarrow]")
    ...     return frame
    ...
    >>> df = to_arrow(df)
    >>> df.dtypes.drop("geometry")
    time_series_id                           string[pyarrow]
    monitoring_location_id                   string[pyarrow]
    parameter_code                           string[pyarrow]
    statistic_id                             string[pyarrow]
    time                              timestamp[us][pyarrow]
    value                                    double[pyarrow]
    unit_of_measure                          string[pyarrow]
    approval_status                          string[pyarrow]
    qualifier                                         object
    last_modified             timestamp[us, tz=UTC][pyarrow]
    daily_id                                 string[pyarrow]
    dtype: object

The conversion reduces the result above to 2.3 MiB. The listing is from
pandas 3. With pandas 2, the time columns are ``timestamp[ns][pyarrow]`` and
``timestamp[ns, tz=UTC][pyarrow]``, and the other types are the same.

- ``convert_dtypes`` turns a float column whose values are all whole numbers
  into an integer column. ``to_arrow`` then casts the float columns to
  ``double[pyarrow]``, so ``value`` stays floating point. pandas 3.1
  deprecated ``convert_integer``, the keyword that disabled the integer
  conversion.
- ``convert_dtypes`` raises ``TypeError`` on a geopandas ``geometry`` column,
  so ``to_arrow`` leaves it out. The result stays a ``GeoDataFrame``, and
  ``df.to_parquet(path)`` writes GeoParquet.
- Columns of lists, such as ``qualifier``, stay ``object``.
- A column with no values in a result becomes ``null[pyarrow]``. For
  example, :func:`~dataretrieval.waterdata.get_monitoring_locations` returns
  no ``aquifer_code`` for stream gages. Before you write results from several
  queries to one Parquet dataset or database table, cast such a column to the
  type it has when populated:

  .. code:: python

      >>> import pandas as pd
      >>> import pyarrow as pa
      >>> sites, md = waterdata.get_monitoring_locations(
      ...     monitoring_location_id=["USGS-05427718", "USGS-01646500"]
      ... )
      >>> sites = to_arrow(sites)
      >>> sites["aquifer_code"].dtype
      null[pyarrow]
      >>> sites = sites.astype({"aquifer_code": pd.ArrowDtype(pa.string())})
      >>> sites["aquifer_code"].dtype
      string[pyarrow]

  The string alias ``"string[pyarrow]"`` selects a different pandas type,
  which Parquet stores as ``large_string``.
