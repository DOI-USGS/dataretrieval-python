.. _columntypes:

Column Types and Memory
-----------------------

The Water Data getters return measurement columns such as ``value`` as
``float64``, and time columns as :ref:`timeconventions` describes. With
pandas 3, text columns that have values, such as ``monitoring_location_id``,
use the pandas default string type. Text columns take most of a result's
memory.


Installing pyarrow
******************

With pandas 3, pandas stores text columns in Arrow memory whenever pyarrow is
installed. No ``dataretrieval`` call changes.

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

Convert the columns other than ``geometry``:

.. code:: python

    >>> cols = df.columns.drop("geometry", errors="ignore")
    >>> df[cols] = df[cols].convert_dtypes(
    ...     dtype_backend="pyarrow", convert_integer=False
    ... )
    >>> df[cols].dtypes
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

The conversion reduces the result above to 2.3 MiB. pandas 2 gives the same
types, except ``[ns]`` timestamps in place of ``[us]``.

- ``convert_integer=False`` keeps ``value`` a floating-point column. Without
  it, ``convert_dtypes`` turns a float column whose values are all whole
  numbers into an integer column. Integer columns stay NumPy ``int64``.
- ``convert_dtypes`` raises ``TypeError`` on a geopandas ``geometry`` column,
  so the conversion leaves it out. The result stays a ``GeoDataFrame``, and
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
      >>> cols = sites.columns.drop("geometry", errors="ignore")
      >>> sites[cols] = sites[cols].convert_dtypes(
      ...     dtype_backend="pyarrow", convert_integer=False
      ... )
      >>> sites["aquifer_code"].dtype
      null[pyarrow]
      >>> sites = sites.astype({"aquifer_code": pd.ArrowDtype(pa.string())})
      >>> sites["aquifer_code"].dtype
      string[pyarrow]

  The string alias ``"string[pyarrow]"`` selects a different pandas type,
  which Parquet stores as ``large_string``.
