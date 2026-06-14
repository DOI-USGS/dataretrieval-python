.. _exceptions:

dataretrieval.exceptions
------------------------

.. automodule:: dataretrieval.exceptions
    :members:
    :show-inheritance:

Resumable chunk interruptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These are raised when a transparently-chunked request is interrupted
mid-stream; the completed work is preserved and ``exc.call.resume()`` continues
it. They are defined in ``dataretrieval.ogc.chunking`` (they carry pandas/httpx
state) but are importable from the top level, e.g.
``from dataretrieval import ChunkInterrupted``.

.. autoclass:: dataretrieval.ChunkInterrupted
    :members:
    :show-inheritance:

.. autoclass:: dataretrieval.QuotaExhausted
    :show-inheritance:

.. autoclass:: dataretrieval.ServiceInterrupted
    :show-inheritance:
