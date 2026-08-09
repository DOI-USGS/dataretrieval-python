.. _exceptions:

dataretrieval.exceptions
------------------------

.. automodule:: dataretrieval.exceptions
    :members:
    :show-inheritance:

Resumable fan-out interruptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

These are raised when a fanned-out request is interrupted mid-stream; the
completed work is preserved and ``exc.call.resume()`` continues it. They are
defined in ``dataretrieval.interruptions`` (they carry pandas/httpx state) but
are importable from the top level, e.g.
``from dataretrieval import FanOutInterrupted``.

``ChunkInterrupted`` is a permanent alias of ``FanOutInterrupted`` -- the same
class object under the name it was first published as -- so ``except
ChunkInterrupted`` and ``except FanOutInterrupted`` are the same handler. The
base class is named for the fan-out rather than for chunking because a Water Use
call fans out without dividing anything: the NWDC simply accepts one location
per request.

.. autoclass:: dataretrieval.FanOutInterrupted
    :members:
    :show-inheritance:

.. autoclass:: dataretrieval.QuotaExhausted
    :show-inheritance:

.. autoclass:: dataretrieval.ServiceInterrupted
    :show-inheritance:
