.. _exceptions:

dataretrieval.exceptions
------------------------

.. automodule:: dataretrieval.exceptions
    :members:
    :show-inheritance:

Resumable fan-out interruptions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A fanned-out call raises these when a transient failure interrupts it
partway; the exception preserves the completed work and
``exc.call.resume()`` continues it. ``dataretrieval.interruptions`` defines
them (they have pandas/httpx state), and the top level exports them, e.g.
``from dataretrieval import FanOutInterrupted``.

``ChunkInterrupted`` is a permanent alias of ``FanOutInterrupted`` (the same
class object under the name it was first published as), so ``except
ChunkInterrupted`` and ``except FanOutInterrupted`` are the same handler. The
base class is named for the fan-out rather than for chunking because a Water Use
call fans out without dividing anything: the NWDC accepts one location
per request.

.. autoclass:: dataretrieval.FanOutInterrupted
    :members:
    :show-inheritance:

.. autoclass:: dataretrieval.QuotaExhausted
    :show-inheritance:

.. autoclass:: dataretrieval.ServiceInterrupted
    :show-inheritance:
