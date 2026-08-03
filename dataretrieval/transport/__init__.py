"""Internal service-neutral HTTP transport and execution policy.

The modules in this package own reusable client lifecycle, authentication,
pagination, retry, response aggregation, progress, and sync-dispatch behavior.
Service and protocol adapters consume these components; this package is not a
public framework API.
"""

__all__: list[str] = []
