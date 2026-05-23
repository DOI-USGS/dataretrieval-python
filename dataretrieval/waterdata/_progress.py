"""A single self-updating status line for paginated / chunked Water Data queries.

Water Data getters fan out two ways the caller can't see: long CQL filters are
split into URL-length-safe *chunks* (``filters.chunked``), and each request
follows ``next`` links across an unknown number of *pages* (``utils._walk_pages``
and ``utils.get_stats_data``). This module surfaces that work as one line on
stderr, rewritten in place as data arrives::

    waterdata · chunk 2/5 · 14 pages · 8,421 rows · 4,870 requests left

It replaces the per-page ``logger.info`` calls that previously narrated the same
events one line at a time.

The active reporter lives in a :class:`~contextvars.ContextVar` rather than being
threaded through every signature: progress is a cross-cutting concern that the
``chunked`` decorator (outer, chunk counts) and the page-walking loops (inner,
page/row/rate-limit counts) both update without knowing about each other. Call
:func:`progress_context` to activate one and :func:`current` to reach it.

By default the line is shown only on an interactive terminal, so notebooks,
redirected logs, and CI stay clean. ``DATARETRIEVAL_PROGRESS`` forces it on
(``1``/``true``) or off (``0``/``false``).
"""

from __future__ import annotations

import contextvars
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TextIO

# The reporter active for the current query. A ContextVar (not a module global)
# so concurrent queries — threads or async tasks sharing a client — each track
# their own progress line.
_active: contextvars.ContextVar[ProgressReporter | None] = contextvars.ContextVar(
    "waterdata_progress", default=None
)


def _enabled_default(stream: TextIO) -> bool:
    """Whether to draw the line: ``DATARETRIEVAL_PROGRESS`` wins, else TTY-only."""
    override = os.getenv("DATARETRIEVAL_PROGRESS")
    if override is not None:
        return override.strip().lower() not in {"", "0", "false", "no", "off"}
    return hasattr(stream, "isatty") and stream.isatty()


class ProgressReporter:
    """Accumulates query progress and rewrites a single status line in place.

    Every update method is a no-op when the reporter is disabled, so call sites
    need no ``if enabled`` guards. The line is redrawn with a leading carriage
    return and padded to erase the previous (possibly longer) contents;
    :meth:`close` terminates it with a newline so the final state persists.
    """

    def __init__(
        self, *, stream: TextIO | None = None, enabled: bool | None = None
    ) -> None:
        self._stream = stream if stream is not None else sys.stderr
        self.enabled = _enabled_default(self._stream) if enabled is None else enabled
        self.total_chunks = 1
        self.current_chunk = 0
        self.pages = 0
        self.rows = 0
        self.rate_remaining: str | None = None
        self._last_len = 0
        self._closed = False

    def set_chunks(self, total: int) -> None:
        """Record how many filter chunks this query was split into."""
        self.total_chunks = max(int(total), 1)

    def start_chunk(self, index: int) -> None:
        """Mark the start of chunk ``index`` (1-based) and redraw."""
        self.current_chunk = index
        self._render()

    def add_page(self, rows: int = 0) -> None:
        """Record one fetched page carrying ``rows`` rows and redraw."""
        self.pages += 1
        self.rows += int(rows)
        self._render()

    def set_rate_remaining(self, value: str | int | None) -> None:
        """Update the remaining-requests count from an ``x-ratelimit-remaining`` header.

        Ignores empty/missing values so a page that omits the header doesn't
        blank out the last known count.
        """
        if value not in (None, ""):
            self.rate_remaining = str(value)

    def _format(self) -> str:
        parts = ["waterdata"]
        if self.total_chunks > 1:
            parts.append(f"chunk {self.current_chunk}/{self.total_chunks}")
        parts.append(f"{self.pages} page" + ("" if self.pages == 1 else "s"))
        if self.rows:
            parts.append(f"{self.rows:,} rows")
        if self.rate_remaining is not None:
            # The header is a string; group it like the row count when it's a
            # plain integer, otherwise show it verbatim.
            rate = self.rate_remaining
            rate = f"{int(rate):,}" if rate.isdigit() else rate
            parts.append(f"{rate} requests left")
        return " · ".join(parts)

    def _render(self) -> None:
        if not self.enabled or self._closed:
            return
        line = self._format()
        pad = max(self._last_len - len(line), 0)
        self._stream.write("\r" + line + " " * pad)
        self._stream.flush()
        self._last_len = len(line)

    def close(self) -> None:
        """Finalize the line with a trailing newline so it persists on screen."""
        if self._closed:
            return
        self._closed = True
        if self.enabled and (self.pages or self.current_chunk):
            self._stream.write("\n")
            self._stream.flush()


@contextmanager
def progress_context(
    *, stream: TextIO | None = None, enabled: bool | None = None
) -> Iterator[ProgressReporter]:
    """Activate a :class:`ProgressReporter` for the duration of a query.

    If a reporter is already active (a nested call), the existing one is yielded
    unchanged so the outermost query owns the single line; only the outermost
    context closes it.
    """
    existing = _active.get()
    if existing is not None:
        yield existing
        return
    reporter = ProgressReporter(stream=stream, enabled=enabled)
    token = _active.set(reporter)
    try:
        yield reporter
    finally:
        _active.reset(token)
        reporter.close()


def current() -> ProgressReporter | None:
    """Return the reporter active for the current query, or ``None``."""
    return _active.get()
