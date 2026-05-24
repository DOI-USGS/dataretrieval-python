"""A single self-updating status line for paginated / chunked Water Data queries.

Water Data getters fan out two ways the caller can't see: long CQL filters are
split into URL-length-safe *chunks* (``filters.chunked``), and each request
follows ``next`` links across an unknown number of *pages* (``utils._walk_pages``
and ``utils.get_stats_data``). This module surfaces that work as one line on
stderr, rewritten in place as data arrives::

    Progress: chunk 2/5 · 14 pages · 8,421 rows · 4,870 requests remaining

It replaces the per-page ``logger.info`` calls that previously narrated the same
events one line at a time.

The active reporter lives in a :class:`~contextvars.ContextVar` rather than being
threaded through every signature: progress is a cross-cutting concern that the
``chunked`` decorator (outer, chunk counts) and the page-walking loops (inner,
page/row/rate-limit counts) both update without knowing about each other. Call
:func:`progress_context` to activate one and :func:`current` to reach it.

By default the line is shown only on an interactive terminal, so notebooks,
redirected logs, and CI stay clean. ``API_USGS_PROGRESS`` forces it on
(``1``/``true``) or off (``0``/``false``).
"""

from __future__ import annotations

import contextvars
import os
import sys
import time
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TextIO


def _format_duration(seconds: float) -> str:
    """Compact human duration: ``45s``, ``12m``, ``1h03m`` (clamped at 0)."""
    secs = int(max(0, seconds))
    if secs < 60:
        return f"{secs}s"
    if secs < 3600:
        return f"{secs // 60}m"
    hours, rem = divmod(secs, 3600)
    minutes = rem // 60
    return f"{hours}h{minutes:02d}m" if minutes else f"{hours}h"


# The reporter active for the current query. A ContextVar (not a module global)
# so concurrent queries — threads or async tasks sharing a client — each track
# their own progress line.
_active: contextvars.ContextVar[ProgressReporter | None] = contextvars.ContextVar(
    "waterdata_progress", default=None
)

# Where to register for an API key. Surfaced once when a query runs without an
# API key configured (no API_USGS_PAT), since unauthenticated callers hit much
# lower rate limits (see the API_USGS_PAT note in the README).
SIGNUP_URL = "https://api.waterdata.usgs.gov/signup/"

# Process-level latch so the "no API key" pointer is shown at most once.
_api_key_hint_shown = False


def _enabled_default(stream: TextIO) -> bool:
    """Whether to draw the line: ``API_USGS_PROGRESS`` wins, else TTY-only."""
    override = os.getenv("API_USGS_PROGRESS")
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
        # Absolute epoch second when the rate-limit window resets, derived from
        # the server's reset header so the rendered countdown stays live.
        self._reset_at: float | None = None
        self._last_len = 0
        self._closed = False

    def set_chunks(self, total: int) -> None:
        """Record how many filter chunks this query was split into."""
        self.total_chunks = max(int(total), 1)

    def start_chunk(self, index: int) -> None:
        """Mark the start of chunk ``index`` (1-based) and redraw.

        Only redraws when actually chunking (``total_chunks > 1``); a
        single-chunk plan has nothing chunk-specific to show yet, so it
        avoids a premature "0 pages" frame before the first page arrives.
        """
        self.current_chunk = index
        if self.total_chunks > 1:
            self._render()

    def add_page(self, rows: int = 0) -> None:
        """Record one fetched page carrying ``rows`` rows and redraw."""
        self.pages += 1
        self.rows += int(rows)
        self._render()

    def set_rate_remaining(
        self, value: str | int | None, reset: str | int | None = None
    ) -> None:
        """Update the rate-limit display from the response headers.

        ``value`` is ``x-ratelimit-remaining``; ``reset`` is the optional
        ``x-ratelimit-reset`` companion. Empty/missing values are ignored so a
        page that omits a header doesn't blank out the last known value. The
        reset value is interpreted as an absolute epoch second when large
        (the conventional form) and as seconds-until-reset otherwise; either
        way it's stored as an absolute deadline so the countdown stays live.
        """
        if value not in (None, ""):
            self.rate_remaining = str(value)
        if reset not in (None, ""):
            try:
                secs = float(reset)
            except (TypeError, ValueError):
                return
            self._reset_at = secs if secs > 1_000_000 else time.time() + secs

    def _format(self) -> str:
        parts: list[str] = []
        if self.total_chunks > 1:
            parts.append(f"chunk {self.current_chunk}/{self.total_chunks}")
        parts.append(f"{self.pages} page" + ("" if self.pages == 1 else "s"))
        if self.rows:
            parts.append(f"{self.rows:,} rows")
        if self.rate_remaining is not None:
            # The header is a string; group it like the row count when it's a
            # plain ASCII integer, otherwise show it verbatim. (``str.isdigit``
            # alone is True for non-decimal unicode digits that ``int`` rejects.)
            rate = self.rate_remaining
            rate = f"{int(rate):,}" if rate.isascii() and rate.isdigit() else rate
            segment = f"{rate} requests remaining"
            if self._reset_at is not None:
                eta = _format_duration(self._reset_at - time.time())
                segment += f", resets in {eta}"
            parts.append(segment)
        return "Progress: " + " · ".join(parts)

    def _render(self) -> None:
        if not self.enabled or self._closed:
            return
        try:
            line = self._format()
            pad = max(self._last_len - len(line), 0)
            self._stream.write("\r" + line + " " * pad)
            self._stream.flush()
            self._last_len = len(line)
        except Exception:  # noqa: BLE001
            # Progress output is best-effort cosmetics; a broken pipe (output
            # piped to ``head``), a closed stream, or an encoding error must
            # never disturb — let alone truncate — the query. Disable so we
            # don't retry on every subsequent page.
            self.enabled = False

    def close(self) -> None:
        """Finalize the line with a trailing newline so it persists on screen.

        If no API key is configured (no ``API_USGS_PAT``), append a one-time
        pointer to API-key registration, since unauthenticated callers hit much
        lower rate limits.
        """
        if self._closed:
            return
        self._closed = True
        if not (self.enabled and (self.pages or self.current_chunk)):
            return
        try:
            self._stream.write("\n")
            self._maybe_hint_api_key()
            self._stream.flush()
        except Exception:  # noqa: BLE001
            self.enabled = False

    def _maybe_hint_api_key(self) -> None:
        global _api_key_hint_shown
        if _api_key_hint_shown or os.getenv("API_USGS_PAT"):
            return
        _api_key_hint_shown = True
        self._stream.write(
            f"No API key detected — register for higher rate limits at {SIGNUP_URL}\n"
        )


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
