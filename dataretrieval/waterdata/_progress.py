"""A single self-updating status line for paginated / chunked Water Data queries.

Water Data getters fan out two ways the caller can't see: large multi-value
requests are split into URL-length-safe *chunks* (``chunking`` module), and each
request follows ``next`` links across an unknown number of *pages*
(``utils._paginate``). This module surfaces that work as one line on stderr,
rewritten in place as data arrives::

    Retrieving: daily · 6 pages · 2,881 rows · 995/1,000 requests remaining

It replaces the per-page ``logger.info`` calls that previously narrated the same
events one line at a time.

The active reporter lives in a :class:`~contextvars.ContextVar` rather than being
threaded through every signature: progress is a cross-cutting concern that the
chunk orchestrator (outer, chunk counts) and the page-walking loop (inner,
page/row/rate-limit counts) both update without knowing about each other. Call
:func:`progress_context` to activate one and :func:`current` to reach it.

By default the line is shown for interactive use — an interactive terminal or a
Jupyter/IPython kernel (like ``tqdm``) — while redirected logs and CI stay clean.
``API_USGS_PROGRESS`` forces it on (``1``/``true``) or off (``0``/``false``).
"""

from __future__ import annotations

import contextvars
import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TextIO


def _group_int(value: str) -> str:
    """Comma-group a plain ASCII integer string; pass anything else through.

    (``str.isdigit`` alone is True for non-decimal unicode digits that ``int``
    rejects, hence the ``isascii`` guard.)
    """
    return f"{int(value):,}" if value.isascii() and value.isdigit() else value


# The reporter active for the current query. A ContextVar (not a module global)
# so the chunk orchestrator and the page loop resolve to the same reporter
# within one query, and an unrelated query in another context can't clobber its
# state. (It does not give concurrent queries sharing one stderr separate
# lines — they would still interleave.)
_active: contextvars.ContextVar[ProgressReporter | None] = contextvars.ContextVar(
    "waterdata_progress", default=None
)

# Where to register for an API key. Surfaced once when a query runs without an
# API key configured (no API_USGS_PAT), since unauthenticated callers hit much
# lower rate limits (see the API_USGS_PAT note in the README).
SIGNUP_URL = "https://api.waterdata.usgs.gov/signup/"

# Process-level latch so the "no API key" pointer is shown at most once.
_api_key_hint_shown = False


def _in_jupyter_kernel() -> bool:
    """True when running inside a Jupyter/IPython *kernel* (notebook, lab,
    qtconsole).

    A kernel's ``stderr`` isn't a TTY, but it honors carriage-return rewrites in
    the cell output area — the same mechanism ``tqdm`` rides on — so the line is
    worth showing there. The plain IPython terminal REPL is a
    ``TerminalInteractiveShell`` (already a TTY), so only the ZMQ kernel needs
    this extra signal. Detected without importing IPython: if it isn't already
    imported, we aren't in a shell.
    """
    ipython = sys.modules.get("IPython")
    if ipython is None:
        return False
    shell = ipython.get_ipython()
    return shell is not None and type(shell).__name__ == "ZMQInteractiveShell"


def _enabled_default(stream: TextIO) -> bool:
    """Whether to draw the line by default.

    ``API_USGS_PROGRESS`` wins when set. Otherwise show it for interactive use —
    a TTY or a Jupyter/IPython kernel — and stay quiet for redirected output,
    logs, and CI.
    """
    override = os.getenv("API_USGS_PROGRESS")
    if override is not None:
        return override.strip().lower() not in {"", "0", "false", "no", "off"}
    if _in_jupyter_kernel():
        return True
    return hasattr(stream, "isatty") and stream.isatty()


class ProgressReporter:
    """Accumulates query progress and rewrites a single status line in place.

    Every update method is a no-op when the reporter is disabled, so call sites
    need no ``if enabled`` guards. The line is redrawn with a leading carriage
    return and padded to erase the previous (possibly longer) contents;
    :meth:`close` terminates it with a newline so the final state persists.
    """

    def __init__(
        self,
        *,
        service: str | None = None,
        stream: TextIO | None = None,
        enabled: bool | None = None,
    ) -> None:
        self._stream = stream if stream is not None else sys.stderr
        self.enabled = _enabled_default(self._stream) if enabled is None else enabled
        # The service/collection being retrieved (e.g. "daily", "peaks"),
        # shown as the line's leading label.
        self.service = service
        self.total_chunks = 1
        self.current_chunk = 0
        self.pages = 0
        self.rows = 0
        self.rate_remaining: str | None = None
        # The hourly request quota (``x-ratelimit-limit``), shown as the
        # denominator when the server reports it.
        self.rate_limit: str | None = None
        # Transient note shown while a sub-request backs off before a
        # retry; cleared by the next page/chunk so it doesn't linger.
        self.retry_note: str | None = None
        self._last_len = 0
        # Whether anything was actually written to the stream — drives whether
        # close() needs a terminating newline. (``current_chunk`` is a poor
        # proxy: ``start_chunk`` sets it even when it doesn't render.)
        self._rendered = False
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
        self.retry_note = None
        if self.total_chunks > 1:
            self._render()

    def add_page(self, rows: int = 0) -> None:
        """Record one fetched page carrying ``rows`` rows and redraw."""
        self.pages += 1
        self.rows += int(rows)
        self.retry_note = None
        self._render()

    def note_retry(self, *, attempt: int, wait: float) -> None:
        """Show that a sub-request is backing off before retry ``attempt``.

        Cleared by the next :meth:`add_page` / :meth:`start_chunk` (or by
        :meth:`close`) so the line returns to normal once the retry resolves.
        """
        # Keep sub-second waits explicit (avoid misleading ``0s``) while
        # rendering whole-second waits without unnecessary ``.0`` noise.
        # ``float()`` to support Python 3.9-3.11: ``round(int, 1)`` returns an
        # int and ``int.is_integer()`` (used below) only exists on 3.12+.
        wait_1dp = round(float(wait), 1)
        if wait_1dp < 1 or not wait_1dp.is_integer():
            secs = f"{wait_1dp:.1f}s"
        else:
            secs = f"{wait_1dp:.0f}s"
        self.retry_note = f"retrying (attempt {attempt}, waiting {secs})"
        self._render()

    def set_rate_remaining(
        self, value: str | int | None, limit: str | int | None = None
    ) -> None:
        """Update the rate-limit display from the response headers.

        ``value`` is ``x-ratelimit-remaining``; ``limit`` is the optional
        ``x-ratelimit-limit`` quota, shown as the denominator. Empty/missing
        values are ignored so a page that omits a header doesn't blank out the
        last known value.
        """
        if value not in (None, ""):
            self.rate_remaining = str(value)
        if limit not in (None, ""):
            self.rate_limit = str(limit)

    def _format(self) -> str:
        parts: list[str] = []
        if self.total_chunks > 1:
            parts.append(f"chunk {self.current_chunk}/{self.total_chunks}")
        parts.append(f"{self.pages} page" + ("" if self.pages == 1 else "s"))
        if self.rows:
            parts.append(f"{self.rows:,} rows")
        if self.rate_remaining is not None:
            remaining = _group_int(self.rate_remaining)
            if self.rate_limit is not None:
                limit = _group_int(self.rate_limit)
                segment = f"{remaining}/{limit} requests remaining"
            else:
                segment = f"{remaining} requests remaining"
            parts.append(segment)
        if self.retry_note is not None:
            parts.append(self.retry_note)
        if self.service:
            return f"Retrieving: {self.service} · " + " · ".join(parts)
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
            self._rendered = True
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
        # A retry note set during the final backoff would otherwise freeze as
        # the persisted last line of a call that has since completed or given
        # up; clear it and redraw (while still un-closed, so ``_render`` runs)
        # so the final state isn't a stale "retrying".
        if self.enabled and self._rendered and self.retry_note is not None:
            self.retry_note = None
            self._render()
        self._closed = True
        if not (self.enabled and self._rendered):
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
        # Set the once-per-process latch only after a successful write, so a
        # failed write (broken pipe) doesn't silently burn the hint for every
        # later query in the process.
        self._stream.write(
            f"No API key detected — register for higher rate limits at {SIGNUP_URL}\n"
        )
        _api_key_hint_shown = True


@contextmanager
def progress_context(
    *,
    service: str | None = None,
    stream: TextIO | None = None,
    enabled: bool | None = None,
) -> Iterator[ProgressReporter]:
    """Activate a :class:`ProgressReporter` for the duration of a query.

    ``service`` labels the line (e.g. ``"Retrieving: daily ..."``). If a reporter
    is already active (a nested call), the existing one is yielded unchanged so
    the outermost query owns the single line; only the outermost context closes
    it (and ``service``/``stream``/``enabled`` of a nested call are ignored).
    """
    existing = _active.get()
    if existing is not None:
        yield existing
        return
    reporter = ProgressReporter(service=service, stream=stream, enabled=enabled)
    token = _active.set(reporter)
    try:
        yield reporter
    finally:
        _active.reset(token)
        reporter.close()


def current() -> ProgressReporter | None:
    """Return the reporter active for the current query, or ``None``."""
    return _active.get()
