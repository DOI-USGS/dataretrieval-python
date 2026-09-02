"""Layered configuration resolution for ``dataretrieval``.

Every tunable setting -- the Water Data API key, the fan-out concurrency cap,
the retry count, and the progress line -- resolves through one ordered chain so
a caller never has to mutate ``os.environ`` to configure a single call.

Sources, highest precedence first:

1. A configuration passed to :func:`configure` -- delivered through a
   :class:`~contextvars.ContextVar`, so a setting applies to the current thread
   or asyncio task and cannot leak into another one.
2. The environment variable for that setting (``API_USGS_PAT``,
   ``API_USGS_CONCURRENT``, ``API_USGS_RETRIES``, ``API_USGS_PROGRESS``).
3. The configuration file (TOML): ``~/.dataretrieval/config.toml``, or the path
   in ``DATARETRIEVAL_CONFIG``. Top-level keys are the package-wide defaults; a
   ``[<adapter>]`` table is that adapter's *default profile*, always in effect;
   a ``[<adapter>.<name>]`` table is a *named profile*, inert until a caller
   selects it with ``<Adapter>Configuration.load("<name>")``.
4. The built-in default.

Each of those four sources is one branch of :func:`_resolve`; ADR 0011 lists the
same order in finer grain, as seven rungs -- three of these sources hold two
apiece. An adapter's own default is not one of the four: a read site such as
:func:`concurrency` passes it in as the ``default`` argument.

Precedence applies **per setting**, not per source: an environment that sets only
``API_USGS_PAT`` leaves a file-provided ``concurrency`` fully in effect. The
environment ranks above the file (ADR 0009). ADR 0011 makes one exception: a
profile named *in code* reaches the chain through :func:`configure`, above the
environment.

A caller configures by passing configuration objects, at most one per adapter::

    with dataretrieval.configure(
        Configuration(api_key=vault.read("usgs/pat")),
        WaterdataConfiguration.load("bulk"),
        NgwmnConfiguration(concurrency=4),
    ):
        ...

Settings are scoped **per adapter** (ADR 0010): a ``[ngwmn]`` table in the file,
or an ``NgwmnConfiguration``, applies to NGWMN calls and no others. Precedence
stays *source-major* (ADR 0010): an adapter-scoped value outranks a package-wide
one only *within* the same source, and within the block source the innermost
block decides.

Each adapter declares the settings it accepts on its own
:class:`BaseConfiguration` subclass, defined in the module that *reads* them
(ADR 0011). The API key is not among them -- it belongs to the gateway fronting
a host, not to an adapter (ADR 0010).

This module is a leaf: it imports only the standard library plus the Python 3.10
``tomli`` backport (ADR 0009). That is also why it holds the adapter *names* but
never imports an adapter -- see :data:`ADAPTERS`. It centralizes each setting's
parser while retaining legacy environment behavior and stricter validation for
the new Python/TOML surfaces.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from functools import partial
from pathlib import Path
from typing import TextIO, overload

# Explicit same-name aliases preserve the facade's public and private compatibility
# symbols for runtime users and static analyzers. Ruff otherwise expands these
# aliases into one import statement each, obscuring the boundary inventory.
# isort: off
from dataretrieval._configuration_core import (
    ADAPTERS as ADAPTERS,
    CONFIG_PATH_ENV as CONFIG_PATH_ENV,
    CONCURRENCY_UNBOUNDED as CONCURRENCY_UNBOUNDED,
    DEFAULT_CONCURRENCY as DEFAULT_CONCURRENCY,
    DEFAULT_PARALLEL_CHUNKS as DEFAULT_PARALLEL_CHUNKS,
    DEFAULT_RETRIES as DEFAULT_RETRIES,
    DEFAULT_STALL_TIMEOUT as DEFAULT_STALL_TIMEOUT,
    ENV_VARS as ENV_VARS,
    SETTINGS as SETTINGS,
    BaseConfiguration as BaseConfiguration,
    Configuration as Configuration,
    _adapter_file_settings as _adapter_file_settings,
    _ALL_SETTINGS as _ALL_SETTINGS,
    _BLANK_MEANS_SET as _BLANK_MEANS_SET,
    _BUILT_IN as _BUILT_IN,
    _Chunked as _Chunked,
    _coerce_typed as _coerce_typed,
    _Concurrent as _Concurrent,
    _current_file as _current_file,
    _env_label as _env_label,
    _Frame as _Frame,
    _named_profiles as _named_profiles,
    _NO_FILE as _NO_FILE,
    _parse_base_url as _parse_base_url,
    _parse_concurrency as _parse_concurrency,
    _parse_parallel_chunks as _parse_parallel_chunks,
    _parse_progress as _parse_progress,
    _parse_retries as _parse_retries,
    _parse_seconds as _parse_seconds,
    _ParsedFile as _ParsedFile,
    _REFUSED_ENV_VARS as _REFUSED_ENV_VARS,
    _Redirectable as _Redirectable,
    _register as _register,
    _REGISTRY as _REGISTRY,
    _reset_file_cache as _reset_file_cache,
    _Retrying as _Retrying,
    _scope as _scope,
    _ScopeKey as _ScopeKey,
    _SettingValue as _SettingValue,
    _UNSET as _UNSET,
    _validated_raw as _validated_raw,
    config_path as config_path,
    settings_for as settings_for,
)

# isort: on
from dataretrieval.exceptions import ConfigurationError as ConfigurationError

__all__ = [
    "ADAPTERS",
    # The package-wide configuration, and the base every adapter subclasses.
    # Public because a caller writes ``Configuration(...)`` at every call site
    # that configures anything, and an adapter module names the base in its own
    # subclass.
    "BaseConfiguration",
    "Configuration",
    "config_path",
    "configure",
    "settings_for",
    "show_configuration",
]

# --- public API ----------------------------------------------------------


@contextmanager
def configure(*configurations: BaseConfiguration) -> Iterator[None]:
    """Apply configuration profiles for the duration of a ``with`` block.

    The highest-precedence source. Takes configuration objects positionally, at
    most one per adapter, and nothing else::

        with dataretrieval.configure(
            Configuration(api_key=secrets["usgs"]),
            WaterdataConfiguration.load("bulk"),
            NgwmnConfiguration(concurrency=4),
        ):
            df, md = waterdata.get_daily(monitoring_location_id=sites)

    The adapter a configuration targets is a property of its class, so the
    caller never restates it -- which is what keeps the adapter roster from
    being spelled once per call site. Naming two configurations for one adapter
    raises: they are the one pairing with no defined order between them.

    Because the block is delivered through a :class:`~contextvars.ContextVar`,
    a value set here applies to the current thread and to asyncio tasks started
    inside the block, and cannot leak into another thread, task, or unrelated
    call the way ``os.environ`` does -- which is what makes it safe for a server
    or notebook handling several users' credentials at once.

    Blocks nest and merge per setting: an inner block that sets only
    ``concurrency`` keeps the outer block's ``api_key``, and an adapter
    configuration in an outer block loses to a package-wide value set by a
    block nested inside it, so the innermost block always decides.

    Parameters
    ----------
    *configurations : BaseConfiguration
        A package-wide :class:`Configuration` and/or one configuration per
        adapter, in any order. Each adapter's class lives in that adapter's
        module -- ``WaterdataConfiguration`` in :mod:`dataretrieval.waterdata`,
        ``NgwmnConfiguration`` in :mod:`dataretrieval.ngwmn`, and so on.

    Yields
    ------
    None

    Raises
    ------
    ConfigurationError
        If an argument is not a configuration, or two of them target the same
        adapter. Raised on entry, before any request. An invalid *value*
        raises earlier still, where the configuration was constructed.

    Examples
    --------
    .. code-block:: python

        # credentials from a secret store, no environment mutation
        with dataretrieval.configure(
            Configuration(api_key=vault.read("usgs/pat"))
        ):
            df, md = waterdata.get_daily(monitoring_location_id="USGS-05114000")

        # a big overnight pull, from a [waterdata.bulk] table in the file
        with dataretrieval.configure(WaterdataConfiguration.load("bulk")):
            df, md = waterdata.get_daily(monitoring_location_id=many_sites)

    See Also
    --------
    show_configuration : Report the effective configuration and where it came from.
    """
    with _scope((*_scope.get(), _frame(configurations))):
        yield


def _frame(configurations: tuple[BaseConfiguration, ...]) -> _Frame:
    """Flatten one ``configure`` call's configurations into a scope frame.

    One frame per block, holding both scopes: a package-wide setting keyed by
    its name, an adapter-scoped one by ``(adapter, name)``. Values are rendered
    back to raw strings here so that every source shares one parser and one set
    of error messages; they were already checked when each configuration was
    constructed, so nothing new can fail at this point except the two
    call-shaped mistakes below. Rendering is therefore all this asks for --
    :func:`_coerce_typed` rather than :func:`_validated_raw`, so a value is not
    put through its grammar a second time on every block entry. Construction
    stays the single validation point, which is where a typo should raise
    anyway: at the line that wrote it, not at a later ``with`` statement.

    Each value is stored with the label naming the configuration it came from,
    because this is the last point where that is known -- see :data:`_Frame`.
    """
    overrides: dict[_ScopeKey, tuple[_SettingValue, str]] = {}
    seen: set[str | None] = set()
    for configuration in configurations:
        if not isinstance(configuration, BaseConfiguration):
            raise ConfigurationError(
                "configure() takes configuration objects, not "
                f"{type(configuration).__name__}. Package-wide settings go on "
                "Configuration(...); a setting for one service goes on that "
                "adapter's configuration, e.g. WaterdataConfiguration(...)."
            )

        adapter = configuration.adapter
        if adapter in seen:
            where = f"the {adapter} adapter" if adapter else "the package-wide settings"
            raise ConfigurationError(
                f"configure() got two configurations for {where}. Precedence "
                "between them would be undefined, so combine them into one."
            )

        seen.add(adapter)
        overrides.update(_configuration_overrides(configuration))
    return overrides


def _configuration_overrides(
    configuration: BaseConfiguration,
) -> dict[_ScopeKey, tuple[_SettingValue, str]]:
    """Render one configuration's values into raw-string overrides."""
    adapter = configuration.adapter
    label = configuration._provenance()
    overrides: dict[_ScopeKey, tuple[_SettingValue, str]] = {}
    for name, value in configuration.values().items():
        key: _ScopeKey = name if adapter is None else (adapter, name)
        raw = (
            None
            if value is None
            else _coerce_typed(name, value, configuration._label(name))
        )
        overrides[key] = (raw, label)
    return overrides


def show_configuration(*, stream: TextIO | None = None) -> None:
    """Print the effective configuration and where each setting came from.

    A debugging aid for "why is this using my old key?". Every value is
    reported with the origin that supplied it, named exactly: which variable,
    which table of the file, and -- when a caller selected one -- which
    profile. The API key is never printed, only whether one is set.

    Parameters
    ----------
    stream : file-like, optional
        Where to write. Defaults to ``sys.stdout``.

    Examples
    --------
    The sample below is generated by running this function, not written by
    hand; ``test_show_configuration_sample_output_is_current`` re-runs it and
    fails if the two drift apart.

    .. code-block:: text

        >>> with dataretrieval.configure(WaterdataConfiguration.load("bulk")):
        ...     dataretrieval.show_configuration()
        config file  /home/u/.dataretrieval/config.toml (found)
        api_key          <set>  /home/u/.dataretrieval/config.toml
        concurrency      16     /home/u/.dataretrieval/config.toml
        retries          8      $API_USGS_RETRIES
        progress         auto   built-in default
        parallel_chunks  1      built-in default
        stall_timeout    60s    built-in default

        A built-in default is package-wide. An adapter may prefer its own for
        its own calls; a value from any source above overrides both.

        adapter overrides
          waterdata  parallel_chunks  8  configure() block [waterdata.bulk]
          ngwmn      concurrency      4  /home/u/.dataretrieval/config.toml [ngwmn]

        profiles in the file: [waterdata.bulk]
          A profile applies only where a row above names it; select one in
          code with <Adapter>Configuration.load("<name>").

        not reported: nldi (not imported, so the settings each accepts are unknown here)
    """
    out = sys.stdout if stream is None else stream
    try:
        path = config_path()
    except ConfigurationError as exc:
        # Resolution itself can fail (a relative override with the working
        # directory removed). That is precisely a configuration a caller would
        # run this to understand, so report it as the file row rather than
        # raising out of the explainer.
        print(f"config file  <unresolved: {exc}>", file=out)
        return

    cell = _ErrorDeduplicatingCell()
    parsed = _show_file_status(out, path, cell)

    rows = [
        (name, cell(partial(_DISPLAYS[name], None)), cell(partial(_origin_label, name)))
        for name in SETTINGS
    ]
    _print_setting_rows(out, rows)
    _show_built_in_default_note(out, rows)
    _show_adapter_overrides(out, cell, {name: label for name, _value, label in rows})
    _show_profiles(out, parsed)
    _show_unimported_adapters(out)


class _ErrorDeduplicatingCell:
    """Render a value, deduplicating consecutive configuration errors.

    The report exists to explain a configuration, and the configurations
    most in need of explaining are the broken ones -- an unparseable file, a
    value that fails its grammar, a profile that no longer exists. Nothing
    here raises: each distinct failure is printed once, in the first place
    it shows up; a repeat is collapsed, so one invalid file does not bury the
    rows that did resolve under ten copies of the same message.
    """

    def __init__(self) -> None:
        self._reported: str | None = None

    def mark_reported(self, exc: ConfigurationError) -> None:
        """Record that *exc* was already printed, so a repeat collapses."""
        self._reported = str(exc)

    def __call__(self, render: Callable[[], object]) -> str:
        try:
            value = render()
        except ConfigurationError as exc:
            if str(exc) == self._reported:
                return "<unreadable>"
            self._reported = str(exc)
            return f"<error: {exc}>"
        return "" if value is None else str(value)


def _show_file_status(
    out: TextIO, path: Path, cell: _ErrorDeduplicatingCell
) -> _ParsedFile:
    """Probe and print the config file status line, returning the parsed file.

    Probing the file once here means a whole-file problem -- unparseable TOML,
    an invalid value at the top level -- is reported on the file row rather
    than repeated in every setting's row below.
    """
    parsed = _NO_FILE
    try:
        _, parsed = _current_file()
        status = "found" if path.exists() else "not found"
    except ConfigurationError as exc:
        cell.mark_reported(exc)
        status = f"ERROR: {exc}"
    print(f"config file  {path} ({status})", file=out)
    return parsed


def _print_setting_rows(out: TextIO, rows: list[tuple[str, str, str]]) -> None:
    """Print the package-wide setting rows in aligned columns."""
    name_width = max(len(name) for name, _value, _label in rows)
    value_width = max(len(value) for _name, value, _label in rows)
    for name, value, label in rows:
        print(f"{name:<{name_width}}  {value:<{value_width}}  {label}", file=out)


def _show_built_in_default_note(out: TextIO, rows: list[tuple[str, str, str]]) -> None:
    """Print the built-in default footnote when at least one row uses it."""
    if any(label == _BUILT_IN for _name, _value, label in rows):
        print(
            "\nA built-in default is package-wide. An adapter may prefer its own "
            "for\nits own calls; a value from any source above overrides both.",
            file=out,
        )


def _show_adapter_overrides(
    out: TextIO,
    cell: Callable[[Callable[[], object]], str],
    package_wide: Mapping[str, str],
) -> None:
    """Print the adapter-scoped settings that differ from the rows above.

    Only settings actually overridden, and only adapters that override one: a
    full adapter-by-setting grid would be mostly inherited values, burying the
    answer to "what will this call use" under the rows that change nothing.

    Each row names its origin exactly, which for a selected profile is the
    profile: ``configure() block [waterdata.bulk]`` rather than a bare block,
    so the report answers *which* profile put that value there.

    An adapter this process has not imported has no vocabulary to resolve
    against, so it is skipped here and named by
    :func:`_show_unimported_adapters` instead.
    """
    overrides = _collect_adapter_overrides(cell, package_wide)
    if not overrides:
        return
    print("\nadapter overrides", file=out)
    a_width = max(len(a) for a, _n, _v, _s in overrides)
    n_width = max(len(n) for _a, n, _v, _s in overrides)
    v_width = max(len(v) for _a, _n, v, _s in overrides)
    for adapter, name, value, label in overrides:
        print(
            f"  {adapter:<{a_width}}  {name:<{n_width}}  {value:<{v_width}}  {label}",
            file=out,
        )


def _collect_adapter_overrides(
    cell: Callable[[Callable[[], object]], str],
    package_wide: Mapping[str, str],
) -> list[tuple[str, str, str, str]]:
    """Gather adapter-scoped settings that differ from the package-wide rows.

    Separated from :func:`_show_adapter_overrides` so the collection logic --
    which carries the nesting -- is not interleaved with the formatting logic.
    """
    overrides: list[tuple[str, str, str, str]] = []
    for adapter in ADAPTERS:
        accepted = settings_for(adapter)
        if accepted is None:
            continue
        overrides.extend(_overrides_for_adapter(adapter, accepted, cell, package_wide))
    return overrides


def _overrides_for_adapter(
    adapter: str,
    accepted: frozenset[str],
    cell: Callable[[Callable[[], object]], str],
    package_wide: Mapping[str, str],
) -> list[tuple[str, str, str, str]]:
    """The overridden settings for one adapter.

    ``package_wide`` is what the rows above already resolved. An adapter-only
    setting has no row above, and no package-wide value it could inherit, so
    its baseline is the built-in default.
    """
    overrides: list[tuple[str, str, str, str]] = []
    for name in _ALL_SETTINGS:
        if name not in accepted:
            continue
        scoped = cell(partial(_origin_label, name, adapter))
        if scoped == package_wide.get(name, _BUILT_IN):
            continue  # inherited from the package-wide value
        value = cell(partial(_DISPLAYS[name], adapter))
        overrides.append((adapter, name, value, scoped))
    return overrides


def _show_profiles(out: TextIO, parsed: _ParsedFile) -> None:
    """Print the named profiles the file defines, selected or not.

    A named profile does nothing until a caller selects it, and that is the
    thing readers of a configuration file get wrong: adding
    ``[waterdata.bulk]`` changes no run on its own. A report that mentioned a
    profile only when one had been selected would leave that silence with
    nothing to explain it -- the file would look ignored.

    Names come from the parsed file, so an unimported adapter's profiles are
    listed too. What such a profile *means* is what needs the import; what it
    is called is a fact about the file, and withholding it here would make the
    section's answer depend on which optional extras happened to be installed.
    """
    defined = [
        f"[{adapter}.{name}]"
        for adapter in ADAPTERS
        for name in sorted(_named_profiles(parsed, adapter))
    ]
    if not defined:
        return
    print(f"\nprofiles in the file: {', '.join(defined)}", file=out)
    print(
        "  A profile applies only where a row above names it; select one in\n"
        '  code with <Adapter>Configuration.load("<name>").',
        file=out,
    )


def _show_unimported_adapters(out: TextIO) -> None:
    """Name the adapters this process cannot report on, and say why.

    An adapter is only known to accept a setting once the module declaring that
    vocabulary has been imported, and NLDI is deliberately imported on demand
    for the geopandas extra. So the rows above cannot cover it. Omitting it
    silently would read as "nothing is configured for nldi", which is a
    different claim and an incorrect one -- this is the cost of validating an
    adapter's keys lazily (ADR 0011).
    """
    unimported = [a for a in ADAPTERS if settings_for(a) is None]
    if unimported:
        print(
            f"\nnot reported: {', '.join(unimported)} "
            "(not imported, so the settings each accepts are unknown here)",
            file=out,
        )


def _origin_label(name: str, adapter: str | None = None) -> str:
    """The origin label for one setting, for :func:`show_configuration`."""
    return _resolve(name, adapter)[1]


# --- resolved settings ---------------------------------------------------


def api_key() -> str | None:
    """The Water Data API key, or ``None`` if none is configured.

    Surrounding whitespace is stripped, so a key read from a file with a
    trailing newline works; a blank value resolves to ``None``.
    """
    raw, _label, _source = _resolve("api_key")
    return raw.strip() or None if raw is not None else None


def concurrency(
    default: int | None = DEFAULT_CONCURRENCY, *, adapter: str | None = None
) -> int | None:
    """Cap on simultaneous chunks; ``None`` means unbounded.

    ``default`` is the caller's own preference for when nothing is configured --
    Water Use ships a lower figure than the OGC getters, because the NWDC is
    only stress-tested to that level. A value resolved from the chain always
    wins over it: a service able to override an explicit setting would make
    ``concurrency=1`` a lie.
    """
    raw, label, _source = _resolve("concurrency", adapter)
    if raw is None:
        return default
    return _parse_concurrency(raw, label)


def retries(*, adapter: str | None = None) -> int:
    """Retries attempted after the first try; ``0`` disables retrying."""
    raw, label, _source = _resolve("retries", adapter)
    if raw is None:
        return DEFAULT_RETRIES
    return _parse_retries(raw, label)


def progress() -> bool | None:
    """Explicit progress-line setting, or ``None`` to auto-detect.

    ``None`` means nothing configured it, so the caller applies its own
    default (a TTY or Jupyter kernel gets the line, redirected output
    doesn't).
    """
    raw, label, source = _resolve("progress")
    if raw is None:
        return None
    # Preserve the legacy environment behavior (any value outside the false
    # set enables progress), while new block/file values are validated strictly.
    return _parse_progress(raw, label, strict=source != _ENV)


def parallel_chunks(*, adapter: str | None = None) -> int:
    """Configured default fan-out for multi-value queries.

    ``1`` (the default) means "chunk only as much as the URL byte limit
    forces". This is the *baseline*;
    :func:`dataretrieval.parallel_chunks` overrides it for one call. Shares
    the name of that context manager because it is the same setting -- this
    is the resolved value, not the scoping block.
    """
    raw, label, _source = _resolve("parallel_chunks", adapter)
    if raw is None:
        return DEFAULT_PARALLEL_CHUNKS
    return _parse_parallel_chunks(raw, label)


def stall_timeout(*, adapter: str | None = None) -> float:
    """Longest a call may go without receiving data before retrying stops.

    Seconds; ``0`` disables the bound. Bounds the wall-clock cost of a dead
    connection, which the retry *count* does not: it counts attempts, not
    seconds. See :attr:`dataretrieval.transport.retry.RetryPolicy.stall_timeout`.
    """
    raw, label, _source = _resolve("stall_timeout", adapter)
    if raw is None:
        return DEFAULT_STALL_TIMEOUT
    return _parse_seconds(raw, label)


@overload
def base_url(*, adapter: str | None = ...) -> str | None: ...


@overload
def base_url(*, adapter: str | None = ..., default: str) -> str: ...


def base_url(*, adapter: str | None = None, default: str | None = None) -> str | None:
    """An adapter's configured base URL, falling back to *default*.

    Settable from code only: an adapter configuration may carry it, and both
    the file and the environment refuse it -- the file at :func:`_accepted_keys`
    and the environment at :data:`_REFUSED_ENV_VARS`, each with an error naming
    the block to write instead. A file that silently redirects a data-retrieval
    library to another host is a supply-chain-shaped hazard, while a
    ``configure`` block keeps the redirect where a reader of the script sees it
    (ADR 0011).

    There is no package-wide default, because there is no one base URL: what an
    adapter's requests are built on is the adapter's own fact, so the service
    passes its own -- ``base_url(adapter="nldi", default=NLDI_API_BASE_URL)``
    -- and the URL stays declared beside the service that owns it. What lives
    here is the *rule* for choosing between them, which was being spelled at
    every read site as ``... or SERVICE_DEFAULT``; a change to it (normalizing
    a trailing slash, say) is one edit rather than five.

    Parameters
    ----------
    adapter : str, optional
        Whose base URL to resolve.
    default : str, optional
        The service's own base, returned when nothing configured one. Omitted,
        the answer is ``None`` -- which is what :func:`show_configuration` asks
        for, having no service default to name.
    """
    raw, label, _source = _resolve("base_url", adapter)
    if raw is None:
        return default
    return _parse_base_url(raw, label)


# --- resolution ----------------------------------------------------------

#: Which source of the chain answered a resolution. Machine-readable so a
#: per-source rule reads the source, never the display label -- :func:`progress`
#: keys its legacy-lenient parsing on ``_ENV``, and the label stays purely
#: presentational.
_BLOCK, _ENV, _FILE, _DEFAULT = "block", "environment", "file", "built-in"


def _resolve(name: str, adapter: str | None = None) -> tuple[str | None, str, str]:
    """Return the raw value for *name*, its origin label, and its source.

    Precedence is *source-major*: the chain walks block, then environment, then
    file, exactly as ADR 0009 defines it -- and *within* each source an
    adapter-scoped value outranks a package-wide one. So a variable exported
    for one run still beats a stale ``[wqp]`` table in the config file --
    ordering by scope first, putting every adapter-scoped value ahead of every
    package-wide one whatever its source, would have quietly inverted that
    (ADR 0010).

    ``adapter`` names the adapter on whose behalf the setting is being read.
    ``None`` resolves the package-wide value, which is also what an adapter
    that declares no interest in this setting gets.

    Returns
    -------
    tuple[str or None, str, str]
        The raw string as written (parsing happens per setting, so each keeps
        its own blank-value rule), the human-readable origin label, and which
        source answered (one of the constants above) -- ``None`` with
        ``_BUILT_IN`` / ``_DEFAULT`` when nothing configured it.
    """
    _check_adapter_known(adapter)
    _check_env_not_refused(name)

    # ``None`` unless this adapter actually reads this setting, so a setting
    # outside its vocabulary resolves package-wide rather than looking for a
    # scope it could never have been written into.
    scoped: str | None = (
        adapter if adapter is not None and _accepts(adapter, name) else None
    )

    from_block = _resolve_from_block(name, scoped)
    if from_block is not None:
        return from_block

    from_env = _resolve_from_env(name)
    if from_env is not None:
        return from_env

    return _resolve_from_file(name, scoped)


def _check_adapter_known(adapter: str | None) -> None:
    """Raise if *adapter* is not in the configurable adapter roster.

    An adapter name nobody recognizes is a typo in *our* source, and its
    failure mode is silence: ``_accepts`` would wave every setting through,
    the file would hold no table under that name, and the read would fall
    through to the package-wide value -- so a ``[waterdata]`` table, or a
    ``WaterdataConfiguration``, would be ignored with nothing raised anywhere.
    """
    if adapter not in (*ADAPTERS, None):
        raise ConfigurationError(
            f"{adapter!r} is not a configurable adapter. The adapters are "
            f"{', '.join(ADAPTERS)}."
        )


def _check_env_not_refused(name: str) -> None:
    """Raise if an environment variable is set for a code-only setting.

    Refused before anything is consulted, not at the environment's turn in
    the chain. The file and the environment refuse ``base_url`` as one rule
    (ADR 0011), so a variable that cannot work is not silently outranked by a
    block that happens to work.
    """
    refused = _REFUSED_ENV_VARS.get(name)
    if refused is not None and refused in os.environ:
        raise ConfigurationError(
            f"{_env_label(refused)} is set, but {name!r} may only be set "
            "in code, in a configure() block, never from the environment. Unset "
            f"it and pass the value on the adapter's configuration, e.g. "
            f"WaterdataConfiguration({name}=...)."
        )


def _resolve_from_block(
    name: str, scoped: str | None
) -> tuple[str | None, str, str] | None:
    """Walk the scope stack for the first block that sets *name*.

    Innermost block first: a value set by a nested block wins over both
    scopes of an enclosing one. Within one block the adapter-scoped value is
    the more specific of the two, so it is asked first.
    """
    for frame in reversed(_scope.get()):
        if scoped is not None and (scoped, name) in frame:
            return (*frame[(scoped, name)], _BLOCK)
        if name in frame:
            return (*frame[name], _BLOCK)
    return None


def _resolve_from_env(name: str) -> tuple[str | None, str, str] | None:
    """Check whether an environment variable supplies the setting.

    No per-adapter environment variables: seven adapters times four settings
    is a namespace nobody can hold in mind, and an exported variable is
    invisible at the call site. See ADR 0010.
    """
    env = ENV_VARS.get(name)
    if env is None:
        return None
    raw = os.environ.get(env)
    if raw is not None and (raw.strip() or name in _BLANK_MEANS_SET):
        return raw, _env_label(env), _ENV
    return None


def _resolve_from_file(name: str, scoped: str | None) -> tuple[str | None, str, str]:
    """Fall through to the configuration file, then the built-in default.

    One load serves both scopes within the file.
    """
    path, parsed = _current_file()

    if scoped is not None:
        from_adapter = _adapter_file_settings(scoped, path, parsed)
        if name in from_adapter:
            return (*from_adapter[name], _FILE)

    if name in parsed.base:
        return parsed.base[name], str(path), _FILE

    return None, _BUILT_IN, _DEFAULT


def _accepts(adapter: str, name: str) -> bool:
    """Whether *adapter* reads the setting *name*.

    An adapter this process has not imported has no vocabulary to consult, so
    every setting is assumed to be in scope for it: the file stays valid either
    way, and an adapter cannot be misreading a setting it has not loaded. See
    :func:`settings_for`.
    """
    accepted = settings_for(adapter)
    return name in _ALL_SETTINGS if accepted is None else name in accepted


def _display_api_key(adapter: str | None = None) -> str:
    """Render the key's presence, never its value."""
    return "<set>" if api_key() else "<not set>"


def _display_concurrency(adapter: str | None = None) -> str:
    value = concurrency(adapter=adapter)
    return CONCURRENCY_UNBOUNDED if value is None else str(value)


def _display_progress(adapter: str | None = None) -> str:
    setting = progress()
    return "auto" if setting is None else ("on" if setting else "off")


#: How each setting renders in :func:`show_configuration`. Keyed by the same
#: names as :data:`_ALL_SETTINGS`, and asserted to cover them, so a setting
#: added to one without the other fails loudly instead of silently printing a
#: neighbour's value in the one report whose whole job is to be trustworthy.
#:
#: Every renderer takes the adapter to resolve for, so the adapter-override
#: rows use this same table rather than a parallel one that the guard below
#: would not cover. ``api_key`` and ``progress`` ignore it -- neither is
#: adapter-scoped, and :func:`_show_adapter_overrides` never asks them.
_DISPLAYS: dict[str, Callable[[str | None], str]] = {
    "api_key": _display_api_key,
    "concurrency": _display_concurrency,
    "retries": lambda adapter: str(retries(adapter=adapter)),
    "progress": _display_progress,
    "parallel_chunks": lambda adapter: str(parallel_chunks(adapter=adapter)),
    "stall_timeout": lambda adapter: f"{stall_timeout(adapter=adapter):g}s",
    "base_url": lambda adapter: base_url(adapter=adapter) or "<service default>",
}

if set(_DISPLAYS) != set(_ALL_SETTINGS):  # pragma: no cover - guards a coding error
    # Not an ``assert``: ``python -O`` strips those, and this guards the one
    # report whose whole job is to be trustworthy about provenance.
    raise RuntimeError(
        "every setting needs a show_configuration renderer; "
        f"missing={sorted(set(_ALL_SETTINGS) - set(_DISPLAYS))} "
        f"extra={sorted(set(_DISPLAYS) - set(_ALL_SETTINGS))}"
    )
