"""Layered configuration resolution for ``dataretrieval``.

Every tunable setting -- the Water Data API key, the fan-out concurrency cap,
the retry count, and the progress line -- resolves through one ordered chain so
a caller never has to mutate ``os.environ`` to configure a single call.

Sources, highest precedence first:

1. An active :func:`configure` block -- a :class:`~contextvars.ContextVar`, so a
   setting applies to the current thread or asyncio task and cannot leak into
   another one.
2. The environment variable for that setting (``API_USGS_PAT``,
   ``API_USGS_CONCURRENT``, ``API_USGS_RETRIES``, ``API_USGS_PROGRESS``).
3. The configuration file (TOML): ``~/.dataretrieval/config.toml``, or the path
   in ``DATARETRIEVAL_CONFIG``. Top-level keys are the defaults; a
   ``[profiles.<name>]`` table layers over them when that profile is selected.
4. The built-in default.

Precedence applies **per setting**, not per source: an environment that sets only
``API_USGS_PAT`` leaves a file-provided ``concurrency`` fully in effect. Putting
the environment above the file follows common deployment conventions and keeps
the original environment-variable interface authoritative (see ADR 0009).

Settings are additionally scoped **per adapter** (ADR 0010). A ``[ngwmn]`` table
in the file, or ``configure(ngwmn={...})``, applies to NGWMN calls and no
others, so one block can be gentle with one service while leaving the rest
alone. Precedence stays *source-major*: the chain still walks block, then
environment, then file, and an adapter-scoped value outranks a package-wide one
only *within* the same source. So a variable exported for one run still beats a
stale adapter table. Which settings an adapter accepts is its own vocabulary --
``concurrency`` means nothing to an adapter that issues one request -- and is
declared by the ``TypedDict`` schemas below. The API key is not among them: it
belongs to the gateway fronting a host, which Water Data and NGWMN share.

This module is a leaf: it imports only the standard library plus the Python 3.10
``tomli`` backport, so any module can depend on it without an import cycle or
pulling in httpx or pandas. It centralizes each setting's parser while retaining
legacy environment behavior and stricter validation for the new Python/TOML
surfaces.
"""

from __future__ import annotations

import math
import os
import stat
import sys
import warnings
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, field
from functools import partial
from numbers import Integral
from pathlib import Path
from types import MappingProxyType
from typing import Any, TextIO, TypedDict

from dataretrieval.exceptions import ConfigurationError

# ``ConfigurationError`` is re-exported; its canonical home and rationale are in
# :mod:`dataretrieval.exceptions`.
__all__ = [
    "configure",
    "show_configuration",
    "config_path",
    # Per-adapter setting schemas. Public because each annotates a parameter of
    # ``configure``, so a type checker names one in an error about a call --
    # and a name a caller cannot import is a poor thing to be shown.
    "WaterdataSettings",
    "NgwmnSettings",
    "NwdcSettings",
    "WqpSettings",
    "NldiSettings",
    "StreamstatsSettings",
]


#: The settings this module resolves, in display order.
SETTINGS: tuple[str, ...] = (
    "api_key",
    "concurrency",
    "retries",
    "progress",
    "parallel_chunks",
    "stall_timeout",
)

#: Environment variable backing a setting (precedence step 2).
#:
#: Not every setting has one. ``parallel_chunks`` is deliberately absent: it
#: fans a query into more sub-requests, each of which spends rate-limit quota,
#: and ``dataretrieval.parallel_chunks`` documents why that must stay a
#: deliberate choice rather than a process-wide default. An environment
#: variable is the wrong shape for it -- exported once in a shell profile,
#: inherited by every subprocess, invisible at the call site. A config-file
#: entry is written deliberately and shows up in :func:`show_configuration`, so the
#: file and :func:`configure` block are the only sources for it.
ENV_VARS: dict[str, str] = {
    "api_key": "API_USGS_PAT",
    "concurrency": "API_USGS_CONCURRENT",
    "retries": "API_USGS_RETRIES",
    "progress": "API_USGS_PROGRESS",
    "stall_timeout": "API_USGS_STALL_TIMEOUT",
}

#: Environment variable holding an explicit path to the configuration file.
CONFIG_PATH_ENV = "DATARETRIEVAL_CONFIG"

#: Environment variable selecting a ``[profiles.<name>]`` table.
PROFILE_ENV = "DATARETRIEVAL_PROFILE"

#: Source label for a setting no source supplied.
_BUILT_IN = "built-in default"

#: TOML table holding the named profiles.
_PROFILES_TABLE = "profiles"

#: Label for the file's top-level table, where keys are the defaults.
_TOP_LEVEL = "top level"

# Built-in defaults (precedence step 4). ``concurrency`` and ``retries`` keep the
# values the environment-only implementation used, so behavior is unchanged for
# anyone who configures nothing.
DEFAULT_CONCURRENCY = 32
DEFAULT_RETRIES = 4
DEFAULT_PARALLEL_CHUNKS = 1
DEFAULT_STALL_TIMEOUT = 60.0
CONCURRENCY_UNBOUNDED = "unbounded"


# --- adapter scope -------------------------------------------------------
#
# A setting means the same thing wherever it applies, but it does not apply
# everywhere (ADR 0010). Each adapter declares the settings it accepts as a
# ``TypedDict``; its ``__annotations__`` *are* the schema, so there is no
# second table to keep in step. Each is also the annotation of that adapter's
# named parameter on :func:`configure`, so ``mypy --strict`` rejects a setting
# the adapter does not read -- by schema name -- before the code runs.
#
# Two settings are deliberately absent from every adapter:
#
# ``api_key``     belongs to the gateway fronting a host, not to an adapter.
#                 Water Data and NGWMN are two adapters on one host sharing
#                 one key and one quota pool -- measured, see ADR 0010 -- so
#                 a per-adapter key would model a distinction that does not
#                 exist. ``credentials`` keeps sole ownership of it.
# ``progress``    describes the caller's terminal, not a service. There is one
#                 progress line per call, so scoping it per adapter could only
#                 produce a contradiction.


class _AnyAdapterSettings(TypedDict, total=False):
    """What every adapter accepts, whatever else it offers.

    Not an adapter itself, so it is not part of the public vocabulary: a caller
    names the adapter they are configuring, never this.
    """

    retries: int | None
    stall_timeout: float | int | None


class WaterdataSettings(_AnyAdapterSettings, total=False):
    """Settings :mod:`dataretrieval.waterdata` accepts."""

    concurrency: int | str | None
    parallel_chunks: int | None


class NgwmnSettings(_AnyAdapterSettings, total=False):
    """Settings :mod:`dataretrieval.ngwmn` accepts."""

    concurrency: int | str | None
    parallel_chunks: int | None


class NwdcSettings(_AnyAdapterSettings, total=False):
    """Settings :mod:`dataretrieval.nwdc` accepts.

    No ``parallel_chunks``: the NWDC is a plain CSV service, so a query fans
    out per location rather than being divided along a URL byte budget.
    """

    concurrency: int | str | None


class WqpSettings(_AnyAdapterSettings, total=False):
    """Settings :mod:`dataretrieval.wqp` accepts."""


class NldiSettings(_AnyAdapterSettings, total=False):
    """Settings :mod:`dataretrieval.nldi` accepts."""


class StreamstatsSettings(_AnyAdapterSettings, total=False):
    """Settings :mod:`dataretrieval.streamstats` accepts."""


# One class per adapter rather than one per capability shape, even though four
# of them are empty bodies today. The names are what a caller sees: a mypy
# error on ``configure(wqp={"concurrency": 2})`` reports the TypedDict by name,
# so it has to be a name they can look up and import. Shapes also do not stay
# grouped -- ``ssl_check`` (ADR 0010, deferred) applies to wqp, nwis and nwdc,
# which is not the fan-out/single-shot split -- so a per-shape class would have
# to be renamed or split the first time an adapter diverges.
_ADAPTER_SCHEMAS: dict[str, type] = {
    "waterdata": WaterdataSettings,
    "ngwmn": NgwmnSettings,
    "nwdc": NwdcSettings,
    "wqp": WqpSettings,
    "nldi": NldiSettings,
    "streamstats": StreamstatsSettings,
}

#: Which settings each adapter accepts, derived from the ``TypedDict`` above so
#: the schema has exactly one definition.
ADAPTER_SETTINGS: dict[str, frozenset[str]] = {
    name: frozenset(schema.__annotations__) for name, schema in _ADAPTER_SCHEMAS.items()
}

#: Adapter names, in the order :func:`show_configuration` reports them.
ADAPTERS: tuple[str, ...] = tuple(ADAPTER_SETTINGS)

# Values that turn the progress line off. Blank counts: ``API_USGS_PROGRESS=``
# has always meant "off", not "unset" -- unlike the numeric knobs, where blank
# falls through to the default.
_PROGRESS_FALSEY = frozenset({"", "0", "false", "no", "off"})

# Settings for which a *blank* environment variable is a value rather than an
# absence. ``API_USGS_PROGRESS=`` has always meant "off". For every other
# setting a blank variable is what container and CI tooling produces when it
# has nothing to pass (``docker run -e API_USGS_PAT``, a workflow secret that
# is absent on a fork), so treating it as configured would let it shadow the
# config file and silently drop the user's API key. Keeping this a property of
# the setting -- rather than a second, lower visit to the environment -- keeps
# the chain at the three tiers the docstring and ADR 0009 describe.
_BLANK_MEANS_SET = frozenset({"progress"})

# Warnings about the config file report the file, not a call site: settings are
# resolved lazily from wherever a getter first needs one, so the user frame is
# a different depth every time and no fixed ``stacklevel`` can name it. Pointing
# at this module consistently at least makes the warnings filterable by module,
# and every message names the offending path and setting.
_WARN_STACKLEVEL = 2
_PROGRESS_TRUTHY = frozenset({"1", "true", "yes", "on"})


class _Unset:
    """Sentinel that distinguishes an omitted override from explicit ``None``."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "<not set>"


# Typed as Any so public annotations describe accepted caller values without
# exposing this private implementation detail in generated signatures.
_UNSET: Any = _Unset()
_SettingValue = str | None

# Overrides from the innermost active ``configure`` block, as raw strings so that
# every source shares one parser and one set of error messages. The selected
# profile rides in the same mapping under ``_PROFILE_KEY`` -- it is not a
# setting, so it never resolves as one, but it inherits the same nesting and
# restore-on-exit for free. The default is an immutable empty mapping: a
# ``configure`` block always replaces the mapping wholesale rather than
# mutating it, and a read-only default makes that impossible to get wrong.
_PROFILE_KEY = "\0profile"
# A package-wide override is keyed by the setting's name; an adapter-scoped one
# by ``(adapter, name)``. One flat mapping rather than a nested one so that
# nesting, per-key inheritance, and restore-on-exit keep falling out of a
# single merge, whichever scope a block sets.
_ScopeKey = str | tuple[str, str]
_NO_OVERRIDES: Mapping[_ScopeKey, _SettingValue] = MappingProxyType({})
_scope: ContextVar[Mapping[_ScopeKey, _SettingValue]] = ContextVar(
    "dataretrieval_configuration", default=_NO_OVERRIDES
)

# Resolved config-file path, memoized on the raw ``DATARETRIEVAL_CONFIG``
# value (see :func:`config_path`).
_path_cache: tuple[str | None, object | None, Path] | None = None

# Parsed configuration file, keyed by file identity, change metadata, and raw
# content. POSIX ctime makes metadata hits reliable; Windows ctime is creation
# time, so cache hits there compare content before reusing the parsed result.
_FileStamp = tuple[int, int, int, int, int, int]
_file_cache: tuple[Path, _FileStamp, bytes, _ParsedFile] | None = None

# The file layer's settings, merged with the selected profile and labelled.
# Keyed on the parsed file's *identity* plus the profile and path it was built
# for, so it falls out of date exactly when ``_file_cache`` is replaced. See
# :func:`_file_settings`.
_merged_cache: (
    tuple[_ParsedFile, str | None, Path, Mapping[str, tuple[str, str]]] | None
) = None

# Validated ``[<adapter>]`` tables, keyed by adapter name and memoized on the
# same parsed-file identity. Separate from ``_merged_cache`` because an adapter
# table is validated only once that adapter is actually used.
_adapter_cache: dict[str, tuple[_ParsedFile, Path, Mapping[str, tuple[str, str]]]] = {}

# Paths already warned about for loose permissions, so the warning fires once.
_permission_warned: set[Path] = set()


@dataclass(frozen=True)
class _ParsedFile:
    """A parsed configuration file: top-level defaults plus named profiles.

    ``exists`` distinguishes "the file is there and defines nothing" from "there
    is no file", which decides whether selecting an undefined profile is a typo
    worth raising on (see :func:`_file_settings`).
    """

    base: dict[str, str] = field(default_factory=dict)
    #: Raw, *unvalidated* TOML tables -- see :func:`_interpret`.
    profiles: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: Raw, *unvalidated* ``[<adapter>]`` tables, keyed by adapter name. Left
    #: unvalidated for the same reason profiles are: a bad value in ``[nldi]``
    #: must not fail a Water Data call that never reads it.
    adapters: dict[str, dict[str, Any]] = field(default_factory=dict)
    exists: bool = False


#: Stand-in for "no configuration file", which is the common case. Shared
#: rather than rebuilt per read so that :func:`_file_settings` can memoize on
#: the parsed file's identity; nothing mutates a ``_ParsedFile``.
_NO_FILE = _ParsedFile()


# --- public API ----------------------------------------------------------


@contextmanager
def configure(
    *,
    api_key: str | None = _UNSET,
    concurrency: int | str | None = _UNSET,
    retries: int | None = _UNSET,
    progress: bool | str | None = _UNSET,
    parallel_chunks: int | None = _UNSET,
    stall_timeout: float | int | None = _UNSET,
    profile: str | None = _UNSET,
    waterdata: WaterdataSettings = _UNSET,
    ngwmn: NgwmnSettings = _UNSET,
    nwdc: NwdcSettings = _UNSET,
    wqp: WqpSettings = _UNSET,
    nldi: NldiSettings = _UNSET,
    streamstats: StreamstatsSettings = _UNSET,
    **unknown: object,
) -> Iterator[None]:
    """Apply configuration for the duration of a ``with`` block.

    The highest-precedence source. Because it is backed by a
    :class:`~contextvars.ContextVar`, a value set here applies to the current
    thread and to asyncio tasks started inside the block, and cannot leak into
    another thread, task, or unrelated call the way ``os.environ`` does --
    which is what makes it safe for a server or notebook handling several
    users' credentials at once::

        with dataretrieval.configure(api_key=secrets["usgs"]):
            df, md = waterdata.get_daily(monitoring_location_id="USGS-05114000")

    Values are validated on entry, so a typo raises here rather than deep in a
    later request. Blocks nest, and merge per setting -- an inner block that
    sets only ``concurrency`` keeps the outer block's ``api_key``.

    Omitting a setting inherits it from an outer block or lower-precedence
    source. Passing ``None`` explicitly suppresses those sources and restores
    the built-in behavior for that setting (no key, automatic progress, and so
    on). ``profile=None`` selects the file's top-level settings even when
    ``DATARETRIEVAL_PROFILE`` is set.

    Parameters
    ----------
    api_key : str, optional
        Water Data API key, sent as ``X-Api-Key`` and only ever to
        ``api.waterdata.usgs.gov``. Prefer reading it from a secret store or
        the environment or configuration file over writing a literal into a
        script. Pass ``None`` to make a call without an ambient key.
    concurrency : int or str, optional
        Cap on simultaneous sub-requests: a positive integer, or
        ``"unbounded"`` to disable the cap.
    retries : int, optional
        Retries attempted after a transient failure; ``0`` disables retrying.
    progress : bool or str, optional
        Whether to draw the progress line. ``None`` leaves the automatic
        behavior (on for a TTY or Jupyter kernel, off otherwise).
    parallel_chunks : int, optional
        Default optional fan-out for multi-value queries. It limits extra
        refinement, but URL-byte safety may already require more sub-requests.
        Sets the baseline that :func:`dataretrieval.parallel_chunks` overrides
        per call. Each sub-request spends rate-limit quota, so raise it only
        for pulls you know are large.
    profile : str, optional
        Name of a ``[profiles.<name>]`` table in the configuration file to
        layer over the file's top-level settings. Pass ``None`` to ignore an
        environment-selected profile.

    Yields
    ------
    None

    Examples
    --------
    .. code-block:: python

        # credentials from a secret store, no environment mutation
        with dataretrieval.configure(api_key=vault.read("usgs/pat")):
            df, md = waterdata.get_daily(monitoring_location_id="USGS-05114000")

        # a big overnight pull, using a profile from the config file
        with dataretrieval.configure(profile="bulk-pull"):
            df, md = waterdata.get_daily(monitoring_location_id=many_sites)

    See Also
    --------
    show_configuration : Report the effective configuration and where it came from.
    """
    supplied = {
        "api_key": api_key,
        "concurrency": concurrency,
        "retries": retries,
        "progress": progress,
        "parallel_chunks": parallel_chunks,
        "stall_timeout": stall_timeout,
    }
    overrides: dict[_ScopeKey, _SettingValue] = {
        name: _normalize_override(name, value)
        for name, value in supplied.items()
        if value is not _UNSET
    }
    adapters = {
        "waterdata": waterdata,
        "ngwmn": ngwmn,
        "nwdc": nwdc,
        "wqp": wqp,
        "nldi": nldi,
        "streamstats": streamstats,
    }
    overrides.update(
        _normalize_adapters(
            {name: t for name, t in adapters.items() if t is not _UNSET}, unknown
        )
    )

    # The selected profile rides in the same mapping as the settings, so
    # nesting and per-key inheritance fall out of one merge. ``_PROFILE_KEY``
    # is not in ``SETTINGS``, so it is never resolved as one.
    merged = {**_scope.get(), **overrides}
    if profile is not _UNSET:
        merged[_PROFILE_KEY] = _normalize_profile(profile)
    token = _scope.set(merged)
    try:
        # An explicitly selected profile is a value supplied to this block, so
        # validate its existence on entry rather than on a later request.
        if profile is not _UNSET and profile is not None:
            _file_settings()
        yield
    finally:
        _scope.reset(token)


def show_configuration(*, stream: TextIO | None = None) -> None:
    """Print the effective configuration and the source of each setting.

    A debugging aid for "why is this using my old key?". The API key is never
    printed -- only whether one is set and where it came from.

    Parameters
    ----------
    stream : file-like, optional
        Where to write. Defaults to ``sys.stdout``.

    Examples
    --------
    .. code-block:: text

        >>> dataretrieval.show_configuration()
        config file  /home/u/.dataretrieval/config.toml (found)
        profile      default
        api_key      <set>       /home/u/.dataretrieval/config.toml
        concurrency  32          built-in default
        retries      8           $API_USGS_RETRIES
        progress     auto        built-in default
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

    # Nothing here raises. This function exists to explain a configuration, and
    # the configurations most in need of explaining are the broken ones -- an
    # unparseable file, a value that fails its grammar, a profile that no
    # longer exists. Each distinct failure is printed once, in the first place
    # it shows up; a repeat is collapsed, so one bad file does not bury the
    # rows that did resolve under ten copies of the same message.
    reported: str | None = None

    def cell(render: Callable[[], object]) -> str:
        nonlocal reported
        try:
            value = render()
        except ConfigurationError as exc:
            if str(exc) == reported:
                return "<unreadable>"
            reported = str(exc)
            return f"<error: {exc}>"
        return "" if value is None else str(value)

    # Probing the whole file layer (not just the parse) means a bad profile --
    # which ``_file_settings`` raises, not ``_load_file`` -- is also reported
    # here rather than in all five rows.
    try:
        _file_settings()
        status = "found" if path.exists() else "not found"
    except ConfigurationError as exc:
        reported = str(exc)
        status = f"ERROR: {exc}"
    print(f"config file  {path} ({status})", file=out)
    print(f"profile      {_active_profile() or 'default'}", file=out)

    rows = [
        (name, cell(partial(_DISPLAYS[name], None)), cell(partial(_source_label, name)))
        for name in SETTINGS
    ]
    name_width = max(len(name) for name, _value, _source in rows)
    value_width = max(len(value) for _name, value, _source in rows)
    for name, value, source in rows:
        print(f"{name:<{name_width}}  {value:<{value_width}}  {source}", file=out)

    # A built-in default is package-wide, and a service may prefer its own for
    # its own calls -- so a row reading "built-in default" is not a promise
    # about every service. Saying so is the honest scope of this report: this
    # module is a leaf and cannot enumerate the services, and a value from any
    # source outranks both kinds of default anyway.
    if any(source == _BUILT_IN for _name, _value, source in rows):
        print(
            "\nA built-in default is package-wide. An adapter may prefer its own "
            "for\nits own calls; a value from any source above overrides both.",
            file=out,
        )

    _show_adapter_overrides(out, cell)


def _show_adapter_overrides(out: TextIO, cell: Callable[..., str]) -> None:
    """Print the adapter-scoped settings that differ from the rows above.

    Only settings actually overridden, and only adapters that override one: a
    full adapter-by-setting grid would be mostly inherited values, burying the
    answer to "what will this call use" under the rows that change nothing.
    """
    overrides: list[tuple[str, str, str, str]] = []
    for adapter in ADAPTERS:
        for name in SETTINGS:
            if name not in ADAPTER_SETTINGS[adapter]:
                continue
            scoped = cell(partial(_source_label, name, adapter))
            if scoped == cell(partial(_source_label, name)):
                continue  # inherited from the package-wide tier
            value = cell(partial(_DISPLAYS[name], adapter))
            overrides.append((adapter, name, value, scoped))

    if not overrides:
        return

    print("\nadapter overrides", file=out)
    a_width = max(len(a) for a, _n, _v, _s in overrides)
    n_width = max(len(n) for _a, n, _v, _s in overrides)
    v_width = max(len(v) for _a, _n, v, _s in overrides)
    for adapter, name, value, source in overrides:
        print(
            f"  {adapter:<{a_width}}  {name:<{n_width}}  {value:<{v_width}}  {source}",
            file=out,
        )


def _env_source_label(env_var: str) -> str:
    """How a value read from ``env_var`` is reported as a source.

    One spelling, because :func:`progress` distinguishes the environment tier
    by comparing against it: two ``f"${...}"`` literals would let a reword of
    the display string change how values parse.
    """
    return f"${env_var}"


def _toml_parser() -> Any:
    """The TOML parser, imported on first use.

    ``import dataretrieval`` imports this module, but the parser is reachable
    only once a configuration file actually exists -- the minority case.
    Importing it eagerly costs every caller ~4 ms of ``tomllib`` regex
    compilation for a file most of them do not have.
    """
    if sys.version_info >= (3, 11):
        import tomllib
    else:  # pragma: no cover - exercised only on Python 3.10
        import tomli as tomllib
    return tomllib


def _source_label(name: str, adapter: str | None = None) -> str:
    """The provenance label for one setting, for :func:`show_configuration`."""
    return _resolve(name, adapter)[1]


def config_path() -> Path:
    """Path to the configuration file, honoring ``DATARETRIEVAL_CONFIG``.

    Memoized on the raw ``DATARETRIEVAL_CONFIG`` value, because this sits on
    the per-request path via :func:`api_key` and building the default costs
    more than the ``stat`` it leads to (``Path.home()`` alone dominates the
    whole resolution). Returning a stable object also lets :func:`_load_file`
    check its cache by identity instead of re-normalizing a fresh ``Path``.

    Returns
    -------
    pathlib.Path
        The explicit path from ``DATARETRIEVAL_CONFIG`` if set, otherwise
        ``~/.dataretrieval/config.toml``. The file need not exist.
    """
    global _path_cache
    override = os.environ.get(CONFIG_PATH_ENV)

    # Probe the memo before doing any work: this runs once per request via
    # ``api_key()``, so the hit path should be a dict lookup and a compare.
    cached = _path_cache
    if cached is not None and cached[0] == override:
        cached_guard, path = cached[1], cached[2]
        # The memo is only valid while whatever the path was *derived from* is
        # unchanged, so each branch records its own guard. A relative override
        # is anchored to the working directory (a later ``os.chdir`` in a
        # per-job notebook or scheduler must not keep reading the previous
        # job's file); the default branch is anchored to ``$HOME``. An absolute
        # override depends on neither and guards with ``None``. ``stat(".")``
        # identifies the directory ~17x cheaper than ``getcwd()``, which
        # reifies the whole path string.
        if cached_guard is None or cached_guard == _path_guard(cached_guard):
            return path

    expanded = (
        Path(override.strip()).expanduser() if override and override.strip() else None
    )
    guard: object | None
    if expanded is None:
        path = _default_home_path()
        guard = _home_id()
    elif expanded.is_absolute():
        path = expanded
        guard = None
    else:
        guard = _cwd_id()
        path = _resolve_against_cwd(expanded)
    _path_cache = (override, guard, path)
    return path


def _default_home_path() -> Path:
    """The default ``~/.dataretrieval/config.toml``, or an unusable path.

    ``Path.home()`` raises ``RuntimeError`` where no home can be resolved at all
    -- a rootless container running as an arbitrary UID with no passwd entry and
    no ``HOME``. That is not a misconfiguration to report: such a deployment
    simply has no config file, and before settings were layered it worked fine
    on the environment alone. So the unexpanded ``~/...`` form is returned
    instead: it does not exist, which keeps the whole file layer inert rather
    than failing every request from inside the header builder, and it still
    reads correctly in :func:`show_configuration` output.
    """
    try:
        home = Path.home()
    except (RuntimeError, OSError):
        return Path("~") / ".dataretrieval" / "config.toml"
    return home / ".dataretrieval" / "config.toml"


def _resolve_against_cwd(relative: Path) -> Path:
    """Resolve a relative override, or report a working directory that is gone.

    A scratch-dir job that removes its own cwd cannot resolve a relative
    ``DATARETRIEVAL_CONFIG`` at all. That surfaces as a :class:`ConfigurationError`
    rather than a bare ``OSError`` escaping onto the request path -- the
    taxonomy contract the rest of this module keeps.
    """
    try:
        return Path.cwd() / relative
    except OSError as exc:
        raise ConfigurationError(
            f"cannot resolve the relative {CONFIG_PATH_ENV} path {str(relative)!r}: "
            f"the working directory is unavailable ({exc})."
        ) from exc


def _path_guard(previous: object) -> object:
    """Re-read whichever guard the cached entry was built with."""
    return _cwd_id() if isinstance(previous, tuple) else _home_id()


def _cwd_id() -> tuple[int, int]:
    """Identify the working directory without building its path string.

    Only identifies the directory; :func:`_resolve_against_cwd` is what turns a
    missing cwd into a :class:`ConfigurationError`. Both are needed, because ``stat``
    on a *deleted* working directory still succeeds -- the process holds the
    open handle -- while resolving its path does not.
    """
    try:
        st = os.stat(".")
    except OSError as exc:
        raise ConfigurationError(
            f"cannot resolve the relative {CONFIG_PATH_ENV} path: the working "
            f"directory is unavailable ({exc})."
        ) from exc
    return (st.st_dev, st.st_ino)


def _home_id() -> str:
    """The home directory as the environment reports it.

    A plain environment read, not ``Path.home()``: this is on the per-request
    path and only needs to detect a *change* (a test or notebook that
    reassigns the home variable after the first resolution), not to resolve
    the path.

    Which variable that is differs by platform, and the memo has to agree with
    the resolver or it watches the wrong thing. ``posixpath.expanduser`` reads
    ``HOME``; ``ntpath.expanduser`` reads ``USERPROFILE`` (then
    ``HOMEDRIVE``/``HOMEPATH``) and ignores ``HOME`` outright. Preferring
    ``HOME`` everywhere means that on Windows -- where Git Bash and MSYS do set
    it -- the memo invalidates on a variable that cannot move the path, and
    misses the ``USERPROFILE`` change that can.
    """
    if os.name == "nt":
        return (
            os.environ.get("USERPROFILE")
            or os.environ.get("HOMEDRIVE", "") + os.environ.get("HOMEPATH", "")
            or ""
        )
    return os.environ.get("HOME") or ""


# --- resolved settings ---------------------------------------------------


def api_key() -> str | None:
    """The Water Data API key, or ``None`` if none is configured.

    Surrounding whitespace is stripped, so a key read from a file with a
    trailing newline works; a blank value resolves to ``None``.
    """
    raw, _source = _resolve("api_key")
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
    raw, source = _resolve("concurrency", adapter)
    if raw is None:
        return default
    return _parse_concurrency(raw, source)


def retries(*, adapter: str | None = None) -> int:
    """Retries attempted after the first try; ``0`` disables retrying."""
    raw, source = _resolve("retries", adapter)
    if raw is None:
        return DEFAULT_RETRIES
    return _parse_retries(raw, source)


def progress() -> bool | None:
    """Explicit progress-line setting, or ``None`` to auto-detect.

    ``None`` means nothing configured it, so the caller applies its own
    default (a TTY or Jupyter kernel gets the line, redirected output
    doesn't).
    """
    raw, source = _resolve("progress")
    if raw is None:
        return None
    # Preserve the legacy environment behavior (any value outside the false
    # set enables progress), while new block/file values are validated strictly.
    # The test goes through :func:`_env_source_label`, which is also what wrote
    # the label -- comparing against a re-spelled ``f"${...}"`` here would make
    # rewording a *display* string silently switch env values to strict parsing.
    strict = source != _env_source_label(ENV_VARS["progress"])
    return _parse_progress(raw, source, strict=strict)


def parallel_chunks(*, adapter: str | None = None) -> int:
    """Configured default fan-out for multi-value queries.

    ``1`` (the default) means "chunk only as much as the URL byte limit
    forces". This is the *baseline*;
    :func:`dataretrieval.parallel_chunks` overrides it for one call. Shares
    the name of that context manager because it is the same setting -- this
    is the resolved value, not the scoping block.
    """
    raw, source = _resolve("parallel_chunks", adapter)
    if raw is None:
        return DEFAULT_PARALLEL_CHUNKS
    return _parse_parallel_chunks(raw, source)


def stall_timeout(*, adapter: str | None = None) -> float:
    """Longest a call may go without receiving data before retrying stops.

    Seconds; ``0`` disables the bound. Bounds the wall-clock cost of a dead
    connection, which the retry *count* does not: it counts attempts, not
    seconds. See :attr:`dataretrieval.transport.retry.RetryPolicy.stall_timeout`.
    """
    raw, source = _resolve("stall_timeout", adapter)
    if raw is None:
        return DEFAULT_STALL_TIMEOUT
    return _parse_seconds(raw, source)


# --- value grammar -------------------------------------------------------
#
# One parser drives each setting's grammar, so a value means the same thing and
# reports the same way whichever source wrote it. Source-level adapters retain
# TOML types and reject Python API type errors before producing raw strings.


def _type_error(source: str, expected: str, value: object) -> ConfigurationError:
    """Build a type error without rendering a possibly secret value."""
    return ConfigurationError(
        f"{source} must be {expected} (got {type(value).__name__})."
    )


def _coerce_typed(name: str, value: object, source: str, *, optional: str = "") -> str:
    """Type-check one source-level value and render it as a raw string.

    Shared by the two *typed* surfaces -- ``configure()`` keyword arguments and
    TOML scalars -- so a value accepted from one is accepted from the other and
    a tightened rule cannot land on only half of them. (The environment is not
    typed: it delivers strings, which go straight to :func:`_validate_raw`.)

    ``optional`` is the only thing that differs between them: the Python
    surface accepts ``None`` and says so in its messages. (Integers are matched
    as :class:`numbers.Integral` for both -- a numpy or pandas integer is a
    legitimate count from Python, and ``tomllib`` only ever yields ``int``, so
    the wider check cannot change a TOML outcome.)
    """
    if name == "api_key":
        if not isinstance(value, str):
            raise _type_error(source, "a string" + optional, value)
        return value
    if name == "progress":
        if isinstance(value, bool):
            return str(value)
        if isinstance(value, str):
            return value
        raise _type_error(source, "a bool or recognized string" + optional, value)
    if name == "concurrency":
        if isinstance(value, bool) or not isinstance(value, (Integral, str)):
            raise _type_error(source, "an integer or 'unbounded'" + optional, value)
        if isinstance(value, str) and value.strip().lower() != CONCURRENCY_UNBOUNDED:
            raise ConfigurationError(f"{source} must be an integer or 'unbounded'.")
        return str(value)
    if name == "stall_timeout":
        # Seconds, so a fractional value is meaningful -- unlike the counts
        # below, which are whole by nature.
        if isinstance(value, bool) or not isinstance(value, (Integral, float)):
            raise _type_error(source, "a number of seconds" + optional, value)
        return str(value)
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise _type_error(source, "an integer" + optional, value)
    return str(value)


def _normalize_override(name: str, value: object) -> _SettingValue:
    """Validate and normalize one value supplied to :func:`configure`."""
    if value is None:
        return None
    source = f"{name}= in configure()"
    raw = _coerce_typed(name, value, source, optional=", or None")
    _validate_raw(name, raw, source)
    return raw


def _normalize_adapters(
    adapters: Mapping[str, Mapping[str, object]],
    unknown: Mapping[str, object] = MappingProxyType({}),
) -> dict[_ScopeKey, _SettingValue]:
    """Validate the per-adapter tables passed to :func:`configure`.

    Each adapter is a *named* parameter on :func:`configure`, annotated with
    its own ``TypedDict``, so a type checker rejects a setting the adapter does
    not read before the code runs. ``unknown`` is what the remaining catch-all
    swept up -- most importantly a *misspelled setting*, since
    ``configure(concurrancy=8)`` lands there rather than raising ``TypeError``.
    Silently accepting and ignoring that would be the worst outcome for this
    module, so it is reported against the settings first and the adapters
    second.
    """
    out: dict[_ScopeKey, _SettingValue] = {}
    for keyword in unknown:
        if keyword in SETTINGS:
            raise ConfigurationError(
                f"{keyword}= in configure() takes a value directly, not a "
                f"table. Write configure({keyword}=...)."
            )
        raise ConfigurationError(
            f"configure() got an unexpected keyword {keyword!r}. Settings are "
            f"passed directly ({', '.join(SETTINGS)}); a table is per-adapter "
            f"({', '.join(ADAPTERS)})."
        )
    for adapter, table in adapters.items():
        if not isinstance(table, Mapping):
            raise _type_error(
                f"{adapter}= in configure()", "a mapping of settings", table
            )
        accepted = ADAPTER_SETTINGS[adapter]
        for name, value in table.items():
            if name not in accepted:
                known = f"It accepts: {', '.join(sorted(accepted))}."
                if name in SETTINGS:
                    raise ConfigurationError(
                        f"{name!r} in configure({adapter}=...) is not a setting "
                        f"the {adapter} adapter reads. {known}"
                    )
                raise ConfigurationError(
                    f"unknown setting {name!r} in configure({adapter}=...). {known}"
                )
            source = f"{name!r} in configure({adapter}=...)"
            out[(adapter, name)] = (
                None
                if value is None
                else _validated_raw(name, value, source, optional=", or None")
            )
    return out


def _validated_raw(name: str, value: object, source: str, *, optional: str = "") -> str:
    """Type-check, render and grammar-check one typed value."""
    raw = _coerce_typed(name, value, source, optional=optional)
    _validate_raw(name, raw, source)
    return raw


def _normalize_profile(value: object) -> str | None:
    """Validate and normalize a profile supplied to :func:`configure`."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise _type_error(
            "profile= in configure()", "a non-empty string or None", value
        )
    profile = value.strip()
    if not profile:
        raise ConfigurationError("profile= in configure() must not be blank.")
    return profile


def _parse_int(
    raw: str,
    source: str,
    *,
    default: int,
    minimum: int,
    examples: str | None = None,
) -> int:
    """Parse a bounded integer setting; blank falls through to *default*.

    Parameters
    ----------
    raw : str
        The value as written, from whichever source supplied it.
    source : str
        Human-readable origin, used as the subject of any error message.
    default : int
        Returned for a blank value, matching the environment-variable
        behavior this replaced.
    minimum : int
        Smallest accepted value.
    examples : str, optional
        Illustrative values appended to the message (e.g. ``"2, 8, 32"``).
    """
    value = raw.strip()
    if value == "":
        return default
    expected = f"an integer >= {minimum}" + (f", e.g. {examples}" if examples else "")
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ConfigurationError(f"{source} must be {expected} (got {raw!r}).") from exc
    if parsed < minimum:
        raise ConfigurationError(f"{source} must be {expected} (got {parsed}).")
    return parsed


def _parse_seconds(raw: str, source: str) -> float:
    """Parse a non-negative duration in seconds; blank falls through.

    Seconds rather than a count, so fractional values are accepted. ``0``
    disables the bound it guards, which is why the floor is zero rather than
    one.
    """
    value = raw.strip()
    if value == "":
        return DEFAULT_STALL_TIMEOUT
    expected = "a finite, non-negative number of seconds"
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ConfigurationError(f"{source} must be {expected} (got {raw!r}).") from exc
    # ``inf`` and ``nan`` both parse as floats and both defeat the bound they
    # are meant to set: ``inf`` makes every wait allowed, and ``nan`` compares
    # false against every threshold. TOML has literal ``inf``/``nan``, so this
    # is reachable from the file as well as from Python.
    if not math.isfinite(parsed) or parsed < 0:
        raise ConfigurationError(f"{source} must be {expected} (got {parsed}).")
    return parsed


def _parse_concurrency(raw: str, source: str) -> int | None:
    """Parse a concurrency cap: a positive int, or ``unbounded`` -> ``None``."""
    if raw.strip().lower() == CONCURRENCY_UNBOUNDED:
        return None
    try:
        return _parse_int(raw, source, default=DEFAULT_CONCURRENCY, minimum=1)
    except ConfigurationError as exc:
        raise ConfigurationError(
            f"{exc} Use '{CONCURRENCY_UNBOUNDED}' to disable the cap."
        ) from exc


def _parse_progress(raw: str, source: str, *, strict: bool) -> bool:
    """Parse a progress toggle, optionally preserving legacy env truthiness."""
    value = raw.strip().lower()
    if strict and not value:
        raise ConfigurationError(f"{source} must not be blank.")
    if value in _PROGRESS_FALSEY:
        return False
    if value in _PROGRESS_TRUTHY:
        return True
    if not strict:
        return True
    expected = ", ".join(sorted(_PROGRESS_TRUTHY | _PROGRESS_FALSEY))
    raise ConfigurationError(f"{source} must be one of {expected} (got {raw!r}).")


# Each integer setting's grammar, named once. The accessor and the eager
# block/TOML validator below both spell the parser this way, so a change to a
# bound (say ``minimum``) cannot leave a ``configure()`` block validating
# against different rules than the value it later resolves.
_parse_retries = partial(_parse_int, default=DEFAULT_RETRIES, minimum=0)
_parse_parallel_chunks = partial(
    _parse_int, default=DEFAULT_PARALLEL_CHUNKS, minimum=1, examples="2, 8, 32"
)

#: Per-setting validators used for eager block and TOML validation.
_VALIDATORS: dict[str, Callable[[str, str], object]] = {
    "concurrency": _parse_concurrency,
    "retries": _parse_retries,
    "progress": partial(_parse_progress, strict=True),
    "parallel_chunks": _parse_parallel_chunks,
    "stall_timeout": _parse_seconds,
}


def _validate_raw(name: str, raw: str, source: str) -> None:
    """Run a setting's grammar validator when it has one."""
    validate = _VALIDATORS.get(name)
    if validate is not None:
        validate(raw, source)


# --- resolution ----------------------------------------------------------


def _resolve(name: str, adapter: str | None = None) -> tuple[str | None, str]:
    """Return the raw value for *name* and a human-readable source label.

    Precedence is *source-major*: the chain walks block, then environment, then
    file, exactly as ADR 0009 defines it -- and *within* each source an
    adapter-scoped value outranks a package-wide one. So a variable exported
    for one run still beats a stale ``[wqp]`` table in the config file, which
    scope-major ordering would have quietly inverted (ADR 0010).

    ``adapter`` names the adapter on whose behalf the setting is being read.
    ``None`` resolves the package-wide value, which is also what an adapter
    that declares no interest in this setting gets.

    Returns
    -------
    tuple[str or None, str]
        The raw string as written (parsing happens per setting, so each keeps
        its own blank-value rule), and where it came from -- ``None`` with
        ``_BUILT_IN`` when nothing configured it.
    """
    # ``None`` unless this adapter actually reads this setting, so a setting
    # outside its schema resolves package-wide rather than looking for a scope
    # it could never have been written into.
    scoped: str | None = (
        adapter if adapter is not None and name in ADAPTER_SETTINGS[adapter] else None
    )

    scope = _scope.get()
    if scoped is not None and (scoped, name) in scope:
        return scope[(scoped, name)], f"configure() block [{scoped}]"
    if name in scope:
        return scope[name], "configure() block"

    # No per-adapter environment variables: seven adapters times four settings
    # is a namespace nobody can hold in mind, and an exported variable is
    # invisible at the call site. See ADR 0010.
    env = ENV_VARS.get(name)
    if env is not None:
        raw = os.environ.get(env)
        if raw is not None and (raw.strip() or name in _BLANK_MEANS_SET):
            return raw, _env_source_label(env)

    if scoped is not None:
        from_adapter = _adapter_file_settings(scoped)
        if name in from_adapter:
            return from_adapter[name]

    from_file = _file_settings()
    if name in from_file:
        return from_file[name]

    return None, _BUILT_IN


def _active_profile() -> str | None:
    """The selected profile name: a :func:`configure` block wins over the env."""
    scope = _scope.get()
    if _PROFILE_KEY in scope:
        return scope[_PROFILE_KEY]
    env = os.environ.get(PROFILE_ENV)
    return env.strip() if env and env.strip() else None


def _file_settings() -> Mapping[str, tuple[str, str]]:
    """File-sourced settings, each with a label naming exactly where it came from.

    A selected ``[profiles.<name>]`` table layers over the file's top-level
    keys per setting, so a profile that only tunes ``concurrency`` still
    inherits the top-level ``api_key`` -- and each value's label names the
    table it actually came from, not merely the profile in effect.

    Selecting a profile the file doesn't define is a typo, and raises. But
    with *no config file at all* there are no profiles to select from and the
    whole file layer is inert, so a lingering ``DATARETRIEVAL_PROFILE`` export
    is ignored rather than failing every request from inside
    :func:`~dataretrieval.utils._default_headers`.
    """
    path = config_path()
    parsed = _load_file(path)
    profile = _active_profile()

    # Settings resolve one at a time and lazily, so this runs once per setting
    # per call -- and with a profile selected it re-ran ``_scalars`` over the
    # whole profile table each time, re-coercing and re-validating values that
    # cannot have changed. ``_load_file`` returns the *same* object while the
    # file is unchanged, so its identity is a sound key: the memo falls out of
    # date exactly when the parsed file does.
    global _merged_cache
    if _merged_cache is not None:
        cached_parsed, cached_profile, cached_path, cached_merged = _merged_cache
        if (
            cached_parsed is parsed
            and cached_profile == profile
            and cached_path == path
        ):
            return cached_merged

    merged: dict[str, tuple[str, str]] = {
        name: (value, str(path)) for name, value in parsed.base.items()
    }

    if profile is None:
        result = MappingProxyType(merged)
        _merged_cache = (parsed, profile, path, result)
        return result
    if profile not in parsed.profiles:
        # With no file at all there are no profiles to select from. A lingering
        # DATARETRIEVAL_PROFILE export is then ignored rather than failing every
        # request -- but a name the caller just typed into configure() is a typo
        # worth reporting at the ``with``, which is what its docstring promises.
        if not parsed.exists and _PROFILE_KEY not in _scope.get():
            # Not memoized: this depends on the active scope, not on the file.
            return MappingProxyType(merged)
        if not parsed.exists:
            raise ConfigurationError(
                f"profile {profile!r} cannot be selected: there is no "
                f"configuration file at {path}."
            )
        raise ConfigurationError(
            f"profile {profile!r} is not defined in {path} "
            f"(add a [{_PROFILES_TABLE}.{profile}] table)."
        )
    label = f"{path} [{_PROFILES_TABLE}.{profile}]"
    selected = _scalars(
        parsed.profiles[profile], path, f"[{_PROFILES_TABLE}.{profile}]"
    )
    merged.update({name: (value, label) for name, value in selected.items()})
    result = MappingProxyType(merged)
    _merged_cache = (parsed, profile, path, result)
    return result


def _adapter_file_settings(adapter: str) -> Mapping[str, tuple[str, str]]:
    """The ``[<adapter>]`` table's settings, validated on first use.

    Kept separate from :func:`_file_settings` because it layers *above* it
    rather than being merged into it: within the file tier an adapter's own
    value outranks the top-level one, and a profile's does not reach in here at
    all -- a profile names a whole configuration, not one adapter's slice.
    """
    path = config_path()
    parsed = _load_file(path)
    table = parsed.adapters.get(adapter)
    if not table:
        return {}

    global _adapter_cache
    cached = _adapter_cache.get(adapter)
    if cached is not None and cached[0] is parsed and cached[1] == path:
        return cached[2]

    where = f"[{adapter}]"
    validated = _scalars(table, path, where, ADAPTER_SETTINGS[adapter])
    label = f"{path} {where}"
    result: Mapping[str, tuple[str, str]] = MappingProxyType(
        {name: (value, label) for name, value in validated.items()}
    )
    _adapter_cache[adapter] = (parsed, path, result)
    return result


def _load_file(path: Path) -> _ParsedFile:
    """Parse the configuration file at *path*, caching until it changes on disk."""
    global _file_cache
    try:
        st = path.stat()
    except FileNotFoundError:
        # No file is the normal case: continue to the built-in default. One
        # shared empty instance rather than a fresh one per read -- nothing
        # mutates a ``_ParsedFile``, and returning the same object each time is
        # what lets callers memoize on its identity.
        return _NO_FILE
    except OSError as exc:
        raise ConfigurationError(f"could not access {path}: {exc}") from exc

    if stat.S_ISDIR(st.st_mode):
        raise ConfigurationError(
            f"configuration path {path} is a directory, not a file."
        )

    # Only a regular file is parsed. Anything else readable -- a character
    # device, a FIFO -- is treated as *empty* configuration without being
    # opened, which is what ``DATARETRIEVAL_CONFIG=/dev/null`` asks for and the
    # only coherent answer for a stream: settings are re-resolved on every
    # request, so a FIFO would hand its contents to the first getter and
    # nothing to the rest, making the API key vanish mid-run. (It would also
    # block on open until a writer appeared.)
    if not stat.S_ISREG(st.st_mode):
        return _ParsedFile(exists=True)

    # POSIX ``st_ctime_ns`` advances on any inode change, so the metadata stamp
    # catches even a rewrite that restores the original mtime (``cp -p``, rsync
    # ``--times``, an editor that preserves timestamps). Windows ctime is
    # *creation* time, so there the stamp cannot see that class of edit and the
    # content compare below is the only correct check -- worth the re-read,
    # since serving a stale API key is the alternative.
    #
    # Dropping this gate (or dropping ctime from the stamp so Windows can use
    # it) has been proposed repeatedly on the grounds that the re-read is
    # wasteful. It is, but it is also the only thing standing between a
    # timestamp-preserving write and a stale credential; a ctime-less stamp is
    # identical across exactly that edit. ``test_file_edit_is_picked_up``
    # pins the behavior. Please do not "optimize" it without a Windows-safe
    # change detector.
    cached = _file_cache
    if (
        os.name != "nt"
        and cached is not None
        and cached[0] is path
        and cached[1] == _file_stamp(st)
    ):
        return cached[3]

    try:
        with path.open("rb") as handle:
            content = handle.read()
            opened_st = os.fstat(handle.fileno())
    except OSError as exc:
        raise ConfigurationError(f"could not read {path}: {exc}") from exc

    if cached is not None and cached[0] is path and cached[2] == content:
        parsed = cached[3]
    else:
        tomllib = _toml_parser()
        try:
            data = tomllib.loads(content.decode("utf-8"))
        except UnicodeDecodeError as exc:
            raise ConfigurationError(f"{path} is not valid UTF-8: {exc}") from exc
        except tomllib.TOMLDecodeError as exc:
            raise ConfigurationError(f"{path} is not valid TOML: {exc}") from exc
        parsed = _interpret(data, path)
    _warn_on_loose_permissions(path, opened_st, parsed)
    _file_cache = (path, _file_stamp(opened_st), content, parsed)
    return parsed


def _file_stamp(st: os.stat_result) -> _FileStamp:
    """Metadata that changes with file replacement, content, or permissions."""
    return (
        st.st_dev,
        st.st_ino,
        st.st_mode,
        st.st_size,
        st.st_mtime_ns,
        st.st_ctime_ns,
    )


def _interpret(data: dict[str, Any], path: Path) -> _ParsedFile:
    """Validate a parsed TOML document into defaults plus profiles.

    Only the top-level table is validated here, because it always applies.
    Profile tables are kept raw and validated in :func:`_file_settings` when
    one is actually selected: a bad value in a profile nobody asked for must
    not fail every request, the same blast-radius rule
    :func:`~dataretrieval.utils._default_headers` follows for the key itself.
    """
    top: dict[str, Any] = {}
    profiles: dict[str, dict[str, Any]] = {}
    adapters: dict[str, dict[str, Any]] = {}

    for key, value in data.items():
        if key in ADAPTER_SETTINGS:
            if not isinstance(value, dict):
                raise ConfigurationError(
                    f"{path}: [{key}] must be a table of settings for the "
                    f"{key} adapter."
                )
            adapters[key] = value
            continue
        if key == _PROFILES_TABLE:
            if not isinstance(value, dict):
                raise ConfigurationError(
                    f"{path}: [{_PROFILES_TABLE}] must be a table of profiles."
                )
            for name, table in value.items():
                if not isinstance(table, dict):
                    raise ConfigurationError(
                        f"{path}: [{_PROFILES_TABLE}.{name}] must be a table."
                    )
                profiles[name] = table
            continue
        if isinstance(value, dict):
            raise ConfigurationError(
                f"{path}: unknown table [{key}]. Per-adapter tables are "
                f"{', '.join(f'[{name}]' for name in ADAPTERS)}; named profiles "
                f"go under [{_PROFILES_TABLE}.{key}]; top-level keys are the "
                "defaults."
            )
        top[key] = value

    return _ParsedFile(_scalars(top, path, _TOP_LEVEL), profiles, adapters, exists=True)


def _scalars(
    table: dict[str, Any],
    path: Path,
    where: str,
    allowed: frozenset[str] | tuple[str, ...] = SETTINGS,
) -> dict[str, str]:
    """Validate and normalize one table's recognized settings.

    ``tomllib`` returns typed scalars (``concurrency = 32`` is an ``int``,
    ``concurrency = "unbounded"`` a ``str``), so types are checked here before
    values pass through the same grammar used by the other sources.
    Unrecognized keys warn rather than raise, so a file written for a newer
    release still works.
    """
    out: dict[str, str] = {}
    for key, value in table.items():
        if key not in allowed:
            if key in ADAPTER_SETTINGS:
                # ``[profiles.gentle.ngwmn]`` reads as "an adapter table inside
                # a profile", which the chain does not model: a profile names a
                # whole configuration and an adapter table narrows one service,
                # and layering them would need a fourth precedence rule nobody
                # has asked for. Refused rather than warned, because a warning
                # here is the silently-ignored typo this module exists to stop.
                raise ConfigurationError(
                    f"{path}: [{key}] at {where} is an adapter table inside a "
                    f"profile, which is not supported. Put [{key}] at the top "
                    "level of the file."
                )
            if key in SETTINGS:
                # A real setting, in a table that does not read it. Unlike an
                # unrecognized name -- which may simply belong to a newer
                # release -- this cannot become meaningful later, and silently
                # ignoring it would leave a caller believing they had tuned
                # something. See ADR 0010.
                raise ConfigurationError(
                    f"{path}: {key!r} at {where} is not a setting that table "
                    f"accepts. It accepts: {', '.join(sorted(allowed))}."
                )
            warnings.warn(
                f"{path}: unknown setting {key!r} at {where} (ignored). "
                f"Known settings: {', '.join(SETTINGS)}.",
                UserWarning,
                stacklevel=2,
            )
            continue
        if key == "parallel_chunks" and where == _TOP_LEVEL:
            # The one setting that spends rate-limit quota, so a value left
            # here applies to every splittable query in every process that
            # reads the file. A profile is opt-in per run, which is the shape
            # this setting wants.
            warnings.warn(
                f"{path}: 'parallel_chunks' at {where} applies to every query "
                "in every process and spends rate-limit quota. Prefer a "
                f"[{_PROFILES_TABLE}.<name>] table selected per run, or the "
                "dataretrieval.parallel_chunks(n) block for a single call.",
                UserWarning,
                stacklevel=_WARN_STACKLEVEL,
            )
        source = f"{path}: {key!r} at {where}"
        raw = _coerce_typed(key, value, source)
        _validate_raw(key, raw, source)
        out[key] = raw
    return out


def _warn_on_loose_permissions(
    path: Path, st: os.stat_result, parsed: _ParsedFile
) -> None:
    """Warn once if a file holding an API key is readable by other users.

    Follows the ``~/.ssh`` and ``.netrc`` convention, but warns rather than
    refusing -- shared filesystems on HPC clusters have their own conventions,
    and refusing to read would strand those users.
    """
    if os.name != "posix" or path in _permission_warned:
        return
    holds_key = "api_key" in parsed.base or any(
        "api_key" in table for table in parsed.profiles.values()
    )
    if not holds_key:
        return
    if stat.S_IMODE(st.st_mode) & 0o077:
        _permission_warned.add(path)
        warnings.warn(
            f"{path} contains an API key and is readable by other users. "
            f"Restrict it with: chmod 600 {path}",
            UserWarning,
            stacklevel=_WARN_STACKLEVEL,
        )


def _display_api_key(adapter: str | None = None) -> str:
    """Render the key's presence, never its value."""
    return "<set>" if api_key() else "<not set>"


def _display_concurrency(adapter: str | None = None) -> str:
    value = concurrency(adapter=adapter)
    return CONCURRENCY_UNBOUNDED if value is None else str(value)


def _display_progress(adapter: str | None = None) -> str:
    setting = progress()
    return "auto" if setting is None else ("on" if setting else "off")


#: How each setting renders in :func:`show_configuration`. Keyed by the same names as
#: :data:`SETTINGS`, and asserted to cover them, so a setting added to one
#: without the other fails loudly instead of silently printing a neighbour's
#: value in the one report whose whole job is to be trustworthy.
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
}

if set(_DISPLAYS) != set(SETTINGS):  # pragma: no cover - guards a coding error
    # Not an ``assert``: ``python -O`` strips those, and this guards the one
    # report whose whole job is to be trustworthy about provenance.
    raise RuntimeError(
        "every setting needs a show_configuration renderer; "
        f"missing={sorted(set(SETTINGS) - set(_DISPLAYS))} "
        f"extra={sorted(set(_DISPLAYS) - set(SETTINGS))}"
    )


def _reset_file_cache() -> None:
    """Drop the parsed-file cache. For tests that rewrite the file in place."""
    global _file_cache, _path_cache, _merged_cache
    _file_cache = None
    _path_cache = None
    _merged_cache = None
    _adapter_cache.clear()
    _permission_warned.clear()
