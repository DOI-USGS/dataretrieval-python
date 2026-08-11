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

Those are the four *sources*, which is the decomposition this module is built
around -- one branch each in :func:`_resolve`. ADR 0011 states the same order
as seven rungs by splitting three of them into the scopes inside: source 1 into
a configuration instance and a selected profile, which cannot disagree because
both name one adapter and two configurations for one adapter raise; source 3
into the ``[<adapter>]`` table above the top-level keys; and source 4 into an
adapter's own built-in preference above the package default. That last scope is
invisible here because this module never supplies it -- it arrives as the
``default`` a read site like :func:`concurrency` passes for its own service.

Precedence applies **per setting**, not per source: an environment that sets only
``API_USGS_PAT`` leaves a file-provided ``concurrency`` fully in effect. Putting
the environment above the file follows common deployment conventions and keeps
the original environment-variable interface authoritative (see ADR 0009) -- with
one exception ADR 0011 carves out: a profile named *in code* is a more
deliberate act than a variable inherited from a shell, and a profile reaches the
chain by being passed to :func:`configure`, which is above the environment.

A caller configures by passing configuration objects, at most one per adapter::

    with dataretrieval.configure(
        Configuration(api_key=vault.read("usgs/pat")),
        WaterdataConfiguration.load("bulk"),
        NgwmnConfiguration(concurrency=4),
    ):
        ...

Settings are scoped **per adapter** (ADR 0010): a ``[ngwmn]`` table in the file,
or an ``NgwmnConfiguration``, applies to NGWMN calls and no others, so one block
can be gentle with one service while leaving the rest alone. Precedence stays
*source-major*: the chain still walks block, then environment, then file, and an
adapter-scoped value outranks a package-wide one only *within* the same source.
So a variable exported for one run still beats a stale adapter table. Within the
block source that tie-break applies per block: an adapter configuration outranks
a package-wide value set by the same ``configure`` call, while a value set by a
block nested inside it wins over both, so the innermost block still decides.

Which settings an adapter accepts is its own vocabulary -- ``concurrency`` means
nothing to an adapter that issues one request -- so each adapter declares them
on its own :class:`BaseConfiguration` subclass, defined in the module that
*reads* them. The API key is not among them: it belongs to the gateway fronting
a host, which Water Data and NGWMN share.

This module is a leaf: it imports only the standard library plus the Python 3.10
``tomli`` backport, so any module can depend on it without an import cycle or
pulling in httpx or pandas. That is also why it holds the adapter *names* but
never imports an adapter -- see :data:`ADAPTERS`. It centralizes each setting's
parser while retaining legacy environment behavior and stricter validation for
the new Python/TOML surfaces.
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
from dataclasses import dataclass, field, fields
from functools import partial
from numbers import Integral
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar, TextIO, TypeVar

from dataretrieval.exceptions import ConfigurationError

# ``ConfigurationError`` is re-exported; its canonical home and rationale are in
# :mod:`dataretrieval.exceptions`.
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


#: The package-wide settings, in the order :func:`show_configuration` reports
#: them. These are the fields of :class:`Configuration`; an adapter may accept a
#: subset of them plus the adapter-only settings below.
SETTINGS: tuple[str, ...] = (
    "api_key",
    "concurrency",
    "retries",
    "progress",
    "parallel_chunks",
    "stall_timeout",
)

#: Settings only an adapter can carry, because they name one service. No
#: package-wide value could mean anything for them: there is no one base URL.
ADAPTER_ONLY_SETTINGS: tuple[str, ...] = ("base_url",)

#: Every setting name this module knows a grammar for.
_ALL_SETTINGS: tuple[str, ...] = SETTINGS + ADAPTER_ONLY_SETTINGS

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

#: Source label for a setting no source supplied.
_BUILT_IN = "built-in default"

#: The table ADR 0011 retired. Named here only so a file written against the
#: earlier design gets an error that says what to write instead, rather than the
#: generic "unknown table" that would send the reader looking for a typo.
_RETIRED_PROFILES_TABLE = "profiles"

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
# exposing this private implementation detail in generated signatures. It is
# also every configuration field's default, which is what makes "left unset"
# distinguishable from an explicit ``None`` meaning "suppress lower sources".
_UNSET: Any = _Unset()
_SettingValue = str | None

# Overrides from the innermost active ``configure`` block, as raw strings so that
# every source shares one parser and one set of error messages.
# A package-wide override is keyed by the setting's name; an adapter-scoped one
# by ``(adapter, name)``. One flat mapping rather than a nested one so that
# nesting, per-key inheritance, and restore-on-exit keep falling out of a
# single merge, whichever scope a block sets.
_ScopeKey = str | tuple[str, str]
# One frame per ``configure`` block, stacked outermost-first. Frames rather than
# a merged mapping are what makes "the innermost block wins" true across *both*
# scopes: an adapter-scoped value outranks a package-wide one only within the
# same frame. Merged, an outer ``configure(WaterdataConfiguration(...))`` would
# beat an inner ``configure(Configuration(concurrency=1))`` -- inverting
# nesting, and silently discarding the per-call ``parallel_chunks(n)`` block.
_Frame = Mapping[_ScopeKey, _SettingValue]
_scope: ContextVar[tuple[_Frame, ...]] = ContextVar(
    "dataretrieval_configuration", default=()
)

# Resolved config-file path, memoized on the raw ``DATARETRIEVAL_CONFIG``
# value (see :func:`config_path`).
_path_cache: tuple[str | None, object | None, Path] | None = None

# Parsed configuration file, keyed by file identity, change metadata, and raw
# content. POSIX ctime makes metadata hits reliable; Windows ctime is creation
# time, so cache hits there compare content before reusing the parsed result.
_FileStamp = tuple[int, int, int, int, int, int]
_file_cache: tuple[Path, _FileStamp, bytes, _ParsedFile] | None = None

# Validated ``[<adapter>]`` tables, keyed by adapter name and memoized on the
# parsed file's identity, because an adapter table is validated only once that
# adapter is actually used.
_adapter_cache: dict[str, tuple[_ParsedFile, Path, Mapping[str, tuple[str, str]]]] = {}

# Paths already warned about for loose permissions, so the warning fires once.
_permission_warned: set[Path] = set()


@dataclass(frozen=True)
class _ParsedFile:
    """A parsed configuration file: package-wide keys plus per-adapter tables.

    ``exists`` distinguishes "the file is there and defines nothing" from "there
    is no file", which decides which of the two messages a caller selecting a
    profile gets (see :func:`_named_profile`).
    """

    base: dict[str, str] = field(default_factory=dict)
    #: Raw, *unvalidated* ``[<adapter>]`` tables, keyed by adapter name. Each
    #: holds that adapter's default-profile keys and, as sub-tables, its named
    #: profiles. Left unvalidated because a bad value in ``[nldi]`` must not
    #: fail a Water Data call that never reads it.
    adapters: dict[str, dict[str, Any]] = field(default_factory=dict)
    exists: bool = False


#: Stand-in for "no configuration file", which is the common case. Shared
#: rather than rebuilt per read so that callers can memoize on the parsed
#: file's identity; nothing mutates a ``_ParsedFile``.
_NO_FILE = _ParsedFile()


# --- configuration profiles ----------------------------------------------
#
# A setting means the same thing wherever it applies, but it does not apply
# everywhere (ADR 0010). Each adapter declares the settings it accepts as the
# fields of a ``BaseConfiguration`` subclass, defined *in the adapter's own
# module* so a setting's definition sits with the code that reads it -- adding
# a Water Data setting no longer edits a service-neutral file (ADR 0011).
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

#: Bound to the concrete subclass so ``WaterdataConfiguration.load(...)`` is
#: typed as a ``WaterdataConfiguration`` rather than the base. ``typing.Self``
#: would say this in one word and arrives in 3.11; the floor is 3.10.
_C = TypeVar("_C", bound="BaseConfiguration")


@dataclass(frozen=True)
class BaseConfiguration:
    """A named set of settings for one adapter -- a *configuration profile*.

    Subclasses declare the settings their adapter reads as fields, and set
    :attr:`adapter` to that adapter's module name. Every field is optional, so
    an empty configuration is legal and one can be built up conditionally.

    Frozen, because a configuration is a value: two with the same settings are
    interchangeable, and one already handed to :func:`configure` must not
    change under the block that entered it.

    Values are checked when the configuration is *constructed*, so a typo
    raises where it was written rather than at a later ``with`` statement or,
    worse, inside a request.
    """

    #: The adapter this configuration targets, by the name of the module a
    #: caller imports. ``None`` on the package-wide :class:`Configuration`,
    #: which every adapter reads. A ``ClassVar``, not a field: the adapter is a
    #: property of the class, which is what stops the caller restating it at
    #: every call site and stops the roster being spelled twice.
    adapter: ClassVar[str | None] = None

    def __post_init__(self) -> None:
        for name, value in self.values().items():
            if value is not None:
                # ``None`` is not a value to check: it means "suppress the
                # lower sources", which every setting accepts.
                _validated_raw(name, value, self._source(name), optional=", or None")
        self.validate()

    def validate(self) -> None:
        """Check rules that span more than one setting.

        Does nothing by default. Per-setting grammar lives in this module's
        parsers and is shared with the file and the environment, so a value
        means the same thing whichever source wrote it; override this only for
        a rule no single setting can express.
        """

    @classmethod
    def settings(cls) -> frozenset[str]:
        """The setting names this configuration accepts."""
        return frozenset(f.name for f in fields(cls))

    def values(self) -> dict[str, Any]:
        """The settings actually supplied, omitting those left unset.

        An omitted setting inherits from an outer block or a lower source; an
        explicit ``None`` suppresses them. Distinguishing the two is the whole
        job of the ``_UNSET`` default, so it is done here rather than by every
        reader.
        """
        return {
            f.name: getattr(self, f.name)
            for f in fields(self)
            if getattr(self, f.name) is not _UNSET
        }

    @classmethod
    def load(cls: type[_C], profile: str) -> _C:
        """Read a named profile for this adapter from the configuration file.

        ``[<adapter>.<profile>]``. Only the keys that table names are carried,
        so the profile still inherits the adapter's default profile and the
        package-wide keys per setting from the tiers below.

        Selecting a profile the file does not define raises: a name a caller
        just typed is a typo worth reporting, not a silent fall-through to
        settings they did not ask for.

        Parameters
        ----------
        profile : str
            The name after the adapter, so ``[waterdata.bulk]`` is ``"bulk"``.

        Returns
        -------
        BaseConfiguration
            An instance of the class it was called on.
        """
        adapter = cls.adapter
        if adapter is None:
            raise ConfigurationError(
                f"{cls.__name__}.load() names a profile for one adapter, and "
                "the package-wide configuration has none. Put shared keys at "
                "the top level of the file."
            )
        return cls(**_named_profile(adapter, profile, cls.settings()))

    def _source(self, name: str) -> str:
        """How one of this configuration's settings is named in an error."""
        return f"{name}= in {type(self).__name__}()"


@dataclass(frozen=True)
class Configuration(BaseConfiguration):
    """Settings that apply to every adapter.

    The package-wide profile: ``adapter`` stays ``None``, so nothing narrows
    and every adapter reads what this sets unless its own configuration, or a
    block nested inside, overrides that setting.

    Parameters
    ----------
    api_key : str, optional
        Water Data API key, sent as ``X-Api-Key`` and only ever to
        ``api.waterdata.usgs.gov``. Prefer reading it from a secret store, the
        environment, or the configuration file over writing a literal into a
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
    stall_timeout : float, optional
        Seconds a call may go without receiving *any* data before retrying
        stops and the failure surfaces. Bounds the wall-clock cost of a dead
        connection, which ``retries`` does not -- it counts attempts, not
        seconds. Progress resets the clock; ``0`` disables the bound.

    Examples
    --------
    .. code-block:: python

        with dataretrieval.configure(Configuration(api_key=vault.read("usgs"))):
            df, md = waterdata.get_daily(monitoring_location_id="USGS-05114000")
    """

    api_key: str | None = _UNSET
    concurrency: int | str | None = _UNSET
    retries: int | None = _UNSET
    progress: bool | str | None = _UNSET
    parallel_chunks: int | None = _UNSET
    stall_timeout: float | int | None = _UNSET


#: The adapters that may be configured, by the name of the module a caller
#: imports. Names only, because this module is a standard-library-only leaf
#: every adapter may import and so cannot import them back.
#:
#: Holding the names here rather than deriving them from the registry below is
#: what lets a ``[nldi]`` table stay valid in a file: NLDI is imported on demand
#: for the geopandas extra, so a roster built from imports would reject a
#: perfectly good table until something happened to import that module, and the
#: verdict would vary by what a caller had touched.
ADAPTERS: tuple[str, ...] = (
    "waterdata",
    "ngwmn",
    "nwdc",
    "wqp",
    "nldi",
    "streamstats",
)

#: Configuration classes that have registered themselves, keyed by adapter.
#: Populated at adapter import, and consulted only to validate a table's
#: *keys* -- which happens the first time that adapter resolves a setting, by
#: which point it is necessarily imported.
_REGISTRY: dict[str, type[BaseConfiguration]] = {}


def _register(cls: type[BaseConfiguration]) -> None:
    """Record an adapter's configuration class. Called at adapter import.

    The roster in :data:`ADAPTERS` and the class are the two halves of one
    declaration, and this is where they are checked to agree: a class naming an
    adapter the roster does not list would be a configuration no file table and
    no report could ever reach.
    """
    adapter = cls.adapter
    if adapter is None or adapter not in ADAPTERS:
        raise ConfigurationError(
            f"{cls.__name__}.adapter is {adapter!r}, which is not one of "
            f"{', '.join(ADAPTERS)}."
        )
    _REGISTRY[adapter] = cls


def settings_for(adapter: str) -> frozenset[str] | None:
    """The settings *adapter* accepts, or ``None`` if it has not been imported.

    ``None`` is not an error and callers must not treat it as one: a file may
    name an adapter this process has never loaded, and rejecting that would
    make a configuration file conditionally valid depending on which optional
    extras happened to be installed. It means "cannot validate these keys yet",
    and the adapter cannot be misreading a setting it has not loaded.
    """
    cls = _REGISTRY.get(adapter)
    return None if cls is None else cls.settings()


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
        adapter. Raised on entry, before any request. A bad *value* raises
        earlier still, where the configuration was constructed.

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
    token = _scope.set((*_scope.get(), _frame(configurations)))
    try:
        yield
    finally:
        _scope.reset(token)


def _frame(configurations: tuple[BaseConfiguration, ...]) -> _Frame:
    """Flatten one ``configure`` call's configurations into a scope frame.

    One frame per block, holding both scopes: a package-wide setting keyed by
    its name, an adapter-scoped one by ``(adapter, name)``. Values are rendered
    back to raw strings here so that every source shares one parser and one set
    of error messages; they were already checked when each configuration was
    constructed, so nothing new can fail at this point except the two
    call-shaped mistakes below.
    """
    overrides: dict[_ScopeKey, _SettingValue] = {}
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
        for name, value in configuration.values().items():
            key: _ScopeKey = name if adapter is None else (adapter, name)
            overrides[key] = (
                None
                if value is None
                else _validated_raw(name, value, configuration._source(name))
            )
    return overrides


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
        api_key          <set>      /home/u/.dataretrieval/config.toml
        concurrency      32         built-in default
        retries          8          $API_USGS_RETRIES
        progress         auto       built-in default
        parallel_chunks  1          built-in default
        stall_timeout    60s        built-in default

        A built-in default is package-wide. An adapter may prefer its own for
        its own calls; a value from any source above overrides both.

        adapter overrides
          ngwmn  concurrency  4  /home/u/.dataretrieval/config.toml [ngwmn]

        not reported: nldi (not imported, so the settings each accepts are
        unknown here)
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

    # Probing the file once here means a whole-file problem -- unparseable
    # TOML, a bad value at the top level -- is reported on the file row rather
    # than repeated in every setting's row below.
    try:
        _current_file()
        status = "found" if path.exists() else "not found"
    except ConfigurationError as exc:
        reported = str(exc)
        status = f"ERROR: {exc}"
    print(f"config file  {path} ({status})", file=out)

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

    _show_adapter_overrides(out, cell, {name: source for name, _value, source in rows})


def _show_adapter_overrides(
    out: TextIO,
    cell: Callable[[Callable[[], object]], str],
    package_wide: Mapping[str, str],
) -> None:
    """Print the adapter-scoped settings that differ from the rows above.

    Only settings actually overridden, and only adapters that override one: a
    full adapter-by-setting grid would be mostly inherited values, burying the
    answer to "what will this call use" under the rows that change nothing.

    An adapter this process has not imported is *named* rather than skipped.
    Its settings are unknown here (:func:`settings_for` returns ``None``), so
    the honest report is that it was not covered -- omitting it silently would
    read as "nothing configured for it", which is a different claim.
    """
    overrides: list[tuple[str, str, str, str]] = []
    unknown: list[str] = []
    for adapter in ADAPTERS:
        accepted = settings_for(adapter)
        if accepted is None:
            unknown.append(adapter)
            continue
        for name in _ALL_SETTINGS:
            if name not in accepted:
                continue
            scoped = cell(partial(_source_label, name, adapter))
            # ``package_wide`` is what the rows above already resolved. Asking
            # again would repeat the work once per adapter *and* consume the
            # shared error-dedupe state, so a broken config's message could be
            # collapsed here before the row that needs it prints. An
            # adapter-only setting has no row above, and no package-wide value
            # it could inherit, so its baseline is the built-in default.
            if scoped == package_wide.get(name, _BUILT_IN):
                continue  # inherited from the package-wide tier
            value = cell(partial(_DISPLAYS[name], adapter))
            overrides.append((adapter, name, value, scoped))

    if overrides:
        print("\nadapter overrides", file=out)
        a_width = max(len(a) for a, _n, _v, _s in overrides)
        n_width = max(len(n) for _a, n, _v, _s in overrides)
        v_width = max(len(v) for _a, _n, v, _s in overrides)
        for adapter, name, value, source in overrides:
            print(
                f"  {adapter:<{a_width}}  {name:<{n_width}}  "
                f"{value:<{v_width}}  {source}",
                file=out,
            )

    if unknown:
        print(
            f"\nnot reported: {', '.join(unknown)} "
            "(not imported, so the settings each accepts are unknown here)",
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


def base_url(*, adapter: str | None = None) -> str | None:
    """An adapter's configured base URL, or ``None`` for its built-in one.

    Settable from code only: an adapter configuration may carry it, and both
    the file and the environment refuse it. A file that silently redirects a
    data-retrieval library to another host is a supply-chain-shaped hazard,
    while a ``configure`` block keeps the redirect where a reader of the script
    sees it (ADR 0011).
    """
    raw, source = _resolve("base_url", adapter)
    if raw is None:
        return None
    return _parse_base_url(raw, source)


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

    Shared by the two *typed* surfaces -- a configuration's fields and TOML
    scalars -- so a value accepted from one is accepted from the other and a
    tightened rule cannot land on only half of them. (The environment is not
    typed: it delivers strings, which go straight to :func:`_validate_raw`.)

    ``optional`` is the only thing that differs between them: the Python
    surface accepts ``None`` and says so in its messages. (Integers are matched
    as :class:`numbers.Integral` for both -- a numpy or pandas integer is a
    legitimate count from Python, and ``tomllib`` only ever yields ``int``, so
    the wider check cannot change a TOML outcome.)
    """
    if name in _STRING_SETTINGS:
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


def _validated_raw(name: str, value: object, source: str, *, optional: str = "") -> str:
    """Type-check, render and grammar-check one typed value."""
    raw = _coerce_typed(name, value, source, optional=optional)
    _validate_raw(name, raw, source)
    return raw


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


def _parse_base_url(raw: str, source: str) -> str:
    """Parse a service base URL: an absolute ``http``/``https`` origin.

    Only the scheme is checked, and deliberately so. This module cannot know
    what a given service's paths look like, but it can refuse the shapes that
    are never a base URL and would fail far from here -- a bare hostname that
    ``httpx`` would reject, or a ``file://`` that is not a service at all.
    """
    value = raw.strip()
    if not value.startswith(("http://", "https://")):
        raise ConfigurationError(
            f"{source} must be an absolute http:// or https:// URL (got {raw!r})."
        )
    return value


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

#: Per-setting validators used for eager configuration and TOML validation.
_VALIDATORS: dict[str, Callable[[str, str], object]] = {
    "concurrency": _parse_concurrency,
    "retries": _parse_retries,
    "progress": partial(_parse_progress, strict=True),
    "parallel_chunks": _parse_parallel_chunks,
    "stall_timeout": _parse_seconds,
    "base_url": _parse_base_url,
}

#: Settings whose Python value is a plain string. Grouped rather than branched
#: on individually in :func:`_coerce_typed`, because the type check and the
#: message are identical; what they mean afterwards is not, so each still has
#: its own accessor and its own grammar in :data:`_VALIDATORS`.
_STRING_SETTINGS = frozenset({"api_key", "base_url"})


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
    # outside its vocabulary resolves package-wide rather than looking for a
    # scope it could never have been written into.
    scoped: str | None = (
        adapter if adapter is not None and _accepts(adapter, name) else None
    )

    # Innermost block first: a value set by a nested block wins over both
    # scopes of an enclosing one. Within one block the adapter-scoped value is
    # the more specific of the two, so it is asked first.
    for frame in reversed(_scope.get()):
        if scoped is not None and (scoped, name) in frame:
            return frame[(scoped, name)], f"configure() block [{scoped}]"
        if name in frame:
            return frame[name], "configure() block"

    # No per-adapter environment variables: seven adapters times four settings
    # is a namespace nobody can hold in mind, and an exported variable is
    # invisible at the call site. See ADR 0010.
    env = ENV_VARS.get(name)
    if env is not None:
        raw = os.environ.get(env)
        if raw is not None and (raw.strip() or name in _BLANK_MEANS_SET):
            return raw, _env_source_label(env)

    # One load serves both file tiers. Reading the file twice -- once for the
    # adapter table, once for the top level -- cost a second stat on every
    # adapter-scoped resolution, and the common case (no table for this
    # adapter) is the one that paid it.
    path, parsed = _current_file()

    if scoped is not None:
        from_adapter = _adapter_file_settings(scoped, path, parsed)
        if name in from_adapter:
            return from_adapter[name]

    if name in parsed.base:
        return parsed.base[name], str(path)

    return None, _BUILT_IN


def _accepts(adapter: str, name: str) -> bool:
    """Whether *adapter* reads the setting *name*.

    An adapter this process has not imported has no vocabulary to consult, so
    every setting is assumed to be in scope for it: the file stays valid either
    way, and an adapter cannot be misreading a setting it has not loaded. See
    :func:`settings_for`.
    """
    accepted = settings_for(adapter)
    return name in _ALL_SETTINGS if accepted is None else name in accepted


def _named_profile(
    adapter: str, profile: str, allowed: frozenset[str]
) -> dict[str, Any]:
    """The ``[<adapter>.<profile>]`` table, checked against *allowed*.

    Returns the TOML scalars as written rather than raw strings, because the
    caller is :meth:`BaseConfiguration.load`, which feeds them straight back
    into the configuration's own typed fields. Values are still checked here,
    with a source that names the file and the table: a grammar error found on
    the way *out* of the file should say which line to fix, not merely which
    field of which class ended up holding it.
    """
    path, parsed = _current_file()
    named = {
        name: table
        for name, table in parsed.adapters.get(adapter, {}).items()
        if isinstance(table, dict)
    }
    if profile not in named:
        if not parsed.exists:
            raise ConfigurationError(
                f"profile {profile!r} cannot be selected for {adapter}: there "
                f"is no configuration file at {path}."
            )
        defined = ", ".join(sorted(named)) or "none"
        raise ConfigurationError(
            f"{path}: no [{adapter}.{profile}] table. Profiles defined for "
            f"{adapter}: {defined}."
        )

    where = f"[{adapter}.{profile}]"
    table = named[profile]

    # A profile is one flat set of settings for one adapter, so a table inside
    # one is a shape the grammar has no reading for -- most likely a file
    # migrated from the retired ``[profiles.bulk.ngwmn]``, where a profile did
    # carry per-service detail. Dropping it silently would leave the author
    # believing they had tuned something. Checked here rather than at parse
    # time for the same reason keys are: a malformed profile for one adapter
    # must not fail another adapter's call.
    nested = sorted(key for key, value in table.items() if isinstance(value, dict))
    if nested:
        raise ConfigurationError(
            f"{path}: {where} contains a table, [{adapter}.{profile}.{nested[0]}]. "
            "A profile names settings for one adapter and nothing else; to "
            "configure two adapters for one run, give each its own profile and "
            "select both in the same configure() block."
        )

    values = _accepted_keys(table, path, where, allowed)
    for name, value in values.items():
        source = f"{path}: {name!r} at {where}"
        _validate_raw(name, _coerce_typed(name, value, source), source)
    return values


def _current_file() -> tuple[Path, _ParsedFile]:
    """The config file as currently loaded: its path and its parsed form.

    One helper so the two always travel together. They are a single fact, and
    handing the top-level tier a different ``_ParsedFile`` than the adapter
    tier saw in the same resolution is exactly the drift that made an
    adapter-scoped read load the file twice.
    """
    path = config_path()
    return path, _load_file(path)


def _adapter_file_settings(
    adapter: str, path: Path, parsed: _ParsedFile
) -> Mapping[str, tuple[str, str]]:
    """The ``[<adapter>]`` table's own keys -- its default profile.

    Layers *above* the file's top-level keys rather than being merged into
    them: within the file tier an adapter's own value outranks the package-wide
    one. The table's sub-tables are its named profiles, which are inert until a
    caller selects one, so they are skipped here (see :func:`_accepted_keys`).

    Validated on first use, not at parse time, so a bad value in ``[nldi]``
    cannot fail a Water Data call -- the blast-radius rule ADR 0010 set.
    """
    table = parsed.adapters.get(adapter)
    if not table:
        return {}

    global _adapter_cache
    cached = _adapter_cache.get(adapter)
    if cached is not None and cached[0] is parsed and cached[1] == path:
        return cached[2]

    where = f"[{adapter}]"
    # An adapter this process has not imported declares no vocabulary, so its
    # table is checked against the package-wide settings alone: refusing a key
    # for want of a schema would make the file's validity depend on which
    # optional extras happened to be installed.
    accepted = settings_for(adapter)
    validated = _scalars(table, path, where, SETTINGS if accepted is None else accepted)
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
    """Validate a parsed TOML document into package-wide keys plus adapter tables.

    Only the top-level table is validated here, because it always applies. An
    adapter's table is kept raw and validated when that adapter first resolves a
    setting: a bad value in ``[nldi]`` must not fail a Water Data call, the same
    blast-radius rule :func:`~dataretrieval.utils._default_headers` follows for
    the key itself. It is also what lets an adapter's vocabulary live in the
    adapter, which this module cannot import.
    """
    top: dict[str, Any] = {}
    adapters: dict[str, dict[str, Any]] = {}

    for key, value in data.items():
        if key in ADAPTERS:
            if not isinstance(value, dict):
                raise ConfigurationError(
                    f"{path}: [{key}] must be a table of settings for the "
                    f"{key} adapter."
                )
            adapters[key] = value
            continue
        if key == _RETIRED_PROFILES_TABLE:
            # A file written against the earlier design, where one profile
            # switched every service at once. The generic message below would
            # send its author hunting for a typo in a table that is spelled
            # exactly as the old docs said, so name the replacement instead.
            raise ConfigurationError(
                f"{path}: [{_RETIRED_PROFILES_TABLE}] is no longer read. A "
                "profile now belongs to one adapter: write [<adapter>.<name>] "
                'and select it with <Adapter>Configuration.load("<name>").'
            )
        if isinstance(value, dict):
            raise ConfigurationError(
                f"{path}: unknown table [{key}]. Per-adapter tables are "
                f"{', '.join(f'[{name}]' for name in ADAPTERS)}; a named profile "
                f"goes under one of them, as [<adapter>.{key}]; top-level keys "
                "are the package-wide defaults."
            )
        top[key] = value

    return _ParsedFile(_scalars(top, path, _TOP_LEVEL), adapters, exists=True)


def _accepted_keys(
    table: dict[str, Any],
    path: Path,
    where: str,
    allowed: frozenset[str] | tuple[str, ...],
) -> dict[str, Any]:
    """Filter one table down to the settings it is allowed to name.

    The key policy for every table in the file, in one place, so the default
    profile and a named profile cannot come to disagree about what is a typo.
    An unrecognized name warns rather than raising, so a file written for a
    newer release still works; a name this release *does* know but that table
    cannot use raises, because that one can never become meaningful.
    """
    out: dict[str, Any] = {}
    for key, value in table.items():
        if isinstance(value, dict):
            # A named profile -- ``[waterdata.bulk]`` parses as a sub-table of
            # ``[waterdata]``. Inert until a caller selects it, so it is
            # neither a setting here nor an error. Only an adapter's table can
            # reach this: the top level rejects unknown tables when it parses,
            # and :func:`_named_profile` refuses a table inside a profile, so a
            # sub-table here is always a profile rather than deeper nesting.
            continue
        if key in ADAPTER_ONLY_SETTINGS:
            # Rejected from the file wherever it appears. A file that silently
            # redirects a data-retrieval library to another host is a
            # supply-chain-shaped hazard; an in-code block keeps the redirect
            # where a reader of the script sees it (ADR 0011).
            raise ConfigurationError(
                f"{path}: {key!r} at {where} may only be set in code, in a "
                "configure() block, never from a file."
            )
        if key not in allowed:
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
                stacklevel=_WARN_STACKLEVEL,
            )
            continue
        out[key] = value
    return out


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
    """
    out: dict[str, str] = {}
    for key, value in _accepted_keys(table, path, where, allowed).items():
        if key == "parallel_chunks" and where == _TOP_LEVEL:
            # The one setting that spends rate-limit quota, so a value left
            # here applies to every splittable query in every process that
            # reads the file. A named profile is opt-in per run, which is the
            # shape this setting wants.
            warnings.warn(
                f"{path}: 'parallel_chunks' at {where} applies to every query "
                "in every process and spends rate-limit quota. Prefer a "
                "[<adapter>.<name>] table selected per run, or the "
                "dataretrieval.parallel_chunks(n) block for a single call.",
                UserWarning,
                stacklevel=_WARN_STACKLEVEL,
            )
        source = f"{path}: {key!r} at {where}"
        raw = _coerce_typed(key, value, source)
        _validate_raw(key, raw, source)
        out[key] = raw
    return out


def _holds_api_key(parsed: _ParsedFile) -> bool:
    """Whether the file names an API key anywhere, including inert tables.

    Inert tables count because the question is what the *file* contains, not
    what this run resolves: a key sitting in a profile nobody selected is just
    as readable to another user on the machine.
    """
    if "api_key" in parsed.base:
        return True
    return any(
        "api_key" in table
        or any("api_key" in p for p in table.values() if isinstance(p, dict))
        for table in parsed.adapters.values()
    )


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
    if not _holds_api_key(parsed):
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


def _reset_file_cache() -> None:
    """Drop the parsed-file cache. For tests that rewrite the file in place."""
    global _file_cache, _path_cache
    _file_cache = None
    _path_cache = None
    _adapter_cache.clear()
    _permission_warned.clear()
