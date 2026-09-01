"""Private model, grammar, and file foundation for configuration.

The public interface and runtime precedence engine live in
``dataretrieval.configuration``.  This lower module keeps the mutually
dependent configuration classes, setting grammar, TOML interpretation,
and file caches together so the public facade can stay small without
introducing callback seams or import cycles.
"""

from __future__ import annotations

import math
import os
import stat
import sys
import warnings
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field, fields
from functools import partial
from numbers import Integral
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar, TypeVar

from dataretrieval._ambient import Ambient
from dataretrieval.exceptions import ConfigurationError

#: Settings only an adapter can carry, because they name one service. No
#: package-wide value could mean anything for them: there is no one base URL.
#:
#: The package-wide roster is :data:`SETTINGS`, declared below the class it is
#: derived from.
ADAPTER_ONLY_SETTINGS: tuple[str, ...] = ("base_url",)

#: Environment variable backing a setting (precedence step 2).
#:
#: Not every setting has one. ``parallel_chunks`` is deliberately absent: it
#: fans a query into more sub-requests, each of which spends rate-limit quota,
#: and ``dataretrieval.parallel_chunks`` documents why that must stay a
#: deliberate choice rather than a process-wide default. An environment
#: variable is process-wide and implicit -- exported once in a shell profile,
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

#: Variables the environment is *refused* for, by setting. Named rather than
#: left out of :data:`ENV_VARS`, so a caller who exports ``API_USGS_BASE_URL``
#: gets an error instead of a silently ignored variable. The file refuses the
#: same key in the same words (:func:`_accepted_keys`): a base URL arriving
#: from outside the code could redirect the library to another host without a
#: reader of the script seeing it (ADR 0011).
#:
#: Derived from :data:`ADAPTER_ONLY_SETTINGS` so the file and the environment
#: cannot drift apart on which settings are code-only.
_REFUSED_ENV_VARS: dict[str, str] = {
    name: f"API_USGS_{name.upper()}" for name in ADAPTER_ONLY_SETTINGS
}

#: Environment variable holding an explicit path to the configuration file.
CONFIG_PATH_ENV = "DATARETRIEVAL_CONFIG"

#: Origin label for a setting no source supplied.
_BUILT_IN = "built-in default"

#: The table ADR 0011 retired. Named here only so a file written against the
#: earlier design gets an error that says what to write instead, rather than the
#: generic "unknown table" that would send the reader looking for a typo.
_RETIRED_PROFILES_TABLE = "profiles"

#: Label for the file's top-level table, where keys are the defaults.
_TOP_LEVEL = "top level"

#: Settings that warn when written at the top level of the file, and what to
#: say. Declared as data, beside the other per-setting policies -- ``ENV_VARS``,
#: ``_REFUSED_ENV_VARS``, ``_BLANK_MEANS_SET``, ``_VALIDATORS``, ``_DISPLAYS`` --
#: so "what is special about ``parallel_chunks``?" is answerable from this block
#: rather than from a condition buried in a validation loop, and so a second
#: quota-spending setting is a row here rather than an edit to shared code.
#:
#: Top level only: a value in a ``[<adapter>.<name>]`` table is opt-in per run,
#: which is the shape a setting that spends quota wants.
_WARN_AT_TOP_LEVEL: dict[str, str] = {
    "parallel_chunks": (
        f"'parallel_chunks' at {_TOP_LEVEL} applies to every query in every "
        "process and spends rate-limit quota. Prefer a [<adapter>.<name>] "
        "table selected per run, or the dataretrieval.parallel_chunks(n) "
        "block for a single call."
    ),
}

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
# the chain in the shape the docstring and ADR 0009 describe.
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
# One frame per ``configure`` block, stacked outermost-first. Frames rather
# than a merged mapping: merged, an outer adapter-scoped block would beat an
# inner package-wide one, inverting the nesting rule ADR 0011 states.
#
# Each entry pairs the raw value with the label naming where it came from, the
# same shape the file source returns (:func:`_adapter_file_settings`). The
# label is built while the configuration object is still in hand, the only
# point where the *profile* is known -- by the time a value reaches the frame,
# one from ``WaterdataConfiguration.load("bulk")`` and one from
# ``WaterdataConfiguration(...)`` are indistinguishable.
_Frame = Mapping[_ScopeKey, tuple[_SettingValue, str]]
_scope: Ambient[tuple[_Frame, ...]] = Ambient("dataretrieval_configuration", ())

# Resolved config-file path, memoized on the raw ``DATARETRIEVAL_CONFIG``
# value (see :func:`config_path`). The guard names its own kind so the memo
# never has to infer which branch built it.
_PathGuard = tuple[str, object]
_path_cache: tuple[str | None, _PathGuard | None, Path] | None = None

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
    #: profiles. Left unvalidated because an invalid value in ``[nldi]`` must
    #: not fail a Water Data call that never reads it.
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
# module* (ADR 0011). Which settings an adapter accepts is the adapter's own
# knowledge; the setting itself is drawn from the shared groups below, so
# ``retries`` is declared once.
#
# Two settings are deliberately absent from every adapter (ADR 0010):
# ``api_key`` belongs to the gateway fronting a host and stays solely owned by
# ``credentials``; ``progress`` describes the caller's terminal rather than a
# service.

#: Bound to the concrete subclass so ``WaterdataConfiguration.load(...)`` is
#: typed as a ``WaterdataConfiguration`` rather than the base. ``typing.Self``
#: would say this in one word and arrives in 3.11; the floor is 3.10.
_C = TypeVar("_C", bound="BaseConfiguration")


#: Memoized :meth:`BaseConfiguration.settings` results, keyed on the class.
#: A hand-rolled dict rather than ``functools.cache`` only because typeshed's
#: wrapper takes ``Hashable`` and mypy does not accept a class for that
#: protocol, and this package carries no ``type: ignore``.
_settings_cache: dict[type[BaseConfiguration], frozenset[str]] = {}


def _settings_of(cls: type[BaseConfiguration]) -> frozenset[str]:
    """The setting names a configuration class accepts, computed once.

    A class constant in everything but spelling: the fields cannot change after
    the class is created, and every adapter-scoped read asks for it -- through
    :func:`_accepts`, before the frame walk and before the file.

    Keyed on the *class* rather than on the adapter name because tests replace a
    registry entry to stand in for an unimported adapter; a name-keyed memo
    would serve them the schema of the class they replaced.
    """
    cached = _settings_cache.get(cls)
    if cached is None:
        cached = _settings_cache[cls] = frozenset(f.name for f in fields(cls))
    return cached


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
    raises where it was written rather than at a later ``with`` statement or
    inside a request.
    """

    #: The adapter this configuration targets, by the name of the module a
    #: caller imports. ``None`` on the package-wide :class:`Configuration`,
    #: which every adapter reads. A ``ClassVar``, not a field: the adapter is a
    #: property of the class, which is what stops the caller restating it at
    #: every call site and stops the roster being spelled twice.
    adapter: ClassVar[str | None] = None

    #: The named profile these settings were read from, or ``None`` for a
    #: configuration written in code. Provenance rather than a setting: it
    #: records *where the values came from*, which is what lets
    #: :func:`show_configuration` name the profile that supplied each value
    #: instead of reporting every block alike.
    #:
    #: A ``ClassVar`` shadowed per instance by :meth:`load`, so it is neither a
    #: field nor part of equality -- two configurations carrying the same
    #: settings stay interchangeable however each was spelled, which is what
    #: "a configuration is a value" means.
    profile: ClassVar[str | None] = None

    def __post_init__(self) -> None:
        for name, value in self.values().items():
            if value is not None:
                # ``None`` is not a value to check: it means "suppress the
                # lower sources", which every setting accepts.
                _validated_raw(name, value, self._label(name), optional=", or None")
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
        return _settings_of(cls)

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
        package-wide keys per setting from the sources below.

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
            An instance of the class it was called on, remembering the profile
            it was read from so :func:`show_configuration` can name it.
        """
        adapter = cls.adapter
        if adapter is None:
            raise ConfigurationError(
                f"{cls.__name__}.load() names a profile for one adapter, and "
                "the package-wide configuration has none. Put shared keys at "
                "the top level of the file."
            )
        loaded = cls(**_named_profile(adapter, profile, cls.settings()))
        # The class is frozen, so the provenance goes on the same way the
        # dataclass sets its own fields. It is deliberately not one of them:
        # the profile name is where these values came from, not one of the
        # values, and :meth:`settings` is built from the fields.
        object.__setattr__(loaded, "profile", profile)
        return loaded

    def _label(self, name: str) -> str:
        """How one of this configuration's settings is named in an error."""
        return f"{name}= in {type(self).__name__}()"

    def _provenance(self) -> str:
        """How :func:`show_configuration` reports a value this supplied.

        The profile is named in the file's own spelling -- ``[waterdata.bulk]``
        -- so the report answers "which profile set this?" rather than only
        "a block did", and the answer is greppable in the file that holds it.
        A configuration written in code has no profile, so it names its adapter
        alone; the package-wide one narrows to nothing and names neither.
        """
        if self.adapter is None:
            return "configure() block"
        scope = self.adapter
        if self.profile is not None:
            scope = f"{scope}.{self.profile}"
        return f"configure() block [{scope}]"


# --- shared setting groups -----------------------------------------------
#
# Each group declares one shared setting once; an adapter composes the groups
# it reads (ADR 0011). Each still documents the settings it takes in its own
# ``Parameters`` section, because that is the signature a caller writes and
# ``base_url`` means something different for every service.
#
# Plain mixins rather than ``BaseConfiguration`` subclasses: a group has no
# adapter and cannot be passed to :func:`configure`, so keeping it off that
# branch leaves one linear base for the behavior. Frozen because a dataclass
# may not mix frozen and non-frozen bases; fields collect in reverse MRO order,
# so an adapter composing all four reads ``retries, stall_timeout, base_url,
# concurrency, parallel_chunks``.


@dataclass(frozen=True)
class _Retrying:
    """Every adapter's retry dials: transient retries and the stall bound."""

    retries: int | None = _UNSET
    stall_timeout: float | int | None = _UNSET


@dataclass(frozen=True)
class _Redirectable:
    """An adapter whose requests can be pointed at another base URL."""

    base_url: str | None = _UNSET


@dataclass(frozen=True)
class _Concurrent:
    """An adapter that issues more than one request per call."""

    concurrency: int | str | None = _UNSET


@dataclass(frozen=True)
class _Chunked:
    """An adapter whose queries divide into sub-requests the caller can fan."""

    parallel_chunks: int | None = _UNSET


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

    # Spelled out rather than composed from the groups above, because this
    # order is also the order :func:`show_configuration` reports the settings
    # in -- :data:`SETTINGS` is derived from it just below -- and composing
    # would hand that reader-facing sequence to MRO linearization. The two
    # adapter-only fields the groups carry are absent by construction here:
    # there is no package-wide base URL.
    api_key: str | None = _UNSET
    concurrency: int | str | None = _UNSET
    retries: int | None = _UNSET
    progress: bool | str | None = _UNSET
    parallel_chunks: int | None = _UNSET
    stall_timeout: float | int | None = _UNSET


#: The package-wide settings, in the order :func:`show_configuration` reports
#: them -- the fields of :class:`Configuration`, derived rather than restated.
#: An adapter may accept a subset of them plus :data:`ADAPTER_ONLY_SETTINGS`.
#:
#: Derived because the two copies had nothing holding them together, in the one
#: module whose job is to stop rosters being duplicated: a field added to the
#: class and forgotten here would work from :func:`configure` and be silently
#: dropped from the file -- :func:`_accepted_keys` would call it an unknown
#: setting -- and never appear in the report. Which is the "a schema no call
#: site can reach" failure ADR 0011 makes impossible by construction. This is
#: what the adapter side already does (:func:`settings_for`); only the
#: package-wide side was hand-maintained.
#:
#: Declared here, below the class, because it cannot be derived before the
#: class exists. Every reader is a call-time lookup or a ``def`` default
#: evaluated further down the module.
SETTINGS: tuple[str, ...] = tuple(f.name for f in fields(Configuration))

#: Every setting name this module knows a grammar for.
_ALL_SETTINGS: tuple[str, ...] = SETTINGS + ADAPTER_ONLY_SETTINGS


#: The adapters that may be configured, by the name of the module a caller
#: imports. Names only, because this module is a standard-library-only leaf
#: every adapter may import and so cannot import them back.
#:
#: Holding the names here rather than deriving them from the registry below is
#: what lets a ``[nldi]`` table stay valid in a file: NLDI is imported on demand
#: for the geopandas extra, so a roster built from imports would reject a valid
#: table until something happened to import that module, and the verdict would
#: vary by what a caller had touched.
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


def _env_label(env_var: str) -> str:
    """How a value read from ``env_var`` is reported as an origin label."""
    return f"${env_var}"


def _toml_parser() -> Any:
    """The TOML parser, imported on first use.

    ``import dataretrieval`` imports this module, but the parser is reachable
    only once a configuration file actually exists -- the minority case.
    """
    if sys.version_info >= (3, 11):
        import tomllib
    else:  # pragma: no cover - exercised only on Python 3.10
        import tomli as tomllib
    return tomllib


def config_path() -> Path:
    """Path to the configuration file, honoring ``DATARETRIEVAL_CONFIG``.

    Memoized on the raw ``DATARETRIEVAL_CONFIG`` value, because this sits on
    the per-request path via :func:`api_key`. Returning a stable object also
    lets :func:`_load_file` check its cache by identity instead of
    re-normalizing a fresh ``Path``.

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
        # override depends on neither and guards with ``None``.
        if cached_guard is None or cached_guard == _path_guard(cached_guard[0]):
            return path

    expanded = (
        Path(override.strip()).expanduser() if override and override.strip() else None
    )
    guard: _PathGuard | None
    if expanded is None:
        path = _default_home_path()
        guard = ("home", _home_id())
    elif expanded.is_absolute():
        path = expanded
        guard = None
    else:
        guard = ("cwd", _cwd_id())
        path = _resolve_against_cwd(expanded)
    _path_cache = (override, guard, path)
    return path


def _default_home_path() -> Path:
    """The default ``~/.dataretrieval/config.toml``, or an unusable path.

    ``Path.home()`` raises ``RuntimeError`` where no home can be resolved at all
    -- a rootless container running as an arbitrary UID with no passwd entry and
    no ``HOME``. That is not a misconfiguration to report: such a deployment
    has no config file, and before settings were layered it worked on the
    environment alone. So the unexpanded ``~/...`` form is returned
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


def _path_guard(kind: str) -> _PathGuard:
    """Re-read the guard of the given kind: ``"cwd"`` or ``"home"``."""
    return (kind, _cwd_id() if kind == "cwd" else _home_id())


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
    the resolver or it watches a different variable. ``posixpath.expanduser``
    reads ``HOME``; ``ntpath.expanduser`` reads ``USERPROFILE`` (then
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


# --- value grammar -------------------------------------------------------
#
# One parser drives each setting's grammar, so a value means the same thing and
# reports the same way whichever source wrote it. Source-level adapters retain
# TOML types and reject Python API type errors before producing raw strings.


def _type_error(label: str, expected: str, value: object) -> ConfigurationError:
    """Build a type error without rendering a possibly secret value."""
    return ConfigurationError(
        f"{label} must be {expected} (got {type(value).__name__})."
    )


def _coerce_string(value: object, label: str, optional: str) -> str:
    if not isinstance(value, str):
        raise _type_error(label, "a string" + optional, value)
    return value


def _coerce_progress(value: object, label: str, optional: str) -> str:
    if isinstance(value, bool):
        return str(value)
    if isinstance(value, str):
        return value
    raise _type_error(label, "a bool or recognized string" + optional, value)


def _coerce_concurrency(value: object, label: str, optional: str) -> str:
    if isinstance(value, bool) or not isinstance(value, (Integral, str)):
        raise _type_error(label, "an integer or 'unbounded'" + optional, value)
    if isinstance(value, str) and value.strip().lower() != CONCURRENCY_UNBOUNDED:
        raise ConfigurationError(f"{label} must be an integer or 'unbounded'.")
    return str(value)


def _coerce_seconds(value: object, label: str, optional: str) -> str:
    # Seconds, so a fractional value is meaningful -- unlike the counts, which
    # are whole by nature.
    if isinstance(value, bool) or not isinstance(value, (Integral, float)):
        raise _type_error(label, "a number of seconds" + optional, value)
    return str(value)


def _coerce_count(value: object, label: str, optional: str) -> str:
    if isinstance(value, bool) or not isinstance(value, Integral):
        raise _type_error(label, "an integer" + optional, value)
    return str(value)


#: Each setting's source-level type policy -- one row per setting, like
#: :data:`_VALIDATORS` holds its grammar. A roster with the completeness guard
#: below rather than an if/elif chain with an implicit integer fallback, so a
#: new setting must declare its type here or fail at import -- not silently
#: parse as an integer from the typed surfaces while the untyped environment
#: accepts it. (Integers are matched as :class:`numbers.Integral` -- a numpy
#: or pandas integer is a legitimate count from Python, and ``tomllib`` only
#: ever yields ``int``, so the wider check cannot change a TOML outcome.)
_TYPES: dict[str, Callable[[object, str, str], str]] = {
    "api_key": _coerce_string,
    "base_url": _coerce_string,
    "progress": _coerce_progress,
    "concurrency": _coerce_concurrency,
    "retries": _coerce_count,
    "parallel_chunks": _coerce_count,
    "stall_timeout": _coerce_seconds,
}

if set(_TYPES) != set(_ALL_SETTINGS):  # pragma: no cover - guards a coding error
    # Not an ``assert``: ``python -O`` strips those, and an unlisted setting
    # would otherwise change meaning by omission.
    raise RuntimeError(
        "every setting needs a type policy in _TYPES; "
        f"missing={sorted(set(_ALL_SETTINGS) - set(_TYPES))} "
        f"extra={sorted(set(_TYPES) - set(_ALL_SETTINGS))}"
    )


def _coerce_typed(name: str, value: object, label: str, *, optional: str = "") -> str:
    """Type-check one source-level value and render it as a raw string.

    Shared by the two *typed* surfaces -- a configuration's fields and TOML
    scalars -- so a value accepted from one is accepted from the other and a
    tightened rule cannot land on only half of them. (The environment is not
    typed: it delivers strings, which go straight to :func:`_validate_raw`.)

    ``optional`` is the only thing that differs between them: the Python
    surface accepts ``None`` and says so in its messages.
    """
    return _TYPES[name](value, label, optional)


def _validated_raw(name: str, value: object, label: str, *, optional: str = "") -> str:
    """Type-check, render and grammar-check one typed value."""
    raw = _coerce_typed(name, value, label, optional=optional)
    _validate_raw(name, raw, label)
    return raw


def _parse_int(
    raw: str,
    label: str,
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
    label : str
        Human-readable origin label, used as the subject of any error message.
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
        raise ConfigurationError(f"{label} must be {expected} (got {raw!r}).") from exc
    if parsed < minimum:
        raise ConfigurationError(f"{label} must be {expected} (got {parsed}).")
    return parsed


def _parse_seconds(raw: str, label: str) -> float:
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
        raise ConfigurationError(f"{label} must be {expected} (got {raw!r}).") from exc
    # ``inf`` and ``nan`` both parse as floats and both defeat the bound they
    # are meant to set: ``inf`` makes every wait allowed, and ``nan`` compares
    # false against every threshold. TOML has literal ``inf``/``nan``, so this
    # is reachable from the file as well as from Python.
    if not math.isfinite(parsed) or parsed < 0:
        raise ConfigurationError(f"{label} must be {expected} (got {parsed}).")
    return parsed


def _parse_concurrency(raw: str, label: str) -> int | None:
    """Parse a concurrency cap: a positive int, or ``unbounded`` -> ``None``."""
    if raw.strip().lower() == CONCURRENCY_UNBOUNDED:
        return None
    try:
        return _parse_int(raw, label, default=DEFAULT_CONCURRENCY, minimum=1)
    except ConfigurationError as exc:
        raise ConfigurationError(
            f"{exc} Use '{CONCURRENCY_UNBOUNDED}' to disable the cap."
        ) from exc


def _parse_base_url(raw: str, label: str) -> str:
    """Parse a service base URL: an absolute ``http``/``https`` origin.

    Only the scheme is checked, and deliberately so. This module cannot know
    what a given service's paths look like, but it can refuse the shapes that
    are never a base URL and would fail far from here -- a bare hostname that
    ``httpx`` would reject, or a ``file://`` that is not a service at all.
    """
    value = raw.strip()
    if not value.startswith(("http://", "https://")):
        raise ConfigurationError(
            f"{label} must be an absolute http:// or https:// URL (got {raw!r})."
        )
    return value


def _parse_progress(raw: str, label: str, *, strict: bool) -> bool:
    """Parse a progress toggle, optionally preserving legacy env truthiness."""
    value = raw.strip().lower()
    if strict and not value:
        raise ConfigurationError(f"{label} must not be blank.")
    if value in _PROGRESS_FALSEY:
        return False
    if value in _PROGRESS_TRUTHY:
        return True
    if not strict:
        return True
    expected = ", ".join(sorted(_PROGRESS_TRUTHY | _PROGRESS_FALSEY))
    raise ConfigurationError(f"{label} must be one of {expected} (got {raw!r}).")


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


def _validate_raw(name: str, raw: str, label: str) -> None:
    """Run a setting's grammar validator when it has one."""
    validate = _VALIDATORS.get(name)
    if validate is not None:
        validate(raw, label)


def _named_profiles(parsed: _ParsedFile, adapter: str) -> dict[str, dict[str, Any]]:
    """The named profiles the file defines for *adapter*, by name.

    A sub-table of an adapter's table is a named profile: ``[waterdata.bulk]``
    parses as a sub-table of ``[waterdata]``, and everything else in that table
    is a setting of the adapter's default profile. The two readers of that rule
    -- selecting a profile and reporting which ones exist -- share this one
    definition so they cannot come to disagree about what a profile is.

    Tables are returned raw, since an adapter this process has not imported has
    no vocabulary to check them against. That is enough to *name* a profile,
    which is all the report needs; reading one still goes through
    :func:`_named_profile`.
    """
    return {
        name: table
        for name, table in parsed.adapters.get(adapter, {}).items()
        if isinstance(table, dict)
    }


def _named_profile(
    adapter: str, profile: str, allowed: frozenset[str]
) -> dict[str, Any]:
    """The ``[<adapter>.<profile>]`` table, checked against *allowed*.

    Returns the TOML scalars as written rather than raw strings, because the
    caller is :meth:`BaseConfiguration.load`, which feeds them straight back
    into the configuration's own typed fields. Values are still checked here,
    with a label that names the file and the table: a grammar error found on
    the way *out* of the file should say which line to fix, not merely which
    field of which class ended up holding it.
    """
    path, parsed = _current_file()
    named = _named_profiles(parsed, adapter)
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

    return {
        name: value
        for name, (value, _raw) in _checked_table(table, path, where, allowed).items()
    }


def _current_file() -> tuple[Path, _ParsedFile]:
    """The config file as currently loaded: its path and its parsed form.

    One helper so the two always travel together. They are a single fact, and
    handing the top-level scope a different ``_ParsedFile`` than the
    adapter scope saw in the same resolution is exactly the drift that made an
    adapter-scoped read load the file twice.
    """
    path = config_path()
    return path, _load_file(path)


def _adapter_file_settings(
    adapter: str, path: Path, parsed: _ParsedFile
) -> Mapping[str, tuple[str, str]]:
    """The ``[<adapter>]`` table's own keys -- its default profile.

    Layers *above* the file's top-level keys rather than being merged into
    them: within the file source an adapter's own value outranks the package-wide
    one. The table's sub-tables are its named profiles, which are inert until a
    caller selects one, so they are skipped here (see :func:`_accepted_keys`).

    Validated on first use, not at parse time, so an invalid value in ``[nldi]``
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
    st = _stat_config_file(path)
    if st is None:
        return _NO_FILE

    if stat.S_ISDIR(st.st_mode):
        raise ConfigurationError(
            f"configuration path {path} is a directory, not a file."
        )

    # Only a regular file is parsed. Anything else readable -- a character
    # device, a FIFO -- is treated as *empty* configuration without being
    # opened, which is what ``DATARETRIEVAL_CONFIG=/dev/null`` asks for and the
    # only coherent answer for a stream: settings are re-resolved on every
    # request, so a FIFO would hand its contents to the first getter and
    # nothing to the rest, making the API key vanish mid-run.
    if not stat.S_ISREG(st.st_mode):
        return _ParsedFile(exists=True)

    cached_parse = _cached_parse_by_metadata(path, st)
    if cached_parse is not None:
        return cached_parse

    content, opened_st = _read_file_content(path)
    parsed = _parse_or_reuse_cache(path, content)
    _warn_on_loose_permissions(path, opened_st, parsed)
    _file_cache = (path, _file_stamp(opened_st), content, parsed)
    return parsed


def _stat_config_file(path: Path) -> os.stat_result | None:
    """Stat the config file, returning ``None`` if it does not exist."""
    try:
        return path.stat()
    except FileNotFoundError:
        return None
    except OSError as exc:
        raise ConfigurationError(f"could not access {path}: {exc}") from exc


def _cached_parse_by_metadata(path: Path, st: os.stat_result) -> _ParsedFile | None:
    """The cached parse when the metadata stamp still matches, else ``None``.

    POSIX ``st_ctime_ns`` advances on any inode change, so the metadata stamp
    catches even a rewrite that restores the original mtime. Windows ctime is
    *creation* time, so there the stamp cannot see that class of edit and the
    content compare in :func:`_parse_or_reuse_cache` is the only check that
    catches it -- the re-read it forces is deliberate, and
    ``test_file_edit_is_picked_up`` pins it. Do not drop the ctime gate (or
    extend the stamp to Windows) without a Windows-safe change detector.
    """
    cached = _file_cache
    if (
        os.name != "nt"
        and cached is not None
        and cached[0] is path
        and cached[1] == _file_stamp(st)
    ):
        return cached[3]
    return None


def _read_file_content(path: Path) -> tuple[bytes, os.stat_result]:
    """Read the file content and return it with the stat of the opened handle."""
    try:
        with path.open("rb") as handle:
            content = handle.read()
            opened_st = os.fstat(handle.fileno())
    except OSError as exc:
        raise ConfigurationError(f"could not read {path}: {exc}") from exc
    return content, opened_st


def _parse_or_reuse_cache(path: Path, content: bytes) -> _ParsedFile:
    """Parse the TOML content, reusing the cache if content is unchanged."""
    cached = _file_cache
    if cached is not None and cached[0] is path and cached[2] == content:
        return cached[3]
    tomllib = _toml_parser()
    try:
        data = tomllib.loads(content.decode("utf-8"))
    except UnicodeDecodeError as exc:
        raise ConfigurationError(f"{path} is not valid UTF-8: {exc}") from exc
    except tomllib.TOMLDecodeError as exc:
        raise ConfigurationError(f"{path} is not valid TOML: {exc}") from exc
    return _interpret(data, path)


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
    setting: an invalid value in ``[nldi]`` must not fail a Water Data call,
    the same blast-radius rule :func:`~dataretrieval.utils._default_headers`
    follows for the key itself. It is also what lets an adapter's vocabulary
    live in the adapter, which this module cannot import.
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
                # unrecognized name -- which may belong to a newer release --
                # this cannot become meaningful later, and silently ignoring it
                # would leave a caller believing they had tuned something. See
                # ADR 0010.
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


def _checked_table(
    table: dict[str, Any],
    path: Path,
    where: str,
    allowed: frozenset[str] | tuple[str, ...],
) -> dict[str, tuple[Any, str]]:
    """Check one table of the file, in both the forms its two readers need.

    Every table in the file comes through here: the top-level keys, an
    adapter's default profile, and a named profile. They differ only in what
    they do with the result -- the chain wants raw strings, a profile being
    loaded wants the TOML scalars to hand back to a configuration's own typed
    fields -- so both are returned and each reader takes its half. Written once
    because the checks are the interesting part and they must not diverge: a
    per-table policy added for one kind of table would otherwise skip the
    other, silently.

    ``tomllib`` returns typed scalars (``concurrency = 32`` is an ``int``,
    ``concurrency = "unbounded"`` a ``str``), so types are checked here before
    values pass through the same grammar used by the other sources.

    Returns
    -------
    dict[str, tuple[Any, str]]
        Each accepted setting's value as written, and as a raw string.
    """
    checked: dict[str, tuple[Any, str]] = {}
    for key, value in _accepted_keys(table, path, where, allowed).items():
        if where == _TOP_LEVEL and key in _WARN_AT_TOP_LEVEL:
            warnings.warn(
                f"{path}: {_WARN_AT_TOP_LEVEL[key]}",
                UserWarning,
                stacklevel=_WARN_STACKLEVEL,
            )
        label = f"{path}: {key!r} at {where}"
        raw = _coerce_typed(key, value, label)
        _validate_raw(key, raw, label)
        checked[key] = (value, raw)
    return checked


def _scalars(
    table: dict[str, Any],
    path: Path,
    where: str,
    allowed: frozenset[str] | tuple[str, ...] = SETTINGS,
) -> dict[str, str]:
    """One table's recognized settings, as the raw strings the chain resolves."""
    return {
        key: raw
        for key, (_value, raw) in _checked_table(table, path, where, allowed).items()
    }


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


def _reset_file_cache() -> None:
    """Drop the parsed-file cache. For tests that rewrite the file in place."""
    global _file_cache, _path_cache
    _file_cache = None
    _path_cache = None
    _adapter_cache.clear()
    _permission_warned.clear()
