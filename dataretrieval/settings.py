"""Layered settings resolution for ``dataretrieval``, built on pydantic-settings.

Every tunable setting -- the Water Data API key, the fan-out concurrency cap,
the retry count, and the progress line -- resolves through one ordered chain so
a caller never has to mutate ``os.environ`` to configure a single call.

Sources, highest precedence first:

1. A settings profile passed to :func:`configure` -- delivered through a
   :class:`~contextvars.ContextVar`, so a setting applies to the current thread
   or asyncio task and cannot leak into another one.
2. The environment variable for that setting (``API_USGS_PAT``,
   ``API_USGS_CONCURRENT``, ``API_USGS_RETRIES``, ``API_USGS_PROGRESS``).
3. The settings file (TOML): ``~/.dataretrieval/config.toml``, or the path in
   ``DATARETRIEVAL_CONFIG``. Top-level keys are the package-wide defaults; a
   ``[<adapter>]`` table is that adapter's *default profile*, always in effect;
   a ``[<adapter>.<name>]`` table is a *named profile*, inert until a caller
   selects it with ``<Adapter>Settings.load("<name>")``.
4. The built-in default.

Those are the four *sources*. ADR 0011 states the same order as seven rungs by
splitting three of them into the scopes inside: source 1 into a settings
instance and a selected profile, which cannot disagree because both name one
adapter and two profiles for one adapter raise; source 3 into the
``[<adapter>]`` table above the top-level keys; and source 4 into an adapter's
own built-in preference above the package default. That last scope is invisible
here because this module never supplies it -- it arrives as the ``default`` a
read site like :func:`concurrency` passes for its own service.

Precedence applies **per setting**, not per source: an environment that sets only
``API_USGS_PAT`` leaves a file-provided ``concurrency`` fully in effect. That is
also how pydantic-settings merges sources, which is why the chain is expressed
as a source tuple rather than as hand-written fallbacks -- see
:meth:`AdapterSettings.settings_customise_sources`.

A caller configures by passing settings profiles, at most one per adapter::

    with dataretrieval.configure(
        Settings(api_key=vault.read("usgs/pat")),
        WaterdataSettings.load("bulk"),
        NgwmnSettings(concurrency=4),
    ):
        ...

Settings are scoped **per adapter** (ADR 0010): a ``[ngwmn]`` table in the file,
or an ``NgwmnSettings``, applies to NGWMN calls and no others. Precedence stays
*source-major*: the chain still walks block, then environment, then file, and an
adapter-scoped value outranks a package-wide one only *within* the same source.

Which settings an adapter accepts is its own vocabulary -- ``concurrency`` means
nothing to an adapter that issues one request -- so each adapter declares them
on its own :class:`AdapterSettings` subclass, defined in the module that *reads*
them. The API key is not among them: it belongs to the gateway fronting a host,
which Water Data and NGWMN share.

Why pydantic-settings (ADR 0012)
--------------------------------

The field declarations, the type coercion, the bounds, the "unknown setting"
rejection and the source merge are pydantic-settings'. What remains here is what
that library has no opinion about: the TOML *grammar* of adapter tables and
named profiles, the file cache, the provenance labels :func:`show_settings`
reports, and the ``ContextVar`` that carries a block. Those are wired in as
:class:`~pydantic_settings.PydanticBaseSettingsSource` subclasses, which is the
library's own extension point.

This module is no longer a standard-library-only leaf -- ADR 0012 withdrew that
constraint deliberately. It still imports no adapter (see :data:`ADAPTERS`) and
nothing from ``dataretrieval`` other than the ``exceptions`` taxonomy leaf, so
it cannot cycle.
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
from numbers import Integral
from pathlib import Path
from types import MappingProxyType
from typing import Any, ClassVar, Literal, TextIO, TypeVar, overload

from pydantic import ValidationError, field_validator
from pydantic.fields import FieldInfo
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
)

from dataretrieval.exceptions import ConfigurationError

# ``ConfigurationError`` is re-exported; its canonical home and rationale are in
# :mod:`dataretrieval.exceptions`.
__all__ = [
    "ADAPTERS",
    # The package-wide settings, and the base every adapter subclasses. Public
    # because a caller writes ``Settings(...)`` at every call site that
    # configures anything, and an adapter module names the base in its own
    # subclass.
    "AdapterSettings",
    "Settings",
    "config_path",
    "configure",
    "settings_for",
    "show_settings",
]


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
#: deliberate choice rather than a process-wide default.
ENV_VARS: dict[str, str] = {
    "api_key": "API_USGS_PAT",
    "concurrency": "API_USGS_CONCURRENT",
    "retries": "API_USGS_RETRIES",
    "progress": "API_USGS_PROGRESS",
    "stall_timeout": "API_USGS_STALL_TIMEOUT",
}

#: Variables the environment is *refused* for, by setting. Named rather than
#: simply left out of :data:`ENV_VARS`, because leaving them out only makes the
#: environment silent: a caller who exports ``API_USGS_BASE_URL`` -- the
#: spelling every other setting's variable predicts -- has redirected nothing
#: and would learn that from the traffic rather than from us.
#:
#: Derived from :data:`ADAPTER_ONLY_SETTINGS` rather than written out beside it,
#: because the two would be spelling one fact -- "this setting is code-only" --
#: in two tables with nothing keeping them in step.
_REFUSED_ENV_VARS: dict[str, str] = {
    name: f"API_USGS_{name.upper()}" for name in ADAPTER_ONLY_SETTINGS
}

#: Environment variable holding an explicit path to the settings file.
CONFIG_PATH_ENV = "DATARETRIEVAL_CONFIG"

#: Source label for a setting no source supplied.
_BUILT_IN = "built-in default"

#: The table ADR 0011 retired. Named here only so a file written against the
#: earlier design gets an error that says what to write instead.
_RETIRED_PROFILES_TABLE = "profiles"

#: Label for the file's top-level table, where keys are the defaults.
_TOP_LEVEL = "top level"

#: Settings that warn when written at the top level of the file, and what to
#: say. Declared as data, beside the other per-setting policies, so "what is
#: special about ``parallel_chunks``?" is answerable from this block rather than
#: from a condition buried in a validation loop.
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
_PROGRESS_TRUTHY = frozenset({"1", "true", "yes", "on"})

# Settings for which a *blank* environment variable is a value rather than an
# absence. ``API_USGS_PROGRESS=`` has always meant "off". For every other
# setting a blank variable is what container and CI tooling produces when it has
# nothing to pass, so treating it as configured would let it shadow the settings
# file and silently drop the user's API key.
_BLANK_MEANS_SET = frozenset({"progress"})

# Warnings about the settings file report the file, not a call site: settings are
# resolved lazily from wherever a getter first needs one, so the user frame is a
# different depth every time and no fixed ``stacklevel`` can name it.
_WARN_STACKLEVEL = 2


# --- provenance ----------------------------------------------------------
#
# pydantic-settings merges sources but does not report which one supplied a
# given field, and ``show_settings`` exists to answer exactly that. Sources are
# called highest-precedence first (``BaseSettings._settings_build_values``
# accumulates with ``deep_update(source_state, state)``, so what is already in
# ``state`` wins), which means the *first* source to claim a key is the one that
# won it. Each source below records its keys into the recorder if not already
# present, so first-writer-wins produces the right label with no second merge.
#
# A ContextVar rather than a parameter because the recorder has to reach two
# places pydantic does not thread state through: the sources, which the library
# constructs, and the field validators, which need it to name the source in an
# error message.
_recorder: ContextVar[dict[str, str] | None] = ContextVar(
    "dataretrieval_settings_recorder", default=None
)


def _record(name: str, label: str) -> None:
    """Note that *label* supplied *name*, unless something already claimed it."""
    recorder = _recorder.get()
    if recorder is not None:
        recorder.setdefault(name, label)


def _label_for(name: str, fallback: str) -> str:
    """The source label for *name*, for an error message naming its origin."""
    recorder = _recorder.get()
    if recorder is None:
        return fallback
    return recorder.get(name, fallback)


def _unwrap(exc: ValidationError) -> ConfigurationError:
    """Recover the :class:`ConfigurationError` a field validator raised.

    Every validator in this module raises :class:`ConfigurationError`, which is
    a ``ValueError``, so pydantic wraps it in a ``ValidationError`` carrying a
    list of errors. The wrapper's rendering names the model and the field in
    pydantic's own vocabulary; ours names the *source* -- ``$API_USGS_RETRIES``,
    or the file and table -- which is the thing a caller can act on. So the
    original is unwrapped and re-raised.

    An error pydantic itself produced (an unknown setting under
    ``extra="forbid"``, a type it rejected before any validator ran) has no
    original to recover, so its message is translated instead.
    """
    for error in exc.errors():
        original = error.get("ctx", {}).get("error")
        if isinstance(original, ConfigurationError):
            return original
    return ConfigurationError(_describe(exc))


def _describe(exc: ValidationError) -> str:
    """Render a pydantic-raised error in this package's vocabulary."""
    parts = []
    for error in exc.errors():
        field = ".".join(str(item) for item in error["loc"]) or "settings"
        if error["type"] == "extra_forbidden":
            parts.append(
                f"{field!r} is not a setting {exc.title} accepts. It accepts: "
                f"{', '.join(sorted(_fields_of(exc.title)))}."
            )
        else:
            parts.append(f"{field}: {error['msg']}")
    return "; ".join(parts)


def _fields_of(title: str) -> frozenset[str]:
    """The settings a registered class named *title* accepts, for a message."""
    known: tuple[type[AdapterSettings], ...] = (Settings, *_REGISTRY.values())
    for cls in known:
        if cls.__name__ == title:
            return cls.settings()
    return frozenset(SETTINGS)


# --- value grammar -------------------------------------------------------
#
# One parser drives each setting's grammar, so a value means the same thing and
# reports the same way whichever source wrote it. Each is a plain function so
# the field validators below and the file's eager top-level check share it.
#
# ``typed`` is the one axis on which the sources differ, and it separates them
# into two groups rather than seven. A settings profile's fields and TOML
# scalars are *typed*: ``retries = "2"`` in the file is a quoted integer, which
# is a mistake worth reporting, and ``Settings(retries="2")`` is the same
# mistake in Python. The environment is *untyped* -- it can only deliver strings
# -- so ``API_USGS_RETRIES=2`` must keep working. Passing ``typed=False`` is
# therefore the environment source's privilege alone, and it parses its own
# values before handing them on, which is what lets every tier below it stay
# strict. (Integers are matched as :class:`numbers.Integral` so a numpy or
# pandas integer is a legitimate count from Python; ``tomllib`` only ever
# yields ``int``, so the wider check cannot change a TOML outcome.)


def _parse_int(
    value: object,
    source: str,
    *,
    default: int,
    minimum: int,
    examples: str | None = None,
    typed: bool = True,
) -> int:
    """Parse a bounded integer setting; blank falls through to *default*."""
    if isinstance(value, bool):
        raise ConfigurationError(f"{source} must be an integer (got bool).")
    expected = f"an integer >= {minimum}" + (f", e.g. {examples}" if examples else "")
    if isinstance(value, str):
        if typed:
            raise ConfigurationError(f"{source} must be {expected} (got str).")
        text = value.strip()
        if text == "":
            return default
        try:
            parsed = int(text)
        except ValueError as exc:
            raise ConfigurationError(
                f"{source} must be {expected} (got {value!r})."
            ) from exc
    elif isinstance(value, Integral):
        parsed = int(value)
    else:
        raise ConfigurationError(
            f"{source} must be {expected} (got {type(value).__name__})."
        )
    if parsed < minimum:
        raise ConfigurationError(f"{source} must be {expected} (got {parsed}).")
    return parsed


def _parse_seconds(value: object, source: str, *, typed: bool = True) -> float:
    """Parse a non-negative duration in seconds; blank falls through.

    Seconds rather than a count, so fractional values are accepted. ``0``
    disables the bound it guards, which is why the floor is zero rather than one.
    """
    expected = "a finite, non-negative number of seconds"
    if isinstance(value, bool):
        raise ConfigurationError(f"{source} must be {expected} (got bool).")
    if isinstance(value, str):
        if typed:
            raise ConfigurationError(f"{source} must be {expected} (got str).")
        text = value.strip()
        if text == "":
            return DEFAULT_STALL_TIMEOUT
        try:
            parsed = float(text)
        except ValueError as exc:
            raise ConfigurationError(
                f"{source} must be {expected} (got {value!r})."
            ) from exc
    elif isinstance(value, (Integral, float)):
        parsed = float(value)
    else:
        raise ConfigurationError(
            f"{source} must be {expected} (got {type(value).__name__})."
        )
    # ``inf`` and ``nan`` both parse as floats and both defeat the bound they
    # are meant to set: ``inf`` makes every wait allowed, and ``nan`` compares
    # false against every threshold. TOML has literal ``inf``/``nan``, so this
    # is reachable from the file as well as from Python.
    if not math.isfinite(parsed) or parsed < 0:
        raise ConfigurationError(f"{source} must be {expected} (got {parsed}).")
    return parsed


def _parse_concurrency(value: object, source: str, *, typed: bool = True) -> int | str:
    """Parse a concurrency cap: a positive int, or ``unbounded``.

    ``"unbounded"`` is a legitimate *string* value for this setting, so unlike
    the other numeric dials a string is not automatically a typed-source
    mistake -- only a string that is not that word.
    """
    if isinstance(value, str) and value.strip().lower() == CONCURRENCY_UNBOUNDED:
        return CONCURRENCY_UNBOUNDED
    if typed and isinstance(value, str):
        raise ConfigurationError(
            f"{source} must be an integer or '{CONCURRENCY_UNBOUNDED}'."
        )
    try:
        return _parse_int(
            value, source, default=DEFAULT_CONCURRENCY, minimum=1, typed=typed
        )
    except ConfigurationError as exc:
        raise ConfigurationError(
            f"{exc} Use '{CONCURRENCY_UNBOUNDED}' to disable the cap."
        ) from exc


def _parse_base_url(value: object, source: str) -> str:
    """Parse a service base URL: an absolute ``http``/``https`` origin.

    Only the scheme is checked, and deliberately so. This module cannot know
    what a given service's paths look like, but it can refuse the shapes that
    are never a base URL and would fail far from here.
    """
    if not isinstance(value, str):
        raise ConfigurationError(
            f"{source} must be a string, or None (got {type(value).__name__})."
        )
    text = value.strip()
    if not text.startswith(("http://", "https://")):
        raise ConfigurationError(
            f"{source} must be an absolute http:// or https:// URL (got {value!r})."
        )
    return text


def _parse_progress(value: object, source: str, *, strict: bool) -> bool:
    """Parse a progress toggle, optionally preserving legacy env truthiness."""
    if isinstance(value, bool):
        return value
    if not isinstance(value, str):
        raise ConfigurationError(
            f"{source} must be a bool or recognized string, or None "
            f"(got {type(value).__name__})."
        )
    text = value.strip().lower()
    if strict and not text:
        raise ConfigurationError(f"{source} must not be blank.")
    if text in _PROGRESS_FALSEY:
        return False
    if text in _PROGRESS_TRUTHY:
        return True
    if not strict:
        # Preserve the legacy environment behavior: any value outside the false
        # set enables progress. New block/file values are validated strictly.
        return True
    expected = ", ".join(sorted(_PROGRESS_TRUTHY | _PROGRESS_FALSEY))
    raise ConfigurationError(f"{source} must be one of {expected} (got {value!r}).")


def _parse_api_key(value: object, source: str) -> str:
    """Parse an API key: any string. Whitespace is stripped at the read site."""
    if not isinstance(value, str):
        raise ConfigurationError(
            f"{source} must be a string, or None (got {type(value).__name__})."
        )
    return value


def _parse_retries(value: object, source: str, *, typed: bool = True) -> int:
    """Retries attempted after the first try; ``0`` disables retrying."""
    return _parse_int(value, source, default=DEFAULT_RETRIES, minimum=0, typed=typed)


def _parse_parallel_chunks(value: object, source: str, *, typed: bool = True) -> int:
    """Baseline fan-out for multi-value queries; at least one chunk."""
    return _parse_int(
        value,
        source,
        default=DEFAULT_PARALLEL_CHUNKS,
        minimum=1,
        examples="2, 8, 32",
        typed=typed,
    )


#: Per-setting grammar, named once. The field validators and the file's eager
#: top-level check both go through this table, so a change to a bound cannot
#: leave a ``configure()`` block validating against different rules than the
#: value it later resolves.
_VALIDATORS: dict[str, Callable[[object, str], object]] = {
    "api_key": _parse_api_key,
    "concurrency": _parse_concurrency,
    "retries": _parse_retries,
    "progress": lambda value, source: _parse_progress(value, source, strict=True),
    "parallel_chunks": _parse_parallel_chunks,
    "stall_timeout": _parse_seconds,
    "base_url": _parse_base_url,
}


def _validate(name: str, value: object, source: str) -> object:
    """Run a setting's grammar. ``None`` is never checked: it clears the tiers."""
    if value is None:
        return None
    return _VALIDATORS[name](value, source)


def _require(name: str, value: object, source: str) -> object:
    """Run a setting's grammar where ``None`` is not one of the answers.

    The chain reads ``None`` as "suppress the lower sources", which is a
    meaningful thing to configure. A caller writing ``parallel_chunks(None)`` is
    not saying that -- there is no lower source for a per-call block to suppress
    -- so that surface needs the same grammar without the escape hatch.
    """
    return _VALIDATORS[name](value, source)


#: How the *untyped* source reads each setting it can supply. The environment
#: delivers strings and nothing else, so it parses its own values here and hands
#: on properly typed ones -- which is what lets every tier below stay strict
#: about a quoted integer, and what keeps the two legacy environment grammars
#: (a blank numeric falling through to the default, an unrecognized ``progress``
#: value meaning "on") contained in the one source that has to honor them.
_ENV_PARSERS: dict[str, Callable[[str, str], object]] = {
    "api_key": lambda raw, source: raw,
    "concurrency": lambda raw, source: _parse_concurrency(raw, source, typed=False),
    "retries": lambda raw, source: _parse_retries(raw, source, typed=False),
    "progress": lambda raw, source: _parse_progress(raw, source, strict=False),
    "stall_timeout": lambda raw, source: _parse_seconds(raw, source, typed=False),
}


# --- settings profiles ----------------------------------------------------
#
# A setting means the same thing wherever it applies, but it does not apply
# everywhere (ADR 0010). Each adapter declares the settings it accepts as the
# fields of an :class:`AdapterSettings` subclass, defined *in the adapter's own
# module* so a setting's definition sits with the code that reads it.
#
# Two settings are deliberately absent from every adapter:
#
# ``api_key``     belongs to the gateway fronting a host, not to an adapter.
#                 Water Data and NGWMN are two adapters on one host sharing one
#                 key and one quota pool -- measured, see ADR 0010.
# ``progress``    describes the caller's terminal, not a service. There is one
#                 progress line per call, so scoping it per adapter could only
#                 produce a contradiction.

#: Bound to the concrete subclass so ``WaterdataSettings.load(...)`` is typed as
#: a ``WaterdataSettings`` rather than the base. ``typing.Self`` would say this
#: in one word and arrives in 3.11; the floor is 3.10.
_S = TypeVar("_S", bound="AdapterSettings")

# A settings profile has two roles: the payload a caller hands to
# ``configure()``, which must carry *only* what that caller wrote, and the
# resolved view the chain produces. One class serves both -- which is what keeps
# an adapter declaring itself exactly once (ADR 0011) -- and the two are told
# apart by how the instance is built: ``cls(...)`` for a payload (see
# :meth:`AdapterSettings.settings_customise_sources`), :func:`_resolved` for the
# chain. No second class hierarchy, and no flag.


class AdapterSettings(BaseSettings):
    """A named set of settings for one adapter -- a *settings profile*.

    Subclasses declare the settings their adapter reads as fields, and set
    :attr:`adapter` to that adapter's module name. Every field is optional, so
    an empty profile is legal and one can be built up conditionally.

    Frozen, because a settings profile is a value: two with the same settings
    are interchangeable, and one already handed to :func:`configure` must not
    change under the block that entered it.

    Values are checked when the profile is *constructed*, so a typo raises where
    it was written rather than at a later ``with`` statement or, worse, inside a
    request.
    """

    model_config = SettingsConfigDict(
        # A setting an adapter does not read is a typo, and saying so at
        # construction is the whole point of a per-adapter vocabulary.
        extra="forbid",
        frozen=True,
        # The chain supplies raw TOML scalars and environment strings; each
        # field's validator is what turns them into values, so pydantic must not
        # coerce them first and hide a type error behind a silent cast.
        strict=False,
        validate_default=False,
    )

    #: The adapter this profile targets, by the name of the module a caller
    #: imports. ``None`` on the package-wide :class:`Settings`, which every
    #: adapter reads. A ``ClassVar``, not a field: the adapter is a property of
    #: the class, which is what stops the caller restating it at every call site
    #: and stops the roster being spelled twice.
    adapter: ClassVar[str | None] = None

    #: The named profile these settings were read from, or ``None`` for one
    #: written in code. Provenance rather than a setting: it records *where the
    #: values came from*, which is what lets :func:`show_settings` name the
    #: profile that supplied each value instead of reporting every block alike.
    #:
    #: A ``ClassVar`` shadowed per instance by :meth:`load`, so it is neither a
    #: field nor part of equality -- two profiles carrying the same settings stay
    #: interchangeable however each was spelled.
    profile: ClassVar[str | None] = None

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        """Constructing a profile directly reads nothing ambient.

        ``WaterdataSettings(concurrency=8)`` written in a script is a *payload*
        describing what that caller asked for, and folding the environment into
        it would make it describe something else -- ``values()`` would report
        settings the caller never wrote, and :func:`configure` would then push
        them into the block as though they had. So the caller's own keywords are
        the only source here.

        The stock ``env_settings``, ``dotenv_settings`` and
        ``file_secret_settings`` are dropped for the same reason, and because
        this package reads a specific set of ``API_USGS_*`` variables and one
        TOML grammar rather than pydantic-settings' generic conventions.

        The *resolution* chain is :data:`_CHAIN`, applied by :func:`_resolved`.
        """
        return (init_settings,)

    @field_validator("*", mode="before")
    @classmethod
    def _check(cls, value: object, info: Any) -> object:
        """Run one setting's grammar, naming the source that supplied it.

        Every field goes through the same table, so the file, the environment
        and a ``configure()`` block cannot come to disagree about what a value
        means. The label is the source recorded for this field during
        resolution, or the constructor that is running now.
        """
        name = info.field_name
        return _validate(name, value, _label_for(name, f"{name}= in {cls.__name__}()"))

    def __init__(self, **values: Any) -> None:
        """Validate the caller's keywords, and nothing ambient.

        Deliberately *not* ``BaseSettings.__init__``. That entry point exists to
        build a settings object once at start-up, and it constructs the four
        stock sources -- two of which snapshot and case-fold the whole of
        ``os.environ`` -- before :meth:`settings_customise_sources` can discard
        them. This package resolves lazily, several times per query, so it pays
        that cost per read rather than per process: measured at ~330 us against
        the ~5 us the rest of a resolution takes.

        Both of this class's roles want it skipped. A profile written in code
        must read nothing ambient by definition, and :func:`_resolved` supplies
        the chain's merged mapping itself. So this is ``BaseModel.__init__`` --
        validate the given keywords into ``self`` -- plus the unwrap that keeps
        a bad value reported in this package's vocabulary rather than pydantic's.
        """
        try:
            self.__pydantic_validator__.validate_python(values, self_instance=self)
        except ValidationError as exc:
            raise _unwrap(exc) from None

    def model_post_init(self, context: Any, /) -> None:
        self.validate_settings()

    def validate_settings(self) -> None:
        """Check rules that span more than one setting.

        Does nothing by default. Per-setting grammar lives in this module's
        parsers and is shared with the file and the environment, so a value
        means the same thing whichever source wrote it; override this only for a
        rule no single setting can express.
        """

    @classmethod
    def settings(cls) -> frozenset[str]:
        """The setting names this profile accepts."""
        return frozenset(cls.model_fields)

    def values(self) -> dict[str, Any]:
        """The settings actually supplied, omitting those left unset.

        An omitted setting inherits from an outer block or a lower source; an
        explicit ``None`` suppresses them. ``model_fields_set`` is pydantic's
        record of that distinction, which is why no sentinel default is needed.
        """
        return {name: getattr(self, name) for name in sorted(self.model_fields_set)}

    @classmethod
    def load(cls: type[_S], profile: str) -> _S:
        """Read a named profile for this adapter from the settings file.

        ``[<adapter>.<profile>]``. Only the keys that table names are carried, so
        the profile still inherits the adapter's default profile and the
        package-wide keys per setting from the tiers below.

        Selecting a profile the file does not define raises: a name a caller just
        typed is a typo worth reporting, not a silent fall-through to settings
        they did not ask for.

        Parameters
        ----------
        profile : str
            The name after the adapter, so ``[waterdata.bulk]`` is ``"bulk"``.

        Returns
        -------
        AdapterSettings
            An instance of the class it was called on, remembering the profile
            it was read from so :func:`show_settings` can name it.
        """
        adapter = cls.adapter
        if adapter is None:
            raise ConfigurationError(
                f"{cls.__name__}.load() names a profile for one adapter, and "
                "the package-wide settings have none. Put shared keys at the "
                "top level of the file."
            )
        loaded = cls(**_named_profile(adapter, profile, cls.settings()))
        # The model is frozen, so the provenance goes on the way pydantic sets
        # its own attributes. It is deliberately not a field: the profile name is
        # where these values came from, not one of the values, and
        # :meth:`settings` is built from the fields.
        object.__setattr__(loaded, "profile", profile)
        return loaded

    def _source(self, name: str) -> str:
        """How one of this profile's settings is named in an error."""
        return f"{name}= in {type(self).__name__}()"

    def _provenance(self) -> str:
        """How :func:`show_settings` reports a value this supplied.

        The profile is named in the file's own spelling -- ``[waterdata.bulk]``
        -- so the report answers "which profile set this?" rather than only "a
        block did", and the answer is greppable in the file that holds it.
        """
        if self.adapter is None:
            return "configure() block"
        scope = self.adapter
        if self.profile is not None:
            scope = f"{scope}.{self.profile}"
        return f"configure() block [{scope}]"


# --- shared setting groups -----------------------------------------------
#
# Which settings an adapter accepts is the adapter's own knowledge, and it says
# so by naming the groups below. What a setting *is* -- its type, the fact that
# ``None`` suppresses the tiers under it -- is this module's. Each group declares
# one shared setting once, and an adapter composes the groups it reads::
#
#     class NgwmnSettings(_Chunked, _Concurrent, _Redirectable, _Retrying,
#                         AdapterSettings):
#         adapter: ClassVar[str] = "ngwmn"
#
# Widening a shared setting's accepted type, or adding one, is one edit rather
# than six. Unlike the dataclass version these annotations are *enforced*:
# pydantic builds its validator from them, so an adapter that drifted to
# ``retries: str | None`` would reject an integer at construction rather than
# type-checking clean and failing when a value reached the chain.
#
# Plain mixins rather than ``AdapterSettings`` subclasses: a group is not a
# settings profile -- it has no adapter and cannot be passed to
# :func:`configure`. Fields are collected in reverse MRO order, so an adapter
# composing all four reads ``retries, stall_timeout, base_url, concurrency,
# parallel_chunks``.


class _Retrying(BaseSettings):
    """Every adapter's retry dials: transient retries and the stall bound."""

    retries: int | None = None
    stall_timeout: float | int | None = None


class _Redirectable(BaseSettings):
    """An adapter whose requests can be pointed at another base URL."""

    base_url: str | None = None


class _Concurrent(BaseSettings):
    """An adapter that issues more than one request per call."""

    concurrency: int | Literal["unbounded"] | None = None


class _Chunked(BaseSettings):
    """An adapter whose queries divide into sub-requests the caller can fan."""

    parallel_chunks: int | None = None


class Settings(AdapterSettings):
    """Settings that apply to every adapter.

    The package-wide profile: ``adapter`` stays ``None``, so nothing narrows and
    every adapter reads what this sets unless its own profile, or a block nested
    inside, overrides that setting.

    Parameters
    ----------
    api_key : str, optional
        Water Data API key, sent as ``X-Api-Key`` and only ever to
        ``api.waterdata.usgs.gov``. Prefer reading it from a secret store, the
        environment, or the settings file over writing a literal into a script.
        Pass ``None`` to make a call without an ambient key.
    concurrency : int or str, optional
        Cap on simultaneous sub-requests: a positive integer, or ``"unbounded"``
        to disable the cap.
    retries : int, optional
        Retries attempted after a transient failure; ``0`` disables retrying.
    progress : bool or str, optional
        Whether to draw the progress line. ``None`` leaves the automatic
        behavior (on for a TTY or Jupyter kernel, off otherwise).
    parallel_chunks : int, optional
        Default optional fan-out for multi-value queries. It limits extra
        refinement, but URL-byte safety may already require more sub-requests.
        Sets the baseline that :func:`dataretrieval.parallel_chunks` overrides
        per call. Each sub-request spends rate-limit quota, so raise it only for
        pulls you know are large.
    stall_timeout : float, optional
        Seconds a call may go without receiving *any* data before retrying stops
        and the failure surfaces. Bounds the wall-clock cost of a dead
        connection, which ``retries`` does not -- it counts attempts, not
        seconds. Progress resets the clock; ``0`` disables the bound.

    Examples
    --------
    .. code-block:: python

        with dataretrieval.configure(Settings(api_key=vault.read("usgs"))):
            df, md = waterdata.get_daily(monitoring_location_id="USGS-05114000")
    """

    # Spelled out rather than composed from the groups above, because this order
    # is also the order :func:`show_settings` reports the settings in --
    # :data:`SETTINGS` is derived from it just below -- and composing would hand
    # that reader-facing sequence to MRO linearization. The two adapter-only
    # fields the groups carry are absent by construction here: there is no
    # package-wide base URL.
    api_key: str | None = None
    concurrency: int | Literal["unbounded"] | None = None
    retries: int | None = None
    progress: bool | str | None = None
    parallel_chunks: int | None = None
    stall_timeout: float | int | None = None


#: The package-wide settings, in the order :func:`show_settings` reports them --
#: the fields of :class:`Settings`, derived rather than restated. An adapter may
#: accept a subset of them plus :data:`ADAPTER_ONLY_SETTINGS`.
SETTINGS: tuple[str, ...] = tuple(Settings.model_fields)

#: Every setting name this module knows a grammar for.
_ALL_SETTINGS: tuple[str, ...] = SETTINGS + ADAPTER_ONLY_SETTINGS


#: The adapters that may be configured, by the name of the module a caller
#: imports. Names only, because this module cannot import an adapter without
#: cycling: every adapter imports it.
#:
#: Holding the names here rather than deriving them from the registry below is
#: what lets a ``[nldi]`` table stay valid in a file: NLDI is imported on demand
#: for the geopandas extra, so a roster built from imports would reject a
#: perfectly good table until something happened to import that module.
ADAPTERS: tuple[str, ...] = (
    "waterdata",
    "ngwmn",
    "nwdc",
    "wqp",
    "nldi",
    "streamstats",
)

#: Settings classes that have registered themselves, keyed by adapter. Populated
#: at adapter import, and consulted only to validate a table's *keys* -- which
#: happens the first time that adapter resolves a setting, by which point it is
#: necessarily imported.
_REGISTRY: dict[str, type[AdapterSettings]] = {}


def _register(cls: type[AdapterSettings]) -> None:
    """Record an adapter's settings class. Called at adapter import.

    The roster in :data:`ADAPTERS` and the class are the two halves of one
    declaration, and this is where they are checked to agree: a class naming an
    adapter the roster does not list would be a profile no file table and no
    report could ever reach.
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
    name an adapter this process has never loaded, and rejecting that would make
    a settings file conditionally valid depending on which optional extras
    happened to be installed. It means "cannot validate these keys yet".
    """
    cls = _REGISTRY.get(adapter)
    return None if cls is None else cls.settings()


# --- the configure() block ------------------------------------------------

# Overrides from the active ``configure`` blocks. A package-wide override is
# keyed by the setting's name; an adapter-scoped one by ``(adapter, name)``. One
# flat mapping rather than a nested one so that nesting, per-key inheritance and
# restore-on-exit keep falling out of a single lookup.
_ScopeKey = str | tuple[str, str]
# One frame per ``configure`` block, stacked outermost-first. Frames rather than
# a merged mapping are what makes "the innermost block wins" true across *both*
# scopes: an adapter-scoped value outranks a package-wide one only within the
# same frame. Merged, an outer ``configure(WaterdataSettings(...))`` would beat
# an inner ``configure(Settings(concurrency=1))`` -- inverting nesting, and
# silently discarding the per-call ``parallel_chunks(n)`` block.
#
# Each entry pairs the value with the label naming where it came from, built
# while the profile is still in hand because that is the only place the
# *profile* is known: a value from ``WaterdataSettings.load("bulk")`` and one
# from ``WaterdataSettings(...)`` are indistinguishable by the time they reach
# the frame.
_Frame = Mapping[_ScopeKey, tuple[Any, str]]
_scope: ContextVar[tuple[_Frame, ...]] = ContextVar(
    "dataretrieval_settings_scope", default=()
)


@contextmanager
def configure(*profiles: AdapterSettings) -> Iterator[None]:
    """Apply settings profiles for the duration of a ``with`` block.

    The highest-precedence source. Takes settings profiles positionally, at most
    one per adapter, and nothing else::

        with dataretrieval.configure(
            Settings(api_key=secrets["usgs"]),
            WaterdataSettings.load("bulk"),
            NgwmnSettings(concurrency=4),
        ):
            df, md = waterdata.get_daily(monitoring_location_id=sites)

    The adapter a profile targets is a property of its class, so the caller never
    restates it -- which is what keeps the adapter roster from being spelled once
    per call site. Naming two profiles for one adapter raises: they are the one
    pairing with no defined order between them.

    Because the block is delivered through a :class:`~contextvars.ContextVar`, a
    value set here applies to the current thread and to asyncio tasks started
    inside the block, and cannot leak into another thread, task, or unrelated
    call the way ``os.environ`` does -- which is what makes it safe for a server
    or notebook handling several users' credentials at once.

    Blocks nest and merge per setting: an inner block that sets only
    ``concurrency`` keeps the outer block's ``api_key``, and an adapter profile
    in an outer block loses to a package-wide value set by a block nested inside
    it, so the innermost block always decides.

    Parameters
    ----------
    *profiles : AdapterSettings
        A package-wide :class:`Settings` and/or one profile per adapter, in any
        order. Each adapter's class lives in that adapter's module --
        ``WaterdataSettings`` in :mod:`dataretrieval.waterdata`,
        ``NgwmnSettings`` in :mod:`dataretrieval.ngwmn`, and so on.

    Yields
    ------
    None

    Raises
    ------
    ConfigurationError
        If an argument is not a settings profile, or two of them target the same
        adapter. Raised on entry, before any request. A bad *value* raises
        earlier still, where the profile was constructed.

    See Also
    --------
    show_settings : Report the effective settings and where they came from.
    """
    token = _scope.set((*_scope.get(), _frame(profiles)))
    try:
        yield
    finally:
        _scope.reset(token)


def _frame(profiles: tuple[AdapterSettings, ...]) -> _Frame:
    """Flatten one ``configure`` call's profiles into a scope frame.

    One frame per block, holding both scopes: a package-wide setting keyed by
    its name, an adapter-scoped one by ``(adapter, name)``. Values were already
    checked when each profile was constructed -- which is where a typo should
    raise, at the line that wrote it rather than at a later ``with`` statement --
    so nothing new can fail here except the two call-shaped mistakes below.

    Each value is stored with the label naming the profile it came from, because
    this is the last point where that is known (see :data:`_Frame`).
    """
    overrides: dict[_ScopeKey, tuple[Any, str]] = {}
    seen: set[str | None] = set()
    for profile in profiles:
        if not isinstance(profile, AdapterSettings):
            raise ConfigurationError(
                "configure() takes settings profiles, not "
                f"{type(profile).__name__}. Package-wide settings go on "
                "Settings(...); a setting for one service goes on that "
                "adapter's profile, e.g. WaterdataSettings(...)."
            )
        adapter = profile.adapter
        if adapter in seen:
            where = f"the {adapter} adapter" if adapter else "the package-wide settings"
            raise ConfigurationError(
                f"configure() got two settings profiles for {where}. Precedence "
                "between them would be undefined, so combine them into one."
            )
        seen.add(adapter)
        label = profile._provenance()
        for name, value in profile.values().items():
            key: _ScopeKey = name if adapter is None else (adapter, name)
            overrides[key] = (value, label)
    return overrides


# --- settings sources -----------------------------------------------------
#
# One :class:`~pydantic_settings.PydanticBaseSettingsSource` per tier of the
# chain. Each returns the settings it can supply for the class being resolved
# and records the label that :func:`show_settings` will report; pydantic-settings
# does the merge, keeping the first value it sees for each key.


class _ChainSource(PydanticBaseSettingsSource):
    """Shared plumbing for this package's four tiers."""

    def __init__(self, settings_cls: type[BaseSettings], adapter: str) -> None:
        super().__init__(settings_cls)
        # ``""`` is how :func:`_resolved` spells "package-wide", because the
        # ContextVar's ``None`` already means "not resolving at all".
        self.adapter: str | None = adapter or None

    def get_field_value(
        self, field: FieldInfo, field_name: str
    ) -> tuple[Any, str, bool]:
        # Required by the ABC, but this package's sources answer for every field
        # at once in ``__call__`` -- a per-field walk would re-read the
        # environment and re-stat the file once per setting.
        raise NotImplementedError  # pragma: no cover

    def _names(self) -> tuple[str, ...]:
        return tuple(self.settings_cls.model_fields)


class _BlockSource(_ChainSource):
    """Tier 1: the innermost active ``configure()`` block."""

    def __call__(self) -> dict[str, Any]:
        values: dict[str, Any] = {}
        frames = _scope.get()
        if not frames:
            return values
        for name in self._names():
            # Innermost block first: a value set by a nested block wins over both
            # scopes of an enclosing one. Within one block the adapter-scoped
            # value is the more specific of the two, so it is asked first.
            for frame in reversed(frames):
                if self.adapter is not None and (self.adapter, name) in frame:
                    value, label = frame[(self.adapter, name)]
                elif name in frame:
                    value, label = frame[name]
                else:
                    continue
                values[name] = value
                _record(name, label)
                break
        return values


class _EnvSource(_ChainSource):
    """Tier 2: the setting's ``API_USGS_*`` variable.

    Package-wide by construction: seven adapters times four settings would be a
    namespace nobody can hold in mind, and an exported variable is inherited by
    every subprocess and invisible at the call site (ADR 0010).
    """

    def __call__(self) -> dict[str, Any]:
        values: dict[str, Any] = {}
        for name in self._names():
            variable = ENV_VARS.get(name)
            if variable is None:
                continue
            raw = os.environ.get(variable)
            if raw is None:
                continue
            # A blank variable is what container and CI tooling produces when it
            # has nothing to pass, so it does not count as configured -- except
            # for ``progress``, where blank has always meant "off".
            if not raw.strip() and name not in _BLANK_MEANS_SET:
                continue
            label = _env_source_label(variable)
            # Parsed here rather than by the field validator, because this is the
            # only point at which the value is known to have come from the
            # environment: by the time pydantic validates the merged mapping,
            # which tier supplied a value is no longer visible. That matters for
            # the two grammars this tier alone is lenient about -- a blank
            # numeric falls through to the default, and any unrecognized
            # ``progress`` value means "on".
            values[name] = _ENV_PARSERS[name](raw, label)
            _record(name, label)
        return values


class _AdapterTableSource(_ChainSource):
    """Tier 3: the ``[<adapter>]`` table -- that adapter's default profile."""

    def __call__(self) -> dict[str, Any]:
        if self.adapter is None:
            return {}
        path, parsed = _current_file()
        table = _adapter_file_settings(self.adapter, path, parsed)
        values: dict[str, Any] = {}
        for name in self._names():
            if name in table:
                value, label = table[name]
                values[name] = value
                _record(name, label)
        return values


class _TopLevelSource(_ChainSource):
    """Tier 4: the file's top-level keys -- the package-wide defaults."""

    def __call__(self) -> dict[str, Any]:
        path, parsed = _current_file()
        if not parsed.base:
            return {}
        values: dict[str, Any] = {}
        label = str(path)
        for name in self._names():
            if name in parsed.base:
                values[name] = parsed.base[name]
                _record(name, label)
        return values


def _env_source_label(variable: str) -> str:
    """How a value read from *variable* is reported as a source."""
    return f"${variable}"


# --- resolution ----------------------------------------------------------


#: The resolution chain, highest precedence first. Declared as data so the
#: ordering is one fact in one place: ADR 0009's "precedence is per setting, not
#: per source" is expressed by walking these in order and keeping the first
#: value seen for each key, rather than by hand-written fallbacks.
_CHAIN: tuple[type[_ChainSource], ...] = (
    _BlockSource,
    _EnvSource,
    _AdapterTableSource,
    _TopLevelSource,
)


def _resolved(adapter: str | None) -> tuple[AdapterSettings, dict[str, str]]:
    """Run the chain for one adapter, returning the values and their sources.

    Building the whole profile rather than one setting matches the blast radius
    the file tier already had: resolution validates an adapter's entire table on
    first use, so a bad ``concurrency`` in ``[waterdata]`` has always been able
    to fail a read of ``retries`` for Water Data -- and must not touch NLDI.

    The sources are walked here and the result handed to ``model_validate``,
    rather than going through ``BaseSettings()`` and letting the library drive
    them. That is a deliberate departure, and it is about cost, not shape --
    the sources and their order are still pydantic-settings'. ``BaseSettings``
    builds the four *stock* sources on every instantiation before
    :meth:`settings_customise_sources` gets to discard them, and two of those
    snapshot and case-fold the whole of ``os.environ``. Settings resolve on the
    request path here, several times per query, so that is not affordable:
    profiling one read put 74% of it in ``_settings_init_sources``, at ~330 us
    against the ~5 us the whole resolution costs this way. ``model_validate``
    runs the same field validators, ``extra="forbid"`` and
    ``model_post_init`` hook, so nothing about the schema half changes.
    """
    cls: type[AdapterSettings] = (
        Settings if adapter is None else _REGISTRY.get(adapter, Settings)
    )
    labels: dict[str, str] = {}
    recording = _recorder.set(labels)
    try:
        merged: dict[str, Any] = {}
        for source in _CHAIN:
            for name, value in source(cls, adapter or "")().items():
                # First writer wins, which is what makes the tuple above a
                # precedence order. ``_record`` follows the same rule, so the
                # label and the value always come from the same tier.
                merged.setdefault(name, value)
        # ``cls(**merged)`` rather than ``model_validate``: the custom
        # ``__init__`` above is what pydantic dispatches to either way, so
        # calling it directly saves a round trip through the validator.
        return cls(**merged), labels
    finally:
        _recorder.reset(recording)


def _resolve(name: str, adapter: str | None = None) -> tuple[Any, str]:
    """Return the value for *name* and a human-readable source label.

    Precedence is *source-major*: the chain walks block, then environment, then
    file, exactly as ADR 0009 defines it -- and *within* each source an
    adapter-scoped value outranks a package-wide one. So a variable exported for
    one run still beats a stale ``[wqp]`` table in the settings file, which
    scope-major ordering would have quietly inverted (ADR 0010).

    Returns
    -------
    tuple[Any, str]
        The value, and where it came from -- ``None`` with :data:`_BUILT_IN`
        when nothing configured it. An explicit ``None`` from a block reads the
        same way at the accessors, which is what makes it a scoped reset to
        built-in behavior.
    """
    # An adapter name nobody recognizes is a typo in *our* source, and its
    # failure mode is silence: the file would hold no table under that name and
    # the read would fall through to the package-wide value, so a
    # ``WaterdataSettings`` would be ignored with nothing raised anywhere.
    if adapter is not None and adapter not in ADAPTERS:
        raise ConfigurationError(
            f"{adapter!r} is not a configurable adapter. The adapters are "
            f"{', '.join(ADAPTERS)}."
        )

    # Refused before anything is consulted, not at the environment's turn in the
    # chain. The file refuses ``base_url`` whether or not a block also set one,
    # and the two surfaces are one rule, so a variable that cannot work must not
    # be silently outranked by a block that happens to work.
    refused = _REFUSED_ENV_VARS.get(name)
    if refused is not None and refused in os.environ:
        raise ConfigurationError(
            f"{_env_source_label(refused)} is set, but {name!r} may only be set "
            "in code, in a configure() block, never from the environment. Unset "
            "it and pass the value on the adapter's settings, e.g. "
            f"WaterdataSettings({name}=...)."
        )

    # ``None`` unless this adapter actually reads this setting, so a setting
    # outside its vocabulary resolves package-wide rather than looking for a
    # scope it could never have been written into.
    scoped = adapter if adapter is not None and _accepts(adapter, name) else None
    instance, labels = _resolved(scoped)
    if name not in type(instance).model_fields:
        # The package-wide profile has no ``base_url``: there is no one base URL,
        # so nothing could have set it and the service's own default stands.
        return None, _BUILT_IN
    if name not in labels:
        return None, _BUILT_IN
    return getattr(instance, name), labels[name]


def _accepts(adapter: str, name: str) -> bool:
    """Whether *adapter* reads the setting *name*.

    An adapter this process has not imported has no vocabulary to consult, so
    every setting is assumed to be in scope for it: the file stays valid either
    way, and an adapter cannot be misreading a setting it has not loaded.
    """
    accepted = settings_for(adapter)
    return name in _ALL_SETTINGS if accepted is None else name in accepted


def _source_label(name: str, adapter: str | None = None) -> str:
    """The provenance label for one setting, for :func:`show_settings`."""
    return _resolve(name, adapter)[1]


# --- resolved settings ---------------------------------------------------


def api_key() -> str | None:
    """The Water Data API key, or ``None`` if none is configured.

    Surrounding whitespace is stripped, so a key read from a file with a
    trailing newline works; a blank value resolves to ``None``.
    """
    value, _source = _resolve("api_key")
    return value.strip() or None if value is not None else None


def concurrency(
    default: int | None = DEFAULT_CONCURRENCY, *, adapter: str | None = None
) -> int | None:
    """Cap on simultaneous chunks; ``None`` means unbounded.

    ``default`` is the caller's own preference for when nothing is configured --
    NWDC ships a lower figure than the OGC getters, because it is only
    stress-tested to that level. A value resolved from the chain always wins over
    it: a service able to override an explicit setting would make
    ``concurrency=1`` a lie.
    """
    value, _source = _resolve("concurrency", adapter)
    if value is None:
        return default
    return None if value == CONCURRENCY_UNBOUNDED else int(value)


def retries(*, adapter: str | None = None) -> int:
    """Retries attempted after the first try; ``0`` disables retrying."""
    value, _source = _resolve("retries", adapter)
    return DEFAULT_RETRIES if value is None else int(value)


def progress() -> bool | None:
    """Explicit progress-line setting, or ``None`` to auto-detect.

    ``None`` means nothing configured it, so the caller applies its own default
    (a TTY or Jupyter kernel gets the line, redirected output doesn't).
    """
    value, _source = _resolve("progress")
    return None if value is None else bool(value)


def parallel_chunks(*, adapter: str | None = None) -> int:
    """Configured default fan-out for multi-value queries.

    ``1`` (the default) means "chunk only as much as the URL byte limit forces".
    This is the *baseline*; :func:`dataretrieval.parallel_chunks` overrides it
    for one call.
    """
    value, _source = _resolve("parallel_chunks", adapter)
    return DEFAULT_PARALLEL_CHUNKS if value is None else int(value)


def stall_timeout(*, adapter: str | None = None) -> float:
    """Longest a call may go without receiving data before retrying stops.

    Seconds; ``0`` disables the bound. Bounds the wall-clock cost of a dead
    connection, which the retry *count* does not: it counts attempts, not
    seconds.
    """
    value, _source = _resolve("stall_timeout", adapter)
    return DEFAULT_STALL_TIMEOUT if value is None else float(value)


@overload
def base_url(*, adapter: str | None = ...) -> str | None: ...


@overload
def base_url(*, adapter: str | None = ..., default: str) -> str: ...


def base_url(*, adapter: str | None = None, default: str | None = None) -> str | None:
    """An adapter's configured base URL, falling back to *default*.

    Settable from code only: an adapter's settings may carry it, and both the
    file and the environment refuse it -- the file at :func:`_accepted_keys` and
    the environment at :data:`_REFUSED_ENV_VARS`, each with an error naming the
    block to write instead. A file that silently redirects a data-retrieval
    library to another host is a supply-chain-shaped hazard, while a
    ``configure`` block keeps the redirect where a reader of the script sees it
    (ADR 0011).

    There is no package-wide default, because there is no one base URL: what an
    adapter's requests are built on is the adapter's own fact, so the service
    passes its own and the URL stays declared beside the service that owns it.

    Parameters
    ----------
    adapter : str, optional
        Whose base URL to resolve.
    default : str, optional
        The service's own base, returned when nothing configured one.
    """
    value, _source = _resolve("base_url", adapter)
    return default if value is None else str(value)


# --- the settings file ---------------------------------------------------
#
# pydantic-settings ships a ``TomlConfigSettingsSource``, and it is deliberately
# not used: it reads a whole file into a flat mapping on every instantiation,
# with no notion of adapter tables or named profiles, no cache, and no
# permission check. Settings resolve on the request path here, so re-reading and
# re-parsing per read is not affordable, and the grammar below is most of what
# the file layer *is*. What the library does own -- the merge and the validation
# -- is what the sources above delegate to it.


@dataclass(frozen=True)
class _ParsedFile:
    """A parsed settings file: package-wide keys plus per-adapter tables.

    ``exists`` distinguishes "the file is there and defines nothing" from "there
    is no file", which decides which of the two messages a caller selecting a
    profile gets (see :func:`_named_profile`).
    """

    base: dict[str, Any] = field(default_factory=dict)
    #: Raw, *unvalidated* ``[<adapter>]`` tables, keyed by adapter name. Each
    #: holds that adapter's default-profile keys and, as sub-tables, its named
    #: profiles. Left unvalidated because a bad value in ``[nldi]`` must not fail
    #: a Water Data call that never reads it.
    adapters: dict[str, dict[str, Any]] = field(default_factory=dict)
    exists: bool = False


#: Stand-in for "no settings file", which is the common case. Shared rather than
#: rebuilt per read so that callers can memoize on the parsed file's identity;
#: nothing mutates a ``_ParsedFile``.
_NO_FILE = _ParsedFile()

# Resolved settings-file path, memoized on the raw ``DATARETRIEVAL_CONFIG``
# value (see :func:`config_path`).
_path_cache: tuple[str | None, object | None, Path] | None = None

# Parsed settings file, keyed by file identity, change metadata, and raw
# content. POSIX ctime makes metadata hits reliable; Windows ctime is creation
# time, so cache hits there compare content before reusing the parsed result.
_FileStamp = tuple[int, int, int, int, int, int]
_file_cache: tuple[Path, _FileStamp, bytes, _ParsedFile] | None = None

# Validated ``[<adapter>]`` tables, keyed by adapter name and memoized on the
# parsed file's identity, because an adapter table is validated only once that
# adapter is actually used.
_adapter_cache: dict[str, tuple[_ParsedFile, Path, Mapping[str, tuple[Any, str]]]] = {}

# Paths already warned about for loose permissions, so the warning fires once.
_permission_warned: set[Path] = set()


def config_path() -> Path:
    """Path to the settings file, honoring ``DATARETRIEVAL_CONFIG``.

    Memoized on the raw ``DATARETRIEVAL_CONFIG`` value, because this sits on the
    per-request path via :func:`api_key` and building the default costs more than
    the ``stat`` it leads to (``Path.home()`` alone dominates the whole
    resolution).

    Returns
    -------
    pathlib.Path
        The explicit path from ``DATARETRIEVAL_CONFIG`` if set, otherwise
        ``~/.dataretrieval/config.toml``. The file need not exist.
    """
    global _path_cache
    override = os.environ.get(CONFIG_PATH_ENV)

    cached = _path_cache
    if cached is not None and cached[0] == override:
        cached_guard, path = cached[1], cached[2]
        # The memo is only valid while whatever the path was *derived from* is
        # unchanged, so each branch records its own guard. A relative override is
        # anchored to the working directory (a later ``os.chdir`` in a per-job
        # notebook or scheduler must not keep reading the previous job's file);
        # the default branch is anchored to ``$HOME``. An absolute override
        # depends on neither and guards with ``None``.
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
    simply has no settings file, and before settings were layered it worked fine
    on the environment alone.
    """
    try:
        home = Path.home()
    except (RuntimeError, OSError):
        return Path("~") / ".dataretrieval" / "config.toml"
    return home / ".dataretrieval" / "config.toml"


def _resolve_against_cwd(relative: Path) -> Path:
    """Resolve a relative override, or report a working directory that is gone."""
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
    missing cwd into a :class:`ConfigurationError`. Both are needed, because
    ``stat`` on a *deleted* working directory still succeeds -- the process holds
    the open handle -- while resolving its path does not.
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

    Which variable that is differs by platform, and the memo has to agree with
    the resolver or it watches the wrong thing. ``posixpath.expanduser`` reads
    ``HOME``; ``ntpath.expanduser`` reads ``USERPROFILE`` (then
    ``HOMEDRIVE``/``HOMEPATH``) and ignores ``HOME`` outright.
    """
    if os.name == "nt":
        return (
            os.environ.get("USERPROFILE")
            or os.environ.get("HOMEDRIVE", "") + os.environ.get("HOMEPATH", "")
            or ""
        )
    return os.environ.get("HOME") or ""


def _toml_parser() -> Any:
    """The TOML parser, imported on first use.

    ``import dataretrieval`` imports this module, but the parser is reachable
    only once a settings file actually exists -- the minority case.
    """
    if sys.version_info >= (3, 11):
        import tomllib
    else:  # pragma: no cover - exercised only on Python 3.10
        import tomli as tomllib
    return tomllib


def _current_file() -> tuple[Path, _ParsedFile]:
    """The settings file as currently loaded: its path and its parsed form."""
    path = config_path()
    return path, _load_file(path)


def _load_file(path: Path) -> _ParsedFile:
    """Parse the settings file at *path*, caching until it changes on disk."""
    global _file_cache
    try:
        st = path.stat()
    except FileNotFoundError:
        # No file is the normal case: continue to the built-in default.
        return _NO_FILE
    except OSError as exc:
        raise ConfigurationError(f"could not access {path}: {exc}") from exc

    if stat.S_ISDIR(st.st_mode):
        raise ConfigurationError(f"settings path {path} is a directory, not a file.")

    # Only a regular file is parsed. Anything else readable -- a character
    # device, a FIFO -- is treated as *empty* settings without being opened,
    # which is what ``DATARETRIEVAL_CONFIG=/dev/null`` asks for and the only
    # coherent answer for a stream: settings are re-resolved on every request, so
    # a FIFO would hand its contents to the first getter and nothing to the rest.
    if not stat.S_ISREG(st.st_mode):
        return _ParsedFile(exists=True)

    # POSIX ``st_ctime_ns`` advances on any inode change, so the metadata stamp
    # catches even a rewrite that restores the original mtime (``cp -p``, rsync
    # ``--times``, an editor that preserves timestamps). Windows ctime is
    # *creation* time, so there the stamp cannot see that class of edit and the
    # content compare below is the only correct check -- worth the re-read, since
    # serving a stale API key is the alternative.
    #
    # Please do not "optimize" this without a Windows-safe change detector;
    # ``test_file_edit_is_picked_up`` pins the behavior.
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
    setting: a bad value in ``[nldi]`` must not fail a Water Data call.
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
            # send its author hunting for a typo in a table spelled exactly as
            # the old docs said, so name the replacement instead.
            raise ConfigurationError(
                f"{path}: [{_RETIRED_PROFILES_TABLE}] is no longer read. A "
                "profile now belongs to one adapter: write [<adapter>.<name>] "
                'and select it with <Adapter>Settings.load("<name>").'
            )
        if isinstance(value, dict):
            raise ConfigurationError(
                f"{path}: unknown table [{key}]. Per-adapter tables are "
                f"{', '.join(f'[{name}]' for name in ADAPTERS)}; a named profile "
                f"goes under one of them, as [<adapter>.{key}]; top-level keys "
                "are the package-wide defaults."
            )
        top[key] = value

    return _ParsedFile(_checked_table(top, path, _TOP_LEVEL, SETTINGS), adapters, True)


def _accepted_keys(
    table: dict[str, Any],
    path: Path,
    where: str,
    allowed: frozenset[str] | tuple[str, ...],
) -> dict[str, Any]:
    """Filter one table down to the settings it is allowed to name.

    The key policy for every table in the file, in one place, so the default
    profile and a named profile cannot come to disagree about what is a typo. An
    unrecognized name warns rather than raising, so a file written for a newer
    release still works; a name this release *does* know but that table cannot
    use raises, because that one can never become meaningful.
    """
    out: dict[str, Any] = {}
    for key, value in table.items():
        if isinstance(value, dict):
            # A named profile -- ``[waterdata.bulk]`` parses as a sub-table of
            # ``[waterdata]``. Inert until a caller selects it, so it is neither
            # a setting here nor an error.
            continue
        if key in ADAPTER_ONLY_SETTINGS:
            # Rejected from the file wherever it appears. A file that silently
            # redirects a data-retrieval library to another host is a
            # supply-chain-shaped hazard (ADR 0011).
            raise ConfigurationError(
                f"{path}: {key!r} at {where} may only be set in code, in a "
                "configure() block, never from a file."
            )
        if key not in allowed:
            if key in SETTINGS:
                # A real setting, in a table that does not read it. Unlike an
                # unrecognized name -- which may simply belong to a newer release
                # -- this cannot become meaningful later, and silently ignoring
                # it would leave a caller believing they had tuned something.
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
) -> dict[str, Any]:
    """Check one table of the file and normalize its values.

    Every table in the file comes through here: the top-level keys, an adapter's
    default profile, and a named profile. Written once because the checks are the
    interesting part and they must not diverge.

    ``tomllib`` returns typed scalars (``concurrency = 32`` is an ``int``,
    ``concurrency = "unbounded"`` a ``str``), and each goes through the same
    grammar the other sources use, with a source that names the file and the
    table -- a grammar error found on the way *out* of the file should say which
    line to fix, not merely which field ended up holding it.
    """
    checked: dict[str, Any] = {}
    for key, value in _accepted_keys(table, path, where, allowed).items():
        if where == _TOP_LEVEL and key in _WARN_AT_TOP_LEVEL:
            warnings.warn(
                f"{path}: {_WARN_AT_TOP_LEVEL[key]}",
                UserWarning,
                stacklevel=_WARN_STACKLEVEL,
            )
        checked[key] = _validate(key, value, f"{path}: {key!r} at {where}")
    return checked


def _adapter_file_settings(
    adapter: str, path: Path, parsed: _ParsedFile
) -> Mapping[str, tuple[Any, str]]:
    """The ``[<adapter>]`` table's own keys -- its default profile.

    Layers *above* the file's top-level keys rather than being merged into them:
    within the file tier an adapter's own value outranks the package-wide one.

    Validated on first use, not at parse time, so a bad value in ``[nldi]``
    cannot fail a Water Data call -- the blast-radius rule ADR 0010 set.
    """
    table = parsed.adapters.get(adapter)
    if not table:
        return {}

    cached = _adapter_cache.get(adapter)
    if cached is not None and cached[0] is parsed and cached[1] == path:
        return cached[2]

    where = f"[{adapter}]"
    # An adapter this process has not imported declares no vocabulary, so its
    # table is checked against the package-wide settings alone: refusing a key
    # for want of a schema would make the file's validity depend on which
    # optional extras happened to be installed.
    accepted = settings_for(adapter)
    validated = _checked_table(
        table, path, where, SETTINGS if accepted is None else accepted
    )
    label = f"{path} {where}"
    result: Mapping[str, tuple[Any, str]] = MappingProxyType(
        {name: (value, label) for name, value in validated.items()}
    )
    _adapter_cache[adapter] = (parsed, path, result)
    return result


def _named_profiles(parsed: _ParsedFile, adapter: str) -> dict[str, dict[str, Any]]:
    """The named profiles the file defines for *adapter*, by name.

    A sub-table of an adapter's table is a named profile: ``[waterdata.bulk]``
    parses as a sub-table of ``[waterdata]``, and everything else in that table
    is a setting of the adapter's default profile. The two readers of that rule
    -- selecting a profile and reporting which ones exist -- share this one
    definition so they cannot come to disagree about what a profile is.
    """
    return {
        name: table
        for name, table in parsed.adapters.get(adapter, {}).items()
        if isinstance(table, dict)
    }


def _named_profile(
    adapter: str, profile: str, allowed: frozenset[str]
) -> dict[str, Any]:
    """The ``[<adapter>.<profile>]`` table, checked against *allowed*."""
    path, parsed = _current_file()
    named = _named_profiles(parsed, adapter)
    if profile not in named:
        if not parsed.exists:
            raise ConfigurationError(
                f"profile {profile!r} cannot be selected for {adapter}: there "
                f"is no settings file at {path}."
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
    # migrated from the retired ``[profiles.bulk.ngwmn]``. Dropping it silently
    # would leave the author believing they had tuned something.
    nested = sorted(key for key, value in table.items() if isinstance(value, dict))
    if nested:
        raise ConfigurationError(
            f"{path}: {where} contains a table, [{adapter}.{profile}.{nested[0]}]. "
            "A profile names settings for one adapter and nothing else; to "
            "configure two adapters for one run, give each its own profile and "
            "select both in the same configure() block."
        )

    return _checked_table(table, path, where, allowed)


def _holds_api_key(parsed: _ParsedFile) -> bool:
    """Whether the file names an API key anywhere, including inert tables.

    Inert tables count because the question is what the *file* contains, not what
    this run resolves: a key sitting in a profile nobody selected is just as
    readable to another user on the machine.
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


# --- the report ----------------------------------------------------------


def _display_api_key(adapter: str | None = None) -> str:
    """Render the key's presence, never its value."""
    return "<set>" if api_key() else "<not set>"


def _display_concurrency(adapter: str | None = None) -> str:
    value = concurrency(adapter=adapter)
    return CONCURRENCY_UNBOUNDED if value is None else str(value)


def _display_progress(adapter: str | None = None) -> str:
    setting = progress()
    return "auto" if setting is None else ("on" if setting else "off")


#: How each setting renders in :func:`show_settings`. Keyed by the same names as
#: :data:`_ALL_SETTINGS`, and asserted to cover them, so a setting added to one
#: without the other fails loudly instead of silently printing a neighbour's
#: value in the one report whose whole job is to be trustworthy.
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
        "every setting needs a show_settings renderer; "
        f"missing={sorted(set(_ALL_SETTINGS) - set(_DISPLAYS))} "
        f"extra={sorted(set(_DISPLAYS) - set(_ALL_SETTINGS))}"
    )


def show_settings(*, stream: TextIO | None = None) -> None:
    """Print the effective settings and the source of each one.

    A debugging aid for "why is this using my old key?". Every value is reported
    with the source that supplied it, named exactly: which variable, which table
    of the file, and -- when a caller selected one -- which profile. The API key
    is never printed, only whether one is set.

    Parameters
    ----------
    stream : file-like, optional
        Where to write. Defaults to ``sys.stdout``.

    Examples
    --------
    The sample below is generated by running this function, not written by hand;
    ``test_show_settings_sample_output_is_current`` re-runs it and fails if the
    two drift apart.

    .. code-block:: text

        >>> with dataretrieval.configure(WaterdataSettings.load("bulk")):
        ...     dataretrieval.show_settings()
        settings file  /home/u/.dataretrieval/config.toml (found)
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
          code with <Adapter>Settings.load("<name>").

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
        print(f"settings file  <unresolved: {exc}>", file=out)
        return

    # Nothing here raises. This function exists to explain a configuration, and
    # the configurations most in need of explaining are the broken ones. Each
    # distinct failure is printed once, in the first place it shows up; a repeat
    # is collapsed, so one bad file does not bury the rows that did resolve.
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

    # Probing the file once here means a whole-file problem -- unparseable TOML,
    # a bad value at the top level -- is reported on the file row rather than
    # repeated in every setting's row below.
    parsed = _NO_FILE
    try:
        _, parsed = _current_file()
        status = "found" if path.exists() else "not found"
    except ConfigurationError as exc:
        reported = str(exc)
        status = f"ERROR: {exc}"
    print(f"settings file  {path} ({status})", file=out)

    rows = [
        (
            name,
            cell(_display_for(name, None)),
            cell(_label_getter(name)),
        )
        for name in SETTINGS
    ]
    name_width = max(len(name) for name, _value, _source in rows)
    value_width = max(len(value) for _name, value, _source in rows)
    for name, value, source in rows:
        print(f"{name:<{name_width}}  {value:<{value_width}}  {source}", file=out)

    # A built-in default is package-wide, and a service may prefer its own for
    # its own calls -- so a row reading "built-in default" is not a promise about
    # every service. Saying so is the honest scope of this report.
    if any(source == _BUILT_IN for _name, _value, source in rows):
        print(
            "\nA built-in default is package-wide. An adapter may prefer its own "
            "for\nits own calls; a value from any source above overrides both.",
            file=out,
        )

    _show_adapter_overrides(out, cell, {name: source for name, _value, source in rows})
    _show_profiles(out, parsed)
    _show_unimported_adapters(out)


def _display_for(name: str, adapter: str | None) -> Callable[[], object]:
    """A thunk rendering one setting, for :func:`show_settings`'s error trap."""
    return lambda: _DISPLAYS[name](adapter)


def _label_getter(name: str, adapter: str | None = None) -> Callable[[], object]:
    """A thunk resolving one setting's provenance label."""
    return lambda: _source_label(name, adapter)


def _show_adapter_overrides(
    out: TextIO,
    cell: Callable[[Callable[[], object]], str],
    package_wide: Mapping[str, str],
) -> None:
    """Print the adapter-scoped settings that differ from the rows above.

    Only settings actually overridden, and only adapters that override one: a
    full adapter-by-setting grid would be mostly inherited values, burying the
    answer to "what will this call use".
    """
    overrides: list[tuple[str, str, str, str]] = []
    for adapter in ADAPTERS:
        accepted = settings_for(adapter)
        if accepted is None:
            continue
        for name in _ALL_SETTINGS:
            if name not in accepted:
                continue
            scoped = cell(_label_getter(name, adapter))
            # ``package_wide`` is what the rows above already resolved. Asking
            # again would repeat the work once per adapter *and* consume the
            # shared error-dedupe state. An adapter-only setting has no row
            # above, so its baseline is the built-in default.
            if scoped == package_wide.get(name, _BUILT_IN):
                continue  # inherited from the package-wide tier
            value = cell(_display_for(name, adapter))
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


def _show_profiles(out: TextIO, parsed: _ParsedFile) -> None:
    """Print the named profiles the file defines, selected or not.

    A named profile does nothing until a caller selects it, and that is the thing
    readers of a settings file get wrong: adding ``[waterdata.bulk]`` changes no
    run on its own. A report that mentioned a profile only when one had been
    selected would leave that silence with nothing to explain it.
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
        '  code with <Adapter>Settings.load("<name>").',
        file=out,
    )


def _show_unimported_adapters(out: TextIO) -> None:
    """Name the adapters this process cannot report on, and say why.

    An adapter is only known to accept a setting once the module declaring that
    vocabulary has been imported, and NLDI is deliberately imported on demand for
    the geopandas extra. Omitting it silently would read as "nothing is
    configured for nldi", which is a different claim and the wrong one.
    """
    unimported = [a for a in ADAPTERS if settings_for(a) is None]
    if unimported:
        print(
            f"\nnot reported: {', '.join(unimported)} "
            "(not imported, so the settings each accepts are unknown here)",
            file=out,
        )


def _reset_file_cache() -> None:
    """Drop the parsed-file cache. For tests that rewrite the file in place."""
    global _file_cache, _path_cache
    _file_cache = None
    _path_cache = None
    _adapter_cache.clear()
    _permission_warned.clear()
