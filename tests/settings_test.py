"""Tests for layered settings resolution (``dataretrieval.settings``)."""

from __future__ import annotations

import asyncio
import inspect
import io
import json
import os
import pathlib
import re
import textwrap
import threading
from dataclasses import dataclass
from typing import ClassVar

import pytest

import dataretrieval
from dataretrieval import settings, streamstats, waterdata
from dataretrieval.ngwmn import NgwmnSettings
from dataretrieval.nwdc import DEFAULT_CONCURRENT_REQUESTS, NwdcSettings
from dataretrieval.settings import Settings
from dataretrieval.streamstats import StreamstatsSettings
from dataretrieval.utils import _default_headers
from dataretrieval.waterdata import WaterdataSettings, endpoints
from dataretrieval.wqp import WqpSettings

WATERDATA_URL = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items"

# Where the base-URL tests redirect to. A host the suite can never reach, so a
# redirect that failed to apply shows up as an unmocked request rather than as
# a real one.
_MIRROR = "https://mirror.example/waterdata"
_MIRROR_RE = re.compile(r"^https://mirror\.example/")
_WATERDATA_RE = re.compile(r"^https://api\.waterdata\.usgs\.gov/")

# One committed page of the ``daily`` collection, shared with the Water Data
# suite. Real response shape rather than a hand-made stub, so a redirect is
# exercised through the same shaping the getters normally do; it carries no
# ``links``, so nothing paginates.
_DAILY_PAGE = json.loads(
    (pathlib.Path(__file__).parent / "data" / "waterdata_ogc_fixtures.json").read_text()
)["daily"]


@pytest.fixture
def config_file(tmp_path, monkeypatch):
    """Write a config file and point ``DATARETRIEVAL_CONFIG`` at it."""

    def write(text: str):
        path = tmp_path / "config.toml"
        path.write_text(text)
        path.chmod(0o600)  # keep the loose-permission warning out of the way
        for env in settings.ENV_VARS.values():
            monkeypatch.delenv(env, raising=False)
        monkeypatch.setenv(settings.CONFIG_PATH_ENV, str(path))
        settings._reset_file_cache()
        return path

    return write


# --- precedence ----------------------------------------------------------


def test_default_when_nothing_is_configured(monkeypatch):
    for env in settings.ENV_VARS.values():
        monkeypatch.delenv(env, raising=False)
    assert settings.api_key() is None
    assert settings.concurrency() == settings.DEFAULT_CONCURRENCY
    assert settings.retries() == settings.DEFAULT_RETRIES
    assert settings.parallel_chunks() == settings.DEFAULT_PARALLEL_CHUNKS
    assert settings.progress() is None


def test_env_is_used_when_no_file_or_block(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    monkeypatch.setenv("API_USGS_CONCURRENT", "4")
    assert settings.api_key() == "env-key"
    assert settings.concurrency() == 4


def test_env_outranks_file(config_file, monkeypatch):
    config_file('api_key = "file-key"\n')
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    assert settings.api_key() == "env-key"


def test_block_outranks_file_and_env(config_file, monkeypatch):
    config_file('api_key = "file-key"\n')
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    with dataretrieval.configure(Settings(api_key="block-key")):
        assert settings.api_key() == "block-key"
    assert settings.api_key() == "env-key"


def test_precedence_is_per_setting_not_per_source(config_file, monkeypatch):
    """An environment key must not blank out file-provided settings."""
    config_file("concurrency = 16\n")
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    monkeypatch.setenv("API_USGS_RETRIES", "9")
    assert settings.concurrency() == 16  # from the file
    assert settings.api_key() == "env-key"  # still from the env
    assert settings.retries() == 9  # still from the env


# --- the configure() block -----------------------------------------------


def test_blocks_nest_and_merge_per_setting():
    with dataretrieval.configure(Settings(api_key="outer", concurrency=4)):
        with dataretrieval.configure(Settings(concurrency=8)):
            assert settings.concurrency() == 8
            assert settings.api_key() == "outer"  # inherited from the outer block
        assert settings.concurrency() == 4  # inner block restored on exit


def test_omitted_setting_inherits_lower_source(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    with dataretrieval.configure(Settings(concurrency=2)):
        assert settings.api_key() == "env-key"


def test_explicit_none_suppresses_lower_sources(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    monkeypatch.setenv("API_USGS_CONCURRENT", "4")
    monkeypatch.setenv("API_USGS_PROGRESS", "true")
    with dataretrieval.configure(
        Settings(api_key=None, concurrency=None, progress=None)
    ):
        assert settings.api_key() is None
        assert settings.concurrency() == settings.DEFAULT_CONCURRENCY
        assert settings.progress() is None
    assert settings.api_key() == "env-key"
    assert settings.concurrency() == 4
    assert settings.progress() is True


@pytest.mark.parametrize(
    "values",
    [
        {"concurrency": 0},
        {"retries": -1},
        {"parallel_chunks": 0},
        {"progress": "flase"},
    ],
)
def test_a_settings_profile_validates_its_own_settings(values):
    """A bad value raises where it was written, not inside a later request.

    Construction is earlier than the ``with``, which is earlier than the
    request the value would otherwise have broken.
    """
    with pytest.raises(settings.ConfigurationError):
        Settings(**values)


@pytest.mark.parametrize(
    ("values", "expected"),
    [
        ({"api_key": 123}, "string"),
        ({"concurrency": 1.5}, "integer"),
        ({"concurrency": "8"}, "integer"),
        ({"retries": "2"}, "integer"),
        ({"progress": []}, "bool"),
        ({"parallel_chunks": True}, "integer"),
    ],
)
def test_settings_reject_values_outside_annotated_types(values, expected):
    with pytest.raises(settings.ConfigurationError, match=expected):
        Settings(**values)


def test_block_accepts_ints_and_strings():
    with dataretrieval.configure(Settings(concurrency="unbounded")):
        assert settings.concurrency() is None
    with dataretrieval.configure(Settings(concurrency=8)):
        assert settings.concurrency() == 8
    with dataretrieval.configure(Settings(progress=False)):
        assert settings.progress() is False
    with dataretrieval.configure(Settings(progress=True)):
        assert settings.progress() is True


def test_configure_takes_configurations_and_nothing_else():
    """The argument is an object, so a stray mapping or keyword cannot pass.

    ``configure(ngwmn={"concurrency": 2})`` was the earlier spelling, and it is
    exactly what a reader of an old script will try. Naming the replacement in
    the error is the difference between a two-minute fix and a search.
    """
    with pytest.raises(settings.ConfigurationError, match="settings profiles"):
        with dataretrieval.configure({"concurrency": 2}):
            pass
    with pytest.raises(settings.ConfigurationError, match="settings profiles"):
        with dataretrieval.configure("waterdata"):
            pass
    # Settings are no longer keywords on ``configure`` at all.
    with pytest.raises(TypeError):
        with dataretrieval.configure(api_key="k"):
            pass


def test_two_configurations_for_one_adapter_raise():
    """They are the one pairing with no defined order between them.

    Silently letting the last win would make a block's meaning depend on
    argument order, which nothing in the surrounding chain does.
    """
    with pytest.raises(settings.ConfigurationError, match="two settings profiles"):
        with dataretrieval.configure(
            WaterdataSettings(concurrency=2),
            WaterdataSettings(retries=1),
        ):
            pass

    # Same rule for the package-wide configuration, which targets no adapter.
    with pytest.raises(settings.ConfigurationError, match="package-wide"):
        with dataretrieval.configure(Settings(retries=1), Settings(retries=2)):
            pass

    # Two *different* adapters in one block is the whole point of the feature.
    with dataretrieval.configure(
        WaterdataSettings(concurrency=2), NgwmnSettings(concurrency=8)
    ):
        assert settings.concurrency(adapter="waterdata") == 2
        assert settings.concurrency(adapter="ngwmn") == 8


def test_a_configuration_resolves_end_to_end(config_file, monkeypatch):
    """Every tier below a passed configuration still applies, per setting."""
    config_file('api_key = "file-key"\nstall_timeout = 15\n')
    monkeypatch.setenv("API_USGS_RETRIES", "9")

    with dataretrieval.configure(Settings(concurrency=3)):
        assert settings.concurrency() == 3  # from the configuration
        assert settings.retries() == 9  # still from the environment
        assert settings.api_key() == "file-key"  # still from the file
        assert settings.stall_timeout() == 15  # still from the file
        assert (
            settings.parallel_chunks() == settings.DEFAULT_PARALLEL_CHUNKS
        )  # still the built-in default

    assert settings.concurrency() == settings.DEFAULT_CONCURRENCY


def test_an_adapter_configuration_narrows_to_one_adapter(monkeypatch):
    """The adapter is a property of the class, so nothing else moves."""
    monkeypatch.delenv("API_USGS_RETRIES")  # pinned by the autouse fixture

    with dataretrieval.configure(NgwmnSettings(retries=1)):
        assert settings.retries(adapter="ngwmn") == 1
        # Every other adapter, and the package-wide read, are untouched --
        # including waterdata, which shares NGWMN's host and its API key.
        for other in ("waterdata", "nwdc", "wqp", "streamstats"):
            assert settings.retries(adapter=other) == settings.DEFAULT_RETRIES
        assert settings.retries() == settings.DEFAULT_RETRIES


# --- isolation (the point of issue #352) ---------------------------------


def test_threads_do_not_leak_credentials_into_each_other():
    """Two threads in different blocks see different keys.

    This is the concurrency complaint in #352: ``os.environ`` is
    process-global, so it cannot express this.
    """
    seen: dict[str, str | None] = {}
    started = threading.Barrier(2)

    def worker(name: str, key: str) -> None:
        with dataretrieval.configure(Settings(api_key=key)):
            started.wait(timeout=5)  # force the blocks to overlap in time
            seen[name] = settings.api_key()

    threads = [
        threading.Thread(target=worker, args=("a", "key-a")),
        threading.Thread(target=worker, args=("b", "key-b")),
    ]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=10)

    assert seen == {"a": "key-a", "b": "key-b"}


def test_asyncio_tasks_do_not_leak_credentials_into_each_other():
    """Concurrent asyncio tasks each keep their own key."""

    async def worker(key: str) -> str | None:
        with dataretrieval.configure(Settings(api_key=key)):
            await asyncio.sleep(0)  # yield, letting the other task interleave
            return settings.api_key()

    async def main() -> list[str | None]:
        return list(await asyncio.gather(worker("key-a"), worker("key-b")))

    assert asyncio.run(main()) == ["key-a", "key-b"]


# --- the file ------------------------------------------------------------


def test_named_profile_is_selected_in_code(config_file):
    """``[<adapter>.<name>]`` reaches the chain only when a caller loads it."""
    config_file(
        'api_key = "shared"\nconcurrency = 4\n\n'
        "[waterdata]\nretries = 2\n\n"
        '[waterdata.bulk]\nconcurrency = "unbounded"\n'
    )

    # Inert until selected: the file alone changes nothing about concurrency.
    assert settings.concurrency(adapter="waterdata") == 4

    with dataretrieval.configure(WaterdataSettings.load("bulk")):
        assert settings.concurrency(adapter="waterdata") is None  # the profile
        assert settings.retries(adapter="waterdata") == 2  # default profile
        assert settings.api_key() == "shared"  # package-wide, from the file
        # It narrows to one adapter, so a sibling on the same host is untouched.
        assert settings.concurrency(adapter="ngwmn") == 4

    assert settings.concurrency(adapter="waterdata") == 4


def test_a_code_selected_profile_outranks_the_environment(config_file, monkeypatch):
    """ADR 0011 inverts ADR 0009's environment-above-file rule for this case.

    A profile named in code is a more deliberate act than a variable inherited
    from a shell, and losing to that variable is what a caller would file a bug
    about.
    """
    config_file("[waterdata.gentle]\nconcurrency = 2\n")
    monkeypatch.setenv("API_USGS_CONCURRENT", "16")

    assert settings.concurrency(adapter="waterdata") == 16
    with dataretrieval.configure(WaterdataSettings.load("gentle")):
        assert settings.concurrency(adapter="waterdata") == 2


def test_several_named_profiles_are_selected_independently(config_file):
    """One block, two adapters, a different named profile for each."""
    config_file(
        "[waterdata.bulk]\nconcurrency = 32\n\n"
        "[waterdata.polite]\nconcurrency = 2\n\n"
        "[ngwmn.gentle]\nconcurrency = 4\n"
    )

    with dataretrieval.configure(
        WaterdataSettings.load("polite"), NgwmnSettings.load("gentle")
    ):
        assert settings.concurrency(adapter="waterdata") == 2
        assert settings.concurrency(adapter="ngwmn") == 4


def test_a_named_profile_layers_per_key_over_the_tiers_below(config_file):
    """Selecting a profile replaces keys, never whole tiers.

    Every level of the file overrides the one below it *per key* (ADR 0011), so
    one adapter-scoped read here draws each of its four settings from a
    different table.
    """
    config_file(
        "concurrency = 16\nretries = 3\nstall_timeout = 30\n\n"
        "[waterdata]\nretries = 2\n\n"
        '[waterdata.bulk]\nconcurrency = "unbounded"\nparallel_chunks = 8\n'
    )

    with dataretrieval.configure(WaterdataSettings.load("bulk")):
        # the profile, over a package-wide key it names...
        assert settings.concurrency(adapter="waterdata") is None
        # ...the default profile, over a package-wide key the profile is silent
        # about...
        assert settings.retries(adapter="waterdata") == 2
        # ...the package-wide key, which neither table touched...
        assert settings.stall_timeout(adapter="waterdata") == 30
        # ...and a setting only the profile names.
        assert settings.parallel_chunks(adapter="waterdata") == 8


def _resolved_settings() -> dict[object, object]:
    """Every setting this process can resolve, package-wide and per adapter.

    A snapshot rather than a handful of assertions, because the claim under
    test is about what a file does *not* change -- and naming the settings
    individually would only prove it for the ones the author thought of.
    """
    snapshot: dict[object, object] = {
        "api_key": settings.api_key(),
        "progress": settings.progress(),
    }
    for adapter in (None, *settings.ADAPTERS):
        snapshot[(adapter, "concurrency")] = settings.concurrency(adapter=adapter)
        snapshot[(adapter, "retries")] = settings.retries(adapter=adapter)
        snapshot[(adapter, "parallel_chunks")] = settings.parallel_chunks(
            adapter=adapter
        )
        snapshot[(adapter, "stall_timeout")] = settings.stall_timeout(adapter=adapter)
        snapshot[(adapter, "base_url")] = settings.base_url(adapter=adapter)
    return snapshot


def test_adding_a_named_profile_changes_nothing_until_it_is_selected(config_file):
    """Inertness is what makes a profile safe to add to a file others share.

    A named profile that could shift a setting on its own would make every
    addition to a shared ``config.toml`` a change to every script reading it,
    which is the failure the retired global ``[profiles.<name>]`` table had.
    """
    shared = 'api_key = "shared"\nconcurrency = 4\n\n[waterdata]\nretries = 2\n'
    config_file(shared)
    before = _resolved_settings()

    config_file(
        shared + '\n[waterdata.bulk]\nconcurrency = "unbounded"\n'
        "retries = 9\nparallel_chunks = 8\nstall_timeout = 5\n"
    )
    assert _resolved_settings() == before

    # ...and the profile does reach the chain once it is named in code, so the
    # comparison above is inertness rather than a profile nothing can select.
    with dataretrieval.configure(WaterdataSettings.load("bulk")):
        assert settings.parallel_chunks(adapter="waterdata") == 8


def test_a_named_profile_cannot_hold_a_nested_table(config_file):
    """``[waterdata.bulk.ngwmn]`` is the retired shape, not a deeper profile.

    A profile carries settings for the one adapter it belongs to, so a table
    inside one has no reading. Refused rather than skipped: silently dropping
    it would leave the author believing they had tuned NGWMN.
    """
    config_file(
        "[waterdata.bulk]\nparallel_chunks = 8\n\n"
        "[waterdata.bulk.ngwmn]\nconcurrency = 2\n"
    )

    # Still inert, like every other problem inside an unselected profile: an
    # unrelated call resolves without ever reading it.
    assert settings.retries(adapter="ngwmn") == settings.DEFAULT_RETRIES
    assert settings.parallel_chunks(adapter="waterdata") == (
        settings.DEFAULT_PARALLEL_CHUNKS
    )

    with pytest.raises(
        settings.ConfigurationError, match=r"\[waterdata\.bulk\.ngwmn\]"
    ):
        WaterdataSettings.load("bulk")


def test_loading_an_undefined_profile_raises(config_file):
    """A name the caller just typed is a typo, not a silent fall-through.

    The message lists what the file *does* define, because a misspelling is
    only obvious next to the spelling that was meant -- and only for this
    adapter, since selecting a profile is per adapter and another service's
    profile names are not candidates for what the caller meant to type.
    """
    config_file(
        "[waterdata]\nconcurrency = 4\n\n"
        "[waterdata.bulk]\nretries = 8\n\n"
        "[waterdata.polite]\nretries = 1\n\n"
        "[ngwmn.gentle]\nconcurrency = 2\n"
    )
    with pytest.raises(settings.ConfigurationError) as excinfo:
        WaterdataSettings.load("bluk")
    message = str(excinfo.value)
    assert "no [waterdata.bluk]" in message
    assert "bulk, polite" in message
    assert "gentle" not in message

    # An adapter with no profiles at all says so rather than trailing off after
    # the colon, which would read as a truncated message.
    config_file("[waterdata]\nconcurrency = 4\n")
    with pytest.raises(settings.ConfigurationError, match="waterdata: none"):
        WaterdataSettings.load("bulk")


def test_loading_a_profile_with_no_file_says_so(tmp_path, monkeypatch):
    monkeypatch.delenv("API_USGS_CONCURRENT")  # pinned by the autouse fixture
    monkeypatch.setenv(settings.CONFIG_PATH_ENV, str(tmp_path / "absent.toml"))
    settings._reset_file_cache()

    with pytest.raises(settings.ConfigurationError, match="no settings file"):
        WaterdataSettings.load("also-gone")


def test_the_package_wide_configuration_has_no_profiles(config_file):
    """A profile belongs to one adapter, so ``Settings`` cannot name one."""
    config_file("[waterdata.bulk]\nconcurrency = 8\n")
    with pytest.raises(settings.ConfigurationError, match="package-wide"):
        Settings.load("bulk")


def test_missing_file_is_not_an_error(tmp_path, monkeypatch):
    monkeypatch.delenv("API_USGS_CONCURRENT")  # pinned by the autouse fixture
    monkeypatch.setenv(settings.CONFIG_PATH_ENV, str(tmp_path / "absent.toml"))
    settings._reset_file_cache()
    assert settings.concurrency() == settings.DEFAULT_CONCURRENCY


def test_malformed_file_raises_pointing_at_the_file(config_file):
    path = config_file("api_key = \n")
    with pytest.raises(settings.ConfigurationError) as excinfo:
        settings.api_key()
    assert "not valid TOML" in str(excinfo.value)
    assert str(path) in str(excinfo.value)


def test_non_utf8_file_raises_config_error(config_file):
    path = config_file("")
    path.write_bytes(b'api_key = "\xff"\n')
    with pytest.raises(settings.ConfigurationError, match="not valid UTF-8"):
        settings.api_key()


def test_config_path_must_not_be_a_directory(tmp_path, monkeypatch):
    monkeypatch.setenv(settings.CONFIG_PATH_ENV, str(tmp_path))
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    settings._reset_file_cache()
    with pytest.raises(settings.ConfigurationError, match="directory"):
        settings.concurrency()


@pytest.mark.skipif(os.name != "posix", reason="needs /dev/null")
def test_dev_null_config_path_means_no_configuration(monkeypatch):
    """``DATARETRIEVAL_CONFIG=/dev/null`` is how a run isolates itself.

    A character device (or the FIFO from process substitution) reads as empty,
    which is exactly "no configuration". Rejecting it would raise from
    ``_default_headers`` on every request -- the opposite of what the caller
    asked for.
    """
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    monkeypatch.delenv("API_USGS_PAT", raising=False)
    monkeypatch.setenv(settings.CONFIG_PATH_ENV, "/dev/null")
    settings._reset_file_cache()
    assert settings.api_key() is None
    assert settings.concurrency() == settings.DEFAULT_CONCURRENCY


@pytest.mark.skipif(os.name != "posix", reason="POSIX directory permissions")
def test_inaccessible_config_path_raises(tmp_path, monkeypatch):
    parent = tmp_path / "blocked"
    parent.mkdir()
    path = parent / "config.toml"
    path.write_text("concurrency = 4\n")
    parent.chmod(0)
    monkeypatch.setenv(settings.CONFIG_PATH_ENV, str(path))
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    settings._reset_file_cache()
    try:
        try:
            path.stat()
        except PermissionError:
            pass
        else:  # pragma: no cover - root or a filesystem that ignores mode bits
            pytest.skip("filesystem does not enforce directory mode bits")
        with pytest.raises(settings.ConfigurationError, match="could not access"):
            settings.concurrency()
    finally:
        parent.chmod(0o700)


def test_unknown_setting_warns_but_is_ignored(config_file):
    config_file('concurrency = 4\napi_kye = "typo"\n')
    with pytest.warns(UserWarning, match="unknown setting"):
        assert settings.concurrency() == 4


def test_unknown_table_raises(config_file):
    """A profile written as ``[bulk]`` instead of ``[waterdata.bulk]``."""
    config_file("[bulk]\nconcurrency = 4\n")
    with pytest.raises(settings.ConfigurationError, match="unknown table"):
        settings.concurrency()


def test_the_retired_profiles_table_names_its_replacement(config_file):
    """Nothing shipped with ``[profiles.<name>]``, but the docs described it.

    The generic "unknown table" message would send its author hunting for a
    typo in a table spelled exactly as they had been told to spell it.
    """
    config_file("[profiles.bulk]\nconcurrency = 4\n")
    with pytest.raises(settings.ConfigurationError, match=r"\[<adapter>\.<name>\]"):
        settings.concurrency()


def test_the_retired_profile_environment_variable_is_ignored(config_file, monkeypatch):
    """``DATARETRIEVAL_PROFILE`` went with the table it selected (ADR 0011).

    A profile is now named in code. A variable exported once in a shell profile
    and inherited by every subprocess is the opposite shape: invisible at the
    call site, and able to switch every service at once. Honoring it under the
    new grammar would restore exactly what the grammar removed.
    """
    config_file('concurrency = 4\n\n[waterdata.bulk]\nconcurrency = "unbounded"\n')
    monkeypatch.setenv("DATARETRIEVAL_PROFILE", "bulk")

    assert settings.concurrency(adapter="waterdata") == 4
    assert "DATARETRIEVAL_PROFILE" not in settings.ENV_VARS.values()


def test_typed_toml_values_are_normalized(config_file):
    """``tomllib`` returns typed values that normalize into shared parsers."""
    config_file("concurrency = 16\nretries = 0\nprogress = true\n")
    assert settings.concurrency() == 16
    assert settings.retries() == 0
    assert settings.progress() is True


@pytest.mark.parametrize(
    "text",
    [
        "api_key = true\n",
        'concurrency = "8"\n',
        'retries = "2"\n',
        "progress = 17\n",
        "parallel_chunks = true\n",
    ],
)
def test_toml_rejects_wrong_scalar_types(config_file, text):
    config_file(text)
    with pytest.raises(settings.ConfigurationError):
        settings.parallel_chunks()


def test_file_edit_is_picked_up(config_file, monkeypatch):
    path = config_file("concurrency = 4\n")
    assert settings.concurrency() == 4
    original = path.stat()
    path.write_text("concurrency = 8\n")
    os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
    # Windows ctime is creation time, so unchanged metadata must fall back to
    # comparing raw content before the parsed cache is reused.
    monkeypatch.setattr(settings.os, "name", "nt")
    assert settings.concurrency() == 8


def test_explicit_config_path_is_expanded(monkeypatch):
    monkeypatch.setenv(settings.CONFIG_PATH_ENV, "~/somewhere/config.toml")
    assert str(settings.config_path()).startswith(os.path.expanduser("~"))
    assert "~" not in str(settings.config_path())


def test_relative_config_path_follows_the_working_directory(tmp_path, monkeypatch):
    """A relative ``DATARETRIEVAL_CONFIG`` is resolved against the *current* cwd.

    The path memo keys on the working directory for exactly this reason: a
    scheduler or notebook that sets a relative path and chdirs per job would
    otherwise keep serving the first job's credentials for the life of the
    process, with ``show_settings()`` reporting the stale path as current.
    """
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    (first / "config.toml").write_text("concurrency = 4\n")
    (second / "config.toml").write_text("concurrency = 9\n")
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    monkeypatch.setenv(settings.CONFIG_PATH_ENV, "config.toml")

    monkeypatch.chdir(first)
    settings._reset_file_cache()
    assert settings.config_path() == first / "config.toml"
    assert settings.concurrency() == 4

    monkeypatch.chdir(second)
    assert settings.config_path() == second / "config.toml"
    assert settings.concurrency() == 9


@pytest.mark.skipif(os.name != "posix", reason="POSIX file modes")
def test_world_readable_file_with_a_key_warns(tmp_path, monkeypatch):
    path = tmp_path / "config.toml"
    path.write_text('api_key = "secret"\n')
    path.chmod(0o644)
    monkeypatch.setenv(settings.CONFIG_PATH_ENV, str(path))
    monkeypatch.delenv("API_USGS_PAT", raising=False)
    settings._reset_file_cache()
    with pytest.warns(UserWarning, match="readable by other users"):
        assert settings.api_key() == "secret"


@pytest.mark.skipif(os.name != "posix", reason="POSIX file modes")
def test_permission_change_is_checked_on_cached_file(config_file):
    path = config_file('api_key = "secret"\n')
    assert settings.api_key() == "secret"
    path.chmod(0o644)
    with pytest.warns(UserWarning, match="readable by other users"):
        assert settings.api_key() == "secret"


@pytest.mark.skipif(os.name != "posix", reason="POSIX file modes")
def test_no_permission_warning_without_a_key(tmp_path, monkeypatch, recwarn):
    path = tmp_path / "config.toml"
    path.write_text("concurrency = 4\n")
    path.chmod(0o644)
    monkeypatch.setenv(settings.CONFIG_PATH_ENV, str(path))
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    settings._reset_file_cache()
    assert settings.concurrency() == 4
    assert not [w for w in recwarn if "readable by other users" in str(w.message)]


# --- value grammar -------------------------------------------------------


def test_api_key_is_stripped_and_blank_means_none(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "  key-with-newline\n")
    assert settings.api_key() == "key-with-newline"
    monkeypatch.setenv("API_USGS_PAT", "   ")
    assert settings.api_key() is None


def test_blank_numeric_env_falls_back_to_the_default(monkeypatch):
    monkeypatch.setenv("API_USGS_CONCURRENT", "")
    monkeypatch.setenv("API_USGS_RETRIES", "")
    assert settings.concurrency() == settings.DEFAULT_CONCURRENCY
    assert settings.retries() == settings.DEFAULT_RETRIES


def test_blank_progress_env_means_off_not_unset(monkeypatch):
    """Preserved from the pre-config behavior: blank disables the line."""
    monkeypatch.setenv("API_USGS_PROGRESS", "")
    assert settings.progress() is False


@pytest.mark.parametrize("value", ["0", "false", "no", "off", "FALSE"])
def test_progress_falsey_values(monkeypatch, value):
    monkeypatch.setenv("API_USGS_PROGRESS", value)
    assert settings.progress() is False


@pytest.mark.parametrize("value", ["1", "true", "yes", "on"])
def test_progress_truthy_values(monkeypatch, value):
    monkeypatch.setenv("API_USGS_PROGRESS", value)
    assert settings.progress() is True


def test_legacy_unknown_progress_env_still_means_on(monkeypatch):
    monkeypatch.setenv("API_USGS_PROGRESS", "legacy-nonempty-value")
    assert settings.progress() is True


@pytest.mark.parametrize("value", ["nope", "-1", "0"])
def test_invalid_concurrency_raises(monkeypatch, value):
    monkeypatch.setenv("API_USGS_CONCURRENT", value)
    with pytest.raises(ValueError):  # ConfigurationError is a ValueError
        settings.concurrency()


def test_unbounded_concurrency(monkeypatch):
    monkeypatch.setenv("API_USGS_CONCURRENT", "unbounded")
    assert settings.concurrency() is None


def test_error_message_names_the_source(config_file, monkeypatch):
    monkeypatch.setenv("API_USGS_CONCURRENT", "nope")
    with pytest.raises(settings.ConfigurationError, match=r"\$?API_USGS_CONCURRENT"):
        settings.concurrency()
    monkeypatch.delenv("API_USGS_CONCURRENT")
    path = config_file('concurrency = "nope"\n')
    # ``match`` is a regex, and a Windows path is mostly escapes:
    # ``C:\\Users\\...`` makes ``\\U`` an invalid escape.
    with pytest.raises(settings.ConfigurationError, match=re.escape(str(path))):
        settings.concurrency()


# --- security ------------------------------------------------------------


def test_show_config_never_prints_the_key(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "super-secret-value")
    out = io.StringIO()
    dataretrieval.show_settings(stream=out)
    text = out.getvalue()
    assert "super-secret-value" not in text
    assert "<set>" in text
    assert "$API_USGS_PAT" in text  # provenance is still reported


def test_show_config_reports_absent_key(monkeypatch):
    monkeypatch.delenv("API_USGS_PAT", raising=False)
    out = io.StringIO()
    dataretrieval.show_settings(stream=out)
    assert "<not set>" in out.getvalue()


def test_file_sourced_key_is_still_host_scoped(config_file):
    """A key from a file gets the same host scoping as one from the env."""
    config_file('api_key = "file-key"\n')
    assert _default_headers(WATERDATA_URL)["X-Api-Key"] == "file-key"
    assert "X-Api-Key" not in _default_headers("https://example.com/data")
    assert "X-Api-Key" not in _default_headers(
        "https://api.waterdata.usgs.gov.evil.com/x"
    )


def test_block_sourced_key_is_still_host_scoped():
    with dataretrieval.configure(Settings(api_key="block-key")):
        assert _default_headers(WATERDATA_URL)["X-Api-Key"] == "block-key"
        assert "X-Api-Key" not in _default_headers("https://example.com/data")


def test_no_public_getter_accepts_a_credential_parameter():
    """Guards the ``**queryables`` catch-all.

    Every Water Data getter forwards unknown keywords as OGC query
    parameters, so a getter that grew an ``api_key`` or ``session``
    parameter could serialize a credential into a URL. Credentials must
    arrive through ``dataretrieval.configure`` instead.
    """
    import inspect

    from dataretrieval import waterdata

    offenders = []
    for name in waterdata.__all__:
        obj = getattr(waterdata, name)
        if not callable(obj) or inspect.isclass(obj):
            continue
        try:
            params = inspect.signature(obj).parameters
        except (TypeError, ValueError):  # pragma: no cover - builtins
            continue
        for forbidden in ("api_key", "session", "token", "apikey"):
            if forbidden in params:
                offenders.append(f"{name}({forbidden}=)")
    assert not offenders, (
        "public getters must not take credential parameters: " + ", ".join(offenders)
    )


@pytest.mark.parametrize("allowed", ["session", "session_id", "sampling_session"])
def test_session_is_not_treated_as_a_credential(allowed):
    """``session`` carries no secret, and the queryable namespace is the
    server's — a substring rule would make any future field containing it
    unreachable behind a credentials message that misstates the problem."""
    from dataretrieval.waterdata.utils import _flatten_queryables

    assert _flatten_queryables({"queryables": {allowed: 1}}) == {allowed: 1}


@pytest.mark.parametrize(
    "forbidden",
    ["api_key", "apikey", "apiKey", "API_KEY", "api-key", "token"],
)
def test_credential_keyword_cannot_enter_queryables(forbidden):
    from dataretrieval import waterdata

    with pytest.raises(TypeError, match=forbidden):
        waterdata.get_daily(
            monitoring_location_id="USGS-01646500", **{forbidden: "secret"}
        )


# --- wiring into the rest of the package ---------------------------------


def test_retry_policy_reads_the_block():
    from dataretrieval.transport.retry import RetryPolicy

    with dataretrieval.configure(Settings(retries=3)):
        assert RetryPolicy.from_settings().max_retries == 3


def test_parallel_chunks_baseline_comes_from_config(config_file):
    from dataretrieval.ogc.chunking import parallel_chunks

    assert settings.parallel_chunks() == 1
    config_file("parallel_chunks = 8\n")
    assert settings.parallel_chunks() == 8
    with parallel_chunks(2):  # an explicit block still wins over the file
        assert settings.parallel_chunks() == 2
    assert settings.parallel_chunks() == 8


def test_parallel_chunks_and_configure_share_one_mechanism():
    """``parallel_chunks(n)`` is sugar for a package-wide ``Settings``.

    They must not be two competing scopes: whichever block is innermost wins,
    so ``show_settings()`` always reports the value the chunker will use.
    """
    from dataretrieval.ogc.chunking import parallel_chunks

    with parallel_chunks(2):
        with dataretrieval.configure(Settings(parallel_chunks=8)):
            assert settings.parallel_chunks() == 8
        assert settings.parallel_chunks() == 2

    with dataretrieval.configure(Settings(parallel_chunks=8)):
        with parallel_chunks(2):
            assert settings.parallel_chunks() == 2
        assert settings.parallel_chunks() == 8


def test_parallel_chunks_has_no_environment_variable():
    """It spends quota, so it is deliberately file/block-only (see ENV_VARS)."""
    assert "parallel_chunks" not in settings.ENV_VARS
    assert "parallel_chunks" in settings.SETTINGS


def test_progress_reporter_reads_the_block():
    from dataretrieval.progress import ProgressReporter

    with dataretrieval.configure(Settings(progress=True)):
        assert ProgressReporter(stream=io.StringIO()).enabled
    with dataretrieval.configure(Settings(progress=False)):
        assert not ProgressReporter(stream=io.StringIO()).enabled


# --- review regressions --------------------------------------------------


def test_blank_env_does_not_mask_the_config_file(config_file, monkeypatch):
    """A blank-but-set env var must not shadow a configured file.

    Container and CI tooling routinely materializes one (``docker run -e
    API_USGS_PAT`` with nothing to pass, a workflow secret absent on a fork).
    Letting that outrank the file silently dropped the API key and sent every
    request unauthenticated.
    """
    config_file('api_key = "file-key"\nconcurrency = 4\nretries = 7\nprogress = true\n')
    for env in settings.ENV_VARS.values():
        monkeypatch.setenv(env, "")

    assert settings.api_key() == "file-key"
    assert settings.concurrency() == 4
    assert settings.retries() == 7
    # ``progress`` is the documented exception: a blank API_USGS_PROGRESS has
    # always meant "off", so for that setting blank *is* a value and outranks
    # the file. The asymmetry is declared once, in settings._BLANK_MEANS_SET.
    assert settings.progress() is False
    assert set(settings._BLANK_MEANS_SET) == {"progress"}


def test_blank_progress_env_keeps_its_legacy_meaning(monkeypatch):
    """With no file, blank keeps the environment-only meaning it always had."""
    monkeypatch.setenv("API_USGS_PROGRESS", "")
    monkeypatch.setenv("API_USGS_CONCURRENT", "")
    assert settings.progress() is False  # blank has always meant "off"
    assert settings.concurrency() == settings.DEFAULT_CONCURRENCY


def test_config_error_is_in_the_error_taxonomy():
    """A broken config surfaces from inside a getter, so it must be catchable."""
    import dataretrieval.exceptions as exceptions

    assert issubclass(settings.ConfigurationError, exceptions.DataRetrievalError)
    assert issubclass(
        settings.ConfigurationError, ValueError
    )  # legacy handlers still work
    assert settings.ConfigurationError is exceptions.ConfigurationError


def test_show_config_reports_a_broken_file_instead_of_raising(config_file):
    """The tool that explains a configuration must survive a broken one."""
    config_file("this is not = valid toml [[[\n")
    out = io.StringIO()
    dataretrieval.show_settings(stream=out)  # must not raise
    text = out.getvalue()
    assert "ERROR:" in text
    # Every setting still gets a row rather than the report dying part-way.
    for name in settings.SETTINGS:
        assert name in text


def test_show_config_reports_a_bad_value_in_its_own_row(monkeypatch):
    monkeypatch.setenv("API_USGS_CONCURRENT", "nope")
    out = io.StringIO()
    dataretrieval.show_settings(stream=out)
    text = out.getvalue()
    assert "<error:" in text
    assert "retries" in text  # unaffected settings still resolve


def test_top_level_parallel_chunks_warns(config_file):
    """It spends quota in every process, so steer it into a profile."""
    with pytest.warns(UserWarning, match="parallel_chunks"):
        config_file("parallel_chunks = 8\n")
        assert settings.parallel_chunks() == 8


def test_parallel_chunks_in_a_named_profile_does_not_warn(config_file, recwarn):
    config_file("[waterdata.bulk]\nparallel_chunks = 8\n")
    with dataretrieval.configure(WaterdataSettings.load("bulk")):
        assert settings.parallel_chunks(adapter="waterdata") == 8
    assert not [w for w in recwarn if "parallel_chunks" in str(w.message)]


@pytest.mark.skipif(os.name != "posix", reason="needs /dev/null")
def test_non_regular_config_path_is_empty_configuration(monkeypatch):
    """``DATARETRIEVAL_CONFIG=/dev/null`` is how a run declares "no config".

    A non-regular path is treated as empty *without being opened*: settings are
    re-resolved per request, so reading a stream would hand its contents to the
    first getter and nothing to the rest (and a FIFO would block on open until
    a writer appeared).
    """
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    monkeypatch.delenv("API_USGS_PAT", raising=False)
    monkeypatch.setenv(settings.CONFIG_PATH_ENV, "/dev/null")
    settings._reset_file_cache()
    assert settings.api_key() is None
    assert settings.concurrency() == settings.DEFAULT_CONCURRENCY
    # Stable across repeated resolutions, unlike a stream that drains.
    assert settings.concurrency() == settings.DEFAULT_CONCURRENCY


def test_broken_config_does_not_break_unrelated_services(config_file):
    """A Water Data config problem must not fail a legacy NWIS/WQP call.

    Config resolution can raise, and ``_default_headers`` runs for every
    service. Resolving the key only after the host check keeps the blast
    radius on the calls that would actually receive it.
    """
    config_file("this is not = valid toml [[[\n")

    # Legacy hosts never get the key, so they never touch the settings.
    assert "X-Api-Key" not in _default_headers("https://waterservices.usgs.gov/nwis/dv")
    assert "X-Api-Key" not in _default_headers("https://www.waterqualitydata.us/data")

    # The authorized host still fails loudly rather than silently going out
    # unauthenticated and hitting the anonymous rate limit.
    with pytest.raises(settings.ConfigurationError):
        _default_headers(WATERDATA_URL)


def test_default_config_path_follows_a_changed_home(tmp_path, monkeypatch):
    """The default path derives from the home variable, so the memo watches it.

    Which variable that is is platform-specific: ``ntpath.expanduser`` reads
    ``USERPROFILE`` and ignores ``HOME``, so setting ``HOME`` on Windows moves
    nothing and this asserted against the runner's real home directory.
    """
    home_var = "USERPROFILE" if os.name == "nt" else "HOME"
    monkeypatch.delenv(settings.CONFIG_PATH_ENV, raising=False)
    monkeypatch.setenv(home_var, str(tmp_path / "first"))
    settings._reset_file_cache()
    first = settings.config_path()
    assert first == tmp_path / "first" / ".dataretrieval" / "config.toml"

    monkeypatch.setenv(home_var, str(tmp_path / "second"))
    assert (
        settings.config_path() == tmp_path / "second" / ".dataretrieval" / "config.toml"
    )


def test_show_config_renderers_cover_every_setting():
    """Guarded with a raise, not an assert, so ``python -O`` keeps the check."""
    assert set(settings._DISPLAYS) == set(settings._ALL_SETTINGS)


def test_unselected_profile_is_not_validated(config_file):
    """A bad value in a profile nobody selected must not fail every request.

    Profile tables are kept raw at parse time and validated only when one is
    actually selected -- the same blast-radius rule ``_default_headers``
    follows for the key itself.
    """
    config_file('api_key = "good"\n\n[waterdata.experimental]\nconcurrency = 0\n')
    assert _default_headers(WATERDATA_URL)["X-Api-Key"] == "good"
    assert settings.concurrency(adapter="waterdata") == (settings.DEFAULT_CONCURRENCY)

    # Selecting it still reports the problem.
    with pytest.raises(settings.ConfigurationError, match="experimental"):
        WaterdataSettings.load("experimental")


def test_unknown_setting_in_an_unselected_profile_is_silent(config_file, recwarn):
    config_file("concurrency = 4\n\n[waterdata.other]\nnot_a_setting = 1\n")
    assert settings.concurrency(adapter="waterdata") == 4
    assert not [w for w in recwarn if "unknown setting" in str(w.message)]


def test_a_malformed_table_does_not_fail_another_adapters_call(config_file):
    """The blast-radius rule, on the tier a whole adapter table sits in.

    Keys are checked when *that* adapter first resolves a setting, so a bad
    value in ``[nldi]`` costs a Water Data call nothing -- which is also what
    lets an adapter's vocabulary live in a module this leaf cannot import.
    """
    config_file(
        'api_key = "good"\n\n[nldi]\nretries = -1\n\n[waterdata]\nretries = 2\n'
    )

    assert settings.retries(adapter="waterdata") == 2
    assert _default_headers(WATERDATA_URL)["X-Api-Key"] == "good"

    # The adapter that owns the table still gets the error, naming the table.
    with pytest.raises(settings.ConfigurationError, match=r"\[nldi\]"):
        settings.retries(adapter="nldi")


def test_a_table_for_an_unimported_adapter_stays_valid(config_file, monkeypatch):
    """A file must not be conditionally valid by which extras are installed.

    NLDI is imported on demand for the geopandas extra, so with the roster
    derived from imports a ``[nldi]`` table would be a typo until something
    happened to import that module. The roster is a plain name tuple instead,
    and an adapter that has registered no class has its keys checked against
    the package-wide settings.
    """
    monkeypatch.delitem(settings._REGISTRY, "nldi", raising=False)
    config_file("retries = 5\n\n[nldi]\nretries = 9\n\n[nldi.gentle]\nretries = 1\n")

    assert settings.settings_for("nldi") is None  # not an error: unknown yet
    assert settings.retries(adapter="nldi") == 9  # its default profile applies
    assert settings.retries(adapter="waterdata") == 5  # and narrows to nldi
    # The named profile under it is as inert as any other.
    assert settings.retries() == 5


def test_show_config_does_not_promise_a_built_in_default_holds_everywhere(
    capsys, monkeypatch
):
    """A row reading "built-in default" is package-wide, not a per-service claim.

    ``concurrency`` resolves to 32 with nothing configured, but a Water Use call
    uses that service's own preference of 4. The report is the tool for "what
    will this actually use", so it must not let the reader take a package-wide
    row as an answer for every service.
    """
    from dataretrieval import settings
    from dataretrieval.nwdc import DEFAULT_CONCURRENT_REQUESTS

    # The suite pins API_USGS_CONCURRENT so dispatch is deterministic; clear it
    # so the two kinds of default are what actually differ here.
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    assert settings.concurrency() != settings.concurrency(DEFAULT_CONCURRENT_REQUESTS)

    dataretrieval.show_settings()
    out = capsys.readouterr().out
    assert "built-in default" in out
    assert "An adapter may prefer its own" in out


# --- adapter-scoped settings (ADR 0010) ----------------------------------


def test_adapter_table_overrides_the_top_level_per_setting(config_file):
    """A ``[ngwmn]`` table narrows one adapter, leaving the rest inherited."""
    config_file("concurrency = 16\nretries = 3\n\n[ngwmn]\nconcurrency = 4\n")

    # The adapter that asked for it gets it...
    assert settings.concurrency(adapter="ngwmn") == 4
    # ...its sibling on the same host does not...
    assert settings.concurrency(adapter="waterdata") == 16
    # ...and the package-wide read is untouched.
    assert settings.concurrency() == 16
    # Per setting, not per table: retries still comes from the top level.
    assert settings.retries(adapter="ngwmn") == 3


def test_one_block_configures_several_adapters(config_file):
    """The requirement ADR 0009 deferred: gentle here, unchanged there."""
    config_file("")

    with dataretrieval.configure(
        Settings(retries=7),
        NgwmnSettings(concurrency=2),
        NwdcSettings(concurrency=8),
    ):
        assert settings.concurrency(adapter="ngwmn") == 2
        assert settings.concurrency(adapter="nwdc") == 8
        assert settings.concurrency(adapter="waterdata") == settings.DEFAULT_CONCURRENCY
        # A package-wide value in the same block still reaches every adapter.
        assert settings.retries(adapter="ngwmn") == 7


def test_environment_outranks_an_adapter_table(config_file, monkeypatch):
    """Precedence is source-major: the env tier is above the file tier.

    Scope-major ordering would invert this the moment anyone added an adapter
    table, so a variable exported for one run would lose to a stale file entry.
    """
    config_file("[ngwmn]\nconcurrency = 4\n")
    monkeypatch.setenv("API_USGS_CONCURRENT", "7")

    assert settings.concurrency(adapter="ngwmn") == 7


def test_adapter_block_outranks_the_package_wide_block(config_file):
    """Within one source, the adapter-scoped value is the more specific one."""
    config_file("")

    with dataretrieval.configure(
        Settings(concurrency=16), NgwmnSettings(concurrency=2)
    ):
        assert settings.concurrency(adapter="ngwmn") == 2
        assert settings.concurrency(adapter="waterdata") == 16


def test_adapter_rejects_a_setting_it_does_not_read(config_file):
    """A single-shot adapter has nothing to fan out, so ``concurrency`` is a typo.

    From code the refusal comes from ``extra="forbid"``: the setting is not a
    field of ``WqpSettings``, so there is nowhere to put it. That is the same
    refusal a type checker makes before the code runs.

    It is a ``ConfigurationError`` rather than the ``TypeError`` a dataclass
    raised before pydantic-settings (ADR 0012). The same mistake written into
    the file has always raised ``ConfigurationError``, so the two surfaces now
    agree, and the message lists the settings the adapter *does* read instead of
    only naming the one it does not.
    """
    with pytest.raises(settings.ConfigurationError, match="concurrency"):
        WqpSettings(concurrency=2)

    config_file("[wqp]\nconcurrency = 2\n")
    with pytest.raises(settings.ConfigurationError, match="not a setting that table"):
        settings.retries(adapter="wqp")


def test_api_key_is_never_adapter_scoped():
    """The key belongs to the gateway fronting a host, not to an adapter.

    Water Data and NGWMN are two adapters on one host sharing one key and one
    quota pool, so a per-adapter key would model a distinction that does not
    exist (ADR 0010).
    """
    for adapter in settings.ADAPTERS:
        accepted = settings.settings_for(adapter)
        assert accepted is not None or adapter == "nldi"
        assert accepted is None or "api_key" not in accepted

    with pytest.raises(settings.ConfigurationError, match="api_key"):
        NgwmnSettings(api_key="x")


def test_a_misspelled_setting_is_not_silently_swallowed():
    """A typo must fail, not be accepted and ignored.

    ``Settings(concurrancy=8)`` is not a field, so ``extra="forbid"`` refuses it
    by name -- the worst outcome for a module whose job is to be trustworthy
    about what a call will use would be to take it and drop it.
    """
    with pytest.raises(settings.ConfigurationError, match="concurrancy"):
        Settings(concurrancy=8)


def test_adapter_roster_names_real_modules_that_register_themselves():
    """Every name in the roster resolves to an adapter that owns a schema.

    Two halves of one declaration: the roster is what parsing a file needs
    (is ``[ngwmn]`` a table or a typo?), and the class is what validating that
    table's keys needs. A name in one and not the other is a configuration
    nothing could reach.
    """
    import importlib

    for adapter in settings.ADAPTERS:
        importlib.import_module(f"dataretrieval.{adapter}")
        accepted = settings.settings_for(adapter)
        assert accepted is not None, f"{adapter} registered no configuration class"
        assert accepted >= {"retries", "stall_timeout", "base_url"}


def test_registering_an_adapter_outside_the_roster_raises():
    """The roster is the authority, so a class cannot invent an adapter."""

    @dataclass(frozen=True)
    class BogusSettings(settings.AdapterSettings):
        adapter: ClassVar[str] = "not-an-adapter"

    with pytest.raises(settings.ConfigurationError, match="not one of"):
        settings._register(BogusSettings)


def test_settings_for_an_unimported_adapter_is_not_an_error(monkeypatch):
    """``None`` means "cannot validate these keys yet", never "invalid".

    NLDI is imported on demand for the geopandas extra, so a roster built from
    imports would reject a perfectly good ``[nldi]`` table until something
    happened to import that module.
    """
    monkeypatch.delitem(settings._REGISTRY, "nldi", raising=False)
    assert settings.settings_for("nldi") is None
    assert "nldi" in settings.ADAPTERS


def test_every_adapter_is_actually_wired_to_a_read_site():
    """A schema nothing passes is worse than no schema.

    ``show_settings()`` would report a ``[nwis]`` override as live while
    every call ignored it -- the report whose whole job is answering "what will
    this call use" being confidently wrong. Importability is the weaker half of
    the invariant: it passed while ``waterdata.get_cql``, eight of nine WQP
    getters, and all of ``nwis`` silently resolved package-wide.
    """
    import pathlib

    source = "\n".join(
        p.read_text(encoding="utf-8")
        for p in pathlib.Path(settings.__file__).parent.rglob("*.py")
        if p.name != "settings.py"
    )
    missing = [a for a in settings.ADAPTERS if f'adapter="{a}"' not in source]
    assert not missing, (
        f"adapters with a schema but no read site: {missing}. Either pass "
        'adapter="<name>" where that adapter builds its policy or fan-out, or '
        "drop it from settings.ADAPTERS."
    )


def test_a_misspelled_adapter_at_a_read_site_raises():
    """The other half of the invariant above, which a grep cannot check.

    ``adapter="waterdatas"`` used to resolve *silently* package-wide: no table
    matches the typo, every setting is accepted because nothing knows the
    schema, and a ``[waterdata]`` table or a ``WaterdataSettings`` is then
    ignored with nothing raised anywhere. The grep only sees that the correctly
    spelled string occurs somewhere; it cannot see a second, wrong one.
    """
    with pytest.raises(settings.ConfigurationError, match="not a configurable"):
        settings.retries(adapter="waterdatas")

    # Every read site funnels through one resolver, so the check reaches them
    # all -- including the accessors that would otherwise return a default.
    with pytest.raises(settings.ConfigurationError, match="not a configurable"):
        settings.base_url(adapter="nwis", default="https://example.invalid")


def test_a_non_finite_stall_timeout_is_refused():
    """``inf`` parses as a float and silently disables the bound it sets."""
    for bad in (float("inf"), float("nan")):
        with pytest.raises(settings.ConfigurationError, match="finite"):
            Settings(stall_timeout=bad)


def test_stall_timeout_resolves_through_the_chain(config_file, monkeypatch):
    """It was read straight from os.environ, so a block and the file were mute."""
    config_file("stall_timeout = 15\n\n[wqp]\nstall_timeout = 300\n")

    assert settings.stall_timeout() == 15
    assert settings.stall_timeout(adapter="wqp") == 300

    monkeypatch.setenv("API_USGS_STALL_TIMEOUT", "42")
    assert settings.stall_timeout() == 42

    with dataretrieval.configure(Settings(stall_timeout=2.5)):
        assert settings.stall_timeout() == 2.5


def test_base_url_applies_from_code_and_is_refused_from_the_file(config_file):
    """A redirect belongs where a reader of the script sees it (ADR 0011).

    A configuration file that silently sent a data-retrieval library to another
    host would be a supply-chain-shaped hazard, so the file refuses the setting
    outright rather than accepting it and being trusted.
    """
    config_file("")

    with dataretrieval.configure(
        WaterdataSettings(base_url="https://mirror.example/ogcapi")
    ):
        assert settings.base_url(adapter="waterdata") == (
            "https://mirror.example/ogcapi"
        )
        # It names one service, so it never reaches another.
        assert settings.base_url(adapter="ngwmn") is None
    assert settings.base_url(adapter="waterdata") is None

    for text in (
        'base_url = "https://evil.example"\n',
        "[waterdata]\nbase_url = 'x'\n",
    ):
        config_file(text)
        with pytest.raises(settings.ConfigurationError, match="only be set in code"):
            settings.base_url(adapter="waterdata")


def test_base_url_must_be_an_absolute_http_url():
    """A bare host would fail far from here, inside the request builder."""
    with pytest.raises(settings.ConfigurationError, match="absolute"):
        WaterdataSettings(base_url="mirror.example")
    with pytest.raises(settings.ConfigurationError, match="absolute"):
        WaterdataSettings(base_url="file:///etc/passwd")


def test_base_url_is_refused_from_the_environment(monkeypatch):
    """The environment is refused out loud, not merely unread.

    ``API_USGS_BASE_URL`` is the spelling every other setting's variable
    predicts, so a caller who exports it believes they have redirected
    something. Leaving it out of ``ENV_VARS`` would make that belief wrong and
    silent; the error names the block to write instead.
    """
    monkeypatch.setenv("API_USGS_BASE_URL", "https://evil.example")

    with pytest.raises(settings.ConfigurationError, match="only be set in code"):
        settings.base_url(adapter="waterdata")

    # Refused even under a block that sets one, matching the file: the variable
    # cannot work, and being quietly outranked is how it survives to a run where
    # nothing outranks it. Unsetting it is the only fix.
    with dataretrieval.configure(WaterdataSettings(base_url=_MIRROR)):
        with pytest.raises(settings.ConfigurationError, match="only be set in code"):
            settings.base_url(adapter="waterdata")

    # A configuration in this state is exactly what show_settings() exists
    # to explain, so it reports the failure rather than raising out of it.
    out = io.StringIO()
    dataretrieval.show_settings(stream=out)
    assert "only be set in code" in out.getvalue()


def test_a_water_data_redirect_moves_every_endpoint_family():
    """Water Data is one adapter serving four APIs, so all four move together.

    A redirect that reached the OGC collections but left samples, statistics,
    and ratings on the service's own host would send most of a caller's traffic
    to the host they were redirecting away from -- the one mistake a redirect
    must not make.
    """
    with dataretrieval.configure(WaterdataSettings(base_url=_MIRROR)):
        moved = {
            name: endpoints.redirected(getattr(endpoints, name))
            for name in ("OGC_API_URL", "SAMPLES_URL", "STATISTICS_API_URL", "STAC_URL")
        }

    assert moved == {
        "OGC_API_URL": f"{_MIRROR}/ogcapi/v0",
        "SAMPLES_URL": f"{_MIRROR}/samples-data",
        "STATISTICS_API_URL": f"{_MIRROR}/statistics/v0",
        "STAC_URL": f"{_MIRROR}/stac/v0",
    }
    # Outside the block the constants are the service's own again.
    assert endpoints.redirected(endpoints.OGC_API_URL) == endpoints.OGC_API_URL

    # The swap is a prefix swap, so an endpoint declared on some other root
    # would be rewritten into nonsense rather than moved. Derived from the
    # module's exports so a fifth family added later is covered the day it
    # lands, which the four literals above cannot be.
    declared = [n for n in endpoints.__all__ if n.endswith("_URL") and n != "BASE_URL"]
    assert declared and all(
        getattr(endpoints, name).startswith(endpoints.BASE_URL) for name in declared
    )


def test_every_water_data_endpoint_use_goes_through_redirected():
    """``redirected()`` is a wrap-at-every-use-site seam, so check every site.

    Water Data is the one adapter that cannot resolve its base at a single
    choke point: four families hang off one root, each building its own URL
    from a constant. The test above proves the constants all derive from
    ``BASE_URL``; this one proves the *use sites* actually pass them through
    the wrapper. Without it a new family module -- or a second use of an
    existing constant -- would send traffic to api.waterdata.usgs.gov from
    inside a block a caller opened precisely to avoid it, with nothing failing:
    what ``redirected()``'s own docstring calls the one mistake a redirect must
    not make.

    Written as an AST scan for the same reason as
    ``test_every_adapter_is_actually_wired_to_a_read_site``: the invariant is a
    fact about the source, and nothing at runtime can observe a use site that
    was simply never written.
    """
    import ast
    import pathlib

    wrapped = {n for n in endpoints.__all__ if n.endswith("_URL")}
    package = pathlib.Path(endpoints.__file__).parent

    bare: list[str] = []
    for path in sorted(package.rglob("*.py")):
        if path.name == "endpoints.py":
            continue  # where the constants are declared and the wrapper lives
        tree = ast.parse(path.read_text(encoding="utf-8"))
        # Every ``redirected(X)`` argument is a legitimate use; anything else
        # naming a constant is not. Collected first so the walk below can tell
        # the two apart by node identity rather than by position.
        allowed = {
            node.args[0]
            for node in ast.walk(tree)
            if isinstance(node, ast.Call)
            and isinstance(node.func, ast.Name)
            and node.func.id == "redirected"
            and node.args
        }
        for node in ast.walk(tree):
            # Loads only: the ``from ... import`` that binds the name and the
            # ``__all__`` re-export (a string, not a Name) are not use sites.
            if (
                isinstance(node, ast.Name)
                and isinstance(node.ctx, ast.Load)
                and node.id in wrapped
                and node not in allowed
            ):
                bare.append(f"{path.name}:{node.lineno}: {node.id}")

    assert not bare, (
        "Water Data endpoint constants used without redirected(): "
        f"{bare}. Wrap each one -- redirected(OGC_API_URL) -- or the request "
        "ignores WaterdataSettings(base_url=...)."
    )


def test_a_code_base_url_redirects_the_adapters_requests(httpx_mock):
    """The setting has to move real traffic, not just resolve to a string.

    Two adapters with unrelated request machinery -- the OGC engine and a plain
    one-shot GET -- because "the configuration reaches the request" is a claim
    about each adapter's wiring, and one of them passing says nothing about the
    other.
    """
    httpx_mock.add_response(method=None, url=_MIRROR_RE, json=_DAILY_PAGE)
    httpx_mock.add_response(method=None, url=_WATERDATA_RE, json=_DAILY_PAGE)

    with dataretrieval.configure(WaterdataSettings(base_url=_MIRROR)):
        waterdata.get_daily(monitoring_location_id="USGS-05427718")
    redirected_url = str(httpx_mock.get_requests()[-1].url)

    # Nothing configured: back to the service's own base, so the redirect is
    # scoped to the block rather than latched somewhere at import.
    waterdata.get_daily(monitoring_location_id="USGS-05427718")
    direct_url = str(httpx_mock.get_requests()[-1].url)

    assert redirected_url.startswith(f"{_MIRROR}/ogcapi/v0/collections/daily/items")
    assert direct_url.startswith(f"{endpoints.OGC_API_URL}/collections/daily/items")

    streamstats_mirror = "https://mirror.example/streamstats"
    with dataretrieval.configure(StreamstatsSettings(base_url=streamstats_mirror)):
        streamstats.download_workspace("workspace-id")
    assert str(httpx_mock.get_requests()[-1].url).startswith(
        f"{streamstats_mirror}/download"
    )


def test_a_redirected_adapter_is_not_sent_the_api_key(httpx_mock):
    """The key is scoped to the host that honors it, and a mirror is not it.

    ``credentials.accepts_api_key`` is checked where the header is attached, so
    a redirect needs no second rule to be safe -- but "needs no rule" is exactly
    the kind of claim that stops being true silently, and the cost of it being
    wrong is a credential handed to whatever host the block named.
    """
    httpx_mock.add_response(method=None, url=_MIRROR_RE, json=_DAILY_PAGE)
    httpx_mock.add_response(method=None, url=_WATERDATA_RE, json=_DAILY_PAGE)

    with dataretrieval.configure(Settings(api_key="secret")):
        with dataretrieval.configure(WaterdataSettings(base_url=_MIRROR)):
            waterdata.get_daily(monitoring_location_id="USGS-05427718")
        redirected_request = httpx_mock.get_requests()[-1]

        # The same key, the same call, the service's own host: the control that
        # keeps this test from passing because no key was configured at all.
        waterdata.get_daily(monitoring_location_id="USGS-05427718")
        direct_request = httpx_mock.get_requests()[-1]

    assert "X-Api-Key" not in redirected_request.headers
    assert direct_request.headers["X-Api-Key"] == "secret"


def test_the_validate_hook_can_refuse_a_combination():
    """Per-setting grammar is shared with the file; this is for the rest.

    The hook is ``validate_settings`` rather than ``validate``: pydantic's
    ``BaseModel`` already owns the latter name.
    """

    class Fussy(settings.AdapterSettings):
        adapter: ClassVar[str] = "waterdata"

        concurrency: int | str | None = None
        parallel_chunks: int | None = None

        def validate_settings(self) -> None:
            supplied = self.values()
            if supplied.get("parallel_chunks", 1) > supplied.get("concurrency", 1):
                raise settings.ConfigurationError(
                    "parallel_chunks above concurrency only queues sub-requests."
                )

    assert Fussy(concurrency=8, parallel_chunks=4).settings() == {
        "concurrency",
        "parallel_chunks",
    }
    with pytest.raises(settings.ConfigurationError, match="only queues"):
        Fussy(concurrency=2, parallel_chunks=8)


def test_show_settings_lists_only_real_overrides(config_file):
    """A full adapter-by-setting grid would bury the answer in inherited rows."""
    config_file("concurrency = 16\n\n[ngwmn]\nconcurrency = 4\n")
    out = io.StringIO()

    dataretrieval.show_settings(stream=out)
    text = out.getvalue()

    assert "adapter overrides" in text
    assert "ngwmn" in text
    # waterdata inherits every setting, so it must not appear as an override.
    override_section = text.split("adapter overrides", 1)[1]
    assert "waterdata" not in override_section
    assert "streamstats" not in override_section


def test_inner_block_can_lower_a_setting_an_outer_block_scoped(config_file):
    """The innermost block wins across *both* scopes, not just within one.

    An adapter-scoped value is the more specific of two written by the same
    block. It must not outrank one written by a block nested *inside* it, or
    the documented recovery from QuotaExhausted -- wait, then re-issue more
    gently -- cannot be expressed once any adapter table is in play.
    """
    config_file("")

    with dataretrieval.configure(WaterdataSettings(concurrency=32)):
        with dataretrieval.configure(Settings(concurrency=1)):
            assert settings.concurrency(adapter="waterdata") == 1
        assert settings.concurrency(adapter="waterdata") == 32


def test_adapter_scope_still_wins_within_one_block(config_file):
    """Depth breaks ties between blocks, never within one."""
    config_file("")

    with dataretrieval.configure(
        Settings(concurrency=16), WaterdataSettings(concurrency=4)
    ):
        assert settings.concurrency(adapter="waterdata") == 4
        assert settings.concurrency(adapter="wqp") == 16


def test_parallel_chunks_block_survives_an_adapter_scoped_outer_block():
    """``parallel_chunks(n)`` is a per-call request and must not be discarded.

    It delegates to a package-wide ``Settings``, so it writes the
    package-wide key -- and before blocks were kept as separate frames, any
    enclosing ``WaterdataSettings(parallel_chunks=...)`` outranked it.
    """
    from dataretrieval.waterdata import parallel_chunks

    with dataretrieval.configure(WaterdataSettings(parallel_chunks=2)):
        with parallel_chunks(16):
            assert settings.parallel_chunks(adapter="waterdata") == 16
        assert settings.parallel_chunks(adapter="waterdata") == 2


# --- the precedence ladder (ADR 0011) ------------------------------------
#
# ADR 0011 states the ladder in seven rungs, highest first:
#
#   1  a configuration instance passed to configure()
#   2  a profile selected in code, <Adapter>Settings.load("<name>")
#   3  the setting's environment variable
#   4  the adapter's default profile in the file, [<adapter>]
#   5  the package-wide keys at the top of the file
#   6  the adapter's built-in preference, passed by the adapter's own read site
#   7  the package built-in default
#
# The tests below walk it as a *chain*: each one knocks the rung above out and
# asserts the next takes over. Seven independent single-rung assertions would
# all still pass if two rungs collapsed into one, which is the mistake worth
# catching -- rungs 2 and 3 are the pair a refactor is most likely to fuse,
# since 2 above 3 is the one place ADR 0011 inverts ADR 0009.
#
# ``nwdc`` and ``concurrency`` are the pair that can express all seven. NWDC is
# the adapter that ships a built-in preference of its own -- 4 concurrent
# requests, because the service is only stress-tested that far -- distinct from
# the package default of 32, and that difference is the only way rungs 6 and 7
# can be told apart at all.

#: Rungs 5, 4 and 2, with a distinct value per rung so a resolved number
#: identifies the table it came from. Top-level keys are written first because
#: TOML assigns a bare key to whichever table header precedes it: moved below
#: ``[nwdc]``, ``concurrency = 15`` would quietly stop being a rung-5 key and
#: become a second rung-4 one, and the tests would still pass by coincidence.
_LADDER_FILE = (
    "concurrency = 15\n"  # rung 5: the package-wide keys
    "retries = 7\n"  # rung 5 again, for a setting no profile names
    "\n[nwdc]\n"
    "concurrency = 14\n"  # rung 4: the adapter's default profile
    "\n[nwdc.tuned]\n"
    "concurrency = 12\n"  # rung 2: inert until load() selects it
)

#: Rung 3, which is not in the file.
_LADDER_ENV = 13

#: Rung 1, which is not in the file either.
_LADDER_INSTANCE = 11


def _nwdc_concurrency() -> int | None:
    """Resolve ``concurrency`` the way NWDC's own fan-out does.

    Through the adapter's read site rather than a bare ``concurrency()``, so
    the built-in preference at rung 6 is really in the chain and the ladder is
    exercised as the adapter experiences it.
    """
    return settings.concurrency(DEFAULT_CONCURRENT_REQUESTS, adapter="nwdc")


def test_a_configuration_instance_tops_the_ladder(config_file, monkeypatch):
    """Rung 1 over every other rung, all six of them present at once."""
    config_file(_LADDER_FILE)
    monkeypatch.setenv("API_USGS_CONCURRENT", str(_LADDER_ENV))

    with dataretrieval.configure(NwdcSettings(concurrency=_LADDER_INSTANCE)):
        assert _nwdc_concurrency() == _LADDER_INSTANCE


def test_a_loaded_profile_beats_the_environment(config_file, monkeypatch):
    """Rung 2 over rung 3 -- the one inversion ADR 0011 exists to make.

    ADR 0009 put the environment above the file, and a named profile lives in
    the file, so the naive reading is that ``API_USGS_CONCURRENT`` in the shell
    wins. It does not: what reaches the chain is the caller *naming* the
    profile in code, which is a more deliberate act than a variable inherited
    from whatever started the process, and losing to that variable is the
    behaviour a caller would file a bug about.

    The inversion is also bounded, which the second half asserts: it covers
    what the profile names and nothing else, so ``retries`` -- which the file
    sets at the top level and no selected profile mentions -- still follows the
    original environment-above-file rule inside the very same block.
    """
    config_file(_LADDER_FILE)
    monkeypatch.setenv("API_USGS_CONCURRENT", str(_LADDER_ENV))
    monkeypatch.setenv("API_USGS_RETRIES", "9")

    assert _nwdc_concurrency() == _LADDER_ENV  # rung 3, until a profile is selected
    with dataretrieval.configure(NwdcSettings.load("tuned")):
        assert _nwdc_concurrency() == 12  # rung 2 wins for the key it names...
        assert settings.retries(adapter="nwdc") == 9  # ...and only that key
    assert _nwdc_concurrency() == _LADDER_ENV  # and the shell has it back on exit


def test_the_environment_beats_the_adapters_default_profile(config_file, monkeypatch):
    """Rung 3 over rung 4: the file's always-on table is still just the file."""
    config_file(_LADDER_FILE)
    monkeypatch.setenv("API_USGS_CONCURRENT", str(_LADDER_ENV))

    assert _nwdc_concurrency() == _LADDER_ENV
    monkeypatch.delenv("API_USGS_CONCURRENT")
    assert _nwdc_concurrency() == 14


def test_the_adapters_default_profile_beats_the_package_wide_keys(config_file):
    """Rung 4 over rung 5: within the file, the narrower table decides."""
    config_file(_LADDER_FILE)

    assert _nwdc_concurrency() == 14
    config_file("concurrency = 15\n")  # the [nwdc] table gone
    assert _nwdc_concurrency() == 15


def test_the_package_wide_keys_beat_the_adapters_built_in_preference(config_file):
    """Rung 5 over rung 6: a user-written value outranks an adapter's taste.

    The adapter's preference is a default, not a cap. One able to override a
    setting the user actually wrote would make that setting a lie -- so a
    top-level key the user never scoped to NWDC still reaches NWDC's calls.
    """
    config_file("concurrency = 15\n")

    assert _nwdc_concurrency() == 15
    config_file("")
    assert _nwdc_concurrency() == DEFAULT_CONCURRENT_REQUESTS


def test_the_adapters_built_in_preference_beats_the_package_built_in_default(
    config_file,
):
    """Rung 6 over rung 7, and only for the adapter that stated a preference."""
    config_file("")

    assert _nwdc_concurrency() == DEFAULT_CONCURRENT_REQUESTS
    assert DEFAULT_CONCURRENT_REQUESTS != settings.DEFAULT_CONCURRENCY
    # It is the read site's own figure, not a property of the adapter, so a
    # caller that states no preference lands on the package default instead --
    # which is what makes rungs 6 and 7 two rungs rather than one.
    assert settings.concurrency(adapter="nwdc") == settings.DEFAULT_CONCURRENCY


def test_the_package_built_in_default_is_the_floor(config_file):
    """Rung 7: with the six rungs above it empty, every setting still resolves.

    The floor is what makes the whole chain optional -- a caller who has
    configured nothing at all gets working values rather than an error.
    """
    config_file("")

    for adapter in (None, *settings.ADAPTERS):
        assert settings.concurrency(adapter=adapter) == (settings.DEFAULT_CONCURRENCY)
        assert settings.retries(adapter=adapter) == settings.DEFAULT_RETRIES
        assert settings.parallel_chunks(adapter=adapter) == (
            settings.DEFAULT_PARALLEL_CHUNKS
        )
        assert settings.stall_timeout(adapter=adapter) == (
            settings.DEFAULT_STALL_TIMEOUT
        )


def test_the_top_two_rungs_cannot_tie(config_file):
    """Rungs 1 and 2 both target one adapter, so no block can hold both.

    That is what stops the ladder needing a tie-break nobody could remember:
    the same-adapter rule refuses the pairing where the order would matter,
    and between *nested* blocks the ordinary rule applies -- the innermost
    decides, whichever kind of configuration it holds.
    """
    config_file(_LADDER_FILE)

    with pytest.raises(settings.ConfigurationError, match="two settings profiles"):
        with dataretrieval.configure(
            NwdcSettings(concurrency=_LADDER_INSTANCE),
            NwdcSettings.load("tuned"),
        ):
            pass

    with dataretrieval.configure(NwdcSettings(concurrency=_LADDER_INSTANCE)):
        with dataretrieval.configure(NwdcSettings.load("tuned")):
            assert _nwdc_concurrency() == 12
    with dataretrieval.configure(NwdcSettings.load("tuned")):
        with dataretrieval.configure(NwdcSettings(concurrency=_LADDER_INSTANCE)):
            assert _nwdc_concurrency() == _LADDER_INSTANCE

    # "Rung 1 above rung 2" is a claim about one adapter, so a *package-wide*
    # instance is not the thing it is talking about: it targets no adapter at
    # all. Alongside a loaded profile in one block the adapter-scoped value is
    # the more specific of the two and wins for that adapter (ADR 0010), while
    # the package-wide value still governs every other adapter.
    with dataretrieval.configure(
        Settings(concurrency=_LADDER_INSTANCE), NwdcSettings.load("tuned")
    ):
        assert _nwdc_concurrency() == 12
        assert settings.concurrency(adapter="wqp") == _LADDER_INSTANCE


def test_load_returns_an_instance_carrying_only_the_profiles_keys(config_file):
    """``load`` is a constructor: it reads one table and returns the class.

    Only what the table names is carried, so every other setting stays unset
    and keeps inheriting from the rungs below rather than being pinned to a
    default the profile never asked for. That is what makes a profile a
    *contribution* to the chain rather than a replacement for it.
    """
    config_file(
        "concurrency = 16\n\n"
        "[waterdata]\nretries = 2\n\n"
        '[waterdata.bulk]\nconcurrency = "unbounded"\nparallel_chunks = 8\n'
    )

    loaded = WaterdataSettings.load("bulk")

    assert isinstance(loaded, WaterdataSettings)
    assert loaded.values() == {"concurrency": "unbounded", "parallel_chunks": 8}
    # ``retries`` was not in the profile, so it stays unset and inherits the
    # ``[waterdata]`` table below. ``model_fields_set`` is what carries that
    # distinction now, so the field itself reads as ``None``.
    assert "retries" not in loaded.model_fields_set
    assert loaded.retries is None


# --- show_settings() reports profiles --------------------------------
#
# The report exists to answer "why is this call using that value?", so every
# row names the source that supplied it. A value from a profile is the case a
# bare "configure() block" answers badly: a configuration written in code and
# one loaded from a table reach the chain by the same route, and only the
# latter has a name in a file the caller can go and read.

#: The file the documented sample is generated from. Exercises every section:
#: package-wide keys, an adapter's default profile, and a named profile.
_SAMPLE_FILE = (
    'api_key = "0123456789abcdef"\n'
    "concurrency = 16\n"
    "\n[ngwmn]\n"
    "concurrency = 4\n"
    "\n[waterdata.bulk]\n"
    "parallel_chunks = 8\n"
)

#: The illustrative path the samples print, standing in for the temporary file
#: the test actually writes. Substituting it is the *only* edit made to the
#: captured output -- everything else has to match what the function printed.
_SAMPLE_PATH = "/home/u/.dataretrieval/config.toml"

#: The two lines above the captured output in both samples.
_SAMPLE_PROMPT = (
    '>>> with dataretrieval.configure(WaterdataSettings.load("bulk")):\n'
    "...     dataretrieval.show_settings()"
)


def _documented_sample() -> str:
    """The sample output embedded in ``show_settings``'s docstring."""
    doc = inspect.getdoc(dataretrieval.show_settings) or ""
    _, _, block = doc.partition(".. code-block:: text\n\n")
    return textwrap.dedent(block).strip("\n")


def test_show_settings_names_the_profile_a_value_came_from(config_file):
    """A value from a profile is reported with that profile, not with "a block".

    ``WaterdataSettings.load("bulk")`` and ``WaterdataSettings(...)``
    enter the chain by the same route and are indistinguishable once their
    values are in the block, so a report that said only ``configure() block``
    left a caller who selected the wrong profile -- or who had forgotten a
    profile was selected at all -- with nothing to look at. The label is the
    table's own spelling, so it is greppable in the file that defines it.
    """
    config_file("[waterdata.bulk]\nconcurrency = 6\n")
    out = io.StringIO()

    with dataretrieval.configure(WaterdataSettings.load("bulk")):
        dataretrieval.show_settings(stream=out)

    assert "configure() block [waterdata.bulk]" in out.getvalue()

    # A configuration written in code has no profile to name, so it names its
    # adapter alone rather than inventing one -- and the package-wide one
    # narrows to nothing, so it names neither.
    out = io.StringIO()
    with dataretrieval.configure(WaterdataSettings(concurrency=6), Settings(retries=3)):
        dataretrieval.show_settings(stream=out)
    text = out.getvalue()

    assert "configure() block [waterdata]" in text
    # The file still *defines* the profile, so it is still listed as available;
    # what must not happen is a value being attributed to it.
    assert "configure() block [waterdata.bulk]" not in text
    retries_row = next(line for line in text.splitlines() if line.startswith("retries"))
    assert retries_row.endswith("configure() block")


def test_a_loaded_profile_remembers_its_name_without_becoming_a_setting(config_file):
    """The profile name is provenance, so it is not a field and not a value.

    Keeping it off the fields is what stops it reaching :meth:`settings`, the
    ``configure()`` frame, and equality: two configurations carrying the same
    settings stay interchangeable however each was spelled, which is what
    makes a configuration a value rather than a record of how it was built.
    """
    config_file("[waterdata.bulk]\nconcurrency = 6\n")

    loaded = WaterdataSettings.load("bulk")
    written = WaterdataSettings(concurrency=6)

    assert loaded.profile == "bulk"
    assert written.profile is None
    assert "profile" not in loaded.settings()
    assert loaded == written


def test_show_settings_lists_the_profiles_the_file_defines(config_file, monkeypatch):
    """A named profile is inert until selected, so the file's are listed too.

    "I added ``[waterdata.bulk]`` and nothing changed" is the confusion this
    section exists for: the profiles are there, and no row above names one
    because no caller selected one. A report that mentioned a profile only
    once it had been selected would leave that silence unexplained.

    Names are read from the parsed file, so an adapter this process never
    imported still has its profiles listed: what a table *means* needs the
    import, what it is called does not, and hiding it would make the section
    depend on which optional extras happened to be installed.
    """
    monkeypatch.delitem(settings._REGISTRY, "nldi", raising=False)
    config_file(
        "[ngwmn]\nconcurrency = 4\n\n"
        "[ngwmn.gentle]\nconcurrency = 2\n\n"
        "[waterdata.bulk]\nparallel_chunks = 8\n\n"
        "[nldi.gentle]\nretries = 1\n"
    )
    out = io.StringIO()

    dataretrieval.show_settings(stream=out)
    text = out.getvalue()
    listed = text.split("profiles in the file: ", 1)[1].splitlines()[0]

    assert listed == "[waterdata.bulk], [ngwmn.gentle], [nldi.gentle]"
    # The adapter's *default* profile is not a named one: it is always in
    # effect and already shows up as a source, so listing it here is noise.
    assert "[ngwmn]" not in listed
    # Inert, and the report says so by never naming one as a source.
    assert "configure() block" not in text


def test_show_settings_reports_an_unimported_adapter(config_file, monkeypatch):
    """An adapter this process cannot report on is named, never omitted.

    NLDI is imported on demand for the geopandas extra, so a process that has
    not touched it cannot say which settings it accepts -- the honest cost of
    validating an adapter's keys lazily (ADR 0011). Leaving it out of the
    report would read as "nothing is configured for nldi", which is a
    different claim from "this report could not check", and the caller cannot
    tell which one they are looking at.
    """
    config_file("")
    monkeypatch.delitem(settings._REGISTRY, "nldi", raising=False)
    out = io.StringIO()

    dataretrieval.show_settings(stream=out)
    text = out.getvalue()

    assert "not reported: nldi" in text
    assert "not imported" in text
    # An adapter that *was* imported is covered by the rows above, so it must
    # not be named as uncoverable.
    assert "waterdata" not in text.split("not reported:", 1)[1]

    # The line is a statement about this process, not about nldi: once the
    # module is imported its configuration registers and the caveat goes away.
    class _AsImported(settings.AdapterSettings):
        adapter: ClassVar[str] = "nldi"

        retries: int | None = None

    monkeypatch.setitem(settings._REGISTRY, "nldi", _AsImported)
    out = io.StringIO()
    dataretrieval.show_settings(stream=out)
    assert "not reported" not in out.getvalue()


def test_show_settings_sample_output_is_current(config_file, monkeypatch):
    """The documented samples are this function's real output, not a drawing.

    Both had drifted from it -- the docstring wrapped a line the function
    prints whole, the user guide had lost a paragraph -- because a sample kept
    by hand is only ever as fresh as the last person who remembered it. So
    the scenario is rebuilt here and the output compared verbatim; the only
    edit is swapping the temporary path for the illustrative one.

    Regenerate by running this test and copying the reported ``actual`` into
    both places, never by editing them to taste.
    """
    path = config_file(_SAMPLE_FILE)
    monkeypatch.setenv("API_USGS_RETRIES", "8")
    # The sample shows the report a caller with the geopandas extra uninstalled
    # sees; in this suite something has usually imported nldi already.
    monkeypatch.delitem(settings._REGISTRY, "nldi", raising=False)

    out = io.StringIO()
    with dataretrieval.configure(WaterdataSettings.load("bulk")):
        dataretrieval.show_settings(stream=out)
    actual = out.getvalue().replace(str(path), _SAMPLE_PATH).strip("\n")

    assert _documented_sample() == f"{_SAMPLE_PROMPT}\n{actual}"

    # The user guide shows the same sample, indented into its code block, and
    # goes stale the same way. Checked here rather than in a docs test because
    # the thing that makes it stale is a change to this function's output.
    guide = (
        pathlib.Path(__file__).resolve().parents[1]
        / "docs"
        / "source"
        / "userguide"
        / "settings.rst"
    )
    if not guide.exists():  # pragma: no cover - docs are absent from an sdist
        pytest.skip("docs tree not present")
    block = textwrap.indent(f"{_SAMPLE_PROMPT}\n{actual}", "   ")
    assert block in guide.read_text(encoding="utf-8")


def test_show_settings_survives_a_malformed_profile(config_file):
    """Explaining a broken configuration is the job, so nothing here validates.

    The section lists what the file *defines*; a profile's keys are checked when a
    caller selects it. So a profile holding a value that fails its grammar --
    or the nested table a file migrated from the retired ``[profiles.<name>]``
    layout still carries -- is reported rather than taking the report down
    with it, which is the one moment a caller most needs it.
    """
    config_file(
        '[waterdata.bulk]\nconcurrency = "nope"\n\n'
        "[ngwmn.gentle]\n\n[ngwmn.gentle.nested]\nconcurrency = 2\n"
    )
    out = io.StringIO()

    dataretrieval.show_settings(stream=out)  # must not raise

    listed = out.getvalue().split("profiles in the file: ", 1)[1].splitlines()[0]
    assert listed == "[waterdata.bulk], [ngwmn.gentle]"
    # Selecting one is where the grammar is checked, and it still is.
    with pytest.raises(settings.ConfigurationError, match="integer"):
        WaterdataSettings.load("bulk")
    with pytest.raises(settings.ConfigurationError, match="contains a table"):
        NgwmnSettings.load("gentle")
