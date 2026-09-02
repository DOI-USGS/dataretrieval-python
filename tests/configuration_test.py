"""Tests for layered configuration resolution (``dataretrieval.configuration``)."""

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
from dataretrieval import _configuration_core as _core
from dataretrieval import configuration, streamstats, waterdata
from dataretrieval.configuration import Configuration
from dataretrieval.ngwmn import NgwmnConfiguration
from dataretrieval.nwdc import DEFAULT_CONCURRENT_REQUESTS, NwdcConfiguration
from dataretrieval.streamstats import StreamstatsConfiguration
from dataretrieval.utils import _default_headers
from dataretrieval.waterdata import WaterdataConfiguration
from dataretrieval.wqp import WqpConfiguration

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
        for env in configuration.ENV_VARS.values():
            monkeypatch.delenv(env, raising=False)
        monkeypatch.setenv(configuration.CONFIG_PATH_ENV, str(path))
        configuration._reset_file_cache()
        return path

    return write


# --- precedence ----------------------------------------------------------


def test_default_when_nothing_is_configured(monkeypatch):
    for env in configuration.ENV_VARS.values():
        monkeypatch.delenv(env, raising=False)
    assert configuration.api_key() is None
    assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY
    assert configuration.retries() == configuration.DEFAULT_RETRIES
    assert configuration.parallel_chunks() == configuration.DEFAULT_PARALLEL_CHUNKS
    assert configuration.progress() is None


def test_env_is_used_when_no_file_or_block(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    monkeypatch.setenv("API_USGS_CONCURRENT", "4")
    assert configuration.api_key() == "env-key"
    assert configuration.concurrency() == 4


def test_env_outranks_file(config_file, monkeypatch):
    config_file('api_key = "file-key"\n')
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    assert configuration.api_key() == "env-key"


def test_block_outranks_file_and_env(config_file, monkeypatch):
    config_file('api_key = "file-key"\n')
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    with dataretrieval.configure(Configuration(api_key="block-key")):
        assert configuration.api_key() == "block-key"
    assert configuration.api_key() == "env-key"


def test_precedence_is_per_setting_not_per_source(config_file, monkeypatch):
    """An environment key must not blank out file-provided settings."""
    config_file("concurrency = 16\n")
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    monkeypatch.setenv("API_USGS_RETRIES", "9")
    assert configuration.concurrency() == 16  # from the file
    assert configuration.api_key() == "env-key"  # still from the env
    assert configuration.retries() == 9  # still from the env


# --- the configure() block -----------------------------------------------


def test_blocks_nest_and_merge_per_setting():
    with dataretrieval.configure(Configuration(api_key="outer", concurrency=4)):
        with dataretrieval.configure(Configuration(concurrency=8)):
            assert configuration.concurrency() == 8
            assert configuration.api_key() == "outer"  # inherited from the outer block
        assert configuration.concurrency() == 4  # inner block restored on exit


def test_omitted_setting_inherits_lower_source(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    with dataretrieval.configure(Configuration(concurrency=2)):
        assert configuration.api_key() == "env-key"


def test_explicit_none_suppresses_lower_sources(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    monkeypatch.setenv("API_USGS_CONCURRENT", "4")
    monkeypatch.setenv("API_USGS_PROGRESS", "true")
    with dataretrieval.configure(
        Configuration(api_key=None, concurrency=None, progress=None)
    ):
        assert configuration.api_key() is None
        assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY
        assert configuration.progress() is None
    assert configuration.api_key() == "env-key"
    assert configuration.concurrency() == 4
    assert configuration.progress() is True


@pytest.mark.parametrize(
    "settings",
    [
        {"concurrency": 0},
        {"retries": -1},
        {"parallel_chunks": 0},
        {"progress": "flase"},
    ],
)
def test_a_configuration_validates_its_own_settings(settings):
    """An invalid value raises where it was written, not inside a later request.

    Construction is earlier than the ``with``, which is earlier than the
    request the value would otherwise have broken.
    """
    with pytest.raises(configuration.ConfigurationError):
        Configuration(**settings)


@pytest.mark.parametrize(
    ("settings", "expected"),
    [
        ({"api_key": 123}, "string"),
        ({"concurrency": 1.5}, "integer"),
        ({"concurrency": "8"}, "integer"),
        ({"retries": "2"}, "integer"),
        ({"progress": []}, "bool"),
        ({"parallel_chunks": True}, "integer"),
    ],
)
def test_configuration_rejects_values_outside_annotated_types(settings, expected):
    with pytest.raises(configuration.ConfigurationError, match=expected):
        Configuration(**settings)


def test_block_accepts_ints_and_strings():
    with dataretrieval.configure(Configuration(concurrency="unbounded")):
        assert configuration.concurrency() is None
    with dataretrieval.configure(Configuration(concurrency=8)):
        assert configuration.concurrency() == 8
    with dataretrieval.configure(Configuration(progress=False)):
        assert configuration.progress() is False
    with dataretrieval.configure(Configuration(progress=True)):
        assert configuration.progress() is True


def test_configure_takes_configurations_and_nothing_else():
    """The argument is an object, so a stray mapping or keyword cannot pass.

    ``configure(ngwmn={"concurrency": 2})`` was the earlier spelling, and it is
    exactly what a reader of an old script will try. Naming the replacement in
    the error is the difference between a two-minute fix and a search.
    """
    with pytest.raises(configuration.ConfigurationError, match="configuration objects"):
        with dataretrieval.configure({"concurrency": 2}):
            pass
    with pytest.raises(configuration.ConfigurationError, match="configuration objects"):
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
    with pytest.raises(configuration.ConfigurationError, match="two configurations"):
        with dataretrieval.configure(
            WaterdataConfiguration(concurrency=2),
            WaterdataConfiguration(retries=1),
        ):
            pass

    # Same rule for the package-wide configuration, which targets no adapter.
    with pytest.raises(configuration.ConfigurationError, match="package-wide"):
        with dataretrieval.configure(
            Configuration(retries=1), Configuration(retries=2)
        ):
            pass

    # Two *different* adapters in one block is the whole point of the feature.
    with dataretrieval.configure(
        WaterdataConfiguration(concurrency=2), NgwmnConfiguration(concurrency=8)
    ):
        assert configuration.concurrency(adapter="waterdata") == 2
        assert configuration.concurrency(adapter="ngwmn") == 8


def test_a_configuration_resolves_end_to_end(config_file, monkeypatch):
    """Every source below a passed configuration still applies, per setting."""
    config_file('api_key = "file-key"\nstall_timeout = 15\n')
    monkeypatch.setenv("API_USGS_RETRIES", "9")

    with dataretrieval.configure(Configuration(concurrency=3)):
        assert configuration.concurrency() == 3  # from the configuration
        assert configuration.retries() == 9  # still from the environment
        assert configuration.api_key() == "file-key"  # still from the file
        assert configuration.stall_timeout() == 15  # still from the file
        assert (
            configuration.parallel_chunks() == configuration.DEFAULT_PARALLEL_CHUNKS
        )  # still the built-in default

    assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY


def test_an_adapter_configuration_narrows_to_one_adapter(monkeypatch):
    """The adapter is a property of the class, so nothing else moves."""
    monkeypatch.delenv("API_USGS_RETRIES")  # pinned by the autouse fixture

    with dataretrieval.configure(NgwmnConfiguration(retries=1)):
        assert configuration.retries(adapter="ngwmn") == 1
        # Every other adapter, and the package-wide read, are untouched --
        # including waterdata, which shares NGWMN's host and its API key.
        for other in ("waterdata", "nwdc", "wqp", "streamstats"):
            assert configuration.retries(adapter=other) == configuration.DEFAULT_RETRIES
        assert configuration.retries() == configuration.DEFAULT_RETRIES


# --- isolation (the point of issue #352) ---------------------------------


def test_threads_do_not_leak_credentials_into_each_other():
    """Two threads in different blocks see different keys.

    This is the concurrency complaint in #352: ``os.environ`` is
    process-global, so it cannot express this.
    """
    seen: dict[str, str | None] = {}
    started = threading.Barrier(2)

    def worker(name: str, key: str) -> None:
        with dataretrieval.configure(Configuration(api_key=key)):
            started.wait(timeout=5)  # force the blocks to overlap in time
            seen[name] = configuration.api_key()

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
        with dataretrieval.configure(Configuration(api_key=key)):
            await asyncio.sleep(0)  # yield, letting the other task interleave
            return configuration.api_key()

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
    assert configuration.concurrency(adapter="waterdata") == 4

    with dataretrieval.configure(WaterdataConfiguration.load("bulk")):
        assert configuration.concurrency(adapter="waterdata") is None  # the profile
        assert configuration.retries(adapter="waterdata") == 2  # default profile
        assert configuration.api_key() == "shared"  # package-wide, from the file
        # It narrows to one adapter, so a sibling on the same host is untouched.
        assert configuration.concurrency(adapter="ngwmn") == 4

    assert configuration.concurrency(adapter="waterdata") == 4


def test_a_code_selected_profile_outranks_the_environment(config_file, monkeypatch):
    """ADR 0011 inverts ADR 0009's environment-above-file rule for this case.

    A profile named in code is a more deliberate act than a variable inherited
    from a shell, and losing to that variable is what a caller would file a bug
    about.
    """
    config_file("[waterdata.gentle]\nconcurrency = 2\n")
    monkeypatch.setenv("API_USGS_CONCURRENT", "16")

    assert configuration.concurrency(adapter="waterdata") == 16
    with dataretrieval.configure(WaterdataConfiguration.load("gentle")):
        assert configuration.concurrency(adapter="waterdata") == 2


def test_several_named_profiles_are_selected_independently(config_file):
    """One block, two adapters, a different named profile for each."""
    config_file(
        "[waterdata.bulk]\nconcurrency = 32\n\n"
        "[waterdata.polite]\nconcurrency = 2\n\n"
        "[ngwmn.gentle]\nconcurrency = 4\n"
    )

    with dataretrieval.configure(
        WaterdataConfiguration.load("polite"), NgwmnConfiguration.load("gentle")
    ):
        assert configuration.concurrency(adapter="waterdata") == 2
        assert configuration.concurrency(adapter="ngwmn") == 4


def test_a_named_profile_layers_per_key_over_the_rungs_below(config_file):
    """Selecting a profile replaces keys, never whole rungs.

    Every rung overrides the one below it *per key* (ADR 0011), so one
    adapter-scoped read here draws each of its four settings from a
    different table.
    """
    config_file(
        "concurrency = 16\nretries = 3\nstall_timeout = 30\n\n"
        "[waterdata]\nretries = 2\n\n"
        '[waterdata.bulk]\nconcurrency = "unbounded"\nparallel_chunks = 8\n'
    )

    with dataretrieval.configure(WaterdataConfiguration.load("bulk")):
        # the profile, over a package-wide key it names...
        assert configuration.concurrency(adapter="waterdata") is None
        # ...the default profile, over a package-wide key the profile is silent
        # about...
        assert configuration.retries(adapter="waterdata") == 2
        # ...the package-wide key, which neither table touched...
        assert configuration.stall_timeout(adapter="waterdata") == 30
        # ...and a setting only the profile names.
        assert configuration.parallel_chunks(adapter="waterdata") == 8


def _resolved_settings() -> dict[object, object]:
    """Every setting this process can resolve, package-wide and per adapter.

    A snapshot rather than a handful of assertions, because the claim under
    test is about what a file does *not* change -- and naming the settings
    individually would only prove it for the ones the author thought of.
    """
    snapshot: dict[object, object] = {
        "api_key": configuration.api_key(),
        "progress": configuration.progress(),
    }
    for adapter in (None, *configuration.ADAPTERS):
        snapshot[(adapter, "concurrency")] = configuration.concurrency(adapter=adapter)
        snapshot[(adapter, "retries")] = configuration.retries(adapter=adapter)
        snapshot[(adapter, "parallel_chunks")] = configuration.parallel_chunks(
            adapter=adapter
        )
        snapshot[(adapter, "stall_timeout")] = configuration.stall_timeout(
            adapter=adapter
        )
        snapshot[(adapter, "base_url")] = configuration.base_url(adapter=adapter)
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
    with dataretrieval.configure(WaterdataConfiguration.load("bulk")):
        assert configuration.parallel_chunks(adapter="waterdata") == 8


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
    assert configuration.retries(adapter="ngwmn") == configuration.DEFAULT_RETRIES
    assert configuration.parallel_chunks(adapter="waterdata") == (
        configuration.DEFAULT_PARALLEL_CHUNKS
    )

    with pytest.raises(
        configuration.ConfigurationError, match=r"\[waterdata\.bulk\.ngwmn\]"
    ):
        WaterdataConfiguration.load("bulk")


def test_loading_an_undefined_profile_raises(config_file):
    """A name the caller just typed is a typo, not a silent fall-through.

    The message lists what the file *does* define, because a misspelling is
    only recognizable next to the spelling that was meant -- and only for this
    adapter, since selecting a profile is per adapter and another service's
    profile names are not candidates for what the caller meant to type.
    """
    config_file(
        "[waterdata]\nconcurrency = 4\n\n"
        "[waterdata.bulk]\nretries = 8\n\n"
        "[waterdata.polite]\nretries = 1\n\n"
        "[ngwmn.gentle]\nconcurrency = 2\n"
    )
    with pytest.raises(configuration.ConfigurationError) as excinfo:
        WaterdataConfiguration.load("bluk")
    message = str(excinfo.value)
    assert "no [waterdata.bluk]" in message
    assert "bulk, polite" in message
    assert "gentle" not in message

    # An adapter with no profiles at all says so rather than trailing off after
    # the colon, which would read as a truncated message.
    config_file("[waterdata]\nconcurrency = 4\n")
    with pytest.raises(configuration.ConfigurationError, match="waterdata: none"):
        WaterdataConfiguration.load("bulk")


def test_loading_a_profile_with_no_file_says_so(tmp_path, monkeypatch):
    monkeypatch.delenv("API_USGS_CONCURRENT")  # pinned by the autouse fixture
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, str(tmp_path / "absent.toml"))
    configuration._reset_file_cache()

    with pytest.raises(configuration.ConfigurationError, match="no configuration file"):
        WaterdataConfiguration.load("also-gone")


def test_the_package_wide_configuration_has_no_profiles(config_file):
    """A profile belongs to one adapter, so ``Configuration`` cannot name one."""
    config_file("[waterdata.bulk]\nconcurrency = 8\n")
    with pytest.raises(configuration.ConfigurationError, match="package-wide"):
        Configuration.load("bulk")


def test_missing_file_is_not_an_error(tmp_path, monkeypatch):
    monkeypatch.delenv("API_USGS_CONCURRENT")  # pinned by the autouse fixture
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, str(tmp_path / "absent.toml"))
    configuration._reset_file_cache()
    assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY


def test_malformed_file_raises_pointing_at_the_file(config_file):
    path = config_file("api_key = \n")
    with pytest.raises(configuration.ConfigurationError) as excinfo:
        configuration.api_key()
    assert "not valid TOML" in str(excinfo.value)
    assert str(path) in str(excinfo.value)


def test_non_utf8_file_raises_config_error(config_file):
    path = config_file("")
    path.write_bytes(b'api_key = "\xff"\n')
    with pytest.raises(configuration.ConfigurationError, match="not valid UTF-8"):
        configuration.api_key()


def test_config_path_must_not_be_a_directory(tmp_path, monkeypatch):
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, str(tmp_path))
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    configuration._reset_file_cache()
    with pytest.raises(configuration.ConfigurationError, match="directory"):
        configuration.concurrency()


@pytest.mark.skipif(os.name != "posix", reason="POSIX directory permissions")
def test_inaccessible_config_path_raises(tmp_path, monkeypatch):
    parent = tmp_path / "blocked"
    parent.mkdir()
    path = parent / "config.toml"
    path.write_text("concurrency = 4\n")
    parent.chmod(0)
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, str(path))
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    configuration._reset_file_cache()
    try:
        try:
            path.stat()
        except PermissionError:
            pass
        else:  # pragma: no cover - root or a filesystem that ignores mode bits
            pytest.skip("filesystem does not enforce directory mode bits")
        with pytest.raises(configuration.ConfigurationError, match="could not access"):
            configuration.concurrency()
    finally:
        parent.chmod(0o700)


def test_unknown_setting_warns_but_is_ignored(config_file):
    config_file('concurrency = 4\napi_kye = "typo"\n')
    with pytest.warns(UserWarning, match="unknown setting"):
        assert configuration.concurrency() == 4


def test_unknown_table_raises(config_file):
    """A profile written as ``[bulk]`` instead of ``[waterdata.bulk]``."""
    config_file("[bulk]\nconcurrency = 4\n")
    with pytest.raises(configuration.ConfigurationError, match="unknown table"):
        configuration.concurrency()


def test_the_retired_profiles_table_names_its_replacement(config_file):
    """Nothing shipped with ``[profiles.<name>]``, but the docs described it.

    The generic "unknown table" message would send its author hunting for a
    typo in a table spelled exactly as they had been told to spell it.
    """
    config_file("[profiles.bulk]\nconcurrency = 4\n")
    with pytest.raises(
        configuration.ConfigurationError, match=r"\[<adapter>\.<name>\]"
    ):
        configuration.concurrency()


def test_the_retired_profile_environment_variable_is_ignored(config_file, monkeypatch):
    """``DATARETRIEVAL_PROFILE`` went with the table it selected (ADR 0011).

    A profile is now named in code. A variable exported once in a shell profile
    and inherited by every subprocess is the opposite shape: invisible at the
    call site, and able to switch every service at once. Honoring it under the
    new grammar would restore exactly what the grammar removed.
    """
    config_file('concurrency = 4\n\n[waterdata.bulk]\nconcurrency = "unbounded"\n')
    monkeypatch.setenv("DATARETRIEVAL_PROFILE", "bulk")

    assert configuration.concurrency(adapter="waterdata") == 4
    assert "DATARETRIEVAL_PROFILE" not in configuration.ENV_VARS.values()


def test_typed_toml_values_are_normalized(config_file):
    """``tomllib`` returns typed values that normalize into shared parsers."""
    config_file("concurrency = 16\nretries = 0\nprogress = true\n")
    assert configuration.concurrency() == 16
    assert configuration.retries() == 0
    assert configuration.progress() is True


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
    with pytest.raises(configuration.ConfigurationError):
        configuration.parallel_chunks()


def test_file_edit_is_picked_up(config_file, monkeypatch):
    path = config_file("concurrency = 4\n")
    assert configuration.concurrency() == 4
    original = path.stat()
    path.write_text("concurrency = 8\n")
    os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
    # Windows ctime is creation time, so unchanged metadata must fall back to
    # comparing raw content before the parsed cache is reused.
    monkeypatch.setattr(configuration.os, "name", "nt")
    assert configuration.concurrency() == 8


def test_explicit_config_path_is_expanded(monkeypatch):
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, "~/somewhere/config.toml")
    assert str(configuration.config_path()).startswith(os.path.expanduser("~"))
    assert "~" not in str(configuration.config_path())


def test_relative_config_path_follows_the_working_directory(tmp_path, monkeypatch):
    """A relative ``DATARETRIEVAL_CONFIG`` is resolved against the *current* cwd.

    The path memo keys on the working directory for exactly this reason: a
    scheduler or notebook that sets a relative path and chdirs per job would
    otherwise keep serving the first job's credentials for the life of the
    process, with ``show_configuration()`` reporting the stale path as current.
    """
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    (first / "config.toml").write_text("concurrency = 4\n")
    (second / "config.toml").write_text("concurrency = 9\n")
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, "config.toml")

    monkeypatch.chdir(first)
    configuration._reset_file_cache()
    assert configuration.config_path() == first / "config.toml"
    assert configuration.concurrency() == 4

    monkeypatch.chdir(second)
    assert configuration.config_path() == second / "config.toml"
    assert configuration.concurrency() == 9


@pytest.mark.skipif(os.name != "posix", reason="POSIX file modes")
def test_world_readable_file_with_a_key_warns(tmp_path, monkeypatch):
    path = tmp_path / "config.toml"
    path.write_text('api_key = "secret"\n')
    path.chmod(0o644)
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, str(path))
    monkeypatch.delenv("API_USGS_PAT", raising=False)
    configuration._reset_file_cache()
    with pytest.warns(UserWarning, match="readable by other users"):
        assert configuration.api_key() == "secret"


@pytest.mark.skipif(os.name != "posix", reason="POSIX file modes")
def test_permission_change_is_checked_on_cached_file(config_file):
    path = config_file('api_key = "secret"\n')
    assert configuration.api_key() == "secret"
    path.chmod(0o644)
    with pytest.warns(UserWarning, match="readable by other users"):
        assert configuration.api_key() == "secret"


@pytest.mark.skipif(os.name != "posix", reason="POSIX file modes")
def test_no_permission_warning_without_a_key(tmp_path, monkeypatch, recwarn):
    path = tmp_path / "config.toml"
    path.write_text("concurrency = 4\n")
    path.chmod(0o644)
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, str(path))
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    configuration._reset_file_cache()
    assert configuration.concurrency() == 4
    assert not [w for w in recwarn if "readable by other users" in str(w.message)]


# --- value grammar -------------------------------------------------------


def test_api_key_is_stripped_and_blank_means_none(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "  key-with-newline\n")
    assert configuration.api_key() == "key-with-newline"
    monkeypatch.setenv("API_USGS_PAT", "   ")
    assert configuration.api_key() is None


def test_blank_numeric_env_falls_back_to_the_default(monkeypatch):
    monkeypatch.setenv("API_USGS_CONCURRENT", "")
    monkeypatch.setenv("API_USGS_RETRIES", "")
    assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY
    assert configuration.retries() == configuration.DEFAULT_RETRIES


def test_blank_progress_env_means_off_not_unset(monkeypatch):
    """Preserved from the pre-config behavior: blank disables the line."""
    monkeypatch.setenv("API_USGS_PROGRESS", "")
    assert configuration.progress() is False


@pytest.mark.parametrize("value", ["0", "false", "no", "off", "FALSE"])
def test_progress_falsey_values(monkeypatch, value):
    monkeypatch.setenv("API_USGS_PROGRESS", value)
    assert configuration.progress() is False


@pytest.mark.parametrize("value", ["1", "true", "yes", "on"])
def test_progress_truthy_values(monkeypatch, value):
    monkeypatch.setenv("API_USGS_PROGRESS", value)
    assert configuration.progress() is True


def test_legacy_unknown_progress_env_still_means_on(monkeypatch):
    monkeypatch.setenv("API_USGS_PROGRESS", "legacy-nonempty-value")
    assert configuration.progress() is True


@pytest.mark.parametrize("value", ["nope", "-1", "0"])
def test_invalid_concurrency_raises(monkeypatch, value):
    monkeypatch.setenv("API_USGS_CONCURRENT", value)
    with pytest.raises(ValueError):  # ConfigurationError is a ValueError
        configuration.concurrency()


def test_unbounded_concurrency(monkeypatch):
    monkeypatch.setenv("API_USGS_CONCURRENT", "unbounded")
    assert configuration.concurrency() is None


def test_error_message_names_the_source(config_file, monkeypatch):
    monkeypatch.setenv("API_USGS_CONCURRENT", "nope")
    with pytest.raises(
        configuration.ConfigurationError, match=r"\$?API_USGS_CONCURRENT"
    ):
        configuration.concurrency()
    monkeypatch.delenv("API_USGS_CONCURRENT")
    path = config_file('concurrency = "nope"\n')
    # ``match`` is a regex, and a Windows path is mostly escapes:
    # ``C:\\Users\\...`` makes ``\\U`` an invalid escape.
    with pytest.raises(configuration.ConfigurationError, match=re.escape(str(path))):
        configuration.concurrency()


# --- security ------------------------------------------------------------


def test_show_config_never_prints_the_key(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "super-secret-value")
    out = io.StringIO()
    dataretrieval.show_configuration(stream=out)
    text = out.getvalue()
    assert "super-secret-value" not in text
    assert "<set>" in text
    assert "$API_USGS_PAT" in text  # provenance is still reported


def test_show_config_reports_absent_key(monkeypatch):
    monkeypatch.delenv("API_USGS_PAT", raising=False)
    out = io.StringIO()
    dataretrieval.show_configuration(stream=out)
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
    with dataretrieval.configure(Configuration(api_key="block-key")):
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

    with dataretrieval.configure(Configuration(retries=3)):
        assert RetryPolicy.from_configuration().max_retries == 3


def test_parallel_chunks_baseline_comes_from_config(config_file):
    from dataretrieval.ogc.chunking import parallel_chunks

    assert configuration.parallel_chunks() == 1
    config_file("parallel_chunks = 8\n")
    assert configuration.parallel_chunks() == 8
    with parallel_chunks(2):  # an explicit block still wins over the file
        assert configuration.parallel_chunks() == 2
    assert configuration.parallel_chunks() == 8


def test_parallel_chunks_and_configure_share_one_mechanism():
    """``parallel_chunks(n)`` is sugar for a package-wide ``Configuration``.

    They must not be two competing scopes: whichever block is innermost wins,
    so ``show_configuration()`` always reports the value the chunker will use.
    """
    from dataretrieval.ogc.chunking import parallel_chunks

    with parallel_chunks(2):
        with dataretrieval.configure(Configuration(parallel_chunks=8)):
            assert configuration.parallel_chunks() == 8
        assert configuration.parallel_chunks() == 2

    with dataretrieval.configure(Configuration(parallel_chunks=8)):
        with parallel_chunks(2):
            assert configuration.parallel_chunks() == 2
        assert configuration.parallel_chunks() == 8


def test_parallel_chunks_has_no_environment_variable():
    """It spends quota, so it is deliberately file/block-only (see ENV_VARS)."""
    assert "parallel_chunks" not in configuration.ENV_VARS
    assert "parallel_chunks" in configuration.SETTINGS


def test_progress_reporter_reads_the_block():
    from dataretrieval.progress import ProgressReporter

    with dataretrieval.configure(Configuration(progress=True)):
        assert ProgressReporter(stream=io.StringIO()).enabled
    with dataretrieval.configure(Configuration(progress=False)):
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
    for env in configuration.ENV_VARS.values():
        monkeypatch.setenv(env, "")

    assert configuration.api_key() == "file-key"
    assert configuration.concurrency() == 4
    assert configuration.retries() == 7
    # ``progress`` is the documented exception: a blank API_USGS_PROGRESS has
    # always meant "off", so for that setting blank *is* a value and outranks
    # the file. The asymmetry is declared once, in configuration._BLANK_MEANS_SET.
    assert configuration.progress() is False
    assert set(configuration._BLANK_MEANS_SET) == {"progress"}


def test_blank_progress_env_keeps_its_legacy_meaning(monkeypatch):
    """With no file, blank keeps the environment-only meaning it always had."""
    monkeypatch.setenv("API_USGS_PROGRESS", "")
    monkeypatch.setenv("API_USGS_CONCURRENT", "")
    assert configuration.progress() is False  # blank has always meant "off"
    assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY


def test_config_error_is_in_the_error_taxonomy():
    """A broken config surfaces from inside a getter, so it must be catchable."""
    import dataretrieval.exceptions as exceptions

    assert issubclass(configuration.ConfigurationError, exceptions.DataRetrievalError)
    assert issubclass(
        configuration.ConfigurationError, ValueError
    )  # legacy handlers still work
    assert configuration.ConfigurationError is exceptions.ConfigurationError


def test_show_config_reports_a_broken_file_instead_of_raising(config_file):
    """The tool that explains a configuration must survive a broken one."""
    config_file("this is not = valid toml [[[\n")
    out = io.StringIO()
    dataretrieval.show_configuration(stream=out)  # must not raise
    text = out.getvalue()
    assert "ERROR:" in text
    # Every setting still gets a row rather than the report dying part-way.
    for name in configuration.SETTINGS:
        assert name in text


def test_show_config_reports_a_bad_value_in_its_own_row(monkeypatch):
    monkeypatch.setenv("API_USGS_CONCURRENT", "nope")
    out = io.StringIO()
    dataretrieval.show_configuration(stream=out)
    text = out.getvalue()
    assert "<error:" in text
    assert "retries" in text  # unaffected settings still resolve


def test_top_level_parallel_chunks_warns(config_file):
    """It spends quota in every process, so steer it into a profile."""
    with pytest.warns(UserWarning, match="parallel_chunks"):
        config_file("parallel_chunks = 8\n")
        assert configuration.parallel_chunks() == 8


def test_parallel_chunks_in_a_named_profile_does_not_warn(config_file, recwarn):
    config_file("[waterdata.bulk]\nparallel_chunks = 8\n")
    with dataretrieval.configure(WaterdataConfiguration.load("bulk")):
        assert configuration.parallel_chunks(adapter="waterdata") == 8
    assert not [w for w in recwarn if "parallel_chunks" in str(w.message)]


@pytest.mark.skipif(os.name != "posix", reason="needs /dev/null")
def test_non_regular_config_path_is_empty_configuration(monkeypatch):
    """``DATARETRIEVAL_CONFIG=/dev/null`` is how a run declares "no config".

    A non-regular path is treated as empty *without being opened*: settings are
    re-resolved per request, so reading a stream would hand its contents to the
    first getter and nothing to the rest (and a FIFO would block on open until
    a writer appeared). Rejecting it would raise from ``_default_headers`` on
    every request -- the opposite of what the caller asked for.
    """
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    monkeypatch.delenv("API_USGS_PAT", raising=False)
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, "/dev/null")
    configuration._reset_file_cache()
    assert configuration.api_key() is None
    assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY
    # Stable across repeated resolutions, unlike a stream that drains.
    assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY


def test_broken_config_does_not_break_unrelated_services(config_file):
    """A Water Data config problem must not fail a legacy NWIS/WQP call.

    Config resolution can raise, and ``_default_headers`` runs for every
    service. Resolving the key only after the host check keeps the blast
    radius on the calls that would actually receive it.
    """
    config_file("this is not = valid toml [[[\n")

    # Legacy hosts never get the key, so they never touch the configuration.
    assert "X-Api-Key" not in _default_headers("https://waterservices.usgs.gov/nwis/dv")
    assert "X-Api-Key" not in _default_headers("https://www.waterqualitydata.us/data")

    # The authorized host still fails loudly rather than silently going out
    # unauthenticated and hitting the anonymous rate limit.
    with pytest.raises(configuration.ConfigurationError):
        _default_headers(WATERDATA_URL)


def test_default_config_path_follows_a_changed_home(tmp_path, monkeypatch):
    """The default path derives from the home variable, so the memo watches it.

    Which variable that is depends on the platform: ``ntpath.expanduser``
    reads ``USERPROFILE`` and ignores ``HOME``, so setting ``HOME`` on Windows
    moves nothing and this asserted against the runner's real home directory.
    """
    home_var = "USERPROFILE" if os.name == "nt" else "HOME"
    monkeypatch.delenv(configuration.CONFIG_PATH_ENV, raising=False)
    monkeypatch.setenv(home_var, str(tmp_path / "first"))
    configuration._reset_file_cache()
    first = configuration.config_path()
    assert first == tmp_path / "first" / ".dataretrieval" / "config.toml"

    monkeypatch.setenv(home_var, str(tmp_path / "second"))
    assert (
        configuration.config_path()
        == tmp_path / "second" / ".dataretrieval" / "config.toml"
    )


def test_show_config_renderers_cover_every_setting():
    """Guarded with a raise, not an assert, so ``python -O`` keeps the check."""
    assert set(configuration._DISPLAYS) == set(configuration._ALL_SETTINGS)


def test_unselected_profile_is_not_validated(config_file):
    """An invalid value in a profile nobody selected must not fail every request.

    Profile tables are kept raw at parse time and validated only when one is
    actually selected -- the same blast-radius rule ``_default_headers``
    follows for the key itself.
    """
    config_file('api_key = "good"\n\n[waterdata.experimental]\nconcurrency = 0\n')
    assert _default_headers(WATERDATA_URL)["X-Api-Key"] == "good"
    assert configuration.concurrency(adapter="waterdata") == (
        configuration.DEFAULT_CONCURRENCY
    )

    # Selecting it still reports the problem.
    with pytest.raises(configuration.ConfigurationError, match="experimental"):
        WaterdataConfiguration.load("experimental")


def test_unknown_setting_in_an_unselected_profile_is_silent(config_file, recwarn):
    config_file("concurrency = 4\n\n[waterdata.other]\nnot_a_setting = 1\n")
    assert configuration.concurrency(adapter="waterdata") == 4
    assert not [w for w in recwarn if "unknown setting" in str(w.message)]


def test_a_malformed_table_does_not_fail_another_adapters_call(config_file):
    """The blast-radius rule, on the source a whole adapter table sits in.

    Keys are checked when *that* adapter first resolves a setting, so an
    invalid value in ``[nldi]`` costs a Water Data call nothing -- which is
    also what lets an adapter's vocabulary live in a module this leaf cannot
    import.
    """
    config_file(
        'api_key = "good"\n\n[nldi]\nretries = -1\n\n[waterdata]\nretries = 2\n'
    )

    assert configuration.retries(adapter="waterdata") == 2
    assert _default_headers(WATERDATA_URL)["X-Api-Key"] == "good"

    # The adapter that owns the table still gets the error, naming the table.
    with pytest.raises(configuration.ConfigurationError, match=r"\[nldi\]"):
        configuration.retries(adapter="nldi")


def test_a_table_for_an_unimported_adapter_stays_valid(config_file, monkeypatch):
    """A file must not be conditionally valid by which extras are installed.

    NLDI is imported on demand for the geopandas extra, so with the roster
    derived from imports a ``[nldi]`` table would be a typo until something
    happened to import that module. The roster is a plain name tuple instead,
    and an adapter that has registered no class has its keys checked against
    the package-wide settings.
    """
    monkeypatch.delitem(configuration._REGISTRY, "nldi", raising=False)
    config_file("retries = 5\n\n[nldi]\nretries = 9\n\n[nldi.gentle]\nretries = 1\n")

    assert configuration.settings_for("nldi") is None  # not an error: unknown yet
    assert configuration.retries(adapter="nldi") == 9  # its default profile applies
    assert configuration.retries(adapter="waterdata") == 5  # and narrows to nldi
    # The named profile under it is as inert as any other.
    assert configuration.retries() == 5


def test_show_config_does_not_promise_a_built_in_default_holds_everywhere(
    capsys, monkeypatch
):
    """A row reading "built-in default" is package-wide, not a per-service claim.

    ``concurrency`` resolves to 32 with nothing configured, but a Water Use call
    uses that service's own preference of 4. The report is the tool for "what
    will this actually use", so it must not let the reader take a package-wide
    row as an answer for every service.
    """
    from dataretrieval import configuration
    from dataretrieval.nwdc import DEFAULT_CONCURRENT_REQUESTS

    # The suite pins API_USGS_CONCURRENT so dispatch is deterministic; clear it
    # so the two kinds of default are what actually differ here.
    monkeypatch.delenv("API_USGS_CONCURRENT", raising=False)
    assert configuration.concurrency() != configuration.concurrency(
        DEFAULT_CONCURRENT_REQUESTS
    )

    dataretrieval.show_configuration()
    out = capsys.readouterr().out
    assert "built-in default" in out
    assert "An adapter may prefer its own" in out


# --- adapter-scoped settings (ADR 0010) ----------------------------------


def test_adapter_table_overrides_the_top_level_per_setting(config_file):
    """A ``[ngwmn]`` table narrows one adapter, leaving the rest inherited."""
    config_file("concurrency = 16\nretries = 3\n\n[ngwmn]\nconcurrency = 4\n")

    # The adapter that asked for it gets it...
    assert configuration.concurrency(adapter="ngwmn") == 4
    # ...its sibling on the same host does not...
    assert configuration.concurrency(adapter="waterdata") == 16
    # ...and the package-wide read is untouched.
    assert configuration.concurrency() == 16
    # Per setting, not per table: retries still comes from the top level.
    assert configuration.retries(adapter="ngwmn") == 3


def test_one_block_configures_several_adapters(config_file):
    """The requirement ADR 0009 deferred: gentle here, unchanged there."""
    config_file("")

    with dataretrieval.configure(
        Configuration(retries=7),
        NgwmnConfiguration(concurrency=2),
        NwdcConfiguration(concurrency=8),
    ):
        assert configuration.concurrency(adapter="ngwmn") == 2
        assert configuration.concurrency(adapter="nwdc") == 8
        assert (
            configuration.concurrency(adapter="waterdata")
            == configuration.DEFAULT_CONCURRENCY
        )
        # A package-wide value in the same block still reaches every adapter.
        assert configuration.retries(adapter="ngwmn") == 7


def test_environment_outranks_an_adapter_table(config_file, monkeypatch):
    """Precedence is source-major: the env source is above the file source.

    Scope-major ordering would invert this the moment anyone added an adapter
    table, so a variable exported for one run would lose to a stale file entry.
    """
    config_file("[ngwmn]\nconcurrency = 4\n")
    monkeypatch.setenv("API_USGS_CONCURRENT", "7")

    assert configuration.concurrency(adapter="ngwmn") == 7


def test_adapter_block_outranks_the_package_wide_block(config_file):
    """Within one source, the adapter-scoped value is the more specific one."""
    config_file("")

    with dataretrieval.configure(
        Configuration(concurrency=16), NgwmnConfiguration(concurrency=2)
    ):
        assert configuration.concurrency(adapter="ngwmn") == 2
        assert configuration.concurrency(adapter="waterdata") == 16


def test_adapter_rejects_a_setting_it_does_not_read(config_file):
    """A single-shot adapter has nothing to fan out, so ``concurrency`` is a typo.

    From code the refusal is a ``TypeError`` from the dataclass itself: the
    setting is not a field of ``WqpConfiguration``, so there is nowhere to put
    it. That is the same refusal a type checker makes before the code runs.
    """
    with pytest.raises(TypeError, match="concurrency"):
        WqpConfiguration(concurrency=2)

    config_file("[wqp]\nconcurrency = 2\n")
    with pytest.raises(
        configuration.ConfigurationError, match="not a setting that table"
    ):
        configuration.retries(adapter="wqp")


def test_api_key_is_never_adapter_scoped():
    """The key belongs to the gateway fronting a host, not to an adapter.

    Water Data and NGWMN are two adapters on one host sharing one key and one
    quota pool, so a per-adapter key would model a distinction that does not
    exist (ADR 0010).
    """
    for adapter in configuration.ADAPTERS:
        accepted = configuration.settings_for(adapter)
        assert accepted is not None or adapter == "nldi"
        assert accepted is None or "api_key" not in accepted

    with pytest.raises(TypeError, match="api_key"):
        NgwmnConfiguration(api_key="x")


def test_a_misspelled_setting_is_not_silently_swallowed():
    """A typo must fail, not be accepted and ignored.

    ``Configuration(concurrancy=8)`` is not a field, so the dataclass refuses
    it by name -- taking it and dropping it would leave a caller believing a
    setting is in force that no call reads, from a module whose job is to be
    trustworthy about what a call will use.
    """
    with pytest.raises(TypeError, match="concurrancy"):
        Configuration(concurrancy=8)


def test_adapter_roster_names_real_modules_that_register_themselves():
    """Every name in the roster resolves to an adapter that owns a schema.

    Two halves of one declaration: the roster is what parsing a file needs
    (is ``[ngwmn]`` a table or a typo?), and the class is what validating that
    table's keys needs. A name in one and not the other is a configuration
    nothing could reach.
    """
    import importlib

    for adapter in configuration.ADAPTERS:
        importlib.import_module(f"dataretrieval.{adapter}")
        accepted = configuration.settings_for(adapter)
        assert accepted is not None, f"{adapter} registered no configuration class"
        assert accepted >= {"retries", "stall_timeout", "base_url"}


def test_registering_an_adapter_outside_the_roster_raises():
    """The roster is the authority, so a class cannot invent an adapter."""

    @dataclass(frozen=True)
    class BogusConfiguration(configuration.BaseConfiguration):
        adapter: ClassVar[str] = "not-an-adapter"

    with pytest.raises(configuration.ConfigurationError, match="not one of"):
        configuration._register(BogusConfiguration)


def test_settings_for_an_unimported_adapter_is_not_an_error(monkeypatch):
    """``None`` means "cannot validate these keys yet", never "invalid".

    NLDI is imported on demand for the geopandas extra, so a roster built from
    imports would reject a valid ``[nldi]`` table until something
    happened to import that module.
    """
    monkeypatch.delitem(configuration._REGISTRY, "nldi", raising=False)
    assert configuration.settings_for("nldi") is None
    assert "nldi" in configuration.ADAPTERS


def test_every_adapter_is_actually_wired_to_a_read_site():
    """A schema nothing passes costs the caller a report they cannot trust.

    ``show_configuration()`` would report a ``[nwis]`` override as live while
    every call ignored it -- the report whose whole job is answering "what will
    this call use" being confidently incorrect. Importability is the weaker
    half of the invariant: it passed while ``waterdata.get_cql``, eight of nine
    WQP getters, and all of ``nwis`` silently resolved package-wide.
    """
    import pathlib

    source = "\n".join(
        p.read_text(encoding="utf-8")
        for p in pathlib.Path(configuration.__file__).parent.rglob("*.py")
        if p.name != "configuration.py"
    )
    missing = [a for a in configuration.ADAPTERS if f'adapter="{a}"' not in source]
    assert not missing, (
        f"adapters with a schema but no read site: {missing}. Either pass "
        'adapter="<name>" where that adapter builds its policy or fan-out, or '
        "drop it from configuration.ADAPTERS."
    )


def test_a_misspelled_adapter_at_a_read_site_raises():
    """The other half of the invariant above, which a grep cannot check.

    ``adapter="waterdatas"`` used to resolve *silently* package-wide: no table
    matches the typo, every setting is accepted because nothing knows the
    schema, and a ``[waterdata]`` table or a ``WaterdataConfiguration`` is then
    ignored with nothing raised anywhere. The grep only sees that the correctly
    spelled string occurs somewhere; it cannot see a second, misspelled one.
    """
    with pytest.raises(configuration.ConfigurationError, match="not a configurable"):
        configuration.retries(adapter="waterdatas")

    # Every read site funnels through one resolver, so the check reaches them
    # all -- including the accessors that would otherwise return a default.
    with pytest.raises(configuration.ConfigurationError, match="not a configurable"):
        configuration.base_url(adapter="nwis", default="https://example.invalid")


def test_a_non_finite_stall_timeout_is_refused():
    """``inf`` parses as a float and silently disables the bound it sets."""
    for bad in (float("inf"), float("nan")):
        with pytest.raises(configuration.ConfigurationError, match="finite"):
            Configuration(stall_timeout=bad)


def test_stall_timeout_resolves_through_the_chain(config_file, monkeypatch):
    """It was read straight from os.environ, so a block and the file were mute."""
    config_file("stall_timeout = 15\n\n[wqp]\nstall_timeout = 300\n")

    assert configuration.stall_timeout() == 15
    assert configuration.stall_timeout(adapter="wqp") == 300

    monkeypatch.setenv("API_USGS_STALL_TIMEOUT", "42")
    assert configuration.stall_timeout() == 42

    with dataretrieval.configure(Configuration(stall_timeout=2.5)):
        assert configuration.stall_timeout() == 2.5


def test_base_url_applies_from_code_and_is_refused_from_the_file(config_file):
    """A redirect belongs where a reader of the script sees it (ADR 0011).

    A configuration file that silently sent a data-retrieval library to another
    host would be a supply-chain-shaped hazard, so the file refuses the setting
    outright rather than accepting it and being trusted.
    """
    config_file("")

    with dataretrieval.configure(
        WaterdataConfiguration(base_url="https://mirror.example/ogcapi")
    ):
        assert configuration.base_url(adapter="waterdata") == (
            "https://mirror.example/ogcapi"
        )
        # It names one service, so it never reaches another.
        assert configuration.base_url(adapter="ngwmn") is None
    assert configuration.base_url(adapter="waterdata") is None

    for text in (
        'base_url = "https://evil.example"\n',
        "[waterdata]\nbase_url = 'x'\n",
    ):
        config_file(text)
        with pytest.raises(
            configuration.ConfigurationError, match="only be set in code"
        ):
            configuration.base_url(adapter="waterdata")


def test_base_url_must_be_an_absolute_http_url():
    """A bare host would fail far from here, inside the request builder."""
    with pytest.raises(configuration.ConfigurationError, match="absolute"):
        WaterdataConfiguration(base_url="mirror.example")
    with pytest.raises(configuration.ConfigurationError, match="absolute"):
        WaterdataConfiguration(base_url="file:///etc/passwd")


def test_base_url_is_refused_from_the_environment(monkeypatch):
    """The environment is refused out loud, not merely unread.

    ``API_USGS_BASE_URL`` is the spelling every other setting's variable
    predicts, so a caller who exports it believes they have redirected
    something. Leaving it out of ``ENV_VARS`` would make that belief false and
    silent; the error names the block to write instead.
    """
    monkeypatch.setenv("API_USGS_BASE_URL", "https://evil.example")

    with pytest.raises(configuration.ConfigurationError, match="only be set in code"):
        configuration.base_url(adapter="waterdata")

    # Refused even under a block that sets one, matching the file: the variable
    # cannot work, and being quietly outranked is how it survives to a run where
    # nothing outranks it. Unsetting it is the only fix.
    with dataretrieval.configure(WaterdataConfiguration(base_url=_MIRROR)):
        with pytest.raises(
            configuration.ConfigurationError, match="only be set in code"
        ):
            configuration.base_url(adapter="waterdata")

    # A configuration in this state is exactly what show_configuration() exists
    # to explain, so it reports the failure rather than raising out of it.
    out = io.StringIO()
    dataretrieval.show_configuration(stream=out)
    assert "only be set in code" in out.getvalue()


def test_a_code_base_url_redirects_every_water_data_endpoint_family(httpx_mock):
    """One Water Data configuration moves every endpoint family together."""
    httpx_mock.add_response(json=_DAILY_PAGE)
    httpx_mock.add_response(json={"data": []})
    httpx_mock.add_response(json={"features": []})
    httpx_mock.add_response(json={"features": []})

    with dataretrieval.configure(WaterdataConfiguration(base_url=_MIRROR)):
        waterdata.get_daily(monitoring_location_id="USGS-05427718")
        waterdata.get_codes("states")
        waterdata.get_stats_por(
            monitoring_location_id="USGS-05427718",
            parameter_code="00060",
            start_date="01-01",
            end_date="01-01",
        )
        waterdata.get_ratings(
            monitoring_location_id="USGS-05427718",
            download_and_parse=False,
        )

    requested = [str(request.url) for request in httpx_mock.get_requests()]
    assert requested[0].startswith(f"{_MIRROR}/ogcapi/v0/collections/daily/items")
    assert requested[1].startswith(f"{_MIRROR}/samples-data/codeservice/states")
    assert requested[2].startswith(f"{_MIRROR}/statistics/v0/observationNormals")
    assert requested[3].startswith(f"{_MIRROR}/stac/v0/search")
    assert all(_WATERDATA_RE.match(url) is None for url in requested)


def test_a_code_base_url_redirects_the_adapters_requests(httpx_mock):
    """The setting has to move real traffic, not just resolve to a string.

    Two adapters with unrelated request machinery -- the OGC engine and a plain
    one-shot GET -- because "the configuration reaches the request" is a claim
    about each adapter's wiring, and one of them passing says nothing about the
    other.
    """
    httpx_mock.add_response(method=None, url=_MIRROR_RE, json=_DAILY_PAGE)
    httpx_mock.add_response(method=None, url=_WATERDATA_RE, json=_DAILY_PAGE)

    with dataretrieval.configure(WaterdataConfiguration(base_url=_MIRROR)):
        waterdata.get_daily(monitoring_location_id="USGS-05427718")
    redirected_url = str(httpx_mock.get_requests()[-1].url)

    # Nothing configured: back to the service's own base, so the redirect is
    # scoped to the block rather than latched somewhere at import.
    waterdata.get_daily(monitoring_location_id="USGS-05427718")
    direct_url = str(httpx_mock.get_requests()[-1].url)

    assert redirected_url.startswith(f"{_MIRROR}/ogcapi/v0/collections/daily/items")
    assert direct_url.startswith(WATERDATA_URL)

    streamstats_mirror = "https://mirror.example/streamstats"
    with dataretrieval.configure(StreamstatsConfiguration(base_url=streamstats_mirror)):
        streamstats.download_workspace("workspace-id")
    assert str(httpx_mock.get_requests()[-1].url).startswith(
        f"{streamstats_mirror}/download"
    )


def test_a_redirected_adapter_is_not_sent_the_api_key(httpx_mock):
    """The key is scoped to the host that honors it, and a mirror is not it.

    ``credentials.accepts_api_key`` is checked where the header is attached, so
    a redirect needs no second rule to be safe -- but "needs no rule" is exactly
    the kind of claim that stops being true silently, and the cost of it being
    false is a credential handed to whatever host the block named.
    """
    httpx_mock.add_response(method=None, url=_MIRROR_RE, json=_DAILY_PAGE)
    httpx_mock.add_response(method=None, url=_WATERDATA_RE, json=_DAILY_PAGE)

    with dataretrieval.configure(Configuration(api_key="secret")):
        with dataretrieval.configure(WaterdataConfiguration(base_url=_MIRROR)):
            waterdata.get_daily(monitoring_location_id="USGS-05427718")
        redirected_request = httpx_mock.get_requests()[-1]

        # The same key, the same call, the service's own host: the control that
        # keeps this test from passing because no key was configured at all.
        waterdata.get_daily(monitoring_location_id="USGS-05427718")
        direct_request = httpx_mock.get_requests()[-1]

    assert "X-Api-Key" not in redirected_request.headers
    assert direct_request.headers["X-Api-Key"] == "secret"


def test_the_validate_hook_can_refuse_a_combination():
    """Per-setting grammar is shared with the file; this is for the rest."""

    @dataclass(frozen=True)
    class Fussy(configuration.BaseConfiguration):
        adapter: ClassVar[str] = "waterdata"

        concurrency: int | str | None = configuration._UNSET
        parallel_chunks: int | None = configuration._UNSET

        def validate(self) -> None:
            supplied = self.values()
            if supplied.get("parallel_chunks", 1) > supplied.get("concurrency", 1):
                raise configuration.ConfigurationError(
                    "parallel_chunks above concurrency only queues sub-requests."
                )

    assert Fussy(concurrency=8, parallel_chunks=4).settings() == {
        "concurrency",
        "parallel_chunks",
    }
    with pytest.raises(configuration.ConfigurationError, match="only queues"):
        Fussy(concurrency=2, parallel_chunks=8)


def test_show_configuration_lists_only_real_overrides(config_file):
    """A full adapter-by-setting grid would bury the answer in inherited rows."""
    config_file("concurrency = 16\n\n[ngwmn]\nconcurrency = 4\n")
    out = io.StringIO()

    dataretrieval.show_configuration(stream=out)
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

    with dataretrieval.configure(WaterdataConfiguration(concurrency=32)):
        with dataretrieval.configure(Configuration(concurrency=1)):
            assert configuration.concurrency(adapter="waterdata") == 1
        assert configuration.concurrency(adapter="waterdata") == 32


def test_adapter_scope_still_wins_within_one_block(config_file):
    """Depth breaks ties between blocks, never within one."""
    config_file("")

    with dataretrieval.configure(
        Configuration(concurrency=16), WaterdataConfiguration(concurrency=4)
    ):
        assert configuration.concurrency(adapter="waterdata") == 4
        assert configuration.concurrency(adapter="wqp") == 16


def test_parallel_chunks_block_survives_an_adapter_scoped_outer_block():
    """``parallel_chunks(n)`` is a per-call request and must not be discarded.

    It delegates to a package-wide ``Configuration``, so it writes the
    package-wide key -- and before blocks were kept as separate frames, any
    enclosing ``WaterdataConfiguration(parallel_chunks=...)`` outranked it.
    """
    from dataretrieval.waterdata import parallel_chunks

    with dataretrieval.configure(WaterdataConfiguration(parallel_chunks=2)):
        with parallel_chunks(16):
            assert configuration.parallel_chunks(adapter="waterdata") == 16
        assert configuration.parallel_chunks(adapter="waterdata") == 2


# --- the precedence ladder (ADR 0011) ------------------------------------
#
# ADR 0011 states the ladder in seven rungs, highest first:
#
#   1  a configuration instance passed to configure()
#   2  a profile selected in code, <Adapter>Configuration.load("<name>")
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
    return configuration.concurrency(DEFAULT_CONCURRENT_REQUESTS, adapter="nwdc")


def test_a_configuration_instance_tops_the_ladder(config_file, monkeypatch):
    """Rung 1 over every other rung, all six of them present at once."""
    config_file(_LADDER_FILE)
    monkeypatch.setenv("API_USGS_CONCURRENT", str(_LADDER_ENV))

    with dataretrieval.configure(NwdcConfiguration(concurrency=_LADDER_INSTANCE)):
        assert _nwdc_concurrency() == _LADDER_INSTANCE


def test_a_loaded_profile_beats_the_environment(config_file, monkeypatch):
    """Rung 2 over rung 3 -- the one inversion ADR 0011 exists to make.

    ADR 0009 put the environment above the file, and a named profile lives in
    the file, so those two rules alone predict that ``API_USGS_CONCURRENT`` in
    the shell wins. It does not: what reaches the chain is the caller *naming*
    the profile in code, which is a more deliberate act than a variable
    inherited from whatever started the process, and losing to that variable
    is the behaviour a caller would file a bug about.

    The inversion is also bounded, which the second half asserts: it covers
    what the profile names and nothing else, so ``retries`` -- which the file
    sets at the top level and no selected profile mentions -- still follows the
    original environment-above-file rule inside the very same block.
    """
    config_file(_LADDER_FILE)
    monkeypatch.setenv("API_USGS_CONCURRENT", str(_LADDER_ENV))
    monkeypatch.setenv("API_USGS_RETRIES", "9")

    assert _nwdc_concurrency() == _LADDER_ENV  # rung 3, until a profile is selected
    with dataretrieval.configure(NwdcConfiguration.load("tuned")):
        assert _nwdc_concurrency() == 12  # rung 2 wins for the key it names...
        assert configuration.retries(adapter="nwdc") == 9  # ...and only that key
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
    assert DEFAULT_CONCURRENT_REQUESTS != configuration.DEFAULT_CONCURRENCY
    # It is the read site's own figure, not a property of the adapter, so a
    # caller that states no preference lands on the package default instead --
    # which is what makes rungs 6 and 7 two rungs rather than one.
    assert (
        configuration.concurrency(adapter="nwdc") == configuration.DEFAULT_CONCURRENCY
    )


def test_the_package_built_in_default_is_the_floor(config_file):
    """Rung 7: with the six rungs above it empty, every setting still resolves.

    The floor is what makes the whole chain optional -- a caller who has
    configured nothing at all gets working values rather than an error.
    """
    config_file("")

    for adapter in (None, *configuration.ADAPTERS):
        assert configuration.concurrency(adapter=adapter) == (
            configuration.DEFAULT_CONCURRENCY
        )
        assert configuration.retries(adapter=adapter) == configuration.DEFAULT_RETRIES
        assert configuration.parallel_chunks(adapter=adapter) == (
            configuration.DEFAULT_PARALLEL_CHUNKS
        )
        assert configuration.stall_timeout(adapter=adapter) == (
            configuration.DEFAULT_STALL_TIMEOUT
        )


def test_the_top_two_rungs_cannot_tie(config_file):
    """Rungs 1 and 2 both target one adapter, so no block can hold both.

    That is what stops the ladder needing a tie-break nobody could remember:
    the same-adapter rule refuses the pairing where the order would matter,
    and between *nested* blocks the ordinary rule applies -- the innermost
    decides, whichever kind of configuration it holds.
    """
    config_file(_LADDER_FILE)

    with pytest.raises(configuration.ConfigurationError, match="two configurations"):
        with dataretrieval.configure(
            NwdcConfiguration(concurrency=_LADDER_INSTANCE),
            NwdcConfiguration.load("tuned"),
        ):
            pass

    with dataretrieval.configure(NwdcConfiguration(concurrency=_LADDER_INSTANCE)):
        with dataretrieval.configure(NwdcConfiguration.load("tuned")):
            assert _nwdc_concurrency() == 12
    with dataretrieval.configure(NwdcConfiguration.load("tuned")):
        with dataretrieval.configure(NwdcConfiguration(concurrency=_LADDER_INSTANCE)):
            assert _nwdc_concurrency() == _LADDER_INSTANCE

    # "Rung 1 above rung 2" is a claim about one adapter, so a *package-wide*
    # instance is not the thing it is talking about: it targets no adapter at
    # all. Alongside a loaded profile in one block the adapter-scoped value is
    # the more specific of the two and wins for that adapter (ADR 0010), while
    # the package-wide value still governs every other adapter.
    with dataretrieval.configure(
        Configuration(concurrency=_LADDER_INSTANCE), NwdcConfiguration.load("tuned")
    ):
        assert _nwdc_concurrency() == 12
        assert configuration.concurrency(adapter="wqp") == _LADDER_INSTANCE


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

    loaded = WaterdataConfiguration.load("bulk")

    assert isinstance(loaded, WaterdataConfiguration)
    assert loaded.values() == {"concurrency": "unbounded", "parallel_chunks": 8}
    assert loaded.retries is configuration._UNSET


# --- show_configuration() reports profiles --------------------------------
#
# The report exists to answer "why is this call using that value?", so every
# row names the source that supplied it. A value from a profile is the case a
# bare "configure() block" label cannot distinguish: a configuration written in
# code and
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
    '>>> with dataretrieval.configure(WaterdataConfiguration.load("bulk")):\n'
    "...     dataretrieval.show_configuration()"
)


def _documented_sample() -> str:
    """The sample output embedded in ``show_configuration``'s docstring."""
    doc = inspect.getdoc(dataretrieval.show_configuration) or ""
    _, _, block = doc.partition(".. code-block:: text\n\n")
    return textwrap.dedent(block).strip("\n")


def test_show_configuration_names_the_profile_a_value_came_from(config_file):
    """A value from a profile is reported with that profile, not with "a block".

    ``WaterdataConfiguration.load("bulk")`` and ``WaterdataConfiguration(...)``
    enter the chain by the same route and are indistinguishable once their
    values are in the block, so a report that said only ``configure() block``
    left a caller who selected a profile they did not intend -- or who had
    forgotten a profile was selected at all -- with nothing to look at. The
    label is the table's own spelling, so it is greppable in the file that
    defines it.
    """
    config_file("[waterdata.bulk]\nconcurrency = 6\n")
    out = io.StringIO()

    with dataretrieval.configure(WaterdataConfiguration.load("bulk")):
        dataretrieval.show_configuration(stream=out)

    assert "configure() block [waterdata.bulk]" in out.getvalue()

    # A configuration written in code has no profile to name, so it names its
    # adapter alone rather than inventing one -- and the package-wide one
    # narrows to nothing, so it names neither.
    out = io.StringIO()
    with dataretrieval.configure(
        WaterdataConfiguration(concurrency=6), Configuration(retries=3)
    ):
        dataretrieval.show_configuration(stream=out)
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

    loaded = WaterdataConfiguration.load("bulk")
    written = WaterdataConfiguration(concurrency=6)

    assert loaded.profile == "bulk"
    assert written.profile is None
    assert "profile" not in loaded.settings()
    assert loaded == written


def test_show_configuration_lists_the_profiles_the_file_defines(
    config_file, monkeypatch
):
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
    monkeypatch.delitem(configuration._REGISTRY, "nldi", raising=False)
    config_file(
        "[ngwmn]\nconcurrency = 4\n\n"
        "[ngwmn.gentle]\nconcurrency = 2\n\n"
        "[waterdata.bulk]\nparallel_chunks = 8\n\n"
        "[nldi.gentle]\nretries = 1\n"
    )
    out = io.StringIO()

    dataretrieval.show_configuration(stream=out)
    text = out.getvalue()
    listed = text.split("profiles in the file: ", 1)[1].splitlines()[0]

    assert listed == "[waterdata.bulk], [ngwmn.gentle], [nldi.gentle]"
    # The adapter's *default* profile is not a named one: it is always in
    # effect and already shows up as a source, so listing it here is noise.
    assert "[ngwmn]" not in listed
    # Inert, and the report says so by never naming one as a source.
    assert "configure() block" not in text


def test_show_configuration_reports_an_unimported_adapter(config_file, monkeypatch):
    """An adapter this process cannot report on is named, never omitted.

    NLDI is imported on demand for the geopandas extra, so a process that has
    not touched it cannot say which settings it accepts -- the cost of
    validating an adapter's keys lazily (ADR 0011). Leaving it out of the
    report would read as "nothing is configured for nldi", which is a
    different claim from "this report could not check", and the caller cannot
    tell which one they are looking at.
    """
    config_file("")
    monkeypatch.delitem(configuration._REGISTRY, "nldi", raising=False)
    out = io.StringIO()

    dataretrieval.show_configuration(stream=out)
    text = out.getvalue()

    assert "not reported: nldi" in text
    assert "not imported" in text
    # An adapter that *was* imported is covered by the rows above, so it must
    # not be named as uncoverable.
    assert "waterdata" not in text.split("not reported:", 1)[1]

    # The line is a statement about this process, not about nldi: once the
    # module is imported its configuration registers and the caveat goes away.
    @dataclass(frozen=True)
    class _AsImported(configuration.BaseConfiguration):
        adapter: ClassVar[str] = "nldi"

        retries: int | None = configuration._UNSET

    monkeypatch.setitem(configuration._REGISTRY, "nldi", _AsImported)
    out = io.StringIO()
    dataretrieval.show_configuration(stream=out)
    assert "not reported" not in out.getvalue()


def test_show_configuration_sample_output_is_current(config_file, monkeypatch):
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
    monkeypatch.delitem(configuration._REGISTRY, "nldi", raising=False)

    out = io.StringIO()
    with dataretrieval.configure(WaterdataConfiguration.load("bulk")):
        dataretrieval.show_configuration(stream=out)
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
        / "configuration.rst"
    )
    if not guide.exists():  # pragma: no cover - docs are absent from an sdist
        pytest.skip("docs tree not present")
    block = textwrap.indent(f"{_SAMPLE_PROMPT}\n{actual}", "   ")
    assert block in guide.read_text(encoding="utf-8")


def test_show_configuration_survives_a_malformed_profile(config_file):
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

    dataretrieval.show_configuration(stream=out)  # must not raise

    listed = out.getvalue().split("profiles in the file: ", 1)[1].splitlines()[0]
    assert listed == "[waterdata.bulk], [ngwmn.gentle]"
    # Selecting one is where the grammar is checked, and it still is.
    with pytest.raises(configuration.ConfigurationError, match="integer"):
        WaterdataConfiguration.load("bulk")
    with pytest.raises(configuration.ConfigurationError, match="contains a table"):
        NgwmnConfiguration.load("gentle")


class TestConfigValueParsing:
    """The coercion layer between a config file / env var and a setting.

    Every one of these is a message a user reads while their config is not
    working, so each names the setting, what it expected, and what it got.
    """

    def test_a_non_numeric_stall_timeout_is_rejected_by_type(self):
        with pytest.raises(configuration.ConfigurationError) as excinfo:
            _core._coerce_seconds("soon", "stall_timeout", "")
        message = str(excinfo.value)
        assert "stall_timeout" in message
        assert "a number of seconds" in message

    def test_a_bool_is_not_a_number_of_seconds(self):
        """``True`` is an ``int`` in Python, so a bare isinstance check would
        accept ``stall_timeout = true`` and silently mean one second."""
        with pytest.raises(configuration.ConfigurationError):
            _core._coerce_seconds(True, "stall_timeout", "")

    def test_a_blank_count_falls_through_to_the_default(self):
        """An empty env var means "unset", not "zero" -- exporting an empty
        string is how a shell unsets a variable in practice."""
        assert _core._parse_int("   ", "API_USGS_RETRIES", default=4, minimum=0) == 4

    def test_a_blank_stall_timeout_falls_through_to_the_default(self):
        assert (
            _core._parse_seconds("  ", "API_USGS_STALL_TIMEOUT")
            == _core.DEFAULT_STALL_TIMEOUT
        )

    def test_a_blank_progress_toggle_is_refused_in_strict_mode(self):
        """A config file is strict: a blank value there is a typo, not an
        unset. The env path stays permissive for backwards compatibility."""
        with pytest.raises(configuration.ConfigurationError, match="must not be blank"):
            _core._parse_progress("", "progress", strict=True)

    def test_an_adapter_key_that_is_not_a_table_says_what_it_should_be(
        self, config_file
    ):
        """``[nldi]`` names an adapter, so ``nldi = 4`` at top level is a
        caller who meant a table; the message must say so rather than
        reporting an unknown setting."""
        config_file("nldi = 4\n")
        with pytest.raises(configuration.ConfigurationError) as excinfo:
            configuration.retries()
        message = str(excinfo.value)
        assert "[nldi]" in message
        assert "table of settings" in message


class TestConfigPathResolutionFailures:
    """The file layer sits on the per-request path, so a filesystem that will
    not answer must not take every query down with it."""

    def test_an_unresolvable_home_leaves_the_file_layer_inert(self, monkeypatch):
        """A container with no passwd entry raises from ``Path.home()``. The
        unexpanded ``~`` form is returned instead: it does not exist, so the
        file layer is empty, and the environment alone still works --
        which is how this package behaved before settings were layered."""
        monkeypatch.setattr(
            _core.Path,
            "home",
            staticmethod(lambda: (_ for _ in ()).throw(RuntimeError)),
        )
        assert _core._default_home_path() == pathlib.Path(
            "~/.dataretrieval/config.toml"
        )

    def test_a_missing_working_directory_is_a_configuration_error(self, monkeypatch):
        """A job that deletes its own cwd cannot resolve a relative
        DATARETRIEVAL_CONFIG. That must surface as this module's own error
        type rather than a bare OSError escaping onto the request path."""
        monkeypatch.setattr(
            _core.Path,
            "cwd",
            staticmethod(lambda: (_ for _ in ()).throw(OSError("gone"))),
        )
        with pytest.raises(configuration.ConfigurationError) as excinfo:
            _core._resolve_against_cwd(pathlib.Path("config.toml"))
        message = str(excinfo.value)
        assert "working directory is unavailable" in message
        assert _core.CONFIG_PATH_ENV in message

    def test_an_unreadable_config_file_names_the_path(self, tmp_path):
        missing = tmp_path / "nope.toml"
        with pytest.raises(configuration.ConfigurationError, match="could not read"):
            _core._read_file_content(missing)

    def test_the_home_memo_watches_the_variable_that_moves_the_path(self, monkeypatch):
        """``ntpath.expanduser`` ignores HOME and reads USERPROFILE, so on
        Windows the memo must watch USERPROFILE or it invalidates on a
        variable that cannot move the path and misses the one that can."""
        monkeypatch.setattr(_core.os, "name", "nt")
        monkeypatch.setenv("USERPROFILE", r"C:\Users\ada")
        monkeypatch.setenv("HOME", "/ignored")
        assert _core._home_id() == r"C:\Users\ada"

        monkeypatch.delenv("USERPROFILE")
        monkeypatch.setenv("HOMEDRIVE", "C:")
        monkeypatch.setenv("HOMEPATH", r"\Users\ada")
        assert _core._home_id() == r"C:\Users\ada"


def test_show_configuration_reports_an_unresolvable_path_as_the_file_row(
    monkeypatch,
):
    """A caller runs ``show_configuration`` precisely when their config is not
    behaving. If path resolution itself fails, raising out of the explainer
    withholds the one answer they came for."""
    monkeypatch.setattr(
        configuration,
        "config_path",
        lambda: (_ for _ in ()).throw(
            configuration.ConfigurationError("working directory is unavailable")
        ),
    )
    out = io.StringIO()
    dataretrieval.show_configuration(stream=out)
    text = out.getvalue()
    assert "config file  <unresolved:" in text
    assert "working directory is unavailable" in text
