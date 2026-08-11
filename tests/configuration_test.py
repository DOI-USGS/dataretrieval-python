"""Tests for layered configuration resolution (``dataretrieval.configuration``)."""

from __future__ import annotations

import asyncio
import io
import os
import re
import threading
from dataclasses import dataclass
from typing import ClassVar

import pytest

import dataretrieval
from dataretrieval import configuration
from dataretrieval.configuration import Configuration
from dataretrieval.ngwmn import NgwmnConfiguration
from dataretrieval.nwdc import NwdcConfiguration
from dataretrieval.utils import _default_headers
from dataretrieval.waterdata import WaterdataConfiguration
from dataretrieval.wqp import WqpConfiguration

WATERDATA_URL = "https://api.waterdata.usgs.gov/ogcapi/v0/collections/daily/items"


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
    """A bad value raises where it was written, not inside a later request.

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
    """Every tier below a passed configuration still applies, per setting."""
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
    """A name the caller just typed is a typo, not a silent fall-through."""
    config_file("[waterdata]\nconcurrency = 4\n\n[waterdata.bulk]\nretries = 8\n")
    with pytest.raises(configuration.ConfigurationError, match="no .waterdata.nope."):
        WaterdataConfiguration.load("nope")


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
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, "/dev/null")
    configuration._reset_file_cache()
    assert configuration.api_key() is None
    assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY


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
    a writer appeared).
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

    Which variable that is is platform-specific: ``ntpath.expanduser`` reads
    ``USERPROFILE`` and ignores ``HOME``, so setting ``HOME`` on Windows moves
    nothing and this asserted against the runner's real home directory.
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
    """A bad value in a profile nobody selected must not fail every request.

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
    """The blast-radius rule, on the tier a whole adapter table sits in.

    Keys are checked when *that* adapter first resolves a setting, so a bad
    value in ``[nldi]`` costs a Water Data call nothing -- which is also what
    lets an adapter's vocabulary live in a module this leaf cannot import.
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
    """Precedence is source-major: the env tier is above the file tier.

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
    it by name -- the worst outcome for a module whose job is to be
    trustworthy about what a call will use would be to take it and drop it.
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
    imports would reject a perfectly good ``[nldi]`` table until something
    happened to import that module.
    """
    monkeypatch.delitem(configuration._REGISTRY, "nldi", raising=False)
    assert configuration.settings_for("nldi") is None
    assert "nldi" in configuration.ADAPTERS


def test_every_adapter_is_actually_wired_to_a_read_site():
    """A schema nothing passes is worse than no schema.

    ``show_configuration()`` would report a ``[nwis]`` override as live while
    every call ignored it -- the report whose whole job is answering "what will
    this call use" being confidently wrong. Importability is the weaker half of
    the invariant: it passed while ``waterdata.get_cql``, eight of nine WQP
    getters, and all of ``nwis`` silently resolved package-wide.
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
