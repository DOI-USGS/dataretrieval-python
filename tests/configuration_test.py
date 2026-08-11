"""Tests for layered configuration resolution (``dataretrieval.configuration``)."""

from __future__ import annotations

import asyncio
import io
import os
import re
import threading

import pytest

import dataretrieval
from dataretrieval import configuration
from dataretrieval.utils import _default_headers

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
    with dataretrieval.configure(api_key="block-key"):
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
    with dataretrieval.configure(api_key="outer", concurrency=4):
        with dataretrieval.configure(concurrency=8):
            assert configuration.concurrency() == 8
            assert configuration.api_key() == "outer"  # inherited from the outer block
        assert configuration.concurrency() == 4  # inner block restored on exit


def test_omitted_setting_inherits_lower_source(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    with dataretrieval.configure(concurrency=2):
        assert configuration.api_key() == "env-key"


def test_explicit_none_suppresses_lower_sources(monkeypatch):
    monkeypatch.setenv("API_USGS_PAT", "env-key")
    monkeypatch.setenv("API_USGS_CONCURRENT", "4")
    monkeypatch.setenv("API_USGS_PROGRESS", "true")
    with dataretrieval.configure(api_key=None, concurrency=None, progress=None):
        assert configuration.api_key() is None
        assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY
        assert configuration.progress() is None
    assert configuration.api_key() == "env-key"
    assert configuration.concurrency() == 4
    assert configuration.progress() is True


def test_block_validates_eagerly():
    """A bad value raises at the ``with``, not inside a later request."""
    with pytest.raises(configuration.ConfigurationError):
        with dataretrieval.configure(concurrency=0):
            pass
    with pytest.raises(configuration.ConfigurationError):
        with dataretrieval.configure(retries=-1):
            pass
    with pytest.raises(configuration.ConfigurationError):
        with dataretrieval.configure(parallel_chunks=0):
            pass
    with pytest.raises(configuration.ConfigurationError):
        with dataretrieval.configure(progress="flase"):
            pass


@pytest.mark.parametrize(
    ("kwargs", "expected"),
    [
        ({"api_key": 123}, "string"),
        ({"concurrency": 1.5}, "integer"),
        ({"concurrency": "8"}, "integer"),
        ({"retries": "2"}, "integer"),
        ({"progress": []}, "bool"),
        ({"parallel_chunks": True}, "integer"),
        ({"profile": 123}, "string"),
    ],
)
def test_block_rejects_values_outside_annotated_types(kwargs, expected):
    with pytest.raises(configuration.ConfigurationError, match=expected):
        with dataretrieval.configure(**kwargs):
            pass


def test_block_accepts_ints_and_strings():
    with dataretrieval.configure(concurrency="unbounded"):
        assert configuration.concurrency() is None
    with dataretrieval.configure(concurrency=8):
        assert configuration.concurrency() == 8
    with dataretrieval.configure(progress=False):
        assert configuration.progress() is False
    with dataretrieval.configure(progress=True):
        assert configuration.progress() is True


# --- isolation (the point of issue #352) ---------------------------------


def test_threads_do_not_leak_credentials_into_each_other():
    """Two threads in different blocks see different keys.

    This is the concurrency complaint in #352: ``os.environ`` is
    process-global, so it cannot express this.
    """
    seen: dict[str, str | None] = {}
    started = threading.Barrier(2)

    def worker(name: str, key: str) -> None:
        with dataretrieval.configure(api_key=key):
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
        with dataretrieval.configure(api_key=key):
            await asyncio.sleep(0)  # yield, letting the other task interleave
            return configuration.api_key()

    async def main() -> list[str | None]:
        return list(await asyncio.gather(worker("key-a"), worker("key-b")))

    assert asyncio.run(main()) == ["key-a", "key-b"]


# --- the file ------------------------------------------------------------


def test_profile_layers_over_top_level(config_file, monkeypatch):
    config_file(
        'api_key = "shared"\nconcurrency = 4\n\n'
        '[profiles.bulk]\nconcurrency = "unbounded"\n'
    )
    with dataretrieval.configure(profile="bulk"):
        assert configuration.concurrency() is None  # from the profile
        assert configuration.api_key() == "shared"  # inherited from the top level
    assert configuration.concurrency() == 4  # outside the block, top level again


def test_profile_selected_by_env(config_file, monkeypatch):
    config_file("concurrency = 4\n\n[profiles.bulk]\nconcurrency = 16\n")
    monkeypatch.setenv(configuration.PROFILE_ENV, "bulk")
    assert configuration.concurrency() == 16


def test_block_profile_outranks_env_profile(config_file, monkeypatch):
    config_file("[profiles.a]\nconcurrency = 2\n\n[profiles.b]\nconcurrency = 3\n")
    monkeypatch.setenv(configuration.PROFILE_ENV, "a")
    with dataretrieval.configure(profile="b"):
        assert configuration.concurrency() == 3


def test_none_profile_selects_top_level(config_file, monkeypatch):
    config_file("concurrency = 4\n\n[profiles.bulk]\nconcurrency = 16\n")
    monkeypatch.setenv(configuration.PROFILE_ENV, "bulk")
    with dataretrieval.configure(profile=None):
        assert configuration.concurrency() == 4
    assert configuration.concurrency() == 16


def test_unknown_profile_raises(config_file):
    config_file("concurrency = 4\n")
    with pytest.raises(configuration.ConfigurationError, match="not defined"):
        with dataretrieval.configure(profile="nope"):
            pass


def test_selected_profile_is_ignored_when_there_is_no_file(tmp_path, monkeypatch):
    """A lingering ``DATARETRIEVAL_PROFILE`` must not break every request.

    With no config file there are no profiles to select from and the whole
    file layer is inert, so the selection is moot rather than a typo. Raising
    here would surface from ``_default_headers`` on every call — including
    legacy services that never read the configuration.
    """
    monkeypatch.delenv("API_USGS_CONCURRENT")  # pinned by the autouse fixture
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, str(tmp_path / "absent.toml"))
    monkeypatch.setenv(configuration.PROFILE_ENV, "long-gone")
    configuration._reset_file_cache()

    assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY
    assert _default_headers(WATERDATA_URL)["User-Agent"].startswith(
        "python-dataretrieval/"
    )


def test_profile_typed_into_configure_is_checked_even_with_no_file(
    tmp_path, monkeypatch
):
    """An explicitly named profile is a typo to report, not ambient state.

    The leniency above is for a *lingering export*, which the caller may not
    even know is set. A name passed to ``configure`` was just typed, and
    silently proceeding on defaults would drop exactly the settings the caller
    asked for — so it raises at the ``with``, as that function documents.
    """
    monkeypatch.delenv("API_USGS_CONCURRENT")  # pinned by the autouse fixture
    monkeypatch.setenv(configuration.CONFIG_PATH_ENV, str(tmp_path / "absent.toml"))
    monkeypatch.delenv(configuration.PROFILE_ENV, raising=False)
    configuration._reset_file_cache()

    with pytest.raises(configuration.ConfigurationError, match="no configuration file"):
        with dataretrieval.configure(profile="also-gone"):
            pass


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
    """A profile written as ``[bulk]`` instead of ``[profiles.bulk]``."""
    config_file("[bulk]\nconcurrency = 4\n")
    with pytest.raises(configuration.ConfigurationError, match="unknown table"):
        configuration.concurrency()


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
    with dataretrieval.configure(api_key="block-key"):
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

    with dataretrieval.configure(retries=3):
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
    """``parallel_chunks(n)`` is sugar for ``configure(parallel_chunks=n)``.

    They must not be two competing scopes: whichever block is innermost wins,
    so ``show_configuration()`` always reports the value the chunker will use.
    """
    from dataretrieval.ogc.chunking import parallel_chunks

    with parallel_chunks(2):
        with dataretrieval.configure(parallel_chunks=8):
            assert configuration.parallel_chunks() == 8
        assert configuration.parallel_chunks() == 2

    with dataretrieval.configure(parallel_chunks=8):
        with parallel_chunks(2):
            assert configuration.parallel_chunks() == 2
        assert configuration.parallel_chunks() == 8


def test_parallel_chunks_has_no_environment_variable():
    """It spends quota, so it is deliberately file/block-only (see ENV_VARS)."""
    assert "parallel_chunks" not in configuration.ENV_VARS
    assert "parallel_chunks" in configuration.SETTINGS


def test_progress_reporter_reads_the_block():
    from dataretrieval.progress import ProgressReporter

    with dataretrieval.configure(progress=True):
        assert ProgressReporter(stream=io.StringIO()).enabled
    with dataretrieval.configure(progress=False):
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


def test_parallel_chunks_in_a_profile_does_not_warn(config_file, recwarn):
    config_file("[profiles.bulk]\nparallel_chunks = 8\n")
    with dataretrieval.configure(profile="bulk"):
        assert configuration.parallel_chunks() == 8
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
    assert set(configuration._DISPLAYS) == set(configuration.SETTINGS)


def test_unselected_profile_is_not_validated(config_file):
    """A bad value in a profile nobody selected must not fail every request.

    Profile tables are kept raw at parse time and validated only when one is
    actually selected -- the same blast-radius rule ``_default_headers``
    follows for the key itself.
    """
    config_file('api_key = "good"\n\n[profiles.experimental]\nconcurrency = 0\n')
    assert _default_headers(WATERDATA_URL)["X-Api-Key"] == "good"
    assert configuration.concurrency() == configuration.DEFAULT_CONCURRENCY

    # Selecting it still reports the problem.
    with pytest.raises(configuration.ConfigurationError, match="experimental"):
        with dataretrieval.configure(profile="experimental"):
            configuration.concurrency()


def test_unknown_setting_in_an_unselected_profile_is_silent(config_file, recwarn):
    config_file("concurrency = 4\n\n[profiles.other]\nnot_a_setting = 1\n")
    assert configuration.concurrency() == 4
    assert not [w for w in recwarn if "unknown setting" in str(w.message)]


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
        ngwmn={"concurrency": 2}, nwdc={"concurrency": 8}, retries=7
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

    with dataretrieval.configure(concurrency=16, ngwmn={"concurrency": 2}):
        assert configuration.concurrency(adapter="ngwmn") == 2
        assert configuration.concurrency(adapter="waterdata") == 16


def test_adapter_rejects_a_setting_it_does_not_read(config_file):
    """A single-shot adapter has nothing to fan out, so ``concurrency`` is a typo."""
    with pytest.raises(configuration.ConfigurationError, match="not a setting the wqp"):
        with dataretrieval.configure(wqp={"concurrency": 2}):
            pass

    config_file("[wqp]\nconcurrency = 2\n")
    with pytest.raises(
        configuration.ConfigurationError, match="not a setting that table"
    ):
        configuration.retries(adapter="wqp")


def test_api_key_is_never_adapter_scoped(config_file):
    """The key belongs to the gateway fronting a host, not to an adapter.

    Water Data and NGWMN are two adapters on one host sharing one key and one
    quota pool, so a per-adapter key would model a distinction that does not
    exist (ADR 0010).
    """
    assert not any("api_key" in s for s in configuration.ADAPTER_SETTINGS.values())

    with pytest.raises(
        configuration.ConfigurationError, match="not a setting the ngwmn"
    ):
        with dataretrieval.configure(ngwmn={"api_key": "x"}):
            pass


def test_a_misspelled_setting_is_not_taken_for_an_adapter():
    """``**adapters`` is a catch-all, so a typo must not be silently swallowed.

    Accepting ``configure(concurrancy=8)`` as an unknown adapter and ignoring
    it would be the worst outcome for a module whose job is to be trustworthy
    about what a call will use.
    """
    with pytest.raises(configuration.ConfigurationError, match="unexpected keyword"):
        with dataretrieval.configure(concurrancy=8):
            pass


def test_adapter_schema_names_a_real_module():
    """Every key in the registry names a module a caller can import."""
    import importlib

    for adapter in configuration.ADAPTERS:
        importlib.import_module(f"dataretrieval.{adapter}")


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
        "drop it from ADAPTER_SETTINGS."
    )


def test_adapter_table_inside_a_profile_is_refused(config_file):
    """``[profiles.x.ngwmn]`` reads as composable and is not; say so."""
    config_file(
        "[profiles.gentle]\nconcurrency = 2\n\n"
        "[profiles.gentle.ngwmn]\nconcurrency = 1\n"
    )

    # Raised at ``with`` entry, where ``configure`` validates the profile it
    # was handed -- before any request, not from inside one.
    with pytest.raises(configuration.ConfigurationError, match="adapter table inside"):
        with dataretrieval.configure(profile="gentle"):
            pass


def test_a_non_finite_stall_timeout_is_refused(config_file):
    """``inf`` parses as a float and silently disables the bound it sets."""
    config_file("")

    for bad in (float("inf"), float("nan")):
        with pytest.raises(configuration.ConfigurationError, match="finite"):
            with dataretrieval.configure(stall_timeout=bad):
                pass


def test_stall_timeout_resolves_through_the_chain(config_file, monkeypatch):
    """It was read straight from os.environ, so a block and the file were mute."""
    config_file("stall_timeout = 15\n\n[wqp]\nstall_timeout = 300\n")

    assert configuration.stall_timeout() == 15
    assert configuration.stall_timeout(adapter="wqp") == 300

    monkeypatch.setenv("API_USGS_STALL_TIMEOUT", "42")
    assert configuration.stall_timeout() == 42

    with dataretrieval.configure(stall_timeout=2.5):
        assert configuration.stall_timeout() == 2.5


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

    with dataretrieval.configure(waterdata={"concurrency": 32}):
        with dataretrieval.configure(concurrency=1):
            assert configuration.concurrency(adapter="waterdata") == 1
        assert configuration.concurrency(adapter="waterdata") == 32


def test_adapter_scope_still_wins_within_one_block(config_file):
    """Depth breaks ties between blocks, never within one."""
    config_file("")

    with dataretrieval.configure(concurrency=16, waterdata={"concurrency": 4}):
        assert configuration.concurrency(adapter="waterdata") == 4
        assert configuration.concurrency(adapter="wqp") == 16


def test_parallel_chunks_block_survives_an_adapter_scoped_outer_block():
    """``parallel_chunks(n)`` is a per-call request and must not be discarded.

    It delegates to ``configure(parallel_chunks=n)``, which writes the
    package-wide key -- so before depth was tracked, any enclosing
    ``configure(waterdata={"parallel_chunks": ...})`` silently outranked it.
    """
    from dataretrieval.waterdata import parallel_chunks

    with dataretrieval.configure(waterdata={"parallel_chunks": 2}):
        with parallel_chunks(16):
            assert configuration.parallel_chunks(adapter="waterdata") == 16
        assert configuration.parallel_chunks(adapter="waterdata") == 2
