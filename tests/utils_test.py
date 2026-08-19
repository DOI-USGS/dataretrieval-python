"""Unit tests for functions in utils.py"""

from unittest import mock

import pandas as pd
import pytest

from dataretrieval import _querying, _wqx, exceptions, nwis, utils


class Test_Ambient:
    def test_implementation_keeps_documented_public_identity(self):
        from dataretrieval._ambient import Ambient

        assert utils.Ambient is Ambient
        assert Ambient.__module__ == "dataretrieval.utils"

    def test_scope_restores_previous_value(self):
        value = utils.Ambient("test_ambient", "default")

        with value("outer"):
            with value("inner"):
                assert value.get() == "inner"
            assert value.get() == "outer"
        assert value.get() == "default"


class Test_query:
    """Tests of the query function (mocked — no live NWIS calls)."""

    def test_url_too_long(self, httpx_mock):
        """A 413 / 414 from the service (an over-long query URL, Issue #64) is
        surfaced as the typed URLTooLong."""
        httpx_mock.add_response(method="GET", status_code=414)
        with pytest.raises(exceptions.URLTooLong):
            nwis.get_iv(sites=["01491000", "01491001"])

    def test_header(self, httpx_mock):
        """query() sends a User-Agent header and returns the response."""
        httpx_mock.add_response(method="GET", json={"value": {"timeSeries": []}})
        url = "https://waterservices.usgs.gov/nwis/dv"
        payload = {
            "format": "json",
            "startDT": "2010-10-01",
            "endDT": "2010-10-10",
            "sites": "01646500",
            "multi_index": True,
        }
        response = utils.query(url, payload)
        assert response.status_code == 200  # GET was successful
        assert "user-agent" in response.request.headers

    def test_no_sites_detection_respects_response_charset(self, httpx_mock):
        """The legacy sentinel is checked after HTTP charset decoding."""
        url = "https://example.invalid/x"
        body = "No sites/data found".encode("utf-16")
        httpx_mock.add_response(
            method="GET",
            url=url,
            content=body,
            headers={"Content-Type": "text/plain; charset=utf-16"},
        )

        with pytest.raises(exceptions.NoSitesError):
            utils.query(url, {})

    def test_query_does_not_opt_into_retry(self, httpx_mock, monkeypatch):
        """The public legacy adapter still surfaces the first transient failure."""
        url = "https://example.invalid/x"
        request_url = f"{url}?a=1"
        httpx_mock.add_response(method="GET", url=request_url, status_code=503)
        httpx_mock.add_response(method="GET", url=request_url, text="unexpected retry")
        monkeypatch.setenv("API_USGS_RETRIES", "1")

        with pytest.raises(exceptions.ServiceUnavailable):
            utils.query(url, {"a": "1"})

        assert len(httpx_mock.get_requests()) == 1

    def test_opt_in_retry_recovers_from_transient(self, httpx_mock, monkeypatch):
        """Active adapters can opt into bounded retry without changing NWIS."""
        import dataretrieval.transport.retry as retry

        url = "https://example.invalid/x"
        request_url = f"{url}?a=1"
        httpx_mock.add_response(method="GET", url=request_url, status_code=503)
        httpx_mock.add_response(method="GET", url=request_url, text="ok")
        monkeypatch.setenv("API_USGS_RETRIES", "1")
        monkeypatch.setattr(retry, "_RETRY_BASE_BACKOFF", 0.0)
        monkeypatch.setattr(retry, "_RETRY_MAX_BACKOFF", 0.0)

        response = _querying._query_with_retry(url, {"a": "1"})

        assert response.text == "ok"
        assert len(httpx_mock.get_requests()) == 2


class Test_error_taxonomy:
    """The unified request-error hierarchy.

    Every module's request failure is catchable as ``DataRetrievalError``.
    A status error is an ``HTTPError`` carrying ``.status_code`` (the retryable
    429 / 5xx subset is ``TransientError``); a connection failure is a
    ``NetworkError``. The sole base is ``DataRetrievalError`` -- no builtin
    (``ValueError`` / ``RuntimeError``) mixins.
    """

    @pytest.mark.parametrize(
        "status, exc_name",
        [
            (400, "HTTPError"),
            (403, "HTTPError"),
            (404, "HTTPError"),
            (429, "RateLimited"),
            (503, "ServiceUnavailable"),
        ],
    )
    def test_query_maps_status_to_typed_error(self, httpx_mock, status, exc_name):
        """``query`` maps each HTTP status to the right typed ``DataRetrievalError``:
        a generic ``HTTPError`` (carrying ``.status_code``) for a fatal 4xx, and
        the transient ``RateLimited`` / ``ServiceUnavailable`` for 429 / 5xx. The
        too-long-URL statuses (413 / 414) are covered separately because their
        message is the actionable remediation, not the bare status number."""
        exc_cls = getattr(exceptions, exc_name)
        url = "https://example.invalid/x"
        httpx_mock.add_response(method="GET", url=f"{url}?a=1", status_code=status)
        with pytest.raises(exc_cls, match=str(status)) as excinfo:
            utils.query(url, {"a": "1"})
        assert isinstance(excinfo.value, exceptions.DataRetrievalError)
        if isinstance(excinfo.value, exceptions.HTTPError):
            assert excinfo.value.status_code == status

    @pytest.mark.parametrize("status", [413, 414])
    def test_query_too_long_url_gives_actionable_message(self, httpx_mock, status):
        """A server 413 / 414 surfaces as ``URLTooLong`` carrying the actionable
        "Modify your query" remediation (the same message as the client-side
        over-long-URL path), not a bare ``HTTP 414`` status line."""
        url = "https://example.invalid/x"
        httpx_mock.add_response(method="GET", url=f"{url}?a=1", status_code=status)
        with pytest.raises(exceptions.URLTooLong, match="Modify your query") as excinfo:
            utils.query(url, {"a": "1"})
        assert isinstance(excinfo.value, exceptions.RequestTooLarge)

    def test_transport_error_wrapped_as_network_error(self, httpx_mock):
        """A connection-level failure (no HTTP response) surfaces as the typed
        ``NetworkError`` -- catchable via ``except DataRetrievalError`` like the
        response-based errors, with the original ``httpx`` exception on
        ``__cause__`` -- rather than leaking a raw ``httpx`` exception."""
        import httpx

        httpx_mock.add_exception(httpx.ConnectError("name resolution failed"))
        with pytest.raises(exceptions.NetworkError) as excinfo:
            utils.query("https://example.invalid/x", {"a": "1"})
        assert isinstance(excinfo.value, exceptions.DataRetrievalError)
        assert not isinstance(excinfo.value, exceptions.HTTPError)  # no status
        assert isinstance(excinfo.value.__cause__, httpx.ConnectError)

    def test_query_failure_catchable_as_base(self, httpx_mock):
        """A bare ``except DataRetrievalError`` catches a legacy query failure."""
        url = "https://example.invalid/y"
        httpx_mock.add_response(method="GET", url=f"{url}?a=1", status_code=400)
        with pytest.raises(exceptions.DataRetrievalError):
            utils.query(url, {"a": "1"})

    def test_uniform_retry_attributes_readable_on_every_error(self):
        """Every error exposes ``.status_code`` / ``.retry_after`` / ``.retryable``
        so a base ``except DataRetrievalError as e`` can branch and retry without
        an ``AttributeError`` on the types that lack a status (URLTooLong,
        NetworkError, NoSitesError, ...). ``.retryable`` marks the 429/5xx and
        connection failures."""
        import httpx

        # (error, status_code, retry_after, retryable)
        cases = [
            (exceptions.error_for_status(404, "x"), 404, None, False),
            (exceptions.error_for_status(429, "x", retry_after=5.0), 429, 5.0, True),
            (exceptions.error_for_status(503, "x"), 503, None, True),
            (exceptions.error_for_status(414, "x"), None, None, False),  # URLTooLong
            (exceptions.NetworkError("x"), None, None, True),
            (exceptions.NoSitesError(httpx.URL("https://x/y")), None, None, False),
            (exceptions.Unchunkable("x"), None, None, False),
        ]
        for err, status, retry_after, retryable in cases:
            assert err.status_code == status, err
            assert err.retry_after == retry_after, err
            assert err.retryable is retryable, err

    def test_no_sites_error_is_data_retrieval_error(self):
        """``NoSitesError`` (the legacy nwis no-data signal) roots at
        ``DataRetrievalError`` and is not a builtin ``ValueError``, so it is
        caught by the unified ``except dataretrieval.DataRetrievalError``."""
        assert issubclass(exceptions.NoSitesError, exceptions.DataRetrievalError)
        assert not issubclass(exceptions.NoSitesError, ValueError)
        import dataretrieval

        assert dataretrieval.NoSitesError is exceptions.NoSitesError

    def test_typed_errors_survive_pickle_and_deepcopy(self):
        """Typed errors round-trip through pickle/deepcopy -- they get pickled
        back from multiprocessing / lithops workers, and their constructor fields
        (status_code, retry_after, url) must survive the trip."""
        import copy
        import pickle  # noqa: S403 - testing trusted compatibility round-trips

        import httpx

        samples = [
            exceptions.error_for_status(404, "not found"),  # bare HTTPError
            exceptions.error_for_status(429, "slow down", retry_after=5.0),
            exceptions.error_for_status(503, "down"),
            exceptions.TransientError("boom", status_code=502, retry_after=1.5),
            exceptions.NoSitesError(httpx.URL("https://example.invalid/x?a=1")),
            exceptions.NetworkError("could not reach the service"),
        ]
        for err in samples:
            for revived in (
                pickle.loads(pickle.dumps(err)),  # noqa: S301 - created above
                copy.deepcopy(err),
            ):
                assert type(revived) is type(err)
                assert str(revived) == str(err)
                if isinstance(err, exceptions.HTTPError):
                    assert revived.status_code == err.status_code
                if isinstance(err, exceptions.TransientError):
                    assert revived.retry_after == err.retry_after
                if isinstance(err, exceptions.NoSitesError):
                    assert revived.url == err.url

    def test_waterdata_exceptions_share_the_root(self):
        """waterdata's typed exceptions are ``DataRetrievalError`` too, so one
        ``except`` clause spans the legacy and waterdata subsystems, and they
        slot under the shared family bases (``HTTPError`` / ``TransientError`` /
        ``RequestTooLarge``)."""
        from dataretrieval.exceptions import (
            RateLimited,
            ServiceUnavailable,
            Unchunkable,
        )
        from dataretrieval.ogc.interruptions import ChunkInterrupted

        for cls in (RateLimited, ServiceUnavailable, Unchunkable, ChunkInterrupted):
            assert issubclass(cls, exceptions.DataRetrievalError)
        # Transient 429/5xx: an HTTPError-with-status, under TransientError.
        assert issubclass(RateLimited, exceptions.TransientError)
        assert issubclass(ServiceUnavailable, exceptions.TransientError)
        assert issubclass(ServiceUnavailable, exceptions.HTTPError)
        # "Too large" failures slot under RequestTooLarge.
        assert issubclass(Unchunkable, exceptions.RequestTooLarge)

    def test_base_exported_at_top_level(self):
        """Users can write ``except dataretrieval.DataRetrievalError``."""
        import dataretrieval

        assert dataretrieval.DataRetrievalError is exceptions.DataRetrievalError

    def test_chunk_interruptions_exported_at_top_level(self):
        """The resumable chunk-interruption exceptions are reachable from the
        top level (``from dataretrieval import ChunkInterrupted``) instead of
        only the internal ``dataretrieval.ogc.interruptions`` module, and
        resolve to the same classes."""
        import dataretrieval
        from dataretrieval.ogc import interruptions

        for name in ("ChunkInterrupted", "QuotaExhausted", "ServiceInterrupted"):
            assert getattr(dataretrieval, name) is getattr(interruptions, name)
            assert name in dataretrieval.__all__
        assert issubclass(dataretrieval.QuotaExhausted, dataretrieval.ChunkInterrupted)
        assert issubclass(
            dataretrieval.ServiceInterrupted, dataretrieval.ChunkInterrupted
        )
        assert issubclass(
            dataretrieval.ChunkInterrupted, dataretrieval.DataRetrievalError
        )

    def test_parallel_chunks_exported_at_top_level_and_waterdata(self):
        """The ``parallel_chunks`` context manager is reachable both from the top
        level (``from dataretrieval import parallel_chunks``) and from the
        user-facing ``dataretrieval.waterdata`` namespace, and both resolve to
        the single object defined in ``dataretrieval.ogc.chunking``."""
        import dataretrieval
        from dataretrieval import waterdata
        from dataretrieval.ogc import chunking

        assert dataretrieval.parallel_chunks is chunking.parallel_chunks
        assert waterdata.parallel_chunks is chunking.parallel_chunks
        assert "parallel_chunks" in dataretrieval.__all__
        assert "parallel_chunks" in waterdata.__all__


class Test_BaseMetadata:
    """Tests of BaseMetadata"""

    def test_init_with_response(self):
        response = mock.MagicMock()
        md = utils.BaseMetadata(response)

        # Test parameters initialized from the API response
        assert md.url is not None
        assert md.query_time is not None
        assert md.header is not None

        # site_info is abstract on BaseMetadata; only nwis/wqp implement it.
        with pytest.raises(NotImplementedError):
            _ = md.site_info

    def test_pickle_keeps_historical_import_path(self):
        """New pickles remain readable by releases predating the class move."""
        import pickle  # noqa: S403 - testing trusted compatibility round-trips

        from dataretrieval._response_metadata import BaseMetadata

        md = BaseMetadata.__new__(BaseMetadata)
        md.url = "https://example.test"
        md.query_time = None
        md.header = {}
        md.comment = None

        payload = pickle.dumps(md)

        assert b"dataretrieval.utils" in payload
        revived = pickle.loads(payload)  # noqa: S301 - payload created above
        assert revived.__class__ is BaseMetadata


class Test_to_str:
    """Tests of the to_str function."""

    def test_to_str_list(self):
        assert utils.to_str([1, "a", 2]) == "1,a,2"

    def test_to_str_tuple(self):
        assert utils.to_str((1, "b", 3)) == "1,b,3"

    def test_to_str_set(self):
        # Sets are unordered, so we check if elements are present
        result = utils.to_str({1, 2})
        assert "1" in result
        assert "2" in result
        assert "," in result

    def test_to_str_generator(self):
        def gen():
            yield from [1, 2, 3]

        assert utils.to_str(gen()) == "1,2,3"

    def test_to_str_pandas_series(self):
        s = pd.Series([10, 20])
        assert utils.to_str(s) == "10,20"

    def test_to_str_pandas_index(self):
        idx = pd.Index(["x", "y"])
        assert utils.to_str(idx) == "x,y"

    def test_to_str_string(self):
        assert utils.to_str("already a string") == "already a string"

    def test_to_str_custom_delimiter(self):
        assert utils.to_str([1, 2, 3], delimiter="|") == "1|2|3"

    def test_to_str_non_iterable(self):
        assert utils.to_str(123) is None


class Test_attach_datetime_columns:
    """Tests of _attach_datetime_columns, which derives <prefix>DateTime UTC
    columns from Date/Time/TimeZone triplets in Samples and WQP CSVs."""

    def test_wqx3_triplet_resolves_to_utc(self):
        df = pd.DataFrame(
            {
                "Activity_StartDate": ["2024-01-09", "2024-02-15"],
                "Activity_StartTime": ["10:00:00", "14:30:00"],
                "Activity_StartTimeZone": ["PST", "EST"],
            }
        )
        df = _wqx._attach_datetime_columns(df)
        assert df["Activity_StartDateTime"][0] == pd.Timestamp(
            "2024-01-09 18:00:00", tz="UTC"
        )
        assert df["Activity_StartDateTime"][1] == pd.Timestamp(
            "2024-02-15 19:30:00", tz="UTC"
        )
        assert df["Activity_StartTimeZone"].tolist() == ["PST", "EST"]

    def test_legacy_wqp_triplet_resolves_to_utc(self):
        df = pd.DataFrame(
            {
                "ActivityStartDate": ["2024-01-09"],
                "ActivityStartTime/Time": ["10:00:00"],
                "ActivityStartTime/TimeZoneCode": ["PST"],
            }
        )
        df = _wqx._attach_datetime_columns(df)
        assert df["ActivityStartDateTime"][0] == pd.Timestamp(
            "2024-01-09 18:00:00", tz="UTC"
        )

    def test_unknown_timezone_is_NaT(self):
        df = pd.DataFrame(
            {
                "Activity_StartDate": ["2024-01-09"],
                "Activity_StartTime": ["10:00:00"],
                "Activity_StartTimeZone": ["BOGUS"],
            }
        )
        df = _wqx._attach_datetime_columns(df)
        assert df["Activity_StartDateTime"].isna().all()

    def test_existing_datetime_column_not_overwritten(self):
        df = pd.DataFrame(
            {
                "Activity_StartDate": ["2024-01-09"],
                "Activity_StartTime": ["10:00:00"],
                "Activity_StartTimeZone": ["PST"],
                "Activity_StartDateTime": ["preexisting"],
            }
        )
        df = _wqx._attach_datetime_columns(df)
        assert df["Activity_StartDateTime"].tolist() == ["preexisting"]


class Test_to_state:
    """Tests of the shared state normalizer in ``codes.states``."""

    def test_accepts_every_encoding(self):
        from dataretrieval.codes.states import to_state

        # name (any case), postal (any case), bare FIPS, and prefixed FIPS all
        # resolve to the same canonical full name.
        for value in ("Wisconsin", "wisconsin", "WI", "wi", "55", "US:55"):
            assert to_state(value) == "Wisconsin"

    def test_converts_to_each_representation(self):
        from dataretrieval.codes.states import to_state

        assert to_state("WI", "name") == "Wisconsin"
        assert to_state("Wisconsin", "postal") == "WI"
        assert to_state("Wisconsin", "fips") == "55"
        assert to_state("Wisconsin", "fips_us") == "US:55"
        # Conversion is independent of the input encoding.
        assert to_state("55", "postal") == "WI"
        assert to_state("wi", "fips_us") == "US:55"

    def test_rejects_unrecognized_state(self):
        from dataretrieval.codes.states import to_state

        for bad in ("XX", "99", "US:99", "Wisconson"):
            with pytest.raises(ValueError, match="not a recognized US state"):
                to_state(bad)

    def test_rejects_unknown_target(self):
        from dataretrieval.codes.states import to_state

        with pytest.raises(ValueError, match="to must be"):
            to_state("WI", "zipcode")

    def test_resolves_an_iterable_element_wise(self):
        from dataretrieval.codes.states import to_state

        # An iterable of mixed encodings returns a list, converted element-wise.
        assert to_state(["WI", "Minnesota", "39"]) == [
            "Wisconsin",
            "Minnesota",
            "Ohio",
        ]
        assert to_state(["WI", "CA"], "fips_us") == ["US:55", "US:06"]
        # A bad element fails the whole call (fail-fast).
        with pytest.raises(ValueError, match="not a recognized US state"):
            to_state(["WI", "XX"])


def test_retrying_get_maps_invalid_url(monkeypatch):
    """Direct active-service GETs do not leak raw httpx InvalidURL errors."""
    import httpx

    monkeypatch.setattr(
        _querying,
        "_get",
        mock.Mock(side_effect=httpx.InvalidURL("invalid URL")),
    )

    with pytest.raises(exceptions.URLTooLong):
        _querying._get_with_retry("https://example.invalid")
