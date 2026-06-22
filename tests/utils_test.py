"""Unit tests for functions in utils.py"""

from unittest import mock

import pandas as pd
import pytest

from dataretrieval import exceptions, nwis, utils


class Test_query:
    """Tests of the query function."""

    def test_url_too_long(self):
        """Test to confirm error when query URL too long.

        Test based on GitHub Issue #64.
        The server may respond with a 414 (converted to URLTooLong by query())
        or abruptly close the connection (a transport error, now wrapped as
        NetworkError). Both are valid responses to an excessively long URL.
        """
        # all sites in MD
        sites, _ = nwis.what_sites(stateCd="MD")
        # raise error by trying to query them all, so URL is way too long
        with pytest.raises((exceptions.URLTooLong, exceptions.NetworkError)):
            nwis.get_iv(sites=sites.site_no.values.tolist())

    def test_header(self):
        """Test checking header info with user-agent is part of query."""
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
        import pickle

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
            for revived in (pickle.loads(pickle.dumps(err)), copy.deepcopy(err)):
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


class Test_BaseMetadata:
    """Tests of BaseMetadata"""

    def test_init_with_response(self):
        response = mock.MagicMock()
        md = utils.BaseMetadata(response)

        # Test parameters initialized from the API response
        assert md.url is not None
        assert md.query_time is not None
        assert md.header is not None

        # Test NotImplementedError parameters
        with pytest.raises(NotImplementedError):
            _ = md.variable_info


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
        df = utils._attach_datetime_columns(df)
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
        df = utils._attach_datetime_columns(df)
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
        df = utils._attach_datetime_columns(df)
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
        df = utils._attach_datetime_columns(df)
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
