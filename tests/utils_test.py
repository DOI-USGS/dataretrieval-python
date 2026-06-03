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
        The server may respond with a 414 (converted to ValueError by query())
        or abruptly close the connection (ConnectionError). Both are valid
        responses to an excessively long URL.
        """
        import httpx

        # all sites in MD
        sites, _ = nwis.what_sites(stateCd="MD")
        # raise error by trying to query them all, so URL is way too long
        with pytest.raises((ValueError, httpx.ConnectError)):
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

    Every module's request failures are catchable as ``DataRetrievalError``,
    while remaining backward-compatible with the built-in type each path
    historically raised (``ValueError`` for the legacy ``query`` path,
    ``RuntimeError`` for the waterdata retryable types).
    """

    @pytest.mark.parametrize(
        "status, exc_name, match, builtin",
        [
            (400, "BadRequestError", "Bad Request", ValueError),
            (404, "NotFoundError", "Page Not Found", ValueError),
            (414, "URLTooLong", "Request URL too long", ValueError),
            (503, "ServiceUnavailable", "Service Unavailable: 503", RuntimeError),
        ],
    )
    def test_query_maps_status_to_typed_error(
        self, httpx_mock, status, exc_name, match, builtin
    ):
        """``query`` maps each HTTP status family to a typed error that is both a
        ``DataRetrievalError`` (new, unified) and the built-in this path
        historically raised for that kind of failure -- ``ValueError`` for a bad
        request, ``RuntimeError`` for a transient 5xx -- with the message kept."""
        exc_cls = getattr(exceptions, exc_name)
        url = "https://example.invalid/x"
        httpx_mock.add_response(method="GET", url=f"{url}?a=1", status_code=status)
        with pytest.raises(exc_cls, match=match) as excinfo:
            utils.query(url, {"a": "1"})
        assert isinstance(excinfo.value, exceptions.DataRetrievalError)
        assert isinstance(excinfo.value, builtin)  # backward compatibility

    def test_query_failure_catchable_as_base(self, httpx_mock):
        """A bare ``except DataRetrievalError`` catches a legacy query failure."""
        url = "https://example.invalid/y"
        httpx_mock.add_response(method="GET", url=f"{url}?a=1", status_code=400)
        with pytest.raises(exceptions.DataRetrievalError):
            utils.query(url, {"a": "1"})

    def test_no_sites_error_is_data_retrieval_error(self):
        """``NoSitesError`` joins the root (was a bare ``Exception``)."""
        assert issubclass(exceptions.NoSitesError, exceptions.DataRetrievalError)
        assert not issubclass(exceptions.NoSitesError, ValueError)  # unchanged

    def test_waterdata_exceptions_share_the_root(self):
        """waterdata's typed exceptions are ``DataRetrievalError`` too, so one
        ``except`` clause spans the legacy and waterdata subsystems — while
        keeping their historical ``RuntimeError`` / ``ValueError`` bases and the
        shared family bases (``TransientError``, ``RequestTooLarge``)."""
        from dataretrieval.waterdata.chunking import (
            ChunkInterrupted,
            RateLimited,
            ServiceUnavailable,
            Unchunkable,
        )

        for cls in (RateLimited, ServiceUnavailable, Unchunkable, ChunkInterrupted):
            assert issubclass(cls, exceptions.DataRetrievalError)
        # Transient transport failures: RuntimeError, under TransientError.
        assert issubclass(RateLimited, exceptions.TransientError)
        assert issubclass(ServiceUnavailable, exceptions.TransientError)
        assert issubclass(ServiceUnavailable, RuntimeError)
        # "Too large" failures: ValueError, under RequestTooLarge.
        assert issubclass(Unchunkable, exceptions.RequestTooLarge)
        assert issubclass(Unchunkable, ValueError)

    def test_base_exported_at_top_level(self):
        """Users can write ``except dataretrieval.DataRetrievalError``."""
        import dataretrieval

        assert dataretrieval.DataRetrievalError is exceptions.DataRetrievalError


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
