"""Unit tests for functions in utils.py"""

from unittest import mock

import pandas as pd
import pytest

from dataretrieval import nwis, utils


class Test_query:
    """Tests of the query function."""

    def test_url_too_long(self):
        """Test to confirm error when query URL too long.

        Test based on GitHub Issue #64.
        The server may respond with a 414 (converted to ValueError by query())
        or abruptly close the connection (ConnectionError). Both are valid
        responses to an excessively long URL.
        """
        import requests as req

        # all sites in MD
        sites, _ = nwis.what_sites(stateCd="MD")
        # raise error by trying to query them all, so URL is way too long
        with pytest.raises((ValueError, req.exceptions.ConnectionError)):
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

    def test_does_not_mutate_caller_payload(self, requests_mock):
        """`query` must not mutate the caller's payload dict.

        Regression: previously the function did
        ``payload[key] = to_str(value, delimiter)`` in place, so list
        values were overwritten with their stringified joins after the
        call returned.
        """
        url = "https://example.com/svc"
        requests_mock.get(url, text="ok")
        payload = {"sites": ["A", "B"], "stateCd": "MD"}
        original = dict(payload)

        utils.query(url, payload)

        assert payload == original
        assert payload["sites"] is original["sites"]

    @pytest.mark.parametrize("status", [401, 403, 405, 408, 429, 501, 504])
    def test_unhandled_4xx_5xx_raises(self, requests_mock, status):
        """`query` must surface every 4xx/5xx, not just the few it formats.

        Regression: codes outside {400, 404, 414, 500, 502, 503} used to
        pass through silently — callers received the error body as if
        it were data.
        """
        url = "https://example.com/svc"
        requests_mock.get(url, status_code=status, text="<html>denied</html>")
        with pytest.raises(ValueError, match=rf"HTTP {status}\b"):
            utils.query(url, {"k": "v"})


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
