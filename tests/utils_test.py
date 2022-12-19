"""Unit tests for functions in utils.py"""
import pytest
from dataretrieval import utils
import dataretrieval.nwis as nwis


class Test_query:
    """Tests of the query function."""

    def test_url_too_long(self):
        """Test to confirm more useful error when query URL too long.

        Test based on GitHub Issue #64
        """
        # all sites in MD
        sites, _ = nwis.what_sites(stateCd='MD')
        # expected error message
        _msg = "Request URL too long. Modify your query to use fewer sites. API response reason: Request-URI Too Long"
        # raise error by trying to query them all, so URL is way too long
        with pytest.raises(ValueError, match=_msg):
            nwis.get_iv(sites=sites.site_no.values.tolist())
