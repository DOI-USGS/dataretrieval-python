"""Unit tests for functions in utils.py"""
import pytest
from dataretrieval import utils
import dataretrieval.nwis as nwis
import unittest.mock as mock


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

    def test_header(self):
        """Test checking header info with user-agent is part of query."""
        url = 'https://waterservices.usgs.gov/nwis/dv'
        payload = {'format': 'json',
                   'startDT': '2010-10-01',
                   'endDT': '2010-10-10',
                   'sites': '01646500',
                   'multi_index': True}
        response = utils.query(url, payload)
        assert response.status_code == 200  # GET was successful
        assert 'user-agent' in response.request.headers

class Test_BaseMetadata:
    """Tests of BaseMetadata"""

    def test_init_with_response(self):
        response = mock.MagicMock()
        md = utils.BaseMetadata(response)
        
        ## Test parameters initialized from the API response
        assert md.url is not None
        assert md.query_time is not None
        assert md.header is not None

        ## Test NotImplementedError parameters 
        with pytest.raises(NotImplementedError):
            md.site_info
        with pytest.raises(NotImplementedError):
            md.variable_info

