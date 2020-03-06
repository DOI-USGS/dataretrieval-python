import pytest
import requests
import datetime

from pandas import DataFrame

from dataretrieval.wqp import get_results, what_sites


def test_get_ratings(requests_mock):
    """Tests water quality portal ratings query"""
    request_url = "https://waterqualitydata.us/Result/Search?siteid=WIDNR_WQX-10032762" \
                  "&characteristicName=Specific+conductance&startDateLo=05-01-2011&startDateHi=09-30-2011" \
                  "&zip=no&mimeType=csv"
    response_file_path = 'data/wqp_results.txt'
    mock_request(requests_mock, request_url, response_file_path)
    ratings, md = get_results(siteid='WIDNR_WQX-10032762',
                          characteristicName = 'Specific conductance',
                          startDateLo='05-01-2011', startDateHi='09-30-2011')
    assert type(ratings) is DataFrame
    assert ratings.size == 315
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}


def test_what_sites(requests_mock):
    """Tests Water quality portal sites query"""
    request_url = "https://waterqualitydata.us/Station/Search?statecode=US%3A34&characteristicName=Chloride&zip=no" \
                  "&mimeType=csv"
    response_file_path = 'data/wqp_sites.txt'
    mock_request(requests_mock, request_url, response_file_path)
    sites, md = what_sites(statecode="US:34", characteristicName="Chloride")
    assert type(sites) is DataFrame
    assert sites.size == 239904
    assert md.header == {"mock_header": "value"}
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}


def mock_request(requests_mock, request_url, file_path):
    with open(file_path) as text:
        requests_mock.get(request_url, text=text.read(), headers={"mock_header": "value"})