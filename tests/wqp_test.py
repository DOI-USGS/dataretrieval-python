import pytest
import requests
import datetime

from pandas import DataFrame

from dataretrieval.wqp import get_results, what_sites


def test_get_ratings(requests_mock):
    """Tests water quality portal ratings query"""
    request_url = "https://www.waterqualitydata.us/Result/Search?siteid=WIDNR_WQX-10032762" \
                  "&characteristicName=Specific+conductance&startDateLo=05-01-2011&startDateHi=09-30-2011" \
                  "&zip=no&mimeType=csv"
    response_file_path = 'tests/data/wqp_results.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = get_results(siteid='WIDNR_WQX-10032762',
                          characteristicName = 'Specific conductance',
                          startDateLo='05-01-2011', startDateHi='09-30-2011')
    assert type(df) is DataFrame
    assert df.size == 315
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def test_what_sites(requests_mock):
    """Tests Water quality portal sites query"""
    request_url = "https://www.waterqualitydata.us/Station/Search?statecode=US%3A34&characteristicName=Chloride&zip=no" \
                  "&mimeType=csv"
    response_file_path = 'tests/data/wqp_sites.txt'
    mock_request(requests_mock, request_url, response_file_path)
    df, md = what_sites(statecode="US:34", characteristicName="Chloride")
    assert type(df) is DataFrame
    assert df.size == 239868
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
    assert md.header == {"mock_header": "value"}
    assert md.comment is None


def mock_request(requests_mock, request_url, file_path):
    with open(file_path) as text:
        requests_mock.get(request_url, text=text.read(), headers={"mock_header": "value"})
