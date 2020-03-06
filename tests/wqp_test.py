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
    with open('data/wqp_results.txt') as text:
        requests_mock.get(request_url, text=text.read())
    ratings, md = get_results(siteid='WIDNR_WQX-10032762',
                          characteristicName = 'Specific conductance',
                          startDateLo='05-01-2011', startDateHi='09-30-2011')
    assert type(ratings) is DataFrame
    assert ratings.size == 315
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)


def test_what_sites(requests_mock):
    """Tests Water quality portal sites query"""
    request_url = "https://waterqualitydata.us/Station/Search?statecode=US%3A34&characteristicName=Chloride&zip=no" \
                  "&mimeType=csv"
    with open('data/wqp_sites.txt') as text:
        requests_mock.get(request_url, text=text.read())
    sites, md = what_sites(statecode="US:34", characteristicName="Chloride")
    assert type(sites) is DataFrame
    assert sites.size == 239904
    assert md.url == request_url
    assert isinstance(md.query_time, datetime.timedelta)
