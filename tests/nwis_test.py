import datetime
import json
import re
import warnings
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest

from dataretrieval.nwis import (
    NWIS_Metadata,
    _read_rdb,
    get_discharge_measurements,
    get_gwlevels,
    get_info,
    get_iv,
    get_pmcodes,
    get_qwdata,
    get_record,
    get_water_use,
    preformat_peaks_response,
    what_sites,
)

START_DATE = "2018-01-24"
END_DATE = "2018-01-25"

DATETIME_COL = "datetime"
SITENO_COL = "site_no"

# Legacy NWIS endpoints these tests mock — this module makes no live calls.
_SITE_RE = re.compile(r"^https://waterservices\.usgs\.gov/nwis/site(\?.*)?$")
_IV_RE = re.compile(r"^https://waterservices\.usgs\.gov/nwis/iv(\?.*)?$")


def _load_mock_json(file_name):
    """Helper to load mock JSON from tests/data."""
    path = Path(__file__).parent / "data" / file_name
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _load_fixture(file_name):
    """Read a raw fixture file (e.g. an RDB response) from tests/data."""
    return (Path(__file__).parent / "data" / file_name).read_text(encoding="utf-8")


def _mock_site(httpx_mock, fixture="waterservices_site.txt"):
    """Mock the legacy NWIS ``site`` endpoint with an RDB fixture."""
    httpx_mock.add_response(method="GET", url=_SITE_RE, text=_load_fixture(fixture))


def _test_iv_service(httpx_mock):
    """Mocked test of instantaneous value service"""
    start = START_DATE
    end = END_DATE
    service = "iv"
    site = ["03339000", "05447500", "03346500"]

    # We use a very simple JSON structure just to satisfy the parser
    mock_json = _load_mock_json("nwis_iv_mock.json")

    # Match the base URL and ensure query parameters are correct
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r"^https://waterservices\.usgs\.gov/nwis/iv(\?.*)?$"),
        json=mock_json,
    )

    return get_record(site, start, end, service=service)


def test_iv_service_answer(httpx_mock):
    df = _test_iv_service(httpx_mock)
    # check multiindex function
    assert df.index.names == [
        SITENO_COL,
        DATETIME_COL,
    ], f"iv service returned incorrect index: {df.index.names}"


def test_preformat_peaks_response():
    # make a data frame with a "peak_dt" datetime column
    # it will have some nan and none values
    data = {"peak_dt": ["2000-03-22", np.nan, None], "peak_va": [1000, 2000, 3000]}
    # turn data into dataframe
    df = pd.DataFrame(data)
    # run preformat function
    df = preformat_peaks_response(df)
    # assertions
    assert "datetime" in df.columns
    assert df["datetime"].isna().sum() == 0


class TestDeprecationWarnings:
    """Verify per-function DeprecationWarning fires with the right replacement.

    The module-level "use waterdata instead" warning fires on import; these
    tests pin the function-specific replacements so users see actionable
    migration guidance the first time they call each NWIS getter.
    """

    @pytest.mark.parametrize(
        "func_name, replacement_substring",
        [
            ("get_dv", "waterdata.get_daily"),
            ("get_iv", "waterdata.get_continuous"),
            ("get_info", "waterdata.get_monitoring_locations"),
            ("what_sites", "waterdata.get_monitoring_locations"),
            ("get_stats", "waterdata.get_stats_por"),
            ("get_discharge_peaks", "waterdata.get_peaks"),
            ("get_ratings", "waterdata.get_ratings"),
            ("get_record", "waterdata.get_*"),
            ("query_waterdata", "waterdata.get_*"),
            ("query_waterservices", "waterdata.get_*"),
        ],
    )
    def test_warn_message_includes_replacement(self, func_name, replacement_substring):
        """Each deprecated function emits a warning naming the right replacement."""
        from dataretrieval.nwis import _NWIS_REMOVAL_DATE, _warn_deprecated

        with pytest.warns(DeprecationWarning, match=func_name) as record:
            _warn_deprecated(func_name)
        message = str(record[0].message)
        assert replacement_substring in message
        assert _NWIS_REMOVAL_DATE in message

    def test_get_iv_fires_deprecation_on_call(self, httpx_mock):
        """End-to-end: a real call routes through _warn_deprecated."""
        httpx_mock.add_response(
            method="GET",
            url=re.compile(r"^https://waterservices\.usgs\.gov/nwis/iv(\?.*)?$"),
            json={"value": {"timeSeries": []}},
        )
        with pytest.warns(DeprecationWarning, match="get_iv.*waterdata.get_continuous"):
            get_iv(sites="01491000")

    def test_nested_calls_emit_one_warning(self, httpx_mock):
        """get_record(service='iv') wraps get_iv -> query_waterservices.

        Without re-entrancy suppression the user would see 3 near-identical
        deprecation warnings for one call; pin the outermost-only contract.
        """
        httpx_mock.add_response(
            method="GET",
            url=re.compile(r"^https://waterservices\.usgs\.gov/nwis/iv(\?.*)?$"),
            json={"value": {"timeSeries": []}},
        )
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            get_record(sites="01491000", service="iv")
        deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        assert len(deprecations) == 1
        assert "get_record" in str(deprecations[0].message)

    @pytest.mark.parametrize(
        "name",
        [
            "get_daily",
            "get_continuous",
            "get_monitoring_locations",
            "get_stats_por",
            "get_stats_date_range",
            "get_peaks",
            "get_ratings",
        ],
    )
    def test_named_replacement_exists_in_waterdata(self, name):
        """Tripwire: every concrete `waterdata.*` named in a deprecation message
        must actually exist, so a user following the migration guidance doesn't
        hit AttributeError.

        Fails loudly if this PR ever lands before its referenced replacement
        does (e.g. before `get_peaks` from #267).
        """
        import dataretrieval.waterdata as wd

        assert callable(getattr(wd, name, None)), (
            f"`waterdata.{name}` is missing — fix `_REPLACEMENTS` in nwis.py "
            "or add the replacement before merging."
        )


class TestDefunct:
    """Verify that defunct functions raise NameError."""

    def test_get_qwdata_raises(self):
        with pytest.raises(NameError, match="get_qwdata"):
            get_qwdata()

    def test_get_discharge_measurements_raises(self):
        with pytest.raises(NameError, match="get_discharge_measurements"):
            get_discharge_measurements()

    def test_get_gwlevels_raises(self):
        with pytest.raises(NameError, match="get_gwlevels"):
            get_gwlevels()

    def test_get_pmcodes_raises(self):
        with pytest.raises(NameError, match="get_pmcodes"):
            get_pmcodes()

    def test_get_water_use_raises(self):
        with pytest.raises(NameError, match="get_water_use"):
            get_water_use()

    def test_get_record_defunct_service_measurements(self):
        with pytest.raises(NameError, match="no longer supported by get_record"):
            get_record(service="measurements")

    def test_get_record_defunct_service_gwlevels(self):
        with pytest.raises(NameError, match="no longer supported by get_record"):
            get_record(service="gwlevels")

    def test_get_record_defunct_service_pmcodes(self):
        with pytest.raises(NameError, match="no longer supported by get_record"):
            get_record(service="pmcodes")

    def test_get_record_defunct_service_water_use(self):
        with pytest.raises(NameError, match="no longer supported by get_record"):
            get_record(service="water_use")


class TestTZ:
    """Tests relating to GitHub Issue #60 — merging IV results across sites
    yields a proper datetime index. Mocked against fixture responses."""

    def _mock(self, httpx_mock):
        _mock_site(httpx_mock)
        httpx_mock.add_response(
            method="GET", url=_IV_RE, json=_load_mock_json("nwis_iv_mock.json")
        )

    def test_multiple_tz_01(self, httpx_mock):
        """Issue #60 - merging IV across sites yields a datetime index."""
        self._mock(httpx_mock)
        sites, _ = what_sites(stateCd="MD")
        iv, _ = get_iv(sites=sites.site_no.values[:25].tolist())
        assert "datetime" in iv.index.names
        assert isinstance(iv.index[0][1], datetime.datetime)

    def test_multiple_tz_02(self, httpx_mock):
        """Issue #60 - the same-tz path also yields a datetime index."""
        self._mock(httpx_mock)
        sites, _ = what_sites(stateCd="MD")
        iv, _ = get_iv(sites=sites.site_no.values[:20].tolist())
        assert "datetime" in iv.index.names
        assert isinstance(iv.index[0][1], datetime.datetime)


class TestSiteseriesCatalogOutput:
    """Tests relating to GitHub Issue #34 — ``seriesCatalogOutput`` adds the
    data-inventory columns (begin_date / end_date / count_nu). Mocked against
    fixture responses (the chosen fixture, not the request param, decides which
    columns come back)."""

    _SERIESCATALOG = "nwis_site_seriescatalog.txt"

    def test_seriesCatalogOutput_get_record(self, httpx_mock):
        """seriesCatalogOutput=True with get_record exposes inventory columns."""
        _mock_site(httpx_mock, self._SERIESCATALOG)
        data = get_record(
            huc="20", parameterCd="00060", service="site", seriesCatalogOutput="True"
        )
        assert "begin_date" in data.columns
        assert "end_date" in data.columns
        assert "count_nu" in data.columns

    def test_seriesCatalogOutput_get_info(self, httpx_mock):
        """seriesCatalogOutput=TRUE with get_info exposes inventory columns."""
        _mock_site(httpx_mock, self._SERIESCATALOG)
        data, _ = get_info(huc="20", parameterCd="00060", seriesCatalogOutput="TRUE")
        assert "begin_date" in data.columns
        assert "end_date" in data.columns
        assert "count_nu" in data.columns

    def test_seriesCatalogOutput_bool(self, httpx_mock):
        """A boolean seriesCatalogOutput is accepted and exposes inventory cols."""
        _mock_site(httpx_mock, self._SERIESCATALOG)
        data, _ = get_info(huc="20", parameterCd="00060", seriesCatalogOutput=True)
        assert "begin_date" in data.columns
        assert "end_date" in data.columns
        assert "count_nu" in data.columns

    def test_expandedrdb_get_record(self, httpx_mock):
        """The default expanded-rdb format omits the inventory columns."""
        _mock_site(httpx_mock)
        data = get_record(
            huc="20", parameterCd="00060", service="site", seriesCatalogOutput="False"
        )
        assert "begin_date" not in data.columns
        assert "end_date" not in data.columns
        assert "count_nu" not in data.columns

    def test_expandedrdb_get_info(self, httpx_mock):
        """get_info default omits the inventory columns."""
        _mock_site(httpx_mock)
        data, _ = get_info(huc="20", parameterCd="00060")
        assert "begin_date" not in data.columns
        assert "end_date" not in data.columns
        assert "count_nu" not in data.columns


def test_empty_timeseries(httpx_mock):
    """Test based on empty case from GitHub Issue #26."""
    sites = "011277906"
    start = "2010-07-20"
    end = "2010-07-20"

    mock_json = _load_mock_json("nwis_iv_empty_mock.json")
    # Match the base URL and ensure query parameters are correct
    httpx_mock.add_response(
        method="GET",
        url=re.compile(r"^https://waterservices\.usgs\.gov/nwis/iv(\?.*)?$"),
        json=mock_json,
    )

    df = get_record(sites=sites, service="iv", start=start, end=end)
    assert df.empty is True


class TestMetaData:
    """Tests of NWIS metadata setting (originally GitHub Issue #73).

    ``site_info`` is a property that lazily re-queries ``what_sites``; mocked
    here against the ``site`` endpoint so it is exercised offline.
    """

    def test_set_metadata_info_site(self, httpx_mock):
        """site_info is populated when ``sites`` is supplied."""
        _mock_site(httpx_mock)
        md = NWIS_Metadata(mock.MagicMock(), sites="01491000")
        assert md.site_info

    def test_set_metadata_info_site_no(self, httpx_mock):
        """site_info is populated when ``site_no`` is supplied."""
        _mock_site(httpx_mock)
        md = NWIS_Metadata(mock.MagicMock(), site_no="01491000")
        assert md.site_info

    def test_set_metadata_info_stateCd(self, httpx_mock):
        """site_info is populated when ``stateCd`` is supplied."""
        _mock_site(httpx_mock)
        md = NWIS_Metadata(mock.MagicMock(), stateCd="RI")
        assert md.site_info

    def test_set_metadata_info_huc(self, httpx_mock):
        """site_info is populated when ``huc`` is supplied."""
        _mock_site(httpx_mock)
        md = NWIS_Metadata(mock.MagicMock(), huc="01")
        assert md.site_info

    def test_set_metadata_info_bbox(self, httpx_mock):
        """site_info is populated when ``bBox`` is supplied."""
        _mock_site(httpx_mock)
        md = NWIS_Metadata(mock.MagicMock(), bBox="-92.8,44.2,-88.9,46.0")
        assert md.site_info

    def test_set_metadata_info_countyCd(self, httpx_mock):
        """site_info is populated when ``countyCd`` is supplied."""
        _mock_site(httpx_mock)
        md = NWIS_Metadata(mock.MagicMock(), countyCd="01001")
        assert md.site_info


class TestReadRdb:
    """Tests for the NWIS-specific _read_rdb wrapper.

    The format-agnostic parser is exercised in tests/rdb_test.py; this
    class pins the wrapper-specific contract — that an empty parser
    result flows through format_response without crashing (issue #171).
    """

    def test_no_sites_flows_through_format_response(self):
        """A "No sites found" response is a legitimate empty result, not an
        error, so callers can check ``df.empty`` rather than catching an
        exception. Regression for issue #171 (previously raised IndexError),
        which now also covers the empty-frame path through ``format_response``.
        """
        no_sites_rdb = (
            "# //Output-Format: RDB\n"
            "# //Response-Status: OK\n"
            "# //Response-Message: No sites found matching all criteria\n"
        )
        df = _read_rdb(no_sites_rdb)
        assert isinstance(df, pd.DataFrame)
        assert df.empty
