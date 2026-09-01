import inspect
import json
import re
import warnings
from pathlib import Path
from unittest import mock

import numpy as np
import pandas as pd
import pytest

import dataretrieval
from dataretrieval import nwis
from dataretrieval.exceptions import DataCurrencyWarning
from dataretrieval.nwis import (
    _DEFUNCT_RECORD_OPTIONS,
    _REPLACEMENTS,
    NWIS_Metadata,
    _read_rdb,
    format_response,
    get_discharge_measurements,
    get_gwlevels,
    get_iv,
    get_pmcodes,
    get_qwdata,
    get_record,
    get_water_use,
    preformat_peaks_response,
)
from dataretrieval.rdb import read_rdb

START_DATE = "2018-01-24"
END_DATE = "2018-01-25"

DATETIME_COL = "datetime"
SITENO_COL = "site_no"

# Legacy NWIS site endpoint these tests mock — this module makes no live calls.
_SITE_RE = re.compile(r"^https://waterservices\.usgs\.gov/nwis/site(\?.*)?$")


# Every concrete ``module.function(args)`` the deprecation tables name, so the
# tripwire below is derived from what ships rather than from a hand-kept list.
# The prose entries (``waterdata.get_*()``) do not name a function and so do
# not match.
_NAMED_REPLACEMENTS = sorted(
    set(
        re.findall(
            r"`(\w+)\.(\w+)\(([^`]*)\)`",
            " ".join(
                [
                    *_REPLACEMENTS.values(),
                    *(r for _, r in _DEFUNCT_RECORD_OPTIONS.values()),
                ]
            ),
        )
    )
)


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


@pytest.mark.parametrize(
    "peak_dt, expected",
    [
        ("1878-06-12", "1878-06-12"),  # fully known
        ("1844-06-00", None),  # day unknown (peak_cd Bd)
        ("1858-00-00", None),  # month unknown (peak_cd Bm)
        ("", None),  # no date at all
        (np.nan, None),
    ],
)
def test_preformat_peaks_response_keeps_every_peak(peak_dt, expected):
    """A peak is never dropped for want of a parseable date.

    NWIS zero-fills the unknown part of a historical peak's date --
    ``YYYY-MM-00`` when the day is not known, ``YYYY-00-00`` when the month is
    not either (the ``Bd`` and ``Bm`` ``peak_cd`` qualifiers). Those are real
    peaks, often a site's largest, and dropping them loses the discharge value
    with the date. A date NWIS only partly knows stays ``NaT`` rather than
    being completed into one it does not have.
    """
    df = pd.DataFrame({"peak_dt": [peak_dt], "peak_va": [563000]})

    df = preformat_peaks_response(df)

    assert len(df) == 1, f"{peak_dt!r} was dropped"
    assert df["peak_va"].iloc[0] == 563000
    if expected is None:
        assert pd.isna(df["datetime"].iloc[0])
    else:
        assert df["datetime"].iloc[0] == pd.Timestamp(expected)


def test_preformat_peaks_response_preserves_peak_dt():
    """``peak_dt`` must survive the reformat.

    The peaks response carries no ``water_yr``, so ``peak_dt`` is the only
    column holding the year of a censored peak -- and the only way a caller can
    tell an unknown day from a known one, since ``peak_cd`` does not always
    carry the qualifier.
    """
    df = pd.DataFrame({"peak_dt": ["1858-00-00"], "peak_va": [563000]})

    df = preformat_peaks_response(df)

    assert df["peak_dt"].iloc[0] == "1858-00-00"


def test_preformat_peaks_response_malformed_frame_still_raises():
    """Only an *empty* peaks frame is a legitimate empty result. A non-empty
    frame with no ``peak_dt`` column is a malformed response -- a truncated or
    altered RDB header -- and must stay loud rather than be returned silently
    without its datetime index.
    """
    df = pd.DataFrame({"peak_va": [1000]})

    with pytest.raises(KeyError, match="peak_dt"):
        format_response(df, service="peaks")


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

    @pytest.mark.parametrize("module_name, func_name, arguments", _NAMED_REPLACEMENTS)
    def test_named_replacement_resolves(self, module_name, func_name, arguments):
        """Tripwire: following a deprecation message literally must produce a
        real call, so a user migrating doesn't hit AttributeError or TypeError.

        Fails loudly if a message lands before its referenced replacement does
        (e.g. before `get_peaks` from #267).
        """
        func = getattr(getattr(dataretrieval, module_name), func_name, None)
        assert callable(func), (
            f"`{module_name}.{func_name}` is missing — fix the replacement "
            "tables in nwis.py or add the replacement before merging."
        )
        for keyword in re.findall(r"(\w+)=", arguments):
            assert keyword in inspect.signature(func).parameters


class TestDefunctRecordOptions:
    """``get_record``'s three inert options advise; they do not raise.

    They are documented parameters of a Production/Stable getter, so they
    follow the published deprecation policy and go when `nwis` does, rather
    than on a release of their own.
    """

    @pytest.mark.parametrize(
        "option, value, replacement",
        [
            ("wide_format", False, "waterdata.get_samples"),
            ("datetime_index", False, "waterdata.get_continuous"),
            ("state", "OH", "nwdc.get_wateruse"),
        ],
    )
    def test_passing_one_advises_and_still_returns_data(
        self, httpx_mock, option, value, replacement
    ):
        _mock_site(httpx_mock)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            df = get_record(sites="01491000", service="site", **{option: value})
        assert not df.empty
        assert [
            w
            for w in caught
            if option in str(w.message) and replacement in str(w.message)
        ]

    @pytest.mark.parametrize("option", sorted(_DEFUNCT_RECORD_OPTIONS))
    def test_naming_an_option_at_its_default_is_silent(self, httpx_mock, option):
        """Passing the declared default asks for nothing the dead option
        cannot give, so it earns no warning -- and the table's "unset" value
        has to be that declared default for the distinction to hold.
        """
        default = inspect.signature(get_record).parameters[option].default
        assert _DEFUNCT_RECORD_OPTIONS[option][0] == default
        _mock_site(httpx_mock)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            get_record(sites="01491000", service="site", **{option: default})
        assert not [w for w in caught if f"`{option}` argument" in str(w.message)]

    def test_defaults_advise_nothing(self, httpx_mock):
        """A caller who never named an option must not be told about one."""
        _mock_site(httpx_mock)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            get_record(sites="01491000", service="site")
        assert not [w for w in caught if "argument is deprecated" in str(w.message)]

    def test_each_advisory_is_emitted_once_per_call(self, httpx_mock):
        """A call naming all three options emits four ``DeprecationWarning``s:
        one for ``get_record`` itself, and one per named option, each with a
        distinct subject and a distinct replacement.
        """
        _mock_site(httpx_mock)
        with warnings.catch_warnings(record=True) as caught:
            warnings.simplefilter("always", DeprecationWarning)
            get_record(
                sites="01491000",
                service="site",
                wide_format=False,
                datetime_index=False,
                state="OH",
            )
        deprecations = [w for w in caught if issubclass(w.category, DeprecationWarning)]
        messages = [str(w.message) for w in deprecations]
        assert len(messages) == 4
        for subject in (
            "`wide_format` argument",
            "`datetime_index` argument",
            "`state` argument",
        ):
            assert sum(subject in m for m in messages) == 1
        # Pins the advisories' hand-counted ``stacklevel``: every one must
        # blame the caller's own line, not a frame inside the package.
        assert {Path(w.filename).name for w in deprecations} == {Path(__file__).name}


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
    result flows through format_response without crashing (issue #171),
    on the plain arm and on the peaks arm alike.
    """

    NO_RESULTS_RDB = (
        "# //Output-Format: RDB\n"
        "# //Response-Status: OK\n"
        "# //Response-Message: No sites found matching all criteria\n"
    )

    def test_no_sites_flows_through_format_response(self):
        """A "No sites found" response is a legitimate empty result, not an
        error, so callers can check ``df.empty`` rather than catching an
        exception. Regression for issue #171 (previously raised IndexError),
        which now also covers the empty-frame path through ``format_response``.
        """
        df = _read_rdb(self.NO_RESULTS_RDB)
        assert isinstance(df, pd.DataFrame)
        assert df.empty

    def test_no_peaks_flows_through_format_response(self):
        """``format_response(service="peaks")`` must tolerate an empty frame.

        The peaks arm runs ``preformat_peaks_response`` before the
        "datetime not in columns" check, and that function popped ``peak_dt``
        unconditionally, so a column-less frame raised ``KeyError`` where every
        other service returned an empty frame (issue #171's contract).

        Both functions are public API, so any caller parsing a peaks RDB
        reaches this -- it is not unreachable behind ``NoSitesError``.
        """
        # Mirror get_discharge_peaks: raw read_rdb, then the peaks-specific
        # format_response.
        df = read_rdb(self.NO_RESULTS_RDB)
        df = format_response(df, service="peaks")
        assert isinstance(df, pd.DataFrame)
        assert df.empty


class TestFormatResponseArgument:
    """``format_response`` indexes a copy, so its argument survives the call.

    The function is public, and both internal callers still hold the frame
    they passed while it runs. Indexing that frame in place moved their
    columns into an index they never asked for.
    """

    @staticmethod
    def _frame(sites):
        return pd.DataFrame(
            {
                "site_no": sites,
                "datetime": pd.date_range("2020-01-01", periods=len(sites), freq="D"),
                "00060": np.arange(float(len(sites))),
            }
        )

    @pytest.mark.parametrize(
        "sites,expected_index",
        [
            pytest.param(["01", "01", "01"], pd.DatetimeIndex, id="single-site"),
            pytest.param(["01", "02", "03"], pd.MultiIndex, id="multi-site"),
        ],
    )
    def test_it_keeps_its_columns_and_index(self, sites, expected_index):
        df = self._frame(sites)
        before = df.copy(deep=True)

        out = format_response(df)

        assert isinstance(out.index, expected_index), "the result must be indexed"
        pd.testing.assert_frame_equal(df, before)


class TestGetRecordDispatch:
    """``get_record`` is a router; each service must reach its own getter.

    The arms are near-identical by eye, which is what makes a mis-wired one
    survive review: every arm forwards ``sites`` except ``ratings``, which
    takes a scalar ``site``. A swap there fails only at request time, for one
    service, in a deprecated facade nobody reads.
    """

    @pytest.mark.parametrize(
        "service,target,site_kwarg",
        [
            ("iv", "get_iv", "sites"),
            ("dv", "get_dv", "sites"),
            ("site", "get_info", "sites"),
            ("peaks", "get_discharge_peaks", "sites"),
            ("stat", "get_stats", "sites"),
            ("ratings", "get_ratings", "site"),
        ],
    )
    def test_each_service_reaches_its_own_getter(self, service, target, site_kwarg):
        frame = pd.DataFrame({"x": [1]})
        with mock.patch(f"dataretrieval.nwis.{target}") as fake:
            fake.return_value = (frame, mock.Mock())
            out = get_record(sites="01491000", service=service)
        assert fake.call_count == 1
        assert fake.call_args.kwargs[site_kwarg] == "01491000"
        # The router returns the frame alone, dropping the metadata half.
        assert out is frame

    def test_unrecognized_service_lists_the_ones_it_serves(self):
        with pytest.raises(ValueError) as excinfo:
            get_record(sites="01491000", service="nope")
        message = str(excinfo.value)
        assert "Invalid service: 'nope'" in message
        assert "'iv'" in message and "'peaks'" in message
        assert "waterdata" in message


def test_html_error_page_instead_of_json_says_what_to_do():
    """A 200 carrying an HTML error page must not surface as a JSON parse error.

    The legacy services answer an outage with a styled page and a 200, so the
    only signal is the body. A caller that gets ``JSONDecodeError`` learns
    nothing actionable; this path names the cause and the move.
    """
    response = mock.Mock()
    response.json.side_effect = ValueError("no json")
    response.text = "<!DOCTYPE html><html><body>USGS</body></html>"
    response.headers = {"Content-Type": "text/html; charset=UTF-8"}
    response.status_code = 200
    response.url = "https://waterservices.usgs.gov/nwis/dv?sites=01646500"

    with pytest.raises(ValueError) as excinfo:
        nwis._parse_json_or_raise(response)
    message = str(excinfo.value)
    assert "HTML response instead of JSON" in message
    assert "Wait and retry" in message
    assert "waterdata" in message


def test_a_non_html_parse_failure_is_re_raised_unchanged():
    """Only HTML gets the rewrite; a genuine malformed-JSON body must not be
    relabelled as a service outage."""
    response = mock.Mock()
    response.json.side_effect = ValueError("Expecting value")
    response.text = "{not json"
    response.headers = {"Content-Type": "application/json"}
    response.status_code = 200
    response.url = "https://waterservices.usgs.gov/nwis/dv"

    with pytest.raises(ValueError, match="Expecting value"):
        nwis._parse_json_or_raise(response)


def test_deprecating_a_getter_with_no_named_replacement_is_refused():
    """``@_deprecated`` promises the caller a replacement, so the decorator
    refuses to be applied to a function whose replacement nobody recorded --
    a deprecation warning naming nothing is worse than none."""
    with pytest.raises(RuntimeError, match="_REPLACEMENTS missing entry"):

        @nwis._deprecated
        def not_a_real_getter():
            pass


def test_utc_localization_of_a_single_datetime_index():
    """NWIS returns naive local timestamps; a frame whose index is a plain
    DatetimeIndex must still come back tz-aware, or two services' frames
    cannot be concatenated."""
    df = pd.DataFrame(
        {"x": [1, 2]},
        index=pd.to_datetime(["2018-01-24 10:30", "2018-01-24 11:30"]),
    )
    out = nwis._localize_datetime_index(df)
    assert str(out.index.tz) == "UTC"


def test_metadata_site_info_is_none_when_no_site_filter_was_used():
    """``site_info`` fetches the sites a query named. A query filtered by
    something else (a parameter code alone) has no sites to describe, and
    guessing one would describe the wrong thing."""
    md = NWIS_Metadata(mock.MagicMock(), parameterCd="00060")
    assert md.site_info is None


def test_utc_localization_of_a_multi_index_datetime_level():
    """``multi_index=True`` puts the timestamp on level 1 under the site id.
    The naive level must still be localized, or a multi-site frame carries
    two different clock conventions in one column."""
    idx = pd.MultiIndex.from_arrays(
        [
            ["01491000", "01491000"],
            pd.to_datetime(["2018-01-24 10:30", "2018-01-24 11:30"]),
        ],
        names=["site_no", "datetime"],
    )
    out = nwis._localize_datetime_index(pd.DataFrame({"x": [1, 2]}, index=idx))
    assert str(out.index.levels[1].tz) == "UTC"


class TestGetInfoSeriesCatalog:
    """``seriesCatalogOutput`` and the expanded site format are mutually
    exclusive on the wire, so the getter picks one and warns when the caller
    asked for the retiring one."""

    @pytest.mark.parametrize("flag", ["True", "TRUE", "true", True])
    def test_asking_for_the_series_catalog_warns_and_forwards_it(
        self, flag, httpx_mock
    ):
        httpx_mock.add_response(method="GET", url=_SITE_RE, text="#\nx\n5s\n")
        with pytest.warns(DataCurrencyWarning, match="qw data endpoint is"):
            nwis.get_info(sites="01491000", seriesCatalogOutput=flag)
        sent = str(httpx_mock.get_requests()[-1].url)
        assert "seriesCatalogOutput=True" in sent
        assert "siteOutput" not in sent

    def test_without_it_the_expanded_site_format_is_requested(self, httpx_mock):
        httpx_mock.add_response(method="GET", url=_SITE_RE, text="#\nx\n5s\n")
        nwis.get_info(sites="01491000")
        sent = str(httpx_mock.get_requests()[-1].url)
        assert "siteOutput=Expanded" in sent
        assert "seriesCatalogOutput" not in sent
