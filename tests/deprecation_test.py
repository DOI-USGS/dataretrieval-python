"""The behavioural claim: downstream CI hygiene must not break the library."""

import warnings

import pytest

import dataretrieval.wqp as wqp
from dataretrieval._deprecation import REMOVALS, warn_deprecated
from dataretrieval.exceptions import DataCurrencyWarning


def test_default_wqp_calls_survive_error_on_deprecationwarning():
    """A downstream project running ``-W error::DeprecationWarning`` -- ordinary
    CI hygiene -- must still be able to call wqp with default arguments.

    ``legacy=True`` is the default on every wqp getter and ``wqp_url`` warns
    unconditionally, so emitting that advisory as a ``DeprecationWarning``
    made the whole adapter uncallable under that filter.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings("error", category=DeprecationWarning)
        with pytest.warns(DataCurrencyWarning):
            wqp.wqp_url("Result")


def test_data_currency_is_not_a_deprecation():
    """The two categories must stay independently filterable: silencing stale
    data must not silence a real removal notice, or vice versa."""
    assert not issubclass(DataCurrencyWarning, DeprecationWarning)
    assert issubclass(DataCurrencyWarning, UserWarning)


def test_warn_deprecated_names_replacement_and_horizon():
    with pytest.warns(
        DeprecationWarning, match=r"on or after 2027-05-06.*use `x` instead"
    ):
        warn_deprecated("`nwis.get_dv`", replacement="`x`", removal=REMOVALS["nwis"])


def test_warn_deprecated_without_a_date_promises_nothing_specific():
    with pytest.warns(DeprecationWarning, match="in a future release"):
        warn_deprecated("The 'a' argument", replacement="'b'")


def test_legacy_only_url_does_not_advise_setting_a_flag_already_set():
    """``what_*(legacy=False)`` must not be told to set ``legacy=False``.

    ``_legacy_only_url`` suppresses the legacy advisory for endpoints with no
    WQX3.0 equivalent. The suppression names a category, so it has to follow
    the advisory when the advisory's category changes.
    """
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        wqp._legacy_only_url("Station", False)
    assert not [w for w in rec if issubclass(w.category, DataCurrencyWarning)]
    assert [w for w in rec if "WQX3.0 profile not available" in str(w.message)]


def test_detail_is_appended_not_interpolated():
    """A multi-sentence ``detail`` must not be spliced inside the advisory."""
    with warnings.catch_warnings(record=True) as rec:
        warnings.simplefilter("always")
        warn_deprecated(
            "The 'a' argument",
            replacement="'b'",
            removal="2027-01-01",
            detail="Because reasons. And more.",
        )
    message = str(rec[0].message)
    assert message.endswith("Because reasons. And more.")
    assert "use 'b' instead." in message
    assert "in a future release" not in message
