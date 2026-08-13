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
