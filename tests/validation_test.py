"""Tests for the shared closed-vocabulary check."""

import pytest

from dataretrieval._validation import require_one_of


def test_accepts_a_valid_option():
    require_one_of("daily", ("daily", "continuous"), name="collection")


def test_message_names_the_parameter_and_the_options():
    with pytest.raises(ValueError) as excinfo:
        require_one_of("hourly", ("daily", "continuous"), name="collection")
    message = str(excinfo.value)
    assert "Invalid collection: 'hourly'" in message
    assert "'daily', 'continuous'" in message


def test_context_qualifies_a_vocabulary_that_depends_on_another_argument():
    with pytest.raises(ValueError, match="for service 'wqp'"):
        require_one_of("x", ("a",), name="profile", context="service 'wqp'")


def test_a_string_vocabulary_is_refused():
    """``str`` is a Collection, so passing one type-checks -- and then ``in``
    silently degrades from membership to a substring test, accepting any
    fragment of a valid option. Refuse it at the one shared chokepoint."""
    with pytest.raises(TypeError, match="not 'csv'"):
        require_one_of("cs", "csv", name="format")
