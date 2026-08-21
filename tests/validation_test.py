"""Tests for the shared argument checks.

Each check is asserted on two things: that it lets a valid call through, and
that its rejection names the move that would fix the call. The second half is
the point of the module -- a caller that is a program can only correct itself
from a message that says what to send instead.
"""

import pytest

from dataretrieval._validation import (
    reject_together,
    require_argument,
    require_exactly_one,
    require_one_of,
    require_together,
)


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


class TestRequireArgument:
    def test_accepts_a_supplied_value(self):
        require_argument("navigation_mode", "UM")

    def test_accepts_a_falsy_but_supplied_value(self):
        """``0`` and ``''`` were supplied; only ``None`` was not."""
        require_argument("distance", 0)

    def test_message_names_the_parameter_and_a_default_remedy(self):
        with pytest.raises(ValueError) as excinfo:
            require_argument("feature_id", None)
        message = str(excinfo.value)
        assert "feature_id is required" in message
        assert "Pass a feature_id value." in message

    def test_context_says_what_made_it_required(self):
        with pytest.raises(ValueError, match="when comid is given"):
            require_argument("navigation_mode", None, context="when comid is given")

    def test_remedy_replaces_the_default(self):
        with pytest.raises(ValueError, match="Pass one of 'UM', 'DM'."):
            require_argument("navigation_mode", None, remedy="Pass one of 'UM', 'DM'.")


class TestRequireTogether:
    def test_accepts_all_supplied(self):
        require_together({"lat": 1.0, "long": 2.0})

    def test_accepts_none_supplied(self):
        """Declining the whole group is a different question from completing it."""
        require_together({"lat": None, "long": None})

    def test_message_names_what_is_missing_and_what_to_do(self):
        with pytest.raises(ValueError) as excinfo:
            require_together({"lat": 1.0, "long": None})
        message = str(excinfo.value)
        assert "lat and long must be given together" in message
        assert "Missing: long" in message
        assert "Pass long, or omit lat." in message

    def test_reports_every_missing_member_of_a_larger_group(self):
        with pytest.raises(ValueError, match="Missing: b and c"):
            require_together({"a": 1, "b": None, "c": None})


class TestRequireExactlyOne:
    def test_accepts_exactly_one(self):
        require_exactly_one({"comid": 1, "feature_source": None})

    def test_none_supplied_says_to_pass_one(self):
        with pytest.raises(ValueError) as excinfo:
            require_exactly_one({"state": None, "county": None, "huc": None})
        message = str(excinfo.value)
        assert "Provide exactly one of state, county or huc" in message
        assert "Supplied: none" in message
        assert "Pass one of state, county or huc." in message

    def test_several_supplied_says_which_to_drop(self):
        with pytest.raises(ValueError) as excinfo:
            require_exactly_one({"state": "WI", "county": "55025", "huc": None})
        message = str(excinfo.value)
        assert "Supplied: state and county" in message
        assert "Drop all but one of state and county." in message


class TestRejectTogether:
    def test_accepts_one_supplied(self):
        reject_together({"lat": 1.0, "comid": None})

    def test_accepts_none_supplied(self):
        """Unlike require_exactly_one, an empty call is not this check's business."""
        reject_together({"lat": None, "comid": None})

    def test_message_names_only_the_conflicting_arguments(self):
        with pytest.raises(ValueError) as excinfo:
            reject_together({"lat": 1.0, "comid": 2, "feature_source": None})
        message = str(excinfo.value)
        assert "lat and comid cannot be combined" in message
        # The argument that was never passed stays out of the remedy.
        assert "feature_source" not in message
        assert "Pass only one of lat or comid." in message

    def test_context_explains_why_they_conflict(self):
        with pytest.raises(ValueError, match="-- they name different origins"):
            reject_together(
                {"lat": 1.0, "comid": 2}, context="they name different origins"
            )
