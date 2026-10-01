"""``codes.counties``: the county table and the ``county`` argument's conversion.

The getters that take ``county`` are tested in their adapter's file; this one
covers what they share -- which identifiers resolve, to what, and which raise.
"""

import pytest

from dataretrieval.codes.counties import apply_county, counties, to_county


@pytest.mark.parametrize(
    ("value", "state"),
    [
        ("55025", None),
        (55025, None),
        (" 55025 ", None),
        ("US:55:025", None),
        ("us:55:025", None),
        ("025", "WI"),
        ("Dane County", "WI"),
        ("dane county", "Wisconsin"),
        ("Dane", "55"),
        ("DANE", "US:55"),
        ("55025", "WI"),
    ],
)
def test_every_form_resolves_to_the_same_county(value, state):
    assert to_county(value, state=state) == "55025"
    assert to_county(value, "fips_us", state=state) == "US:55:025"
    assert to_county(value, "name", state=state) == "Dane County"


def test_an_integer_keeps_its_leading_zero():
    """Autauga County, Alabama is 01001; as an integer it is 1001."""
    assert to_county(1001) == "01001"
    assert to_county("1001") == "01001"


@pytest.mark.parametrize(
    ("value", "state", "name"),
    [
        ("Orleans", "LA", "Orleans Parish"),
        ("Juneau", "AK", "Juneau City and Borough"),
        ("Bethel", "AK", "Bethel Census Area"),
        ("Anchorage", "AK", "Anchorage Municipality"),
        ("Adjuntas", "PR", "Adjuntas Municipio"),
        ("Capitol", "CT", "Capitol Planning Region"),
        ("St. Croix", "VI", "St. Croix Island"),
        ("St Louis County", "MO", "St. Louis County"),
        ("District of Columbia", "DC", "District of Columbia"),
        ("Charles City", "VA", "Charles City County"),
    ],
)
def test_a_county_equivalent_matches_without_its_designation(value, state, name):
    """Parishes, boroughs, municipios, and planning regions resolve like
    counties; periods are optional, as in "St Louis"."""
    assert to_county(value, "name", state=state) == name


def test_a_list_resolves_element_wise():
    assert to_county(["55025", "US:17:031", 1001]) == ["55025", "17031", "01001"]
    assert to_county(("Dane", "Iowa"), "name", state="WI") == [
        "Dane County",
        "Iowa County",
    ]


@pytest.mark.parametrize(
    ("value", "state", "match"),
    [
        ("Dane", None, r"names repeat across states\. Pass the state too"),
        ("025", None, "not a recognized US county"),
        ("99999", None, "not a recognized US county"),
        (True, None, "not a recognized US county"),
        ("55025", "IL", "is in Wisconsin, not Illinois"),
        ("Fairfax", "VA", "'Fairfax County', 'Fairfax City'. Pass the full name"),
        ("Baltimore", "MD", "more than one county in Maryland"),
        ("Dain", "WI", "Did you mean 'Dane County'"),
        ("Dane", ["WI", "IL"], "must be a single state"),
        ("Dane", "Atlantis", "not a recognized US state"),
    ],
)
def test_an_unresolvable_county_raises_with_a_remedy(value, state, match):
    with pytest.raises(ValueError, match=match):
        to_county(value, state=state)


def test_a_retired_connecticut_county_points_to_the_planning_regions():
    """Connecticut replaced its counties with planning regions in 2022; the
    Water Data services accept only the new codes."""
    with pytest.raises(ValueError, match="planning regions, codes 09110 to 09190"):
        to_county("09003")


def test_to_rejects_an_unknown_representation():
    with pytest.raises(ValueError, match="to"):
        to_county("55025", "postal")


def test_the_table_covers_the_states_and_territories_and_only_them():
    from dataretrieval.codes.states import fips_codes

    assert {f[:2] for f in counties} == set(fips_codes.values())
    assert all(len(f) == 5 and f.isdigit() for f in counties)
    assert not [f for f in counties if f.endswith("000")]


class TestApplyCounty:
    @staticmethod
    def _render(fips):
        return {"county_code": fips}

    def test_absent_is_a_no_op_and_leaves_state_alone(self):
        args = {"state": "WI"}
        assert apply_county(args, render=self._render, reject=()) == {"state": "WI"}

    def test_state_qualifies_the_counties_and_is_consumed(self):
        args = {"county": "Dane", "state": "WI", "limit": 5}
        assert apply_county(args, render=self._render, reject=()) == {
            "county_code": ["55025"],
            "limit": 5,
        }

    def test_a_native_parameter_cannot_be_combined_with_county(self):
        with pytest.raises(ValueError, match="county and county_code cannot be"):
            apply_county(
                {"county": "55025", "county_code": "025"},
                render=self._render,
                reject=("county_code",),
            )

    def test_an_unrecognized_county_names_the_native_parameters(self):
        with pytest.raises(ValueError, match="Or pass state_code or county_code"):
            apply_county(
                {"county": "99999"},
                render=self._render,
                reject=("state_code", "county_code"),
            )


@pytest.mark.live
def test_the_table_matches_the_water_data_counties_collection():
    """Each code and name is the one the services filter on.

    The table is a snapshot of this collection. A failure means the Census
    Bureau or the service changed a county; regenerate the table from the
    collection (``country_code=US``, without ``000`` and the states
    ``codes.states`` does not cover).
    """
    from dataretrieval.codes.states import fips_codes
    from dataretrieval.waterdata import get_reference_table

    df, _ = get_reference_table("counties")
    covered = set(fips_codes.values())
    live = {
        row.state_fips_code + row.county_fips_code: row.county_name
        for row in df[df["country_code"] == "US"].itertuples()
        if row.state_fips_code in covered and row.county_fips_code != "000"
    }
    assert live == counties
