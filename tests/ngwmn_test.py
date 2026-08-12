"""Tests for the NGWMN OGC getters (``dataretrieval.ngwmn``).

These are mocked against toy FeatureCollections shaped like the real NGWMN OGC
API (``api.waterdata.usgs.gov/ngwmn/ogcapi``) -- two features per collection,
with the real property names and value types. What is being tested is our own
request building and result shaping, and a two-row fixture exercises that just
as well as a live query does, without depending on USGS uptime or on a
particular well still having records.

The one exception is :func:`test_state_queryables_still_diverge_upstream`, which
is marked ``live``: it asserts something about the *upstream* API that a mock
cannot tell us, because the mock is what would need updating.
"""

import re
from urllib.parse import parse_qs, urlsplit

import pytest
from pandas import DataFrame

import dataretrieval
from dataretrieval import ngwmn, settings
from dataretrieval.utils import BaseMetadata

# Agency-qualified ids in the multi-agency form NGWMN uses (not all ``USGS-``).
_SITE = "USGS-272838082142201"
_LITH_SITE = "AKDNR-535134236016630"
_OTHER_SITE = "USGS-404159100494601"


def _items_re(collection):
    return re.compile(
        r"^https://api\.waterdata\.usgs\.gov/ngwmn/ogcapi/collections/"
        + collection
        + r"/items"
    )


def _schema_re(collection):
    return re.compile(
        r"^https://api\.waterdata\.usgs\.gov/ngwmn/ogcapi/collections/"
        + collection
        + r"/schema$"
    )


def _feature(properties, *, id_, geometry=None):
    """One GeoJSON feature in NGWMN's shape.

    NGWMN's observation collections omit the ``geometry`` key entirely rather
    than sending ``null``, so it is left out unless a caller passes one.
    """
    feature = {"type": "Feature", "properties": properties, "id": id_}
    if geometry is not None:
        feature["geometry"] = geometry
    return feature


def _collection(features):
    """A FeatureCollection in NGWMN's shape.

    Deliberately omits ``numberReturned``/``numberMatched``, which NGWMN does
    not send (the main Water Data API does) -- the pagination and shaping code
    keys off ``features`` for exactly this reason, and a fixture that supplied
    the counts would stop covering that.

    ``links`` is omitted too, so there is no ``next`` to follow. NGWMN does send
    a ``next`` link on the final page, and paging stops only because that page
    comes back with no features; :func:`test_pagination_follows_next_link`
    covers that shape explicitly.
    """
    return {"type": "FeatureCollection", "features": features}


# --- toy fixtures, one per collection ---------------------------------------
# Property names and value types are copied from real responses; only the number
# of rows is reduced. Note the numeric-looking strings (``"4.37"``,
# ``"0"``) -- NGWMN really does send those as strings, and the dialect's
# coercion is what turns them into numbers, so the fixtures keep them as strings.

_SITES = _collection(
    [
        _feature(
            {
                "monitoring_location_id": _SITE,
                "agency_code": "USGS",
                "agency_name": "U.S. Geological Survey",
                "monitoring_location_number": "272838082142201",
                "monitoring_location_name": "Toy Well 1",
                "state_name": "Wisconsin",
                "county_name": "Lafayette County",
                "site_type": "WELL",
                "altitude": 999.5,
                "national_aquifer_code": "S300CAMORD",
                "aquifer_type_code": "CONFINED",
                "wl_sn_flag": "Yes",
            },
            id_=_SITE,
            geometry={"type": "Point", "coordinates": [-90.269831, 42.520036]},
        ),
        _feature(
            {
                "monitoring_location_id": _OTHER_SITE,
                "agency_code": "USGS",
                "agency_name": "U.S. Geological Survey",
                "monitoring_location_number": "404159100494601",
                "monitoring_location_name": "Toy Well 2",
                "state_name": "Wisconsin",
                "county_name": "Dane County",
                "site_type": "WELL",
                "altitude": 1012.0,
                "national_aquifer_code": "S300CAMORD",
                "aquifer_type_code": "UNCONFINED",
                "wl_sn_flag": "No",
            },
            id_=_OTHER_SITE,
            geometry={"type": "Point", "coordinates": [-100.828, 40.699]},
        ),
    ]
)

_PROVIDERS = _collection(
    [
        _feature(
            {
                "agency_name": "WISCONSIN DEPARTMENT OF NATURAL RESOURCES, WI",
                "agency_code": "WI001",
                "organization_type": "NWIS",
                "state": "WI",
                "link": "",
            },
            id_="WI001",
        ),
        _feature(
            {
                "agency_name": "U.S. GEOLOGICAL SURVEY, WI",
                "agency_code": "USGS",
                "organization_type": "NWIS",
                "state": "WI",
                "link": "",
            },
            id_="USGS",
        ),
    ]
)


def _water_level(site, obs_number, sample_time, depth):
    return _feature(
        {
            "agency_code": site.split("-")[0],
            "monitoring_location_number": site.split("-")[1],
            "monitoring_location_id": site,
            "monitoring_location_obs_number": obs_number,
            "sample_time": sample_time,
            "orig_unit": "ft",
            "orig_value": depth,
            "accuracy_unit": "ft",
            "accuracy_value": "Unknown",
            "water_depth_below_land_surface_ft": depth,
            "water_level_above_site_datum_ft": depth,
            "monitoring_location_vertical_datum": "NAVD88",
            "water_level_above_navd88_ft": depth,
        },
        id_=1872300 + obs_number,
    )


# Three observations spanning 1978-2023, so a date window can select a strict
# subset (see ``test_get_water_level_datetime_subsets``).
_WATER_LEVELS = _collection(
    [
        _water_level(_SITE, 1, "1978-05-17T19:25:00-00:00", "4.37"),
        _water_level(_SITE, 2, "2022-06-01T12:00:00-00:00", "5.12"),
        _water_level(_SITE, 3, "2023-06-01T12:00:00-00:00", "5.44"),
    ]
)

_LITHOLOGY = _collection(
    [
        _feature(
            {
                "agency_code": "AKDNR",
                "monitoring_location_number": "535134236016630",
                "monitoring_location_id": _LITH_SITE,
                "monitoring_location_obs_number": 1,
                "lithology_id": "AKDNR.535134236016630.LITH.1",
                "lithology_description": "glacial alluvium",
                "lithology_controlled_concept": "unknown",
                "lithology_depth_from": "0",
                "lithology_depth_to": "70",
                "lithology_depth_from_unit": "ft",
                "lithology_depth_to_unit": "ft",
            },
            id_=9,
        ),
        _feature(
            {
                "agency_code": "AKDNR",
                "monitoring_location_number": "535134236016630",
                "monitoring_location_id": _LITH_SITE,
                "monitoring_location_obs_number": 2,
                "lithology_id": "AKDNR.535134236016630.LITH.2",
                "lithology_description": "bedrock",
                "lithology_controlled_concept": "unknown",
                "lithology_depth_from": "70",
                "lithology_depth_to": "150",
                "lithology_depth_from_unit": "ft",
                "lithology_depth_to_unit": "ft",
            },
            id_=10,
        ),
    ]
)

_CONSTRUCTION = _collection(
    [
        _feature(
            {
                "agency_code": "USGS",
                "monitoring_location_number": "272838082142201",
                "monitoring_location_id": _SITE,
                "monitoring_location_obs_number": 1,
                "type": "casing",
                "depth_from": "0",
                "depth_to": "208",
                "depth_from_unit": "Unknown",
                "depth_to_unit": "Unknown",
                "material": None,
                "diameter": "8.00",
                "diameter_unit": "in",
            },
            id_=7741,
        ),
        _feature(
            {
                "agency_code": "USGS",
                "monitoring_location_number": "272838082142201",
                "monitoring_location_id": _SITE,
                "monitoring_location_obs_number": 2,
                "type": "screen",
                "depth_from": "208",
                "depth_to": "218",
                "depth_from_unit": "Unknown",
                "depth_to_unit": "Unknown",
                "material": None,
                "diameter": "8.00",
                "diameter_unit": "in",
            },
            id_=7742,
        ),
    ]
)


def _mock(httpx_mock, collection, body):
    """Serve ``body`` for every ``/items`` request against ``collection``."""
    httpx_mock.add_response(method="GET", url=_items_re(collection), json=body)


def _queries(httpx_mock, collection=None):
    """Parsed query strings of the ``/items`` requests that were sent, in order."""
    out = []
    for req in httpx_mock.get_requests():
        url = str(req.url)
        if "/items" not in url:
            continue
        if collection and f"/collections/{collection}/items" not in url:
            continue
        out.append(parse_qs(urlsplit(url).query))
    return out


# --- sites -------------------------------------------------------------------


def test_get_sites(httpx_mock):
    """A sites query returns one tidy row per monitoring location, carrying
    geometry by default, and reports the collection URL in its metadata."""
    _mock(httpx_mock, "sites", _SITES)

    df, md = ngwmn.get_sites(state="Wisconsin", limit=10)

    assert isinstance(df, DataFrame)
    assert isinstance(md, BaseMetadata)
    assert len(df) == 2
    assert "monitoring_location_id" in df.columns
    assert df["state_name"].dropna().eq("Wisconsin").all()
    assert "geometry" in df.columns
    assert "ngwmn/ogcapi/collections/sites" in str(md.url)


def test_get_sites_skip_geometry(httpx_mock):
    """``skip_geometry=True`` is forwarded to the collection and the resulting
    frame has no geometry column."""
    # Same fixture minus the geometry key, which is what the collection sends back
    # when asked to skip it.
    bare = _collection(
        [_feature(f["properties"], id_=f["id"]) for f in _SITES["features"]]
    )
    _mock(httpx_mock, "sites", bare)

    df, _ = ngwmn.get_sites(monitoring_location_id=_SITE, skip_geometry=True)

    assert isinstance(df, DataFrame)
    assert "geometry" not in df.columns
    # Sent as the OGC spelling ``skipGeometry``, not the Python arg name.
    assert _queries(httpx_mock, "sites")[0]["skipGeometry"] == ["true"]


def test_get_sites_state_accepts_name_postal_or_fips(httpx_mock):
    """The single ``state`` parameter accepts a full name, postal code, or FIPS
    code, and all three are normalized to the full ``state_name`` that the
    ``sites`` collection actually queries on."""
    _mock(httpx_mock, "sites", _SITES)

    for encoding in ("Wisconsin", "WI", "55"):
        ngwmn.get_sites(state=encoding, skip_geometry=True)

    sent = _queries(httpx_mock, "sites")
    assert len(sent) == 3
    for qs in sent:
        assert qs["state_name"] == ["Wisconsin"]
        # The shim rewrites into ``state_name``; raw ``state`` must not leak
        # through, or the collection would silently ignore it.
        assert "state" not in qs


# --- providers ---------------------------------------------------------------


def test_get_providers(httpx_mock):
    """Providers carry agency/organization columns and have no geometry."""
    _mock(httpx_mock, "providers", _PROVIDERS)

    df, _ = ngwmn.get_providers(state="WI")

    assert isinstance(df, DataFrame)
    assert len(df) == 2
    assert {"agency_code", "organization_type", "state"}.issubset(df.columns)
    assert "geometry" not in df.columns


def test_get_providers_state_accepts_name_postal_or_fips(httpx_mock):
    """``get_providers`` normalizes any state encoding to the uppercase postal
    code that the ``providers`` collection queries on -- the other half of the
    asymmetry that ``_STATE_QUERYABLE`` papers over."""
    _mock(httpx_mock, "providers", _PROVIDERS)

    for encoding in ("Wisconsin", "WI", "55"):
        ngwmn.get_providers(state=encoding)

    sent = _queries(httpx_mock, "providers")
    assert len(sent) == 3
    for qs in sent:
        assert qs["state"] == ["WI"]
        assert "state_name" not in qs


# --- observations ------------------------------------------------------------


def test_get_water_level(httpx_mock):
    """Water-level observations are keyed by ``sample_time`` (NGWMN's timestamp
    column, not the Water Data ``time``) and scoped to the requested site."""
    _mock(httpx_mock, "waterLevelObs", _WATER_LEVELS)

    df, _ = ngwmn.get_water_level(monitoring_location_id=_SITE)

    assert isinstance(df, DataFrame)
    assert len(df) == 3
    assert "sample_time" in df.columns
    assert (df["monitoring_location_id"] == _SITE).all()


def test_get_water_level_coerces_dialect_columns(httpx_mock):
    """The NGWMN dialect coerces ``sample_time`` to datetimes and the depth /
    level columns to numbers, even though the collection sends all of them as
    strings."""
    _mock(httpx_mock, "waterLevelObs", _WATER_LEVELS)

    df, _ = ngwmn.get_water_level(monitoring_location_id=_SITE)

    assert df["sample_time"].dtype.kind == "M"
    for col in (
        "water_depth_below_land_surface_ft",
        "water_level_above_site_datum_ft",
        "water_level_above_navd88_ft",
    ):
        assert df[col].dtype.kind == "f", col
    # Sorted by the dialect's sort columns (``sample_time`` first).
    assert df["sample_time"].is_monotonic_increasing


def test_get_water_level_datetime_subsets(httpx_mock):
    """A bounded ``datetime`` is forwarded as an OGC interval, so the collection
    returns a subset of the full record rather than us filtering client-side."""
    _mock(httpx_mock, "waterLevelObs", _WATER_LEVELS)
    full, _ = ngwmn.get_water_level(monitoring_location_id=_SITE)

    httpx_mock.reset()
    # What the collection would return for the window: the 1978 observation drops.
    windowed_body = _collection(_WATER_LEVELS["features"][1:])
    _mock(httpx_mock, "waterLevelObs", windowed_body)

    # Offsets given explicitly: a naive input would be resolved against the
    # machine's local zone, making the expected wire value machine-dependent.
    windowed, _ = ngwmn.get_water_level(
        monitoring_location_id=_SITE,
        datetime=["2022-01-01T00:00:00Z", "2024-01-01T00:00:00Z"],
    )

    assert 0 < len(windowed) < len(full)
    qs = _queries(httpx_mock, "waterLevelObs")[0]
    # A two-element range becomes one OGC interval, not two params.
    assert qs["datetime"] == ["2022-01-01T00:00:00Z/2024-01-01T00:00:00Z"]


def test_get_lithology(httpx_mock):
    _mock(httpx_mock, "lithologyObs", _LITHOLOGY)

    df, _ = ngwmn.get_lithology(monitoring_location_id=_LITH_SITE)

    assert isinstance(df, DataFrame)
    assert len(df) == 2
    assert (df["monitoring_location_id"] == _LITH_SITE).all()
    assert "lithology_description" in df.columns


def test_get_well_construction(httpx_mock):
    _mock(httpx_mock, "constructionObs", _CONSTRUCTION)

    df, _ = ngwmn.get_well_construction(monitoring_location_id=_SITE)

    assert isinstance(df, DataFrame)
    assert len(df) == 2
    assert (df["monitoring_location_id"] == _SITE).all()
    assert set(df["type"]) == {"casing", "screen"}


def test_observation_collections_return_plain_dataframe(httpx_mock):
    """NGWMN's observation features carry no ``geometry`` key at all (not even
    ``null``). The shaping layer has to special-case that, so assert the result
    is a plain frame with no geometry column rather than a GeoDataFrame."""
    _mock(httpx_mock, "waterLevelObs", _WATER_LEVELS)

    df, _ = ngwmn.get_water_level(monitoring_location_id=_SITE)

    assert type(df) is DataFrame
    assert "geometry" not in df.columns


# --- multi-value fan-out and pagination --------------------------------------


def test_multi_site_is_comma_joined_into_one_request(httpx_mock):
    """A multi-value ``monitoring_location_id`` that fits in the URL goes out as
    a single comma-joined request, not one request per site.

    Fan-out is the fallback for when the joined URL would exceed
    ``chunking._OGC_URL_BYTE_LIMIT``; the splitting logic itself is covered in
    ``waterdata_chunking_test.py``, so this only pins the common small-request
    path.
    """
    _mock(httpx_mock, "waterLevelObs", _WATER_LEVELS)

    ngwmn.get_water_level(monitoring_location_id=[_SITE, _OTHER_SITE])

    sent = _queries(httpx_mock, "waterLevelObs")
    assert len(sent) == 1
    assert sent[0]["monitoring_location_id"] == [f"{_SITE},{_OTHER_SITE}"]


def test_pagination_follows_next_link(httpx_mock):
    """Paging follows ``rel="next"`` and stops on the first page with no
    features.

    This is the shape NGWMN actually sends: it supplies a ``next`` link even on
    the last page, so an implementation that trusted the link alone would loop
    forever. Termination comes from the empty ``features`` array.
    """
    page_url = (
        "https://api.waterdata.usgs.gov/ngwmn/ogcapi/collections/"
        "waterLevelObs/items?cursor=next-page"
    )
    first = {
        **_collection(_WATER_LEVELS["features"][:2]),
        "links": [{"rel": "next", "href": page_url, "type": "application/geo+json"}],
    }
    second = {
        **_collection(_WATER_LEVELS["features"][2:]),
        "links": [{"rel": "next", "href": page_url, "type": "application/geo+json"}],
    }
    # The last page carries the same ``next`` link but no features.
    last = {
        **_collection([]),
        "links": [{"rel": "next", "href": page_url, "type": "application/geo+json"}],
    }
    for body in (first, second, last):
        httpx_mock.add_response(method="GET", url=_items_re("waterLevelObs"), json=body)

    df, _ = ngwmn.get_water_level(monitoring_location_id=_SITE)

    assert len(df) == 3


def test_empty_result_returns_typed_empty_frame(httpx_mock):
    """A 200 carrying no features yields an empty frame whose columns come from
    the collection schema, not a crash and not a shapeless frame."""
    httpx_mock.add_response(
        method="GET",
        url=_schema_re("waterLevelObs"),
        json={"properties": {"monitoring_location_id": {}, "sample_time": {}}},
    )
    _mock(httpx_mock, "waterLevelObs", _collection([]))

    df, _ = ngwmn.get_water_level(monitoring_location_id=_SITE)

    assert df.empty
    assert "monitoring_location_id" in df.columns


def test_a_configured_base_url_redirects_ngwmn_alone(httpx_mock):
    """Two adapters share this host, and a redirect must still name only one.

    NGWMN and Water Data are served from ``api.waterdata.usgs.gov``, so a URL
    cannot tell them apart -- which is why the settings table an OGC call reads
    is declared by the adapter rather than derived from its base. Redirecting
    NGWMN therefore has to leave Water Data where it was, and the Water Data
    mock here is never requested: the assertion is on the whole request list.
    """
    mirror = "https://mirror.example/ngwmn"
    httpx_mock.add_response(
        method="GET",
        url=re.compile(rf"^{re.escape(mirror)}/collections/sites/items"),
        json=_SITES,
    )
    _mock(httpx_mock, "sites", _SITES)

    with dataretrieval.configure(ngwmn.NgwmnSettings(base_url=mirror)):
        df, md = ngwmn.get_sites(state="Wisconsin", limit=10)

    assert len(df) == 2
    assert str(md.url).startswith(f"{mirror}/collections/sites/items")
    assert [urlsplit(str(r.url)).netloc for r in httpx_mock.get_requests()] == [
        "mirror.example"
    ]

    # And Water Data, the other adapter on the real host, was never named by it.
    with dataretrieval.configure(ngwmn.NgwmnSettings(base_url=mirror)):
        assert settings.base_url(adapter="waterdata") is None


# --- live upstream monitor ---------------------------------------------------


@pytest.mark.live
def test_state_queryables_still_diverge_upstream():
    """The NGWMN ``sites`` and ``providers`` collections expose DIFFERENT state
    queryables (``sites`` -> ``state_name`` full name; ``providers`` ->
    ``state`` 2-letter code). The single-``state`` shim
    (``ngwmn._STATE_QUERYABLE``) exists ONLY to paper over that asymmetry.

    If this test fails, the upstream API has unified the two queryables and the
    shim (``_STATE_QUERYABLE``) can be removed in favor of a single pass-through
    parameter.

    Kept live on purpose: it is a claim about the upstream API, and mocking it
    would make it assert only that our fixture still says what we wrote.
    """
    import httpx

    from dataretrieval.ngwmn import NGWMN_OGC_API_URL
    from dataretrieval.utils import _default_headers

    headers = _default_headers(NGWMN_OGC_API_URL)

    def queryables(collection):
        resp = httpx.get(
            f"{NGWMN_OGC_API_URL}/collections/{collection}/queryables",
            headers=headers,
            timeout=60,
        )
        resp.raise_for_status()
        return set(resp.json().get("properties") or {})

    sites_q = queryables("sites")
    providers_q = queryables("providers")
    assert "state_name" in sites_q and "state" not in sites_q, sites_q
    assert "state" in providers_q and "state_name" not in providers_q, providers_q
