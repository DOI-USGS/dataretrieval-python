"""Tests for ``dataretrieval.waterdata.chunking``.

These tests exercise the joint planner with a fake ``build_request``
whose URL byte length is a deterministic function of its inputs:

- non-chunkable args contribute ``base_bytes``,
- every multi-value list contributes ``len(",".join(map(str, v)))``,
- the ``filter`` kwarg contributes ``len(filter)``.

That isolates planner behaviour from the real HTTP request builder.
The one exception is
``test_joint_planner_url_construction_long_filter_and_long_sites``,
which uses the real ``_construct_api_requests`` so URL-encoding
surprises (``%``, ``+``, ``/``, ``&``, …) can't pass against a fake
and then fail in production.
"""

import datetime
import sys
from unittest import mock
from urllib.parse import quote_plus

import pandas as pd
import pytest

if sys.version_info < (3, 10):
    pytest.skip("Skip entire module on Python < 3.10", allow_module_level=True)

from dataretrieval.waterdata import chunking as _chunking
from dataretrieval.waterdata.chunking import (
    _LIST_SEP,
    _OR_SEP,
    _QUOTA_HEADER,
    ChunkInterrupted,
    ChunkPlan,
    QuotaExhausted,
    RateLimited,
    RequestExceedsQuota,
    RequestTooLarge,
    ServiceInterrupted,
    ServiceUnavailable,
    _chunked_session,
    _extract_axes,
    _read_remaining,
    multi_value_chunked,
)
from dataretrieval.waterdata.utils import _construct_api_requests


class _FakeReq:
    __slots__ = ("url", "body")

    def __init__(self, url, body=None):
        self.url = url
        self.body = body


def _fake_build(*, base=200, **kwargs):
    """Fake build_request: URL length deterministic in its inputs.

    Mirrors the GET-routed shape: payload goes in the URL, body is None.
    List/string values are URL-encoded via ``quote_plus`` so the fake's
    byte count matches what the real ``_construct_api_requests`` would
    produce; otherwise an alphanumeric test could pass against the fake
    but fail in production once values containing ``%``, ``+``, ``/``,
    ``&`` etc. (which expand under encoding) reach the same code path.
    """
    bytes_ = base
    for v in kwargs.values():
        if isinstance(v, (list, tuple)):
            bytes_ += len(quote_plus(",".join(map(str, v))))
        elif isinstance(v, str):
            bytes_ += len(quote_plus(v))
    return _FakeReq("x" * bytes_)


def test_never_chunk_covers_all_date_range_params():
    """``_NEVER_CHUNK`` and ``_DATE_RANGE_PARAMS`` are maintained in
    separate modules (chunker vs request builder) for layering reasons,
    but every date-range param MUST be excluded from chunking — a
    range value isn't an enumerable set to split. Guard against drift:
    adding a new param to ``_DATE_RANGE_PARAMS`` without also adding
    it to ``_NEVER_CHUNK`` would silently let the chunker try to
    comma-join an interval string."""
    from dataretrieval.waterdata.chunking import _NEVER_CHUNK
    from dataretrieval.waterdata.utils import _DATE_RANGE_PARAMS

    missing = _DATE_RANGE_PARAMS - _NEVER_CHUNK
    assert not missing, (
        f"_DATE_RANGE_PARAMS contains entries not in _NEVER_CHUNK: "
        f"{sorted(missing)}. Add them to chunking._NEVER_CHUNK."
    )


def test_extract_axes_picks_up_list_dims_and_filter():
    """Every multi-value list parameter becomes one axis with ``","``
    joiner; the cql-text filter becomes one axis with ``" OR "`` joiner
    and its atoms are the top-level OR-clauses."""
    args = {
        "monitoring_location_id": ["USGS-A", "USGS-B"],
        "parameter_code": ["00060", "00065"],
        "filter": "a='1' OR b='2' OR c='3'",
    }
    axes = _extract_axes(args)
    by_key = {ax.arg_key: ax for ax in axes}
    assert set(by_key) == {"monitoring_location_id", "parameter_code", "filter"}
    assert by_key["monitoring_location_id"].joiner == _LIST_SEP
    assert by_key["monitoring_location_id"].atoms == ("USGS-A", "USGS-B")
    assert by_key["parameter_code"].joiner == _LIST_SEP
    assert by_key["filter"].joiner == _OR_SEP
    assert by_key["filter"].atoms == ("a='1'", "b='2'", "c='3'")


def test_extract_axes_skips_singletons_and_never_chunk_params():
    """Length-1 lists and ``_NEVER_CHUNK`` params (``bbox``, ``limit``,
    date intervals, ...) produce no axes — there's nothing to split."""
    args = {
        "monitoring_location_id": ["USGS-A"],  # length 1
        "bbox": [-95, 40, -90, 45],
        "limit": 100,
        "filter": "a='1'",  # one clause, no OR to split
    }
    assert _extract_axes(args) == []


def test_chunk_plan_returns_passthrough_when_no_chunkable_axes():
    """Scalar args with nothing to chunk → passthrough, even at a
    URL limit the request technically exceeds (the server may 414,
    but ``ChunkPlan`` has nothing to split)."""
    args = {"monitoring_location_id": "scalar-only"}
    plan = ChunkPlan(args, _fake_build, url_limit=10)
    assert plan.axes == []
    assert plan.total == 1


def test_chunk_plan_greedy_halving_targets_largest_axis_chunk():
    """The biggest chunk across all axes halves first — when one list
    axis dominates URL bytes, only it gets split until it stops being
    the largest."""
    args = {
        "monitoring_location_id": ["X" * 30, "Y" * 30, "Z" * 30, "W" * 30],
        "parameter_code": ["00060", "00065"],
    }
    # full URL ≈ 200 + 123 + 12 = 335; force splitting the heavy axis only.
    plan = ChunkPlan(args, _fake_build, url_limit=310)
    assert len(plan.chunks["monitoring_location_id"]) > 1
    assert len(plan.chunks["parameter_code"]) == 1


def test_chunk_plan_raises_request_too_large_at_singleton_floor():
    """Limit below the singleton-per-axis floor → ``RequestTooLarge``;
    there's nothing left to shrink."""
    args = {"monitoring_location_id": ["A", "B"]}
    # base=200 alone exceeds limit=100; chunking can't help.
    with pytest.raises(RequestTooLarge, match="smallest reducible"):
        ChunkPlan(args, _fake_build, url_limit=100)


def test_chunk_plan_fans_out_filter_when_list_alone_cannot_fit():
    """When the request can only fit by chunking BOTH a list axis AND
    the filter axis, the plan ends up with chunk counts >1 on at
    least one of the two axis kinds."""
    clauses = [f"f='{i}'" for i in range(10)]
    args = {
        "monitoring_location_id": ["A" * 10, "B" * 10, "C" * 10, "D" * 10],
        "filter": " OR ".join(clauses),
    }
    plan = ChunkPlan(args, _fake_build, url_limit=240)
    # At least one axis must end up split.
    assert any(len(plan.chunks[ax.arg_key]) > 1 for ax in plan.axes)


def test_chunk_plan_minimizes_total_sub_requests():
    """When both axes need shrinking, picking smaller filter chunks
    frees URL budget for larger list chunks, and vice versa. The
    planner should pick the allocation with the *fewest* total
    sub-requests, not just the first allocation that fits."""
    # 16 short clauses (no inflation under URL encoding so the math is
    # tractable). Each clause = 5 bytes (e.g. "f='0'"); full filter ≈
    # 16*5 + 15*4 = 140 bytes raw.
    clauses = [f"f='{i}'" for i in range(16)]
    args = {
        "sites": ["S" * 30 for _ in range(8)],  # 8 sites @ 30 chars
        "filter": " OR ".join(clauses),
    }
    # Tight limit forces both axes to participate.
    plan = ChunkPlan(args, _fake_build, url_limit=380)
    # Plan must beat the bail-floor-style worst case (8 singletons × 16
    # filter chunks = 128 sub-requests) by a healthy margin.
    assert plan.total < 128


def test_chunk_plan_raises_when_smallest_plan_doesnt_fit():
    """If even the most aggressive joint plan (singleton lists +
    singleton filter clauses) still exceeds the limit, surface
    RequestTooLarge — there's nothing left to shrink."""
    args = {
        "monitoring_location_id": ["A" * 10, "B" * 10],
        "filter": "x='12345' OR x='67890'",  # min clause is 9 chars
    }
    # Base 200 + singleton site (10) + singleton clause (9) = 219; limit
    # below 219 → no joint plan can fit.
    with pytest.raises(RequestTooLarge):
        ChunkPlan(args, _fake_build, url_limit=210)


def test_chunk_plan_passthrough_when_request_fits():
    """URL under limit → trivial passthrough plan (no axes, total=1),
    and ``iter_sub_args`` yields exactly one sub-args dict equal to
    the original args."""
    args = {"monitoring_location_id": ["A", "B", "C"], "limit": 100}
    plan = ChunkPlan(args, _fake_build, url_limit=8000)
    assert plan.axes == []
    assert plan.total == 1
    subs = list(plan.iter_sub_args())
    assert len(subs) == 1
    assert subs[0] == args


def test_multi_value_chunked_passes_through_when_url_fits():
    """No planning needed → decorator calls underlying function exactly once
    with the original args."""
    calls = []

    @multi_value_chunked(build_request=_fake_build, url_limit=8000)
    def fetch(args):
        calls.append(args)
        return pd.DataFrame(), mock.Mock(
            elapsed=datetime.timedelta(seconds=0.1), headers={}
        )

    fetch({"monitoring_location_id": ["A", "B"]})
    assert len(calls) == 1
    assert calls[0]["monitoring_location_id"] == ["A", "B"]


def test_multi_value_chunked_emits_cartesian_product():
    """Two chunkable axes, each split into 2 chunks → exactly 4 sub-requests,
    each pairing one chunk from each axis."""
    calls = []

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    def fetch(args):
        calls.append({k: v for k, v in args.items() if k in ("sites", "pcodes")})
        return pd.DataFrame(), mock.Mock(
            elapsed=datetime.timedelta(seconds=0.1), headers={}
        )

    fetch(
        {
            "sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10],
            "pcodes": ["P1" * 10, "P2" * 10, "P3" * 10, "P4" * 10],
        }
    )
    # Both heavy → planner should split both axes. Confirm a cartesian shape:
    # every unique site-chunk pairs with every unique pcode-chunk.
    sites_seen = {tuple(c["sites"]) for c in calls}
    pcodes_seen = {tuple(c["pcodes"]) for c in calls}
    assert len(calls) == len(sites_seen) * len(pcodes_seen)
    assert len(sites_seen) > 1
    assert len(pcodes_seen) > 1


def test_multi_value_chunked_emits_3d_cartesian_product():
    """Three chunkable axes, each forced to split → exhaustive cartesian
    product across all three. Verifies the halving loop in
    ``ChunkPlan._plan`` handles N>2 axes uniformly and the ``ChunkedCall``
    ``itertools.product`` enumerates every combination exactly once."""
    calls = []

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    def fetch(args):
        calls.append(tuple(tuple(args[k]) for k in ("sites", "pcodes", "stats")))
        return pd.DataFrame(), mock.Mock(
            elapsed=datetime.timedelta(seconds=0.1), headers={}
        )

    fetch(
        {
            "sites": ["S" * 12 + str(i) for i in range(4)],
            "pcodes": ["P" * 12 + str(i) for i in range(4)],
            "stats": ["T" * 12 + str(i) for i in range(4)],
        }
    )

    # Three independent axes — every (site_chunk, pcode_chunk, stat_chunk)
    # triple must appear exactly once. Confirm:
    sites_seen = {c[0] for c in calls}
    pcodes_seen = {c[1] for c in calls}
    stats_seen = {c[2] for c in calls}

    assert len(sites_seen) > 1, "sites axis was not split"
    assert len(pcodes_seen) > 1, "pcodes axis was not split"
    assert len(stats_seen) > 1, "stats axis was not split"

    # Cartesian shape: # sub-requests == product of unique chunks across axes
    expected = len(sites_seen) * len(pcodes_seen) * len(stats_seen)
    assert len(calls) == expected, (
        f"expected {expected} cartesian-product sub-requests, got {len(calls)}"
    )
    # And no triple repeats (exhaustive enumeration, no duplicates).
    assert len(set(calls)) == len(calls)
    # The chunked values, when unioned across calls, recover the original list.
    assert {x for tup in sites_seen for x in tup} == {
        "S" * 12 + str(i) for i in range(4)
    }
    assert {x for tup in pcodes_seen for x in tup} == {
        "P" * 12 + str(i) for i in range(4)
    }
    assert {x for tup in stats_seen for x in tup} == {
        "T" * 12 + str(i) for i in range(4)
    }


def test_multi_value_chunked_lazy_url_limit(monkeypatch):
    """``url_limit=None`` → resolve chunking._WATERDATA_URL_BYTE_LIMIT at call
    time, so tests that patch the constant affect this decorator too."""
    calls = []

    @multi_value_chunked(build_request=_fake_build)  # url_limit defaults to None
    def fetch(args):
        calls.append(args)
        return pd.DataFrame(), mock.Mock(
            elapsed=datetime.timedelta(seconds=0.1), headers={}
        )

    monkeypatch.setattr(_chunking, "_WATERDATA_URL_BYTE_LIMIT", 240)
    # 4 sites of 10 chars → exceeds 240 → planner splits.
    fetch({"sites": ["S" * 10 + str(i) for i in range(4)]})
    assert len(calls) > 1, "patched constant should drive chunking"


def test_chunked_session_shared_across_sub_requests():
    """Every sub-request of one chunked call sees the same
    ``requests.Session`` on the ``_chunked_session`` ContextVar, so
    downstream paginated helpers (``_walk_pages``) can reuse the
    connection pool instead of handshaking fresh on each sub-request."""
    sessions_seen = []

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    def fetch(args):
        sessions_seen.append(_chunked_session.get())
        return pd.DataFrame(), mock.Mock(
            elapsed=datetime.timedelta(seconds=0.1), headers={}
        )

    # Outside a chunked call: no session published.
    assert _chunked_session.get() is None

    fetch({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})

    # Plan must actually fan out — otherwise the test isn't exercising
    # the shared-session path.
    assert len(sessions_seen) > 1
    # Every sub-request saw a Session, not None.
    assert all(s is not None for s in sessions_seen)
    # And it was the same object every time.
    assert len({id(s) for s in sessions_seen}) == 1
    # On exit the ContextVar is reset to its default.
    assert _chunked_session.get() is None


def test_chunked_session_isolated_per_resume():
    """A follow-up ``resume`` after an interruption opens a fresh
    session — the previous one was closed when its ``resume`` returned.
    The ContextVar is reset between calls so leakage can't carry
    a closed session into the retry."""
    state = {"i": 0, "blow_up": True}

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 1 and state["blow_up"]:
            raise RateLimited("429: Too many requests.")
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            mock.Mock(
                elapsed=datetime.timedelta(seconds=0.1),
                headers={_QUOTA_HEADER: "500"},
            ),
        )

    with pytest.raises(QuotaExhausted) as excinfo:
        fetch({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})

    # First resume's session is closed; ContextVar is reset.
    assert _chunked_session.get() is None

    state["blow_up"] = False
    excinfo.value.call.resume()
    # Second resume's session is also cleaned up.
    assert _chunked_session.get() is None


def _quota_response(remaining: int | str | None) -> mock.Mock:
    """A mock requests.Response-like object whose ``x-ratelimit-remaining``
    header reflects the given value (None → header absent)."""
    resp = mock.Mock(elapsed=datetime.timedelta(seconds=0.1))
    resp.headers = {} if remaining is None else {_QUOTA_HEADER: str(remaining)}
    return resp


def test_read_remaining_parses_header():
    assert _read_remaining(_quota_response(42)) == 42


def test_read_remaining_returns_none_when_header_missing():
    """No rate-limit header → ``None`` so ``ChunkedCall`` can branch
    on ``is None`` instead of comparing against a magic sentinel."""
    assert _read_remaining(_quota_response(None)) is None


def test_read_remaining_returns_none_on_malformed_header():
    """Non-integer header value → ``None`` so a parse failure doesn't
    trip the quota check."""
    assert _read_remaining(_quota_response("not-a-number")) is None


def test_request_exceeds_quota_after_first_chunk():
    """Plan totals 4 sub-requests. The first response reports
    ``x-ratelimit-remaining=1`` — only 2 sub-requests fit total
    (the one just issued + 1 more). The wrapper must raise
    ``RequestExceedsQuota`` *before* issuing chunk 2, and the
    exception must carry a ``.call`` handle so the first chunk's
    already-fetched data is recoverable."""
    calls: list[dict] = []

    def fetch(args):
        calls.append(args)
        return pd.DataFrame({"sites": list(args["sites"])}), _quota_response(1)

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)

    with pytest.raises(RequestExceedsQuota) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})

    err = excinfo.value
    assert err.planned_chunks == 4
    assert err.available == 2  # remaining=1 + the chunk we just spent
    assert err.deficit == 2
    assert len(calls) == 1, "only the first chunk should have been issued"
    # The originating ChunkedCall is exposed on .call so the first
    # chunk's already-fetched data is recoverable.
    assert err.call is not None
    assert err.call.completed_chunks == 1
    assert not err.call.partial_frame.empty


def test_request_exceeds_quota_message_reports_deficit():
    """The error must surface planned / available / deficit so callers
    know precisely how far over budget the call is."""
    e = RequestExceedsQuota(planned_chunks=10, available=4, deficit=6)
    msg = str(e)
    assert "10" in msg
    assert "4" in msg
    assert "6" in msg


def test_request_exceeds_quota_not_raised_when_plan_fits():
    """If ``x-ratelimit-remaining`` is large enough to cover the rest
    of the plan, ``ChunkedCall`` proceeds normally."""
    remaining_seq = iter([100, 99, 98, 97])

    def fetch(args):
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(next(remaining_seq)),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    df, _ = decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})
    assert len(df) == 4


def test_no_quota_check_when_header_absent():
    """Without an ``x-ratelimit-remaining`` header ``ChunkedCall``
    has no quota signal and must NOT synthesize a
    ``RequestExceedsQuota``; every planned sub-request runs."""

    def fetch(args):
        return pd.DataFrame({"sites": list(args["sites"])}), _quota_response(None)

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    df, _ = decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})
    assert len(df) == 4


def test_quota_exhausted_on_mid_call_429():
    """Mid-call 429 (a concurrent caller drained the window) surfaces
    as ``QuotaExhausted`` carrying the partial frame plus the chunk
    offset so callers can resume after the window resets."""
    state = {"i": 0}

    def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 2:
            # Match _walk_pages's wrapping: a generic mid-pagination
            # RuntimeError with the typed RateLimited as __cause__.
            try:
                raise RateLimited("429: Too many requests made.")
            except RateLimited as cause:
                raise RuntimeError(
                    "Paginated request failed after collecting 0 page(s): "
                    "429: Too many requests made."
                ) from cause
        return (
            pd.DataFrame({"i": [i], "sites": list(args["sites"])}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(QuotaExhausted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10]})

    err = excinfo.value
    assert err.completed_chunks == 2  # chunks 0 and 1 completed; 429 hit on i=2
    assert err.total_chunks == 5
    assert err.partial_frame is not None
    assert set(err.partial_frame["i"]) == {0, 1}


def test_quota_exhausted_on_first_chunk_429_has_no_partial_response():
    """A 429 on the very first sub-request means no responses have
    completed; ``partial_response`` is ``None`` (and ``partial_frame``
    is empty) so callers can branch on that to distinguish "abort
    before any data arrived" from "abort after partial collection"."""

    def fetch(args):
        raise RateLimited("429: Too many requests made.")

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(QuotaExhausted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10]})
    err = excinfo.value
    assert err.completed_chunks == 0
    assert err.partial_response is None
    assert err.partial_frame.empty


def test_quota_exhausted_resume_picks_up_where_429_stopped():
    """After a mid-call 429 ``ChunkedCall`` raises ``QuotaExhausted``;
    once the window resets, ``e.call.resume()`` re-issues only the
    sub-requests that hadn't completed and returns the full combined
    result. Chunks completed before the 429 are not re-fetched."""
    # The fake fetch 429s on the third call, then succeeds on every
    # subsequent call. We track which sub-args have been issued so we
    # can assert chunks 0/1 aren't re-fetched on resume.
    fetched_sites: list[tuple[str, ...]] = []
    rate_limited_once = {"fired": False}

    def fetch(args):
        if len(fetched_sites) == 2 and not rate_limited_once["fired"]:
            rate_limited_once["fired"] = True
            raise RateLimited("429: Too many requests made.")
        site_tuple = tuple(args["sites"])
        fetched_sites.append(site_tuple)
        return (
            pd.DataFrame({"sites": list(site_tuple)}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    sites = ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10]

    # First attempt: 429 on the third sub-request.
    with pytest.raises(QuotaExhausted) as excinfo:
        decorated({"sites": sites})
    err = excinfo.value
    assert err.completed_chunks == 2
    pre_resume_count = len(fetched_sites)
    assert pre_resume_count == 2  # chunks 0 and 1 completed

    # Resume: re-issues only the still-pending sub-requests.
    df, _ = err.call.resume()

    # Three more fetches happened on resume (chunks 2, 3, 4); chunks 0
    # and 1 were not re-fetched.
    assert len(fetched_sites) - pre_resume_count == 3, (
        f"expected 3 new fetches on resume (chunks 2, 3, 4); got "
        f"{len(fetched_sites) - pre_resume_count}"
    )
    # Every original site appears in the combined frame exactly once.
    assert sorted(df["sites"].tolist()) == sorted(sites)


def test_quota_exhausted_resume_can_reraise_on_persistent_429():
    """If the window is still empty when the caller resumes,
    ``call.resume()`` raises ``QuotaExhausted`` again — the
    ``ChunkedCall``'s in-flight state carries forward, so a
    subsequent resume after a longer wait still picks up cleanly."""
    state = {"attempts": 0}

    def fetch(args):
        i = state["attempts"]
        state["attempts"] += 1
        # First attempt 429s on chunk 2. Resume attempt 429s on what
        # would be chunk 2 again (still the first un-completed
        # sub-request).
        if i == 2 or i == 3:
            raise RateLimited("429: Too many requests made.")
        return (
            pd.DataFrame({"i": [i], "sites": list(args["sites"])}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    sites = ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10]

    with pytest.raises(QuotaExhausted) as first:
        decorated({"sites": sites})
    with pytest.raises(QuotaExhausted) as second:
        first.value.call.resume()

    # Both exceptions report the same completed_chunks count — the
    # second resume didn't make progress (it 429'd on the same chunk).
    assert first.value.completed_chunks == 2
    assert second.value.completed_chunks == 2


def test_resume_produces_dataset_identical_to_uninterrupted_run():
    """End-to-end resume equivalence: the same chunked query run twice
    — once straight through, once with a mid-stream 429 +
    ``call.resume()`` — must yield byte-identical combined frames.
    Guards against off-by-one errors in the resume cursor (re-fetching
    the chunk that 429'd, or skipping past it) and any ordering drift
    ``_combine_chunk_frames`` might introduce when its input list is
    built incrementally."""

    def make_fetch(rate_limit_at_call: int | None):
        """Build a fresh fetch_once whose Nth call raises ``RateLimited``
        (once) and whose every other call returns a deterministic frame
        keyed by the sub-args's sites."""
        state = {"calls": 0, "tripped": False}

        def fetch(args):
            state["calls"] += 1
            if state["calls"] == rate_limit_at_call and not state["tripped"]:
                state["tripped"] = True
                raise RateLimited("429: Too many requests made.")
            sites = list(args["sites"])
            return (
                pd.DataFrame(
                    {
                        "id": sites,
                        "first_site": [sites[0]] * len(sites),
                        "chunk_size": [len(sites)] * len(sites),
                    }
                ),
                _quota_response(500),
            )

        return fetch

    # 16 sites at url_limit=240 forces several chunks; the chunking
    # plan is deterministic, so both runs traverse the same sub-args
    # sequence.
    sites = ["S" * 10 + str(i) for i in range(16)]

    # Run A: uninterrupted.
    fetch_a = make_fetch(rate_limit_at_call=None)
    decorated_a = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch_a)
    df_a, _ = decorated_a({"sites": sites})

    # Run B: trigger 429 on the third sub-request, then resume.
    fetch_b = make_fetch(rate_limit_at_call=3)
    decorated_b = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch_b)
    with pytest.raises(QuotaExhausted) as excinfo:
        decorated_b({"sites": sites})
    # The 429 must hit mid-stream — otherwise the test isn't exercising
    # what we think it is.
    assert 0 < excinfo.value.completed_chunks < excinfo.value.total_chunks
    df_b, _ = excinfo.value.call.resume()

    # Sanity: both runs must have actually chunked (otherwise the
    # 429-mid-stream branch wasn't exercised).
    assert excinfo.value.total_chunks > 1

    # The combined DataFrames must be byte-identical: same rows in the
    # same order, same dtypes. ``check_like=False`` keeps row-order
    # comparison strict so a permutation introduced by the resume path
    # would still fail.
    pd.testing.assert_frame_equal(df_a, df_b)

    # And every original site must be present exactly once.
    assert sorted(df_a["id"].tolist()) == sorted(sites)


def test_chunker_passes_through_non_429_runtime_error():
    """A non-429 ``RuntimeError`` (e.g. a 500) is not a quota signal;
    it must propagate unchanged so callers see the real cause."""
    state = {"i": 0}

    def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 2:
            raise RuntimeError("500: Internal server error.")
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(RuntimeError, match=r"^500:"):
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10]})


def test_chunker_wraps_service_unavailable_as_resumable():
    """A typed ``ServiceUnavailable`` (HTTP 5xx) is a transient
    transport failure: ``ChunkedCall`` must wrap it as
    ``ServiceInterrupted`` carrying the partial state, parallel to how
    a 429 becomes ``QuotaExhausted``. Once the upstream recovers,
    ``.call.resume()`` resumes only the still-pending sub-requests."""
    state = {"i": 0, "blow_up": True}

    def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 2 and state["blow_up"]:
            try:
                raise ServiceUnavailable("503: Service unavailable.")
            except ServiceUnavailable as cause:
                raise RuntimeError(str(cause)) from cause
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(ServiceInterrupted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10]})
    err = excinfo.value
    # Resumable: handle on .call with already-completed work preserved.
    assert err.call is not None
    assert err.completed_chunks == 2
    assert err.total_chunks == 5
    assert not err.call.partial_frame.empty
    # Upstream recovers; resuming completes the call.
    state["blow_up"] = False
    df, _ = err.call.resume()
    assert set(df["sites"]) == {"S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10}


def test_chunk_interrupted_base_class_catches_both():
    """``ChunkInterrupted`` is the common base for 429/5xx
    interruptions, so callers who want one retry policy across all
    transient failures can catch the base class. ``QuotaExhausted``
    and ``ServiceInterrupted`` must both subclass it."""
    assert issubclass(QuotaExhausted, ChunkInterrupted)
    assert issubclass(ServiceInterrupted, ChunkInterrupted)
    # Sanity: ``ChunkInterrupted`` is itself a ``RuntimeError`` so
    # bare ``except RuntimeError`` callers don't suddenly miss the
    # wrapped failures after this refactor.
    assert issubclass(ChunkInterrupted, RuntimeError)


def test_connection_error_wrapped_as_service_interrupted():
    """A bare ``requests.exceptions.ConnectionError`` (or any other
    transport-level RequestException) doesn't inherit from
    ``RuntimeError``; without the widened catch in ``_issue`` it
    would escape uncaught and the user would lose the resumable
    handle to ``.call.resume()``. Verify ``ChunkedCall`` wraps it as
    ``ServiceInterrupted`` so partial progress is preserved."""
    import requests as _requests

    state = {"i": 0, "blow_up": True}

    def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 2 and state["blow_up"]:
            raise _requests.exceptions.ConnectionError("connection reset")
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(ServiceInterrupted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10]})

    err = excinfo.value
    assert err.completed_chunks == 2
    assert err.call is not None
    # The transport exception is on __cause__ so callers can drill in if needed.
    assert isinstance(err.__cause__, _requests.exceptions.ConnectionError)
    # Resume after the upstream recovers.
    state["blow_up"] = False
    df, _ = err.call.resume()
    assert set(df["sites"]) == {"S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10}


def test_service_interrupted_exposes_partial_frame_and_response():
    """Both ``QuotaExhausted`` AND ``ServiceInterrupted`` carry
    ``partial_frame`` / ``partial_response`` directly on the
    exception. Previously only ``QuotaExhausted`` had them, so a
    generic ``except ChunkInterrupted as exc: log(exc.partial_frame)``
    crashed with AttributeError on 5xx."""
    state = {"i": 0}

    def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 2:
            try:
                raise ServiceUnavailable("503: Service unavailable.")
            except ServiceUnavailable as cause:
                raise RuntimeError(str(cause)) from cause
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(ServiceInterrupted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10]})
    err = excinfo.value
    # Direct attribute access works for both subclasses now.
    assert hasattr(err, "partial_frame")
    assert hasattr(err, "partial_response")
    assert not err.partial_frame.empty
    assert err.partial_response is not None


def test_partial_frame_snapshot_stable_across_resume():
    """``exc.partial_frame`` / ``exc.partial_response`` snapshot the
    state at raise time. Calling ``exc.call.resume()`` advances the
    underlying ``ChunkedCall`` but must NOT mutate the snapshot on
    the exception — otherwise a diagnostic that reads
    ``exc.partial_frame`` after a resume sees post-resume state under
    a name that promises pre-resume state."""
    state = {"i": 0, "blow_up": True}

    def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 2 and state["blow_up"]:
            raise RateLimited("429: Too many requests.")
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(QuotaExhausted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10]})
    err = excinfo.value
    snapshot_rows = len(err.partial_frame)
    assert snapshot_rows > 0  # two chunks worth of data captured

    # Resume; the live view on .call grows.
    state["blow_up"] = False
    err.call.resume()
    assert len(err.call.partial_frame) > snapshot_rows

    # The exception's snapshot must NOT advance.
    assert len(err.partial_frame) == snapshot_rows


def test_partial_frame_snapshot_is_a_copy_when_single_chunk():
    """``_combine_chunk_frames`` returns ``non_empty[0]`` verbatim on
    its single-frame fast path. ``ChunkInterrupted.__init__`` must
    therefore defensively ``.copy()`` so an in-place mutation of the
    underlying chunk frame (e.g. user diagnostic code adding a
    column on the live view) doesn't leak through the snapshot.
    Companion to ``test_partial_frame_snapshot_stable_across_resume``,
    which uses ≥2 completed chunks and so goes through
    ``pd.concat`` (which already produces a fresh frame)."""
    state = {"i": 0, "blow_up": True}

    def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 1 and state["blow_up"]:
            raise RateLimited("429: Too many requests.")
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(500),
        )

    # 4 sites at url_limit=240 → 2 sub-requests. The 429 fires on the
    # SECOND sub-request, so the exception captures exactly ONE
    # completed chunk — the path where _combine_chunk_frames aliases.
    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(QuotaExhausted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})
    err = excinfo.value
    assert err.completed_chunks == 1

    snapshot_cols = list(err.partial_frame.columns)
    # Mutate the underlying chunk in place — the snapshot must NOT
    # reflect the mutation.
    err.call._chunks[0][0]["extra"] = 0
    assert list(err.partial_frame.columns) == snapshot_cols
    assert "extra" not in err.partial_frame.columns


def test_combine_chunk_responses_returns_independent_headers():
    """The aggregated response's ``.headers`` must be a fresh
    ``CaseInsensitiveDict`` — mutations by downstream callers
    (logging hooks, metadata extensions) must not back-propagate into
    the underlying chunk response's headers, which still live on
    ``ChunkedCall._chunks``."""
    from dataretrieval.waterdata.chunking import _combine_chunk_responses

    r0 = mock.Mock(
        elapsed=datetime.timedelta(seconds=0.1), headers={"X-Foo": "0"}, url="u0"
    )
    r1 = mock.Mock(
        elapsed=datetime.timedelta(seconds=0.2), headers={"X-Foo": "1"}, url="u1"
    )
    head = _combine_chunk_responses([r0, r1], canonical_url=None)

    # Aggregate carries the last chunk's headers...
    assert head.headers["X-Foo"] == "1"
    # ...but mutating the aggregate must not back-propagate.
    head.headers["X-Trace-Id"] = "abc"
    assert "X-Trace-Id" not in r1.headers
    assert "X-Trace-Id" not in r0.headers


def test_paginate_terminates_on_empty_string_cursor():
    """``_paginate``'s loop predicate is ``while cursor is not None``.
    Parse-response wrappers in ``_walk_pages`` / ``get_stats_data``
    coerce falsy non-None values to None so an empty-string next-
    cursor (a real-but-unusual end-of-stream sentinel some pagination
    APIs use) doesn't trap us in an infinite ``follow_up('')`` loop."""
    import datetime as _dt
    from unittest import mock as _mock

    import requests as _requests

    from dataretrieval.waterdata import utils as _utils

    # Synthesize an OGC response with numberReturned > 0 and a "next"
    # link whose href is an empty string — simulating a server-side
    # sentinel that ``_next_req_url`` reads as ``""``.
    body_with_empty_next = {
        "numberReturned": 1,
        "features": [{"id": "1", "properties": {"val": "a"}}],
        "links": [{"rel": "next", "href": ""}],
    }
    resp = _mock.MagicMock(spec=_requests.Response)
    resp.status_code = 200
    resp.url = "https://example.com/items?limit=1"
    resp.elapsed = _dt.timedelta(seconds=0.1)
    resp.headers = {}
    resp.json.return_value = body_with_empty_next

    client = _mock.MagicMock(spec=_requests.Session)
    client.send.return_value = resp

    req = _mock.MagicMock(spec=_requests.PreparedRequest)
    req.method = "GET"
    req.headers = {}
    req.body = None
    req.url = "https://example.com/items?limit=1"

    df, final = _utils._walk_pages(geopd=False, req=req, client=client)

    # Single send + zero follow-ups: the loop terminated on the empty cursor.
    assert client.send.called
    assert not client.request.called
    assert len(df) == 1


def test_combine_chunk_frames_does_not_collapse_none_ids():
    """``drop_duplicates(subset='id')`` treats NaN==NaN as duplicate,
    so a blanket dedup would collapse every id-less row into one —
    silent data loss. The function must dedupe only the id-bearing
    rows and preserve id-less rows verbatim."""
    import numpy as np

    from dataretrieval.waterdata.chunking import _combine_chunk_frames

    # Frame A has real ids; frame B has feature-IDs of None for two
    # different rows that must both survive.
    df_a = pd.DataFrame({"id": ["x", "y"], "val": [1, 2]})
    df_b = pd.DataFrame({"id": [np.nan, np.nan], "val": [3, 4]})
    combined = _combine_chunk_frames([df_a, df_b])

    # 4 rows preserved: 2 id-bearing + 2 id-less (NaN rows NOT merged).
    assert len(combined) == 4
    assert sorted(combined["val"].tolist()) == [1, 2, 3, 4]


def test_combine_chunk_frames_still_dedupes_overlapping_ids():
    """The original dedup contract — overlapping OR-clause partitions
    that produce duplicate-id rows across chunks must still collapse
    to one row — has to keep working when ids ARE present."""
    from dataretrieval.waterdata.chunking import _combine_chunk_frames

    df_a = pd.DataFrame({"id": ["x", "y"], "val": [1, 2]})
    df_b = pd.DataFrame({"id": ["y", "z"], "val": [2, 3]})
    combined = _combine_chunk_frames([df_a, df_b])
    assert sorted(combined["id"].tolist()) == ["x", "y", "z"]


def test_retry_after_surfaces_on_quota_exhausted():
    """If the 429 response includes a ``Retry-After`` header, that
    delay must travel from the typed transport exception
    (``RateLimited.retry_after``) onto ``QuotaExhausted`` so callers
    can honor the server's hint instead of guessing a wait."""
    state = {"i": 0}

    def fetch(args):
        state["i"] += 1
        if state["i"] >= 3:
            try:
                raise RateLimited("429: Too many requests.", retry_after=42.0)
            except RateLimited as cause:
                raise RuntimeError(str(cause)) from cause
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(QuotaExhausted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10]})
    assert excinfo.value.retry_after == 42.0


def test_quota_exhausted_message_points_at_resume():
    """The error message must surface the chunk offset and the resume
    affordance — ``partial_frame`` is a footgun without it."""
    e = QuotaExhausted(
        completed_chunks=7,
        total_chunks=20,
    )
    msg = str(e)
    assert "7/20" in msg
    assert "429" in msg
    assert ".call.resume()" in msg


def test_request_bytes_rejects_non_sizable_body():
    """``_request_bytes`` requires a deterministic byte count up front;
    silently treating an unknown body as zero would under-chunk and let
    the request blow past the server's POST-body limit. Generators,
    iterables, and file-like objects must surface as ``TypeError``."""
    from dataretrieval.waterdata.chunking import _request_bytes

    class _FakeReqWithGenBody:
        url = "https://example.com/foo"
        body = (b"x" for _ in range(3))

    with pytest.raises(TypeError, match="cannot size a request body"):
        _request_bytes(_FakeReqWithGenBody())


def test_request_bytes_handles_supported_body_types():
    """Sanity-check the supported body types: None (GET), bytes (raw
    POST), str (JSON-as-string POST)."""
    from dataretrieval.waterdata.chunking import _request_bytes

    class _Req:
        def __init__(self, url, body):
            self.url = url
            self.body = body

    assert _request_bytes(_Req("ab", None)) == 2
    assert _request_bytes(_Req("ab", b"cd")) == 4
    assert _request_bytes(_Req("ab", "cd")) == 4
    assert _request_bytes(_Req("ab", bytearray(b"cd"))) == 4


def test_multi_value_chunked_restores_canonical_url():
    """When chunking fans out, the aggregated response's ``.url`` must
    reflect the *user's original* query (rebuilt from the unchunked
    args), not the first chunk's URL. Callers logging ``md.url`` for
    reproducibility need the full query."""
    sites = ["S" * 10 + str(i) for i in range(4)]
    sub_urls: list[str] = []

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    def fetch(args):
        # Each sub-response carries the chunked sub_args's URL, so
        # without canonical restoration the first chunk's URL would
        # leak through to md.url.
        sub_url = _fake_build(**args).url
        sub_urls.append(sub_url)
        resp = mock.Mock(elapsed=datetime.timedelta(seconds=0.1))
        resp.headers = {}
        resp.url = sub_url
        return pd.DataFrame(), resp

    _df, md = fetch({"sites": sites})

    assert len(sub_urls) > 1, "test setup error: chunker didn't fan out"
    # md.url must equal the URL the unchunked query would have produced.
    assert md.url == _fake_build(sites=sites).url
    # And differ from every sub-request's URL (each carries a smaller list).
    assert all(md.url != u for u in sub_urls)
    # The canonical URL is strictly bigger byte-wise than any sub-request.
    assert all(len(md.url) > len(u) for u in sub_urls)


def test_extract_axes_skips_filter_passed_as_list():
    """Defensive guard: ``filter`` is documented as a string. If a caller
    mistakenly passes it as a list, ``_extract_axes`` must NOT create a
    comma-joined list axis for it — comma-joining CQL clauses inside
    the URL would produce a malformed filter expression. The filter
    axis is built only via top-level-OR splitting of the string form."""
    args = {
        "monitoring_location_id": ["USGS-A", "USGS-B"],
        "filter": ["a='1'", "a='2'"],  # malformed input
        "filter_lang": ["cql-text", "cql-json"],  # ditto
    }
    keys = {ax.arg_key for ax in _extract_axes(args)}
    assert keys == {"monitoring_location_id"}


def test_extract_axes_skips_scalar_contract_params():
    """``limit`` and ``skip_geometry`` are scalars by contract
    (``int | None`` and ``bool | None`` respectively). If a caller smuggles
    a list through type erasure (e.g. ``limit=["100","200"]`` after a
    bad cast), ``_extract_axes`` must NOT treat it as a multi-value
    axis. Chunking ``limit`` would silently fan into separate
    paginated queries with different per-request caps; chunking
    ``skip_geometry`` would emit sub-requests with conflicting
    geometry-output settings."""
    args = {
        "monitoring_location_id": ["USGS-A", "USGS-B"],
        "limit": ["100", "200"],
        "skip_geometry": ["true", "false"],
    }
    keys = {ax.arg_key for ax in _extract_axes(args)}
    assert keys == {"monitoring_location_id"}


def test_joint_planner_url_construction_long_filter_and_long_sites():
    """Realistic stress: 20 datetime OR-clauses combined with 100 USGS
    site IDs. Every sub-request URL built from the plan must fit the
    8000-byte limit, the joint planner must beat the naive "filter at
    bail-floor, chunk lists" approach, and the partitioned filters
    must union to the user's original filter expression.

    Uses the real ``_construct_api_requests`` builder so the test
    catches URL-encoding surprises that a fake builder would miss.
    """
    # Realistic AGENCY-ID site format: USGS-{8 digits}. 500 sites is
    # enough to force the URL well past the 8000-byte server limit
    # without any filter contribution.
    sites = [f"USGS-{i:08d}" for i in range(500)]
    # 20 datetime equality clauses; each ~30 bytes raw, more after URL
    # encoding (the apostrophes and `:` characters expand).
    clauses = [
        f"time='2024-{m:02d}-{d:02d}T00:00:00Z'"
        for m in range(1, 6)
        for d in (1, 8, 15, 22)
    ]
    assert len(clauses) == 20
    filter_expr = " OR ".join(clauses)

    args = {
        "service": "daily",
        "monitoring_location_id": sites,
        "filter": filter_expr,
    }
    url_limit = 8000

    plan = ChunkPlan(args, _construct_api_requests, url_limit)
    assert plan.total > 1, "expected non-trivial plan for over-limit request"

    # Walk every sub-request the plan would issue and assert URL fits.
    over_limit = []
    for sub_args in plan.iter_sub_args():
        req = _construct_api_requests(**sub_args)
        url_len = len(req.url) + (len(req.body) if req.body else 0)
        if url_len > url_limit:
            over_limit.append((url_len, sub_args))
    assert not over_limit, (
        f"{len(over_limit)} sub-request(s) exceeded the URL limit; "
        f"first: {over_limit[0]}"
    )

    # Each axis's chunks must union back to its original atoms exactly
    # once — no clause or site dropped, no duplicates introduced.
    for axis in plan.axes:
        seen = [a for chunk in plan.chunks[axis.arg_key] for a in chunk]
        assert sorted(seen) == sorted(axis.atoms), (
            f"axis {axis.arg_key} partition lost or duplicated atoms"
        )

    # Plan must beat the bail-floor-style worst case (singleton sites
    # × all filter clauses singleton = 500 * 20 = 10,000) — uniform
    # greedy halving of these inputs cuts that by at least 20×.
    assert plan.total < 500, (
        f"joint plan emitted {plan.total} sub-requests (expected <500)"
    )


def test_combine_chunk_frames_all_empty_preserves_geo_type():
    """Regression: when every chunk returns an empty frame,
    ``_combine_chunk_frames`` must not downgrade an empty
    ``GeoDataFrame`` to a plain ``DataFrame``. The whole reason the
    function drops empties before concat is to prevent that downgrade
    — the all-empty short-circuit was independently dropping it."""
    pytest.importorskip("geopandas")
    import geopandas as gpd

    from dataretrieval.waterdata.chunking import _combine_chunk_frames

    empty_gdfs = [gpd.GeoDataFrame() for _ in range(3)]
    combined = _combine_chunk_frames(empty_gdfs)
    assert isinstance(combined, gpd.GeoDataFrame), (
        f"all-empty combine returned {type(combined).__name__}; expected GeoDataFrame"
    )


def test_combine_chunk_frames_single_frame_is_safe_to_mutate():
    """Regression: the single-completed-chunk fast path returned the
    underlying chunk frame verbatim, so a caller mutating
    ``call.partial_frame`` (documented as a live view) would mutate
    ``_chunks[0][0]`` in place. The fast path now returns a copy."""
    from dataretrieval.waterdata.chunking import _combine_chunk_frames

    chunk = pd.DataFrame({"id": ["A", "B"], "value": [1, 2]})
    returned = _combine_chunk_frames([chunk])
    returned["new_col"] = "x"
    assert "new_col" not in chunk.columns


def test_iter_sub_args_passthrough_yields_a_copy():
    """Regression: the no-axes passthrough yielded ``self.args``
    directly while the chunked branch did ``dict(self.args)``. A
    ``fetch_once`` that mutated the dict it received would silently
    corrupt ``ChunkPlan.args``. The passthrough now copies too."""
    args = {"monitoring_location_id": ["USGS-A"], "limit": 100}
    plan = ChunkPlan(args, _fake_build, url_limit=8000)
    sub = next(plan.iter_sub_args())
    sub["monitoring_location_id"] = "mutated"
    sub["new_key"] = "leaked"
    assert plan.args["monitoring_location_id"] == ["USGS-A"]
    assert "new_key" not in plan.args


def test_quota_check_fires_after_every_chunk_not_just_first():
    """Regression: ``_check_quota_after_first`` was gated on
    ``len(_chunks) == 1`` so it only fired after chunk 0; a concurrent
    caller draining the window mid-call (or a partially-rolled-over
    quota on resume) went undetected. The check now fires after every
    non-final chunk."""
    # 4-chunk plan. Chunks 0 and 1 report plenty of remaining quota;
    # chunk 2's response reports remaining=0 with one chunk still
    # pending. The check must fire after chunk 2, NOT silently let
    # chunk 3 hit a mid-stream 429.
    responses = iter([500, 500, 0])
    calls: list[dict] = []

    def fetch(args):
        calls.append(args)
        return pd.DataFrame({"sites": list(args["sites"])}), _quota_response(
            next(responses)
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(RequestExceedsQuota) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})
    err = excinfo.value
    assert err.planned_chunks == 4
    # 3 completed + 0 remaining = 3 available; 1 pending; deficit 1.
    assert err.available == 3
    assert err.deficit == 1
    assert len(calls) == 3, "only chunks 0-2 should have been issued"
    # .call carries the in-flight call so the user can recover.
    assert err.call is not None
    assert err.call.completed_chunks == 3
