"""Tests for ``dataretrieval.ogc.chunking``.

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

import asyncio
import concurrent.futures
import datetime
import sys
import warnings
from unittest import mock
from urllib.parse import quote_plus

import httpx
import numpy as np
import pandas as pd
import pytest

if sys.version_info < (3, 10):
    pytest.skip("Skip entire module on Python < 3.10", allow_module_level=True)

from dataretrieval.exceptions import DataRetrievalError
from dataretrieval.ogc import chunking as _chunking
from dataretrieval.ogc.chunking import (
    _LIST_SEP,
    _NEVER_CHUNK,
    _OR_SEP,
    _QUOTA_HEADER,
    ChunkedCall,
    ChunkInterrupted,
    ChunkPlan,
    QuotaExhausted,
    RateLimited,
    RetryPolicy,
    ServiceInterrupted,
    ServiceUnavailable,
    Unchunkable,
    _chunked_client,
    _combine_chunk_frames,
    _combine_chunk_responses,
    _extract_axes,
    _request_bytes,
    _retry,
    _retryable,
    _safe_request_bytes,
    multi_value_chunked,
)
from dataretrieval.waterdata import utils as _utils
from dataretrieval.waterdata.utils import _DATE_RANGE_PARAMS, _construct_api_requests


def _aiozero(_d):
    """An async no-op sleep — monkeypatched over ``asyncio.sleep`` (via
    the chunking module's binding) so retry backoff doesn't wait in tests."""

    async def _noop():
        return None

    return _noop()


def _recording_sleep(slept):
    """An ``_aiozero`` variant that appends each requested delay to ``slept``
    before resolving — for tests that need to assert what would have been waited."""

    def _record(delay):
        slept.append(delay)
        return _aiozero(delay)

    return _record


class _FakeReq:
    """Stand-in for ``httpx.Request`` whose ``_request_bytes`` shape
    is ``len(str(url)) + len(content)``."""

    __slots__ = ("url", "content")

    def __init__(self, url, content=b""):
        self.url = url
        self.content = (
            content
            if isinstance(content, (bytes, bytearray))
            else (content.encode("utf-8") if isinstance(content, str) else b"")
        )


def _fake_build(*, base=200, **kwargs):
    """Fake build_request: URL length deterministic in its inputs.

    Mirrors the GET-routed shape: payload goes in the URL, body is empty.
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
    """Scalar args with nothing to chunk and a request within the limit →
    passthrough (no axes)."""
    args = {"monitoring_location_id": "scalar-only"}
    plan = ChunkPlan(args, _fake_build, url_limit=1000)
    assert plan.axes == []
    assert plan.total == 1


def test_chunk_plan_raises_when_unchunkable_request_exceeds_limit():
    """A request with nothing to chunk that still exceeds the byte limit (e.g.
    a single large CQL ``IN`` clause with no top-level ``OR``) raises
    Unchunkable instead of being shipped for the server to reject with an
    opaque HTTP 414."""
    args = {"monitoring_location_id": "scalar-only"}
    with pytest.raises(Unchunkable):
        ChunkPlan(args, _fake_build, url_limit=10)


def test_chunk_plan_passes_through_unchunkable_cql_json_over_limit():
    """A cql-json filter is outside the chunker's domain (it splits only
    cql-text), so an over-budget cql-json request is passed through unchanged
    instead of raising — the server judges it, not us. Guards against the
    chunker hijacking the deliberate cql-json passthrough."""
    args = {"filter": "a OR b OR c", "filter_lang": "cql-json"}
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
    """Limit below the singleton-per-axis floor → ``Unchunkable``;
    there's nothing left to shrink."""
    args = {"monitoring_location_id": ["A", "B"]}
    # base=200 alone exceeds limit=100; chunking can't help.
    with pytest.raises(Unchunkable, match="smallest reducible"):
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
    Unchunkable — there's nothing left to shrink."""
    args = {
        "monitoring_location_id": ["A" * 10, "B" * 10],
        "filter": "x='12345' OR x='67890'",  # min clause is 9 chars
    }
    # Base 200 + singleton site (10) + singleton clause (9) = 219; limit
    # below 219 → no joint plan can fit.
    with pytest.raises(Unchunkable):
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
    async def fetch(args):
        calls.append(args)
        return pd.DataFrame(), mock.Mock(
            elapsed=datetime.timedelta(seconds=0.1), headers={}
        )

    fetch({"monitoring_location_id": ["A", "B"]})
    assert len(calls) == 1
    assert calls[0]["monitoring_location_id"] == ["A", "B"]


def test_multi_value_chunked_emits_3d_cartesian_product():
    """Three chunkable axes, each forced to split → exhaustive cartesian
    product across all three. Verifies the halving loop in
    ``ChunkPlan._plan`` handles N>2 axes uniformly and the ``ChunkedCall``
    ``itertools.product`` enumerates every combination exactly once."""
    calls = []

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    async def fetch(args):
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
    async def fetch(args):
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
    ``httpx.AsyncClient`` on the ``_chunked_client`` ContextVar, so
    downstream paginated helpers (``_walk_pages``) can reuse the
    connection pool instead of handshaking fresh on each sub-request."""
    sessions_seen = []

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    async def fetch(args):
        sessions_seen.append(_chunked_client.get())
        return pd.DataFrame(), mock.Mock(
            elapsed=datetime.timedelta(seconds=0.1), headers={}
        )

    # Outside a chunked call: no session published (in this thread/context).
    assert _chunked_client.get() is None

    fetch({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})

    # Plan must actually fan out — otherwise the test isn't exercising
    # the shared-session path.
    assert len(sessions_seen) > 1
    # Every sub-request saw a Session, not None.
    assert all(s is not None for s in sessions_seen)
    # And it was the same object every time.
    assert len({id(s) for s in sessions_seen}) == 1
    # The portal's worker context is torn down on exit, so the calling
    # thread's ContextVar still reads its default.
    assert _chunked_client.get() is None


def test_chunked_session_isolated_per_resume():
    """A follow-up ``resume`` after an interruption opens a fresh
    session — the previous one was closed when its ``resume`` returned.
    The ContextVar is reset between runs so leakage can't carry
    a closed session into the retry."""
    state = {"i": 0, "blow_up": True}
    sessions_seen = []

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    async def fetch(args):
        sessions_seen.append(_chunked_client.get())
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

    # First run published a shared client to its sub-requests; the calling
    # thread's ContextVar is unaffected (reads its default).
    assert _chunked_client.get() is None
    first_run_sessions = list(sessions_seen)
    assert first_run_sessions and all(s is not None for s in first_run_sessions)

    state["blow_up"] = False
    excinfo.value.call.resume()
    # Second run's ContextVar is also reset in the calling thread.
    assert _chunked_client.get() is None
    # The resume opened a FRESH client, distinct from the first run's, so no
    # closed client leaks across runs.
    resume_sessions = sessions_seen[len(first_run_sessions) :]
    assert resume_sessions and all(s is not None for s in resume_sessions)
    assert {id(s) for s in resume_sessions}.isdisjoint(
        {id(s) for s in first_run_sessions}
    )


def _quota_response(remaining: int | str | None) -> mock.Mock:
    """A mock httpx.Response-like object whose ``x-ratelimit-remaining``
    header reflects the given value (None → header absent)."""
    resp = mock.Mock(elapsed=datetime.timedelta(seconds=0.1))
    resp.headers = {} if remaining is None else {_QUOTA_HEADER: str(remaining)}
    return resp


def test_quota_exhausted_on_mid_call_429():
    """Mid-call 429 (a concurrent caller drained the window) surfaces
    as ``QuotaExhausted`` carrying the partial frame plus the chunk
    offset so callers can resume after the window resets."""
    state = {"i": 0}

    async def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 2:
            # Match _walk_pages's wrapping: a generic mid-pagination
            # DataRetrievalError with the typed RateLimited as __cause__.
            try:
                raise RateLimited("429: Too many requests made.")
            except RateLimited as cause:
                raise DataRetrievalError(
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
    # Async fan-out: every non-failing sub-request completes (the gather
    # runs all of them; only i==2 raises), so 4 of 5 complete.
    assert err.completed_chunks == 4  # only the i==2 sub-request failed
    assert err.total_chunks == 5
    assert err.partial_frame is not None
    assert set(err.partial_frame["i"]) == {0, 1, 3, 4}


def test_quota_exhausted_on_first_chunk_429_has_no_partial_response():
    """A 429 on the very first sub-request means no responses have
    completed; ``partial_response`` is ``None`` (and ``partial_frame``
    is empty) so callers can branch on that to distinguish "abort
    before any data arrived" from "abort after partial collection"."""

    async def fetch(args):
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
    # One sub-request (the chunk containing the failing site) 429s on the
    # first gather, then succeeds once the window resets. Under the async
    # fan-out every OTHER sub-request completes on the first gather, so
    # resume re-issues only the single still-pending chunk. We track which
    # sub-args have been issued to assert the completed chunks aren't
    # re-fetched.
    fetched_sites: list[tuple[str, ...]] = []
    failing_site = "S3" * 10
    rate_limited_once = {"fired": False}

    async def fetch(args):
        if failing_site in args["sites"] and not rate_limited_once["fired"]:
            rate_limited_once["fired"] = True
            raise RateLimited("429: Too many requests made.")
        site_tuple = tuple(args["sites"])
        fetched_sites.append(site_tuple)
        return (
            pd.DataFrame({"sites": list(site_tuple)}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    sites = ["S1" * 10, "S2" * 10, failing_site, "S4" * 10, "S5" * 10]

    # First attempt: 429 on the chunk carrying the failing site; the other
    # four sub-requests complete.
    with pytest.raises(QuotaExhausted) as excinfo:
        decorated({"sites": sites})
    err = excinfo.value
    assert err.completed_chunks == 4
    pre_resume_count = len(fetched_sites)
    assert pre_resume_count == 4  # every chunk but the failing one completed

    # Resume: re-issues only the still-pending sub-request.
    df, _ = err.call.resume()

    # Exactly one more fetch happened on resume (the chunk that 429'd);
    # the four already-completed chunks were not re-fetched.
    assert len(fetched_sites) - pre_resume_count == 1, (
        f"expected 1 new fetch on resume (the failing chunk); got "
        f"{len(fetched_sites) - pre_resume_count}"
    )
    # Every original site appears in the combined frame exactly once.
    assert sorted(df["sites"].tolist()) == sorted(sites)


def test_quota_exhausted_resume_can_reraise_on_persistent_429():
    """If the window is still empty when the caller resumes,
    ``call.resume()`` raises ``QuotaExhausted`` again — the
    ``ChunkedCall``'s in-flight state carries forward, so a
    subsequent resume after a longer wait still picks up cleanly."""
    # Key the failure on the chunk's CONTENT (one persistently-429ing
    # site) rather than a global call counter: under the async fan-out
    # every other sub-request completes, and the same still-pending
    # sub-request re-fails on resume — so the completed count is stable.
    failing_site = "S3" * 10

    async def fetch(args):
        if failing_site in args["sites"]:
            raise RateLimited("429: Too many requests made.")
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    sites = ["S1" * 10, "S2" * 10, failing_site, "S4" * 10, "S5" * 10]

    with pytest.raises(QuotaExhausted) as first:
        decorated({"sites": sites})
    with pytest.raises(QuotaExhausted) as second:
        first.value.call.resume()

    # Both exceptions report the same completed_chunks count — every
    # sub-request but the persistently-429ing one completed on the first
    # gather, and the resume re-issued only that one, which 429'd again.
    assert first.value.completed_chunks == 4
    assert second.value.completed_chunks == 4


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

        async def fetch(args):
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

    async def fetch(args):
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

    async def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 2 and state["blow_up"]:
            try:
                raise ServiceUnavailable("503: Service unavailable.")
            except ServiceUnavailable as cause:
                raise DataRetrievalError(str(cause)) from cause
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
    # Async fan-out: only the i==2 sub-request fails; the gather completes
    # the other four, so 4 of 5 are recorded before the failure surfaces.
    assert err.completed_chunks == 4
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
    # ``ChunkInterrupted`` roots at ``DataRetrievalError`` like the rest of the
    # taxonomy (no ``RuntimeError`` mixin), so one ``except DataRetrievalError``
    # spans chunked and single-shot failures alike.
    assert issubclass(ChunkInterrupted, DataRetrievalError)
    assert not issubclass(ChunkInterrupted, RuntimeError)


def test_chunk_interrupted_pickles_as_degraded_across_process_boundary():
    """A real ChunkInterrupted carries a live ChunkedCall whose ``fetch`` is not
    stdlib-picklable, so a worker raising it inside a multiprocessing /
    ProcessPoolExecutor pool could not ship it back. ``__getstate__`` drops
    ``.call`` and pickles the documented degraded ``call=None`` state -- counts
    and retry hint preserved, ``.resume()`` gone (un-resumable cross-process)."""
    import pickle

    plan = ChunkPlan(
        {"monitoring_location_id": ["A", "B", "C"]}, _fake_build, url_limit=8000
    )
    # A local function isn't picklable by reference -- mirrors production, where
    # ChunkedCall.fetch is the undecorated _fetch_once shadowed by its wrapper.
    call = ChunkedCall(plan, lambda args: (pd.DataFrame(), None))
    exc = call.wrap_failure(RateLimited("429: too many requests", retry_after=12.0))
    assert isinstance(exc, QuotaExhausted) and exc.call is call
    # the live fetch handle alone can't pickle (the whole point of the override)
    with pytest.raises((pickle.PicklingError, AttributeError)):
        pickle.dumps(exc.call.fetch)

    revived = pickle.loads(pickle.dumps(exc))
    assert isinstance(revived, QuotaExhausted)
    assert revived.call is None  # degraded: no cross-process resume handle
    assert revived.completed_chunks == exc.completed_chunks
    assert revived.total_chunks == exc.total_chunks
    assert revived.retry_after == 12.0
    assert str(revived) == str(exc)


def test_chunk_interrupted_with_partial_data_pickles_intact():
    """The degrade drops only the live ``.call``; the captured *partial work*
    must still cross the boundary so a worker can report what it salvaged.
    Exercises the path the no-completed-chunks case above doesn't: a real
    ``partial_frame`` (rows) and ``partial_response`` (a live ``httpx.Response``,
    which must itself remain picklable)."""
    import pickle

    plan = ChunkPlan(
        {"monitoring_location_id": ["A", "B", "C"]}, _fake_build, url_limit=8000
    )
    call = ChunkedCall(plan, lambda args: (pd.DataFrame(), None))
    # One sub-request completed before the failure: a real frame + response.
    call._chunks[0] = (
        pd.DataFrame({"id": ["A"]}),
        httpx.Response(
            200,
            request=httpx.Request("GET", "https://example.invalid/a"),
            json={"features": []},
        ),
    )
    exc = call.wrap_failure(ServiceUnavailable("503: down"))
    assert exc.completed_chunks == 1
    assert not exc.partial_frame.empty and exc.partial_response is not None

    revived = pickle.loads(pickle.dumps(exc))
    assert revived.call is None
    assert revived.partial_frame["id"].tolist() == ["A"]
    assert isinstance(revived.partial_response, httpx.Response)
    assert revived.partial_response.status_code == 200


def test_connection_error_wrapped_as_service_interrupted():
    """A bare ``httpx.ConnectError`` (or any other transport-level
    ``httpx.HTTPError``) doesn't inherit from ``RuntimeError``;
    without the widened catch in ``_issue`` it would escape uncaught
    and the user would lose the resumable handle to ``.call.resume()``.
    Verify ``ChunkedCall`` wraps it as ``ServiceInterrupted`` so
    partial progress is preserved."""
    state = {"i": 0, "blow_up": True}

    async def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 2 and state["blow_up"]:
            raise httpx.ConnectError("connection reset")
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(ServiceInterrupted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10]})

    err = excinfo.value
    # Async fan-out: only the i==2 sub-request fails; the other four complete.
    assert err.completed_chunks == 4
    assert err.call is not None
    # The transport exception is on __cause__ so callers can drill in if needed.
    assert isinstance(err.__cause__, httpx.ConnectError)
    # Resume after the upstream recovers.
    state["blow_up"] = False
    df, _ = err.call.resume()
    assert set(df["sites"]) == {"S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10}


def test_invalid_url_wrapped_as_service_interrupted():
    """``httpx.InvalidURL`` inherits from ``Exception``, NOT from
    ``httpx.HTTPError``. Without the widened catch in ``_issue`` /
    ``_classify_chunk_error`` an oversize follow-up URL escapes as
    raw ``InvalidURL`` and the user loses ``.call.resume()`` access
    to the partial state. Mirror the ConnectError test."""
    state = {"i": 0, "blow_up": True}

    async def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 2 and state["blow_up"]:
            raise httpx.InvalidURL("URL is too long: 65536 bytes > 65000")
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(500),
        )

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(ServiceInterrupted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10, "S5" * 10]})

    err = excinfo.value
    # Async fan-out: only the i==2 sub-request fails; the other four complete.
    assert err.completed_chunks == 4
    assert err.call is not None
    assert isinstance(err.__cause__, httpx.InvalidURL)
    # The top-level message must surface the underlying cause text so
    # the user doesn't have to traverse ``__cause__`` to know what
    # actually failed (previously the message was generic "Service
    # error after K/N sub-requests; ... resume() once the upstream
    # recovers", with the real "URL too long" only visible via
    # ``.__cause__``).
    assert "InvalidURL" in str(err)
    assert "URL is too long" in str(err)


def test_service_interrupted_exposes_partial_frame_and_response():
    """Both ``QuotaExhausted`` AND ``ServiceInterrupted`` carry
    ``partial_frame`` / ``partial_response`` directly on the
    exception. Previously only ``QuotaExhausted`` had them, so a
    generic ``except ChunkInterrupted as exc: log(exc.partial_frame)``
    crashed with AttributeError on 5xx."""
    state = {"i": 0}

    async def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 2:
            try:
                raise ServiceUnavailable("503: Service unavailable.")
            except ServiceUnavailable as cause:
                raise DataRetrievalError(str(cause)) from cause
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

    async def fetch(args):
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

    async def fetch(args):
        i = state["i"]
        state["i"] += 1
        if i == 1 and state["blow_up"]:
            raise RateLimited("429: Too many requests.")
        return (
            pd.DataFrame({"sites": list(args["sites"])}),
            _quota_response(500),
        )

    # 2 sites at url_limit=240 → 2 singleton sub-requests. The 429 fires
    # on the SECOND sub-request and the gather completes the other, so the
    # exception captures exactly ONE completed chunk — the path where
    # _combine_chunk_frames aliases its single non-empty frame.
    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(QuotaExhausted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10]})
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
    ``httpx.Headers`` — mutations by downstream callers (logging
    hooks, metadata extensions) must not back-propagate into the
    underlying chunk response's headers, which still live on
    ``ChunkedCall._chunks``."""
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
    # Synthesize an OGC response with numberReturned > 0 and a "next"
    # link whose href is an empty string — simulating a server-side
    # sentinel that ``_next_req_url`` reads as ``""``.
    body_with_empty_next = {
        "numberReturned": 1,
        "features": [{"id": "1", "properties": {"val": "a"}}],
        "links": [{"rel": "next", "href": ""}],
    }
    resp = mock.MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.url = "https://example.com/items?limit=1"
    resp.elapsed = datetime.timedelta(seconds=0.1)
    resp.headers = {}
    resp.json.return_value = body_with_empty_next

    client = mock.AsyncMock(spec=httpx.AsyncClient)
    client.send.return_value = resp

    req = mock.MagicMock(spec=httpx.Request)
    req.method = "GET"
    req.headers = {}
    req.content = b""
    req.url = "https://example.com/items?limit=1"

    df, _ = asyncio.run(_utils._walk_pages(geopd=False, req=req, client=client))

    # Single send + zero follow-ups: the loop terminated on the empty cursor.
    assert client.send.called
    assert not client.request.called
    assert len(df) == 1


def test_combine_chunk_frames_does_not_collapse_none_ids():
    """``drop_duplicates(subset='id')`` treats NaN==NaN as duplicate,
    so a blanket dedup would collapse every id-less row into one —
    silent data loss. The function must dedupe only the id-bearing
    rows and preserve id-less rows verbatim."""
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

    async def fetch(args):
        state["i"] += 1
        if state["i"] >= 3:
            try:
                raise RateLimited("429: Too many requests.", retry_after=42.0)
            except RateLimited as cause:
                raise DataRetrievalError(str(cause)) from cause
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


def test_request_bytes_sums_url_and_content():
    """``_request_bytes`` returns ``len(str(url)) + len(content)``.

    ``httpx.Request`` always carries ``.content`` as ``bytes`` (the
    constructor normalises ``data``/``json``/``content`` inputs), so
    the chunker just needs to size that single attribute alongside
    the URL.
    """
    # GET request with no body
    req = httpx.Request("GET", "https://x.example/ab")
    assert _request_bytes(req) == len("https://x.example/ab")

    # POST request with content
    req = httpx.Request("POST", "https://x.example/ab", content=b"cd")
    assert _request_bytes(req) == len("https://x.example/ab") + 2


def test_safe_request_bytes_treats_invalid_url_as_overflow():
    """``httpx.URL`` enforces a 64 KB cap per URL component and raises
    ``httpx.InvalidURL`` for anything bigger — e.g. comma-joining all
    California stream sites in one query. The planner's halving loop
    must keep shrinking past that cap rather than crashing; the
    contract is that ``_safe_request_bytes`` returns ``url_limit + 1``
    (a value strictly greater than the limit) when ``build_request``
    raises ``InvalidURL``."""

    def build_request(**kwargs):
        raise httpx.InvalidURL("URL too long")

    url_limit = 8000
    assert _safe_request_bytes(build_request, {}, url_limit) == url_limit + 1


def test_chunk_plan_handles_initial_url_overflow():
    """A user query whose unchunked URL exceeds the 64 KB
    ``httpx.URL`` cap (e.g. 5000+ site IDs comma-joined) must not
    crash ``ChunkPlan.__init__``; the planner falls back to a
    worst-case sub-request URL for ``canonical_url`` and proceeds to
    halve the over-limit axes normally."""
    real_build = _fake_build

    def overflowing_build(**kwargs):
        # Mimic httpx: any single sub-arg whose ``sites`` list has
        # more than 2 entries fails URL construction (proxy for a
        # 64 KB overflow at the worst case).
        if len(kwargs.get("sites", [])) > 2:
            raise httpx.InvalidURL("URL > 64 KB")
        return real_build(**kwargs)

    sites = ["S" * 10 + str(i) for i in range(8)]
    plan = ChunkPlan({"sites": sites}, overflowing_build, url_limit=8000)
    # Planner kept halving until every worst-case sub-arg had ≤2 sites.
    assert all(len(c) <= 2 for c in plan.chunks["sites"])
    assert plan.total > 1
    # canonical_url fell back to a constructable worst-case URL.
    assert plan.canonical_url is not None


def test_multi_value_chunked_restores_canonical_url():
    """When chunking fans out, the aggregated response's ``.url`` must
    reflect the *user's original* query (rebuilt from the unchunked
    args), not the first chunk's URL. Callers logging ``md.url`` for
    reproducibility need the full query."""
    sites = ["S" * 10 + str(i) for i in range(4)]
    sub_urls: list[str] = []

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    async def fetch(args):
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
        url_len = len(str(req.url)) + len(req.content)
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
    """An all-empty chunk list preserves the ``GeoDataFrame`` type.
    Dropping empties before concat exists precisely to prevent type
    downgrade; the all-empty branch must honor the same contract."""
    pytest.importorskip("geopandas")
    import geopandas as gpd

    empty_gdfs = [gpd.GeoDataFrame() for _ in range(3)]
    combined = _combine_chunk_frames(empty_gdfs)
    assert isinstance(combined, gpd.GeoDataFrame), (
        f"all-empty combine returned {type(combined).__name__}; expected GeoDataFrame"
    )


def test_combine_chunk_frames_single_frame_is_safe_to_mutate():
    """``_combine_chunk_frames`` returns a frame independent of its
    input on the single-chunk fast path — a caller mutating
    ``call.partial_frame`` (a live view) must not back-propagate into
    the underlying ``_chunks[0][0]`` frame."""
    chunk = pd.DataFrame({"id": ["A", "B"], "value": [1, 2]})
    returned = _combine_chunk_frames([chunk])
    returned["new_col"] = "x"
    assert "new_col" not in chunk.columns


def test_iter_sub_args_passthrough_yields_a_copy():
    """``ChunkPlan.iter_sub_args`` yields a fresh dict on every path
    (passthrough and chunked), so a ``fetch_once`` that mutates the
    dict it receives cannot corrupt ``ChunkPlan.args``."""
    args = {"monitoring_location_id": ["USGS-A"], "limit": 100}
    plan = ChunkPlan(args, _fake_build, url_limit=8000)
    sub = next(plan.iter_sub_args())
    sub["monitoring_location_id"] = "mutated"
    sub["new_key"] = "leaked"
    assert plan.args["monitoring_location_id"] == ["USGS-A"]
    assert "new_key" not in plan.args


# --- async fan-out path ----------------------------------------------------
#
# Every sub-request is gathered over one ``httpx.AsyncClient`` and
# concurrency is bounded purely by that client's connection pool, sized
# from ``API_USGS_CONCURRENT``. The conftest's ``_pin_chunker_env``
# autouse pins ``API_USGS_CONCURRENT=1`` (a single connection) for the
# whole suite; each test below raises it so the gather can dispatch
# sub-requests under a wider pool. The decorated async fetcher is the
# SAME one used on both first-run and resume. No real ``httpx.AsyncClient``
# round-trip occurs (the fakes return mock data), even though
# :meth:`ChunkedCall._run` opens one for pool management.


def _async_chunked_fetch(monkeypatch, fetch_async, *, max_concurrent=16):
    """Decorate a deterministic chunkable async fetch with a wide-pool
    gather forced on via ``API_USGS_CONCURRENT``."""
    monkeypatch.setenv("API_USGS_CONCURRENT", str(max_concurrent))
    return multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch_async)


def _atom_id(args):
    """Build a deterministic id for a sub-args dict — used as the dedup key."""
    return ",".join(args["sites"]) if isinstance(args["sites"], list) else args["sites"]


def _ok_response(remaining=None):
    headers = {} if remaining is None else {_QUOTA_HEADER: str(remaining)}
    return mock.Mock(elapsed=datetime.timedelta(seconds=0.1), headers=headers)


def test_async_fan_out_emits_one_call_per_sub_request(monkeypatch):
    """The fan-out hits every sub-args exactly once, dispatched
    concurrently."""
    seen_args = []

    async def fetch_async(args):
        seen_args.append(tuple(args["sites"]))
        return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response()

    fetch = _async_chunked_fetch(monkeypatch, fetch_async)

    df, _ = fetch({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})

    # Planner halves the 4-site list, so 2 sub-args → 2 async calls.
    assert len(seen_args) > 1
    # Every sub-args atom is union-recovered.
    assert sorted({s for tup in seen_args for s in tup}) == sorted(
        ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]
    )
    # Frames concat to one row per sub-request id, in deterministic order.
    assert len(df) == len(seen_args)


def test_async_fan_out_aggregates_headers_from_latest_completion(monkeypatch):
    """Aggregated headers reflect the most recently completed chunk.

    Completion order can differ from index order in parallel mode, so
    rate-limit headers should come from whichever sub-request finished
    last, not from the highest sub-args index.
    """

    async def fetch_async(args):
        if "S1" * 10 in args["sites"]:
            await asyncio.sleep(0.02)
            return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response(remaining=11)
        return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response(remaining=77)

    fetch = _async_chunked_fetch(monkeypatch, fetch_async)
    _, response = fetch({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})
    assert response.headers[_QUOTA_HEADER] == "11"


def test_async_fan_out_failure_yields_resumable_call(monkeypatch):
    """A transient 5xx mid-fan-out raises ``ServiceInterrupted`` whose
    ``.call`` is a ``ChunkedCall`` holding the completed sub-requests
    in a sparse index map. ``exc.call.resume()`` re-issues only the
    unfinished sub-requests — through the same async fetcher and the same
    async runner, just on a fresh gather."""
    # One async fetcher serves both first-run and resume. On the first
    # gather it lets exactly one sub-request succeed and fails the rest
    # transiently; once ``blow_up`` is cleared the resume gather completes
    # every still-pending sub-request. ``calls`` counts every invocation
    # across both gathers so we can assert resume only re-issued the owed
    # sub-requests.
    state = {"first_success": False, "blow_up": True}
    calls = {"n": 0}

    async def fetch_async(args):
        calls["n"] += 1
        if state["blow_up"]:
            # Let the first dispatched sub-request through, fail the rest.
            if not state["first_success"]:
                state["first_success"] = True
                return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response(
                    remaining=99
                )
            raise ServiceUnavailable("503: simulated")
        return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response(remaining=99)

    fetch = _async_chunked_fetch(monkeypatch, fetch_async)

    with pytest.raises(ServiceInterrupted) as exc_info:
        fetch({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})

    interrupted = exc_info.value
    assert interrupted.call is not None, "interruption must be resumable"
    # Exactly one sub-request completed; the rest still owe.
    assert interrupted.completed_chunks == 1
    assert interrupted.total_chunks > 1

    # Resume re-issues only the missing sub-requests, via the same async
    # runner the first run used.
    state["blow_up"] = False
    calls_before = calls["n"]
    df, _ = interrupted.call.resume()
    calls_on_resume = calls["n"] - calls_before
    assert calls_on_resume == interrupted.total_chunks - 1
    # Final frame unions every sub-args.
    assert len(df) == interrupted.total_chunks


def test_async_fan_out_resume_applies_finalize(monkeypatch):
    """The ``finalize`` injected for a wide-pool call survives the
    interruption (carried on the ``ChunkedCall`` through the anyio portal),
    so ``exc.call.resume()`` still returns the finalized shape — guarding
    the run -> resume -> finalize path. Partials stay raw (no finalize in
    the exception ctor)."""

    def finalize(frame, response):
        return frame.assign(finalized=True), ("MD", response)

    state = {"first_success": False, "blow_up": True}

    async def fetch_async(args):
        if state["blow_up"]:
            if not state["first_success"]:
                state["first_success"] = True
                return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response(
                    remaining=99
                )
            raise ServiceUnavailable("503: simulated")
        return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response(remaining=99)

    fetch = _async_chunked_fetch(monkeypatch, fetch_async)

    sites = ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]
    with pytest.raises(ServiceInterrupted) as exc_info:
        fetch({"sites": sites}, finalize=finalize)

    # Partial snapshot stays raw — building the exception must not finalize.
    assert "finalized" not in exc_info.value.partial_frame.columns
    # Resume applies the finalize carried on the ChunkedCall.
    state["blow_up"] = False
    df, md = exc_info.value.call.resume()
    assert "finalized" in df.columns
    assert md[0] == "MD"


def test_wide_concurrency_uses_async_fetcher_with_no_warning(monkeypatch):
    """A wide ``API_USGS_CONCURRENT`` is honored directly by the single
    async fetcher: every sub-request fans out across it and NO
    ``UserWarning`` is emitted."""
    calls = []
    monkeypatch.setenv("API_USGS_CONCURRENT", "16")

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    async def fetch(args):
        calls.append(tuple(args["sites"]))
        return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response()

    with warnings.catch_warnings():
        warnings.simplefilter("error")  # any UserWarning would fail the test
        df, _ = fetch({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})

    assert len(calls) > 1  # the gather fanned out across every sub-request
    assert len(df) == len(calls)


def test_async_fan_out_runs_inside_running_event_loop(monkeypatch):
    """The parallel fan-out works even when the caller is already inside a
    running event loop (Jupyter / async apps): the anyio blocking portal
    runs it in a worker thread, so it does not raise a nested
    ``asyncio.run`` error."""
    monkeypatch.setenv("API_USGS_CONCURRENT", "16")
    async_calls = []

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    async def fetch(args):  # the single async fetcher
        async_calls.append(tuple(args["sites"]))
        return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response()

    async def driver():  # call the sync getter from within a running loop
        # The sync wrapper drives the async core through the anyio portal in
        # a worker thread, so it works even inside a running event loop
        # without raising a nested-``asyncio.run`` error.
        return fetch({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})

    df, _ = asyncio.run(driver())
    assert len(async_calls) > 1  # every sub-request ran on the async path
    assert len(df) == len(async_calls)


def test_async_fan_out_cancellation_wins_over_transient_sibling(monkeypatch):
    """``asyncio.CancelledError`` raised by any sub-request must
    propagate unmodified, even when a sibling raises a recognized
    transient (which would otherwise wrap as a resumable
    :class:`ChunkInterrupted`). Cancellation is asyncio's abort
    signal — letting a transient-classification path consume it
    would silently swallow the user's stop request.

    ``fetch_async`` has no ``await`` in its body, so the gather schedules
    the tasks in submission order and each runs synchronously to its
    raise — making ``call_count`` deterministic: 1 = first chunk
    (success), 2 = second chunk (transient), 3 = third chunk (cancel).

    Through the sync→async blocking portal an in-flight cancellation
    surfaces to the caller as ``concurrent.futures.CancelledError`` (the
    thread-boundary cancellation type) rather than ``asyncio.CancelledError``
    — either way it propagates unmodified rather than being swallowed and
    wrapped as a resumable ``ChunkInterrupted``.
    """
    call_count = {"async": 0}

    async def fetch_async(args):
        call_count["async"] += 1
        if call_count["async"] == 1:
            return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response(remaining=99)
        if call_count["async"] == 2:
            raise ServiceUnavailable("503: transient sibling")
        if call_count["async"] == 3:
            raise asyncio.CancelledError("user cancel")
        return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response(remaining=99)

    fetch = _async_chunked_fetch(monkeypatch, fetch_async)

    # 8 × 20-byte sites force the planner to >=3 sub-args under
    # url_limit=240, so the fan-out gather sees at least the
    # transient (call 2) AND the cancellation (call 3).
    sites = [f"S{i}" * 10 for i in range(1, 9)]

    with pytest.raises((asyncio.CancelledError, concurrent.futures.CancelledError)):
        fetch({"sites": sites})


def test_combine_chunk_responses_does_not_mutate_input_urls():
    """Regression for the _set_response_url aliasing bug.

    ``_combine_chunk_responses`` shallow-copies the first response;
    if the canonical-URL override is applied by mutating the bound
    ``request.url``, the shallow alias back-propagates the URL change
    into the underlying chunk-0 response — breaking the documented
    'input responses are not mutated' invariant. The fix is to swap
    in a fresh ``httpx.Request`` rather than mutate the existing one.
    """
    req1 = httpx.Request("GET", "https://example.com/chunk0")
    req2 = httpx.Request("GET", "https://example.com/chunk1")
    r1 = httpx.Response(200, request=req1)
    r2 = httpx.Response(200, request=req2)

    out = _combine_chunk_responses(
        [r1, r2], canonical_url="https://canonical.example/full"
    )
    assert str(out.url) == "https://canonical.example/full"
    # The inputs and their bound requests must be untouched.
    assert str(r1.url) == "https://example.com/chunk0"
    assert str(r2.url) == "https://example.com/chunk1"
    assert str(req1.url) == "https://example.com/chunk0"
    assert str(req2.url) == "https://example.com/chunk1"


# ---------------------------------------------------------------------------
# Retry-with-backoff: RetryPolicy + _retryable + driver + decorator wiring.
# Conftest pins API_USGS_RETRIES=0, so these tests opt in explicitly and
# patch the chunking module's ``asyncio.sleep`` to a no-op (no real backoff).
# ---------------------------------------------------------------------------


def _wrap_cause(transport_exc):
    """Wrap ``transport_exc`` the way ``_walk_pages`` does — a base
    ``DataRetrievalError`` with the typed transport error on ``__cause__`` — so
    chain-walking is exercised realistically."""
    try:
        raise DataRetrievalError("Paginated request failed") from transport_exc
    except DataRetrievalError as wrapped:
        return wrapped


# -- RetryPolicy (pure value object) ----------------------------------------


def test_retry_policy_backoff_honors_retry_after():
    policy = RetryPolicy()
    # A server Retry-After overrides the computed backoff verbatim.
    assert policy.backoff(attempt=1, retry_after=7.5) == 7.5
    assert policy.backoff(attempt=4, retry_after=2.0) == 2.0


def test_retry_policy_backoff_full_jitter_within_ceiling():
    policy = RetryPolicy(base_backoff=2.0, max_backoff=30.0)
    for attempt, ceiling in [(1, 2.0), (2, 4.0), (3, 8.0), (5, 30.0)]:
        samples = [policy.backoff(attempt, None) for _ in range(200)]
        assert all(0.0 <= s <= ceiling for s in samples)
        # Full jitter genuinely varies and reaches below the ceiling.
        assert min(samples) < ceiling


def test_retry_policy_should_retry_exhaustion():
    policy = RetryPolicy(max_retries=2)
    assert policy.should_retry(attempt=1, retry_after=None)
    assert policy.should_retry(attempt=2, retry_after=None)
    assert not policy.should_retry(attempt=3, retry_after=None)


def test_retry_policy_long_retry_after_escalates():
    policy = RetryPolicy(max_retries=5, retry_after_cap=60.0)
    assert policy.should_retry(attempt=1, retry_after=30.0)  # absorbed inline
    assert not policy.should_retry(attempt=1, retry_after=120.0)  # escalates


def test_retry_policy_from_env(monkeypatch):
    monkeypatch.setenv("API_USGS_RETRIES", "2")
    assert RetryPolicy.from_env().max_retries == 2
    monkeypatch.setenv("API_USGS_RETRIES", "0")
    assert RetryPolicy.from_env().max_retries == 0
    monkeypatch.delenv("API_USGS_RETRIES", raising=False)
    assert RetryPolicy.from_env().max_retries == _chunking._RETRIES_DEFAULT
    monkeypatch.setenv("API_USGS_RETRIES", "-1")
    with pytest.raises(ValueError):
        RetryPolicy.from_env()
    monkeypatch.setenv("API_USGS_RETRIES", "lots")
    with pytest.raises(ValueError):
        RetryPolicy.from_env()


def test_retry_policy_rejects_invalid_settings():
    with pytest.raises(ValueError):
        RetryPolicy(max_retries=-1)
    with pytest.raises(ValueError):
        RetryPolicy(base_backoff=-0.5)
    with pytest.raises(ValueError):
        RetryPolicy(max_backoff=-1.0)


def test_retry_policy_from_env_honors_monkeypatched_constants(monkeypatch):
    # The timing knobs are read from the module constants at call time, so
    # monkeypatching them (as the module comment promises) takes effect.
    monkeypatch.setattr(_chunking, "_RETRY_MAX_BACKOFF", 0.0)
    monkeypatch.setattr(_chunking, "_RETRY_BASE_BACKOFF", 0.0)
    policy = RetryPolicy.from_env()
    assert policy.max_backoff == 0.0 and policy.base_backoff == 0.0


# -- _retryable taxonomy ----------------------------------------------------


def test_retryable_taxonomy():
    from dataretrieval.exceptions import HTTPError

    assert _retryable(RateLimited("429", retry_after=5.0)) == (True, 5.0)
    assert _retryable(ServiceUnavailable("503")) == (True, None)
    assert _retryable(httpx.ReadTimeout("slow")) == (True, None)
    assert _retryable(httpx.ConnectError("down")) == (True, None)
    # InvalidURL is resumable but NOT retryable (a too-long cursor won't fix).
    assert _retryable(httpx.InvalidURL("too long")) == (False, None)
    # A fatal HTTP error (a plain HTTPError, not a TransientError) is never
    # retried; nor is a bare RuntimeError.
    assert _retryable(HTTPError("400", status_code=400)) == (False, None)
    assert _retryable(HTTPError("403", status_code=403)) == (False, None)
    assert _retryable(RuntimeError("400")) == (False, None)


def test_retryable_skips_wrapped_midpagination_transient():
    # A transient surfaced mid-pagination is re-wrapped as DataRetrievalError by
    # _paginate; it must NOT be auto-retried (re-walking from page 1
    # would re-spend quota) — it escalates to the resumable handle instead.
    # Only the raw, top-level (initial-request) transient is retryable.
    assert _retryable(_wrap_cause(RateLimited("429", retry_after=3.0))) == (False, None)
    assert _retryable(RateLimited("429", retry_after=3.0)) == (True, 3.0)


# -- async driver (the single retry driver; sync facade drives it) ----------
#
# The retry loop lives in ``_retry``. These tests pin its behavioral
# contracts (transient-then-success, exhausted-reraises,
# non-retryable-not-retried, long-retry-after-escalates), run via
# ``asyncio.run``; the sleep is patched to a no-op so backoff doesn't
# actually wait.


def test_retry_transient_then_recovers(monkeypatch):
    monkeypatch.setattr(_chunking.asyncio, "sleep", _aiozero)
    calls = {"n": 0}

    async def afn():
        calls["n"] += 1
        if calls["n"] <= 2:
            raise RateLimited("429")
        return "ok"

    out = asyncio.run(_retry(afn, RetryPolicy(max_retries=3, base_backoff=0.0)))
    assert out == "ok"
    assert calls["n"] == 3  # two failures + one success


def test_retry_exhausted_reraises(monkeypatch):
    monkeypatch.setattr(_chunking.asyncio, "sleep", _aiozero)
    calls = {"n": 0}

    async def afn():
        calls["n"] += 1
        raise ServiceUnavailable("503")

    with pytest.raises(ServiceUnavailable):
        asyncio.run(_retry(afn, RetryPolicy(max_retries=2, base_backoff=0.0)))
    assert calls["n"] == 3  # first attempt + 2 retries


def test_retry_non_retryable_not_retried(monkeypatch):
    slept: list[float] = []

    monkeypatch.setattr(_chunking.asyncio, "sleep", _recording_sleep(slept))
    calls = {"n": 0}

    async def afn():
        calls["n"] += 1
        raise RuntimeError("400: bad request")

    with pytest.raises(RuntimeError):
        asyncio.run(_retry(afn, RetryPolicy(max_retries=3)))
    assert calls["n"] == 1 and slept == []


def test_retry_long_retry_after_escalates(monkeypatch):
    slept: list[float] = []

    monkeypatch.setattr(_chunking.asyncio, "sleep", _recording_sleep(slept))
    calls = {"n": 0}

    async def afn():
        calls["n"] += 1
        raise RateLimited("429", retry_after=999.0)

    with pytest.raises(RateLimited):
        asyncio.run(_retry(afn, RetryPolicy(max_retries=5, retry_after_cap=60.0)))
    assert calls["n"] == 1 and slept == []  # no inline wait for a long window


# -- async driver (sleep-patched original) ----------------------------------


def test_retry_transient_then_success(monkeypatch):
    monkeypatch.setattr(_chunking.asyncio, "sleep", _aiozero)
    calls = {"n": 0}

    async def afn():
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ReadTimeout("slow")
        return "ok"

    out = asyncio.run(_retry(afn, RetryPolicy(max_retries=3, base_backoff=0.0)))
    assert out == "ok" and calls["n"] == 2


# -- end-to-end through the decorator --------------------------------------


def test_chunker_retries_transient_then_completes(monkeypatch):
    """A transient on one sub-request is retried transparently; the
    decorated call completes with no ChunkInterrupted."""
    monkeypatch.setenv("API_USGS_RETRIES", "3")
    monkeypatch.setattr(_chunking.asyncio, "sleep", _aiozero)
    state = {"failed": False}

    async def fetch(args):
        # Fail the first sub-request once, then succeed everywhere.
        if not state["failed"]:
            state["failed"] = True
            raise RateLimited("429: Too many requests made.")
        return pd.DataFrame({"sites": list(args["sites"])}), _quota_response(500)

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    sites = ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]
    df, _ = decorated({"sites": sites})
    assert sorted(df["sites"]) == sorted(sites)  # all recovered despite the 429


def test_chunker_exhausted_retries_still_resumable(monkeypatch):
    """When retries are exhausted the failure still surfaces as a
    resumable ChunkInterrupted — retries don't swallow the escape hatch."""
    monkeypatch.setenv("API_USGS_RETRIES", "2")
    monkeypatch.setattr(_chunking.asyncio, "sleep", _aiozero)
    attempts = {"n": 0}

    async def fetch(args):
        sites = list(args["sites"])
        if "S1" * 10 in sites:
            attempts["n"] += 1
            raise ServiceUnavailable("503: service unavailable")
        return pd.DataFrame({"sites": sites}), _quota_response(500)

    decorated = multi_value_chunked(build_request=_fake_build, url_limit=240)(fetch)
    with pytest.raises(ServiceInterrupted) as excinfo:
        decorated({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})
    assert excinfo.value.call is not None
    assert attempts["n"] == 3  # first attempt + 2 retries before giving up


def test_async_fan_out_retries_transient_then_completes(monkeypatch):
    """The parallel path retries a transient sub-request and completes."""
    monkeypatch.setenv("API_USGS_RETRIES", "3")

    monkeypatch.setattr(_chunking.asyncio, "sleep", _aiozero)
    state = {"failed": False}

    async def fetch_async(args):
        if not state["failed"]:
            state["failed"] = True
            raise ServiceUnavailable("503: transient")
        return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response()

    fetch = _async_chunked_fetch(monkeypatch, fetch_async)
    df, _ = fetch({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})
    assert len(df) > 1  # every sub-args atom recovered after the retry


def test_async_fan_out_surfaces_fatal_over_transient(monkeypatch):
    """A non-transient bug in one sub-request surfaces raw rather than
    being masked behind a resumable interruption from a transient sibling."""
    monkeypatch.setenv("API_USGS_RETRIES", "2")

    monkeypatch.setattr(_chunking.asyncio, "sleep", _aiozero)

    async def fetch_async(args):
        # One chunk carries a deterministic programmer error; the rest are
        # transient. The real bug must win over the resumable transient.
        if "S1" * 10 in args["sites"]:
            raise ValueError("deterministic bug")
        raise ServiceUnavailable("503: transient")

    fetch = _async_chunked_fetch(monkeypatch, fetch_async)
    with pytest.raises(ValueError, match="deterministic bug"):
        fetch({"sites": ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]})


# --- finalize hook (resume finalizes; partials stay raw) -------------------
#
# Regression for the bug where ``exc.call.resume()`` returned the chunker's
# raw ``(frame, httpx.Response)`` instead of the post-processed shape a normal
# getter call yields. The fix injects a ``finalize`` transform applied at the
# terminal resume()/resume_async() returns. The partial_* accessors stay RAW
# so building/inspecting a ChunkInterrupted never triggers finalize's side
# effects (for OGC, _deal_with_empty issues a schema network GET on an empty
# frame — that must NOT fire inside the exception constructor).


def test_resume_finalizes_but_partials_stay_raw(monkeypatch):
    """resume() applies the injected ``finalize``; ``partial_frame`` /
    ``partial_response`` are the raw snapshot, and constructing the
    ``ChunkInterrupted`` must not invoke ``finalize`` at all (no side effects
    such as a schema fetch in the exception ctor)."""
    calls = {"finalize": 0}

    def finalize(frame, response):
        # Stand in for the OGC pipeline: mark the frame and wrap the response.
        calls["finalize"] += 1
        return frame.assign(finalized=True), ("METADATA", response)

    # Fail the 2nd issued sub-request once (the 1st completes, so partial
    # state is non-empty), then succeed on resume. Conftest pins a single
    # connection and no retries, so the failure surfaces immediately.
    state = {"n": 0}

    @multi_value_chunked(build_request=_fake_build, url_limit=240)
    async def fetch(args):
        state["n"] += 1
        if state["n"] == 2:
            raise ServiceUnavailable("503: simulated")
        return pd.DataFrame({"id": [_atom_id(args)]}), _ok_response()

    sites = ["S1" * 10, "S2" * 10, "S3" * 10, "S4" * 10]
    with pytest.raises(ServiceInterrupted) as exc_info:
        fetch({"sites": sites}, finalize=finalize)

    interrupted = exc_info.value
    assert interrupted.completed_chunks >= 1
    # Building the exception did NOT run finalize — no network/side effects.
    assert calls["finalize"] == 0
    # Partial snapshot is the RAW combined frame/response (not finalized).
    assert "finalized" not in interrupted.partial_frame.columns
    assert not isinstance(interrupted.partial_response, tuple)

    # Resume DOES finalize and yields the same shape a normal call would.
    df, md = interrupted.call.resume()
    assert "finalized" in df.columns
    assert md[0] == "METADATA"
    assert calls["finalize"] >= 1
