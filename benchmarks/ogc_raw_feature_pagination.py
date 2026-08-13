"""Benchmark raw OGC feature aggregation against per-page frame conversion.

Run from the repository root::

    python benchmarks/ogc_raw_feature_pagination.py

The default workload shapes 10,000 features in ten pages for both spatial and
nonspatial collections. ``page_frames`` reproduces PR #373's per-page
conversion boundary with the current equivalent feature shaper;
``raw_features`` uses the experiment's completed-chunk boundary. The report
includes conversion count, median wall time, and median isolated-process peak
RSS. Guardrails are informational, not assertions, because timing and memory
measurements vary by machine.
"""

from __future__ import annotations

import argparse
import gc
import json
import platform
import resource
import statistics
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd

from dataretrieval.ogc.engine import _combine_feature_pages
from dataretrieval.ogc.shaping import _feature_frame

Feature = dict[str, Any]
Pages = list[list[Feature]]
Strategy = Callable[[Pages, bool], tuple[pd.DataFrame, int]]


@dataclass(frozen=True)
class Measurement:
    """Median measurements for one strategy and geometry mode."""

    mode: str
    strategy: str
    conversions: int
    wall_seconds: float
    peak_mib: float


def _pages(row_count: int, page_size: int, *, spatial: bool) -> Pages:
    """Build deterministic GeoJSON pages outside the measured region."""
    features: list[Feature] = []
    for index in range(row_count):
        feature: Feature = {
            "type": "Feature",
            "id": f"feature-{index}",
            "properties": {
                "value": index / 10,
                "name": f"site-{index % 100}",
                "quality": {"approved": index % 2 == 0},
            },
        }
        if spatial:
            feature["geometry"] = {
                "type": "Point",
                "coordinates": [-125.0 + index / row_count, 25.0 + index / row_count],
            }
        features.append(feature)
    return [
        features[start : start + page_size] for start in range(0, row_count, page_size)
    ]


def _page_frames(pages: Pages, spatial: bool) -> tuple[pd.DataFrame, int]:
    """Model the former implementation: shape every page, then concatenate."""
    frames = [
        _feature_frame(page, geopd=spatial, include_geometry=spatial) for page in pages
    ]
    return pd.concat(frames, ignore_index=True), len(frames)


def _raw_features(pages: Pages, spatial: bool) -> tuple[pd.DataFrame, int]:
    """Run the current implementation: flatten pages, then shape once."""
    features = _combine_feature_pages(pages, row_cap=None)
    return (
        _feature_frame(features, geopd=spatial, include_geometry=spatial),
        1,
    )


def _peak_rss_mib() -> float:
    """Return this process's maximum resident set size in MiB."""
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    # macOS reports bytes; Linux and the other supported CI platforms report KiB.
    return peak / (1024 * 1024) if sys.platform == "darwin" else peak / 1024


def _run_worker(
    name: str,
    *,
    spatial: bool,
    row_count: int,
    page_size: int,
) -> dict[str, float | int]:
    """Run one isolated measurement and return its JSON payload."""
    command = [
        sys.executable,
        str(Path(__file__).resolve()),
        "--worker",
        name,
        "--rows",
        str(row_count),
        "--page-size",
        str(page_size),
    ]
    if spatial:
        command.append("--spatial")
    output = subprocess.check_output(command, text=True)
    return json.loads(output)


def _measure(
    name: str,
    *,
    spatial: bool,
    repeats: int,
    expected_rows: int,
    page_size: int,
) -> Measurement:
    """Measure one strategy repeatedly in fresh processes."""
    results = [
        _run_worker(
            name,
            spatial=spatial,
            row_count=expected_rows,
            page_size=page_size,
        )
        for _ in range(repeats)
    ]
    conversions = {int(item["conversions"]) for item in results}
    row_counts = {int(item["rows"]) for item in results}
    if len(conversions) != 1 or row_counts != {expected_rows}:
        raise RuntimeError(f"{name} produced inconsistent output")

    return Measurement(
        mode="spatial" if spatial else "nonspatial",
        strategy=name,
        conversions=conversions.pop(),
        wall_seconds=statistics.median(float(item["wall_seconds"]) for item in results),
        peak_mib=statistics.median(float(item["peak_mib"]) for item in results),
    )


def _ratio(current: float, baseline: float) -> float:
    return current / baseline if baseline else float("inf")


def _print_report(measurements: list[Measurement]) -> None:
    """Print a Markdown-friendly result table and informational guardrails."""
    print(f"Python {platform.python_version()} on {platform.platform()}")
    print("\n| mode | strategy | conversions | wall (s) | peak RSS MiB |")
    print("| --- | --- | ---: | ---: | ---: |")
    for item in measurements:
        print(
            f"| {item.mode} | {item.strategy} | {item.conversions} | "
            f"{item.wall_seconds:.4f} | {item.peak_mib:.2f} |"
        )

    print("\nInformational guardrails (raw / page-frame):")
    for mode in ("spatial", "nonspatial"):
        by_name = {item.strategy: item for item in measurements if item.mode == mode}
        baseline = by_name["page_frames"]
        current = by_name["raw_features"]
        wall_ratio = _ratio(current.wall_seconds, baseline.wall_seconds)
        peak_ratio = _ratio(current.peak_mib, baseline.peak_mib)
        wall_status = "PASS" if wall_ratio <= 1.10 else "REVIEW"
        peak_status = "PASS" if peak_ratio <= 1.25 else "REVIEW"
        print(
            f"- {mode}: wall {wall_ratio:.2f}x ({wall_status}, <=1.10x); "
            f"peak {peak_ratio:.2f}x ({peak_status}, <=1.25x)"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--rows", type=int, default=10_000)
    parser.add_argument("--page-size", type=int, default=1_000)
    parser.add_argument("--repeats", type=int, default=5)
    parser.add_argument(
        "--worker",
        choices=("page_frames", "raw_features"),
        help=argparse.SUPPRESS,
    )
    parser.add_argument("--spatial", action="store_true", help=argparse.SUPPRESS)
    args = parser.parse_args()
    if args.rows <= 0 or args.page_size <= 0 or args.repeats <= 0:
        parser.error("--rows, --page-size, and --repeats must be positive")

    strategies: dict[str, Strategy] = {
        "page_frames": _page_frames,
        "raw_features": _raw_features,
    }
    if args.worker is not None:
        pages = _pages(args.rows, args.page_size, spatial=args.spatial)
        gc.collect()
        started = time.perf_counter()
        frame, conversions = strategies[args.worker](pages, args.spatial)
        wall_seconds = time.perf_counter() - started
        print(
            json.dumps(
                {
                    "rows": len(frame),
                    "conversions": conversions,
                    "wall_seconds": wall_seconds,
                    "peak_mib": _peak_rss_mib(),
                }
            )
        )
        return

    measurements: list[Measurement] = []
    for spatial in (True, False):
        for name in strategies:
            measurements.append(
                _measure(
                    name,
                    spatial=spatial,
                    repeats=args.repeats,
                    expected_rows=args.rows,
                    page_size=args.page_size,
                )
            )

    _print_report(measurements)


if __name__ == "__main__":
    main()
