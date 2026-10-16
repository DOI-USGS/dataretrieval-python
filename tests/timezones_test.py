"""Component tests for the time-zone table, ``dataretrieval.codes.timezones``.

The table turns the abbreviation a service writes beside a local time into a UTC
offset, so an entry is right when it agrees with the code list that service
draws from: the WQX TimeZone domain (Water Quality Portal) and the USGS NWIS
time-zone list (Samples). Snapshots of both are in ``tests/data``; the offline
tests check the table against them, and the live tests check the snapshots
against the published lists.
"""

from __future__ import annotations

import csv
import io
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
import pytest

from dataretrieval import waterdata, wqp
from dataretrieval.codes import tz

DATA = Path(__file__).parent / "data"
USGS_URL = "https://water.usgs.gov/XML/NWIS/5.0/ReferenceLists/TimeZoneList.xml"
WQX_URL = "https://cdx.epa.gov/wqx/download/DomainValues/TimeZone.CSV"

# Codes the two lists define differently. The table follows WQX for these: the
# Water Quality Portal returns ``SST`` for American Samoa, while USGS records
# (in Samples, and passed through by WQP) write that zone as ``ZP-11``.
WQX_WINS = {"SST", "BST"}


def _usgs_offset(text: str) -> str:
    # "-11:00" -> "-1100"; the list writes zero offsets as " 00:00".
    text = text.strip().replace(":", "")
    return text if text[0] in "+-" else "+" + text


def parse_usgs(content: bytes) -> dict[str, str]:
    """Map each standard and daylight code in the USGS list to its offset."""
    codes: dict[str, str] = {}
    for item in ET.fromstring(content).iter("ListItem"):
        codes[item.findtext("Code", "")] = _usgs_offset(item.findtext("UTCOffset", ""))
        dst = item.findtext("DSTCode", "null")
        if dst != "null":
            codes[dst] = _usgs_offset(item.findtext("DSTUTCOffset", ""))
    return codes


def parse_wqx(content: bytes) -> dict[str, str]:
    """Map each code in the WQX TimeZone domain to its offset."""
    codes: dict[str, str] = {}
    for row in csv.DictReader(io.StringIO(content.decode("utf-8"))):
        hours = float(row["Offset"])
        sign = "-" if hours < 0 else "+"
        minutes = round(abs(hours) * 60)
        codes[row["Code"]] = f"{sign}{minutes // 60:02d}{minutes % 60:02d}"
    return codes


USGS = parse_usgs((DATA / "usgs_timezones.xml").read_bytes())
WQX = parse_wqx((DATA / "wqx_timezones.csv").read_bytes())


@pytest.mark.parametrize("code", sorted(WQX))
def test_table_matches_the_wqx_domain(code):
    assert tz.get(code) == WQX[code]


@pytest.mark.parametrize("code", sorted(set(USGS) - WQX_WINS))
def test_table_matches_the_usgs_list(code):
    assert tz.get(code) == USGS[code]


def test_the_lists_disagree_only_where_the_table_says_so():
    """``WQX_WINS`` names every conflict, so a new one has to be decided."""
    conflicts = {c for c in USGS.keys() & WQX.keys() if USGS[c] != WQX[c]}
    assert conflicts == WQX_WINS


# --- live upstream monitors --------------------------------------------------


@pytest.mark.live
@pytest.mark.parametrize(
    ("url", "parse", "snapshot"),
    [(USGS_URL, parse_usgs, USGS), (WQX_URL, parse_wqx, WQX)],
    ids=["usgs", "wqx"],
)
def test_published_list_matches_the_snapshot(url, parse, snapshot):
    """If this fails, the published list changed: refresh its snapshot in
    ``tests/data`` from ``url`` and reconcile the table with the new entries."""
    resp = httpx.get(url, timeout=60, follow_redirects=True)
    resp.raise_for_status()
    assert parse(resp.content) == snapshot


def _samples(location):
    df, _ = waterdata.get_samples(
        service="results", profile="fullphyschem", monitoring_location_id=location
    )
    return df


def _wqp(location):
    df, _ = wqp.get_results(siteid=location, legacy=False, dataProfile="fullPhysChem")
    return df


@pytest.mark.live
@pytest.mark.parametrize(
    ("fetch", "location", "zone"),
    [
        (_samples, "USGS-141037169391201", "ZP-11"),  # American Samoa
        (_samples, "USGS-16854500", "GST"),  # Guam
        (_wqp, "11NPSWRD_WQX-NPSA_F_07TUT_FW", "SST"),  # American Samoa
        (_wqp, "USGS-141037169391201", "ZP-11"),  # USGS code passed through
        (_wqp, "USGS-16854500", "GST"),  # Guam
    ],
)
def test_pacific_times_resolve(fetch, location, zone):
    """Every timed row for these Pacific monitoring locations has a UTC time."""
    df = fetch(location)
    timed = df[df["Activity_StartTime"].notna()]
    assert zone in set(timed["Activity_StartTimeZone"])
    assert timed["Activity_StartDateTime"].notna().all()
