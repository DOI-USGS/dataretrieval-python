"""Tests for NADP functions."""

import os

import pytest

from dataretrieval import nadp

pytestmark = pytest.mark.skip(
    reason="NADP module deprecated; removal scheduled 2026-11-01. "
    "Tests hit live NADP services and were causing CI flakes."
)


class TestMDNmap:
    """Testing the mercury deposition network map functions.

    This set of tests actually queries the services themselves to ensure there
    have been no upstream changes to paths or file names. Tests created
    because there was an upstream change to paths that broke ``dataretrieval``
    functionality.
    """

    def test_get_annual_MDN_map_zip(self, tmp_path):
        """Test the get_annual_MDN_map function zip return."""
        z_path = nadp.get_annual_MDN_map(
            measurement_type="conc", year="2010", path=tmp_path
        )
        # assert path matches expectation (now returns the path directory)
        assert z_path == str(tmp_path)
        # assert unpacked directory exists
        exp_dir = os.path.join(tmp_path, "Hg_conc_2010")
        assert os.path.exists(exp_dir)
        # assert tif exists in directory
        assert os.path.exists(os.path.join(exp_dir, "conc_Hg_2010.tif"))


class TestNTNmap:
    """Testing the national trends network map functions."""

    def test_get_annual_NTN_map_zip(self, tmp_path):
        """Test the get_annual_NTN_map function zip return."""
        z_path = nadp.get_annual_NTN_map(
            measurement_type="Precip", year="2015", path=tmp_path
        )
        # assert path matches expectation
        assert z_path == str(tmp_path)
        # assert unpacked directory exists
        exp_dir = os.path.join(tmp_path, "Precip_2015")
        assert os.path.exists(exp_dir)
        # assert tif exists in directory
        assert os.path.exists(os.path.join(exp_dir, "Precip_2015.tif"))
