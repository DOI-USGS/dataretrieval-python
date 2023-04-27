"""Tests for NADP functions."""
import os
import dataretrieval.nadp as nadp


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
            measurement_type='conc', year='2010', path=tmp_path)
        exp_path = os.path.join(tmp_path, 'Hg_conc_2010.zip')
        # assert path matches expectation
        assert z_path == str(exp_path)
        # assert unpacked zip exists as a directory
        assert os.path.exists(exp_path[:-4])
        # assert tif exists in directory
        assert os.path.exists(os.path.join(z_path[:-4], 'conc_Hg_2010.tif'))


class TestNTNmap:
    """Testing the national trends network map functions."""

    def test_get_annual_NTN_map_zip(self, tmp_path):
        """Test the get_annual_NTN_map function zip return."""
        z_path = nadp.get_annual_NTN_map(
            measurement_type='Precip', year='2015', path=tmp_path)
        exp_path = os.path.join(tmp_path, 'Precip_2015.zip')
        # assert path matches expectation
        assert z_path == str(exp_path)
        # assert unpacked zip exists as a directory
        assert os.path.exists(exp_path[:-4])
        # assert tif exists in directory
        assert os.path.exists(os.path.join(z_path[:-4], 'Precip_2015.tif'))
