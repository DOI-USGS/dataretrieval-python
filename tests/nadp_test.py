"""Tests for NADP functions."""
import pytest
import os
try:
    import dataretrieval.nadp as nadp
except ImportError:
    pass  # happens when GDAL is not available


class TestMDNmap:
    """Testing the mercury deposition network map functions.

    This set of tests actually queries the services themselves to ensure there
    have been no upstream changes to paths or file names. Tests created
    because there was an upstream change to paths that broke ``dataretrieval``
    functionality.
    """

    @pytest.mark.xfail(reason='This test requires GDAL which is not on the CI runner.')
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

    @pytest.mark.xfail(reason='This test requires GDAL which is not on the CI runner.')
    def test_get_annual_MDN_map_buffer(self):
        """Test the get_annual_MDN_map function buffer return."""
        gmem = nadp.get_annual_MDN_map(measurement_type='dep', year='2008')
        # assert gmem is a GDALMemFile object
        assert isinstance(gmem, nadp.GDALMemFile)


class TestNTNmap:
    """Testing the national trends network map functions."""

    @pytest.mark.xfail(reason='This test requires GDAL which is not on the CI runner.')
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

    @pytest.mark.xfail(reason='This test requires GDAL which is not on the CI runner.')
    def test_get_annual_NTN_map_buffer(self):
        """Test the get_annual_NTN_map function buffer return."""
        gmem = nadp.get_annual_NTN_map(
            measurement_type='conc', measurement='NO3', year='1996')
        # assert gmem is a GDALMemFile object
        assert isinstance(gmem, nadp.GDALMemFile)
