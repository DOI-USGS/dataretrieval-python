"""A removed module must fail on import with a message naming what to do instead."""

from __future__ import annotations

import pytest

_NWIS_IMPORTS = [
    "import dataretrieval.nwis",
    "from dataretrieval import nwis",
    "from dataretrieval.nwis import get_dv",
]


@pytest.mark.parametrize("statement", _NWIS_IMPORTS)
def test_nwis_import_names_the_replacement_and_the_pin(statement: str) -> None:
    with pytest.raises(ImportError) as excinfo:
        exec(statement, {})
    message = str(excinfo.value)
    assert "dataretrieval.waterdata" in message
    assert 'pip install "dataretrieval<=1.4.0"' in message
