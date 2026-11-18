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


# ``import dataretrieval`` never loads the ``nwis`` stub, so attribute access
# is answered by the package ``__getattr__`` instead.
_NWIS_ATTRIBUTE_ACCESS = [
    "import dataretrieval\ndataretrieval.nwis.get_dv()",
    "import dataretrieval as dr\ndr.nwis",
    "import dataretrieval\ngetattr(dataretrieval, 'nwis')",
]


@pytest.mark.parametrize("statement", _NWIS_ATTRIBUTE_ACCESS)
def test_nwis_attribute_access_names_the_replacement_and_the_pin(
    statement: str,
) -> None:
    with pytest.raises(AttributeError) as excinfo:
        exec(statement, {})
    message = str(excinfo.value)
    assert "dataretrieval.waterdata" in message
    assert 'pip install "dataretrieval<=1.4.0"' in message


def test_hasattr_nwis_is_false_rather_than_raising() -> None:
    # IDEs, Sphinx, and ``inspect`` probe with ``hasattr``; it must not crash.
    import dataretrieval

    assert not hasattr(dataretrieval, "nwis")


def test_unknown_attribute_keeps_the_standard_message() -> None:
    with pytest.raises(AttributeError, match="has no attribute 'not_a_module'"):
        exec("import dataretrieval\ndataretrieval.not_a_module", {})
