"""One advisory mechanism, and one place to read the removal horizons.

Four spellings of "tell the caller something is going away" had grown up
independently -- a dated decorator in :mod:`~dataretrieval.nwis`, an undated
kwarg shim in :mod:`~dataretrieval.waterdata.utils`, an undated module-level
notice in :mod:`~dataretrieval.wqp`, and one bare :func:`warnings.warn` with
no category at all. Only one carried a date, so the horizons could not be
audited or bumped in one place, and the category was a per-author choice.

The category matters more than the wording. A ``DeprecationWarning`` is a
promise that a *name in this package* is going away, so a downstream project
running ``-W error::DeprecationWarning`` is right to fail on it. An advisory
that an upstream *dataset* has stopped being updated is not that -- the API is
fine and the caller has nothing to migrate to -- and it belongs under
:class:`~dataretrieval.exceptions.DataCurrencyWarning` instead. See
:data:`REMOVALS` for the horizons this package has published.
"""

from __future__ import annotations

import warnings

#: Published removal horizons, by the surface each covers. A date here is a
#: commitment already made in a released warning message; read it rather than
#: spelling a date at the call site, so bumping one is a single edit.
REMOVALS: dict[str, str] = {
    "nwis": "2027-05-06",
    "waterdata.get_cql(service=)": "2027-08-09",
}


def warn_deprecated(
    subject: str,
    *,
    replacement: str,
    removal: str | None = None,
    detail: str = "",
    stacklevel: int = 2,
) -> None:
    """Emit this package's one deprecation advisory.

    Parameters
    ----------
    subject
        What is going away, as the caller spells it (``"nwis.get_dv"``, the
        keyword ``"stateFips"``).
    replacement
        What to use instead. Named in every message because a deprecation
        without a migration path is only an inconvenience.
    removal
        Date from :data:`REMOVALS`, or ``None`` when no horizon has been
        published -- which reads as "a future release" rather than inventing
        a commitment.
    detail
        Optional sentence appended after the advisory, for a rename whose
        reason is worth giving. Appended, never interpolated into the
        message, so a multi-sentence detail cannot corrupt the wording.
    stacklevel
        Frames to skip so the warning is attributed to the caller's own line,
        not to this function.
    """
    horizon = f"on or after {removal}" if removal else "in a future release"
    message = (
        f"{subject} is deprecated and will be removed from `dataretrieval` "
        f"{horizon}; use {replacement} instead."
    )
    warnings.warn(
        f"{message} {detail}" if detail else message,
        DeprecationWarning,
        stacklevel=stacklevel + 1,
    )
