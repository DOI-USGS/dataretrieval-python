"""Deprecated alias for :mod:`dataretrieval.nwdc`.

The module was named for one subset of what the service offers. The National
Water Availability Assessment Data Companion serves ten modeled datasets, of
which the water-use models are five; the rest are hydrologic,
atmospheric-forcing, and assessment outputs. Every other adapter in this
package is named for its service -- ``ngwmn``, ``nldi``, ``wqp``,
``streamstats``, ``nwis`` -- so this one is now ``nwdc``.

Importing this module emits a :class:`DeprecationWarning` and re-exports
:mod:`dataretrieval.nwdc`'s public surface. The re-exported objects are the
*same objects*, not copies -- ``wateruse.get_wateruse is nwdc.get_wateruse``
-- so calls and identity comparisons behave identically through either
spelling.

It is an alias for reading, not a second name for the module. This is a
distinct module object holding its own references to the five public names,
so it does not forward *assignment* or private names: rebinding
``wateruse.get_wateruse`` leaves ``nwdc``'s global untouched (and so has no
effect on anything ``nwdc`` does internally), and ``wateruse._WATERUSE_HOST``
does not exist. Code that monkeypatches, or that reaches for a private, must
name :mod:`dataretrieval.nwdc` directly -- which is the point of the
deprecation.

``dataretrieval.__init__`` deliberately imports :mod:`dataretrieval.nwdc`
rather than this module, so ``import dataretrieval`` stays silent. The warning
fires only for code that names ``wateruse`` itself.
"""

from __future__ import annotations

from dataretrieval import nwdc as _nwdc
from dataretrieval._deprecation import REMOVALS, warn_deprecated
from dataretrieval.nwdc import *  # noqa: F403  (re-export the public surface)

#: When the alias may be deleted. Read from the shared horizon table rather
#: than spelled here, so it is audited and bumped with every other published
#: removal; matches the dated-removal convention :mod:`dataretrieval.nwis`
#: uses.
NWDC_RENAME_REMOVAL_DATE = REMOVALS["wateruse"]

__all__ = list(_nwdc.__all__)

warn_deprecated(
    "`dataretrieval.wateruse`",
    replacement="`dataretrieval.nwdc`",
    removal=NWDC_RENAME_REMOVAL_DATE,
    detail="The service is the National Water Availability Assessment Data "
    "Companion, and water use is one of the ten datasets it serves.",
    # 1 lands the warning on the line that imported this module -- an import
    # has no deeper user frame to point at.
    stacklevel=1,
)
