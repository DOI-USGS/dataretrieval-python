from importlib.metadata import PackageNotFoundError, version

try:
    __version__ = version("dataretrieval")
except PackageNotFoundError:
    __version__ = "version-unknown"

from dataretrieval.nadp import *  # noqa: F403
from dataretrieval.nwis import *  # noqa: F403
from dataretrieval.samples import *  # noqa: F403
from dataretrieval.streamstats import *  # noqa: F403
from dataretrieval.utils import *  # noqa: F403
from dataretrieval.waterdata import *  # noqa: F403
from dataretrieval.waterwatch import *  # noqa: F403
from dataretrieval.wqp import *  # noqa: F403
