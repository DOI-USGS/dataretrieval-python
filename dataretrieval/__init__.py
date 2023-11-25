from importlib.metadata import PackageNotFoundError, version

from dataretrieval.nadp import *
from dataretrieval.nwis import *
from dataretrieval.streamstats import *
from dataretrieval.utils import *
from dataretrieval.waterwatch import *
from dataretrieval.wqp import *

try:
    __version__ = version('dataretrieval')
except PackageNotFoundError:
    __version__ = 'version-unknown'
