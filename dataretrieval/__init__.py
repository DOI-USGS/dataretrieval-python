from importlib.metadata import version
from importlib.metadata import PackageNotFoundError

try:
    __version__ = version('dataretrieval')
except PackageNotFoundError:
    __version__ = "version-unknown"