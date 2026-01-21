from importlib.metadata import version as get_version, PackageNotFoundError

try:
    __version__ = get_version(__name__)
    version = __version__
except PackageNotFoundError:
    version = "Unknown"

from .client import Client

__all__ = ["Client", "__version__"]
