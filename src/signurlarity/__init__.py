"""Signurlarity - Fast S3 presigned URL generation without boto3."""

from __future__ import annotations

from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as get_version

try:
    __version__ = get_version(__name__)
    version = __version__
except PackageNotFoundError:
    version = "Unknown"

from .client import Client

__all__ = ["Client", "__version__"]
