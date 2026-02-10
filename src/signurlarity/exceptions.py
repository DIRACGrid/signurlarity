from __future__ import annotations


class PresignError(Exception):
    """Base exception for presigning errors."""

    pass


class InvalidURLError(PresignError):
    """Raised when URL format is invalid."""

    pass


class ExpirationError(PresignError):
    """Raised when expiration parameters are invalid."""

    pass


class SignatureError(PresignError):
    """Raised when signature generation fails."""

    pass


class NoSuchBucketError(PresignError):
    """Raised when bucket does not exist or is not accessible."""

    pass


class BucketAlreadyExistsError(PresignError):
    """Raised when trying to create a bucket that already exists."""

    pass


class BucketAlreadyOwnedByYouError(PresignError):
    """Raised when trying to create a bucket you already own."""

    pass


class ClientError(Exception):
    pass
