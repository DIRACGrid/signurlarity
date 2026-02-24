from __future__ import annotations


class SignurlarityError(Exception): ...


class PresignError(SignurlarityError):
    """Base exception for presigning errors."""

    pass


class InvalidURLError(SignurlarityError):
    """Raised when URL format is invalid."""

    pass


class ExpirationError(SignurlarityError):
    """Raised when expiration parameters are invalid."""

    pass


class SignatureError(SignurlarityError):
    """Raised when signature generation fails."""

    pass


class NoSuchBucketError(SignurlarityError):
    """Raised when bucket does not exist or is not accessible."""

    pass


class BucketAlreadyExistsError(SignurlarityError):
    """Raised when trying to create a bucket that already exists."""

    pass


class BucketAlreadyOwnedByYouError(SignurlarityError):
    """Raised when trying to create a bucket you already own."""

    pass
