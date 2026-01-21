class PresignError(Exception):
    """Base exception for presigning errors"""

    pass


class InvalidURLError(PresignError):
    """Raised when URL format is invalid"""

    pass


class ExpirationError(PresignError):
    """Raised when expiration parameters are invalid"""

    pass


class SignatureError(PresignError):
    """Raised when signature generation fails"""

    pass


class ClientError(Exception):
    pass
