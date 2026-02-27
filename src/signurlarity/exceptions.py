"""Exception classes for Signurlarity S3 client operations.

This module defines all custom exceptions that can be raised by the
Signurlarity client during S3 operations.
"""

from __future__ import annotations


class SignurlarityError(Exception):
    """Base exception for all Signurlarity errors.

    All custom exceptions in this package inherit from this base class,
    making it easy to catch all signurlarity-specific errors.

    Example:
        >>> from signurlarity import Client
        >>> from signurlarity.exceptions import SignurlarityError
        >>> try:
        ...     client = Client(...)
        ...     client.head_bucket(Bucket="nonexistent")
        ... except SignurlarityError as e:
        ...     print(f"S3 operation failed: {e}")

    """

    pass


class PresignError(SignurlarityError):
    """Raised when presigned URL or POST generation fails.

    This exception is raised when there are issues with:
    - Missing or invalid parameters (Bucket, Key, etc.)
    - Request signing failures
    - HTTP request execution errors

    Example:
        >>> client.generate_presigned_url(
        ...     "get_object",
        ...     Params={"Bucket": ""},  # Empty bucket name
        ...     ExpiresIn=3600,
        ... )
        PresignError: Missing required parameter 'Bucket' in Params

    """

    pass


class InvalidURLError(SignurlarityError):
    """Raised when an S3 URL format is invalid.

    This exception indicates that a provided URL does not match the
    expected S3 URL format and cannot be parsed correctly.

    Example:
        >>> # Invalid endpoint URL format
        >>> client = Client(endpoint_url="not-a-valid-url", ...)
        InvalidURLError: Invalid URL format

    """

    pass


class ExpirationError(SignurlarityError):
    """Raised when expiration time parameters are invalid.

    This exception is raised when the ExpiresIn parameter is outside
    the valid range (1 to 604800 seconds / 7 days) for presigned URLs.

    Example:
        >>> client.generate_presigned_url(
        ...     "get_object",
        ...     Params={"Bucket": "mybucket", "Key": "mykey"},
        ...     ExpiresIn=1000000,  # Too large
        ... )
        ExpirationError: Expires must be between 1 and 604800 seconds (7 days)

    """

    pass


class SignatureError(SignurlarityError):
    """Raised when AWS Signature V4 generation fails.

    This exception indicates a problem with generating the AWS Signature V4
    authentication signature, typically due to invalid credentials or
    internal signing logic errors.

    Example:
        >>> # Invalid credentials
        >>> client = Client(
        ...     endpoint_url="https://s3.amazonaws.com",
        ...     aws_access_key_id="",  # Empty key
        ...     aws_secret_access_key="invalid",
        ... )
        SignatureError: Failed to generate signature

    """

    pass


class NoSuchBucketError(SignurlarityError):
    """Raised when an S3 bucket does not exist or is not accessible.

    This exception is raised during bucket operations when:
    - The bucket does not exist
    - You don't have permission to access the bucket
    - The bucket name is invalid

    Corresponds to HTTP 404 or 403 status codes.

    Example:
        >>> client.head_bucket(Bucket="nonexistent-bucket")
        NoSuchBucketError: Bucket 'nonexistent-bucket' does not exist or is not accessible

        >>> client.head_object(Bucket="private-bucket", Key="file.txt")
        NoSuchBucketError: Access denied to bucket 'private-bucket'. Check credentials and permissions.

    """

    pass


class BucketAlreadyExistsError(SignurlarityError):
    """Raised when trying to create a bucket that already exists.

    This exception is raised when attempting to create a bucket with a name
    that's already taken by another AWS account. Bucket names are globally
    unique across all AWS accounts.

    Corresponds to HTTP 409 Conflict status code.

    Example:
        >>> client.create_bucket(Bucket="existing-bucket-name")
        BucketAlreadyExistsError: Bucket 'existing-bucket-name' already exists

    """

    pass


class BucketAlreadyOwnedByYouError(SignurlarityError):
    """Raised when trying to create a bucket you already own.

    This exception is raised when attempting to create a bucket that
    already exists in your own AWS account. Unlike BucketAlreadyExistsError,
    this indicates you own the bucket.

    Corresponds to HTTP 409 Conflict status code.

    Example:
        >>> client.create_bucket(Bucket="my-existing-bucket")
        BucketAlreadyOwnedByYouError: Bucket 'my-existing-bucket' already exists and is owned by you

    """

    pass
