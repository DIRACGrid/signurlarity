"""Async S3 client for presigned URL generation and basic S3 operations."""

from __future__ import annotations

from typing import Any

import httpx

from .._base import _BaseClient
from ..exceptions import PresignError


class AsyncClient(_BaseClient):
    """Async S3 client for generating presigned URLs and performing S3 operations.

    This is a lightweight, boto3-compatible async client that focuses on presigned URL
    generation and basic S3 operations without the boto3 dependency overhead.
    Uses async connection pooling via httpx for better performance in async applications.

    Args:
        endpoint_url: S3 endpoint URL. Examples:
                     - AWS: 'https://s3.amazonaws.com' or 'https://s3.us-west-2.amazonaws.com'
                     - MinIO: 'http://localhost:9000'
                     - Custom S3-compatible services
        aws_access_key_id: AWS access key ID for authentication
        aws_secret_access_key: AWS secret access key for authentication
        httpx_max_connections: Optional maximum number of connections in the pool.
                              If not specified, uses httpx default limits.

    Example:
        >>> # Basic async usage with explicit cleanup
        >>> client = AsyncClient(
        ...     endpoint_url="https://s3.us-west-2.amazonaws.com",
        ...     aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        ...     aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ... )
        >>> url = await client.generate_presigned_url(
        ...     "get_object",
        ...     Params={"Bucket": "mybucket", "Key": "mykey"},
        ...     ExpiresIn=3600,
        ... )
        >>> await client.close()

        >>> # Using async context manager for automatic cleanup (recommended)
        >>> async with AsyncClient(
        ...     endpoint_url="https://s3.us-west-2.amazonaws.com",
        ...     aws_access_key_id="AKIAIOSFODNN7EXAMPLE",
        ...     aws_secret_access_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        ... ) as client:
        ...     url = await client.generate_presigned_url(
        ...         "get_object",
        ...         Params={"Bucket": "mybucket", "Key": "mykey"},
        ...         ExpiresIn=3600,
        ...     )

        >>> # Using with MinIO or other S3-compatible services
        >>> async with AsyncClient(
        ...     endpoint_url="http://localhost:9000",
        ...     aws_access_key_id="minioadmin",
        ...     aws_secret_access_key="minioadmin",
        ... ) as client:
        ...     # Perform S3 operations asynchronously
        ...     await client.create_bucket(Bucket="test-bucket")
        ...     url = await client.generate_presigned_url(
        ...         "put_object",
        ...         Params={"Bucket": "test-bucket", "Key": "upload.txt"},
        ...         ExpiresIn=3600,
        ...     )

        >>> # Concurrent operations with asyncio
        >>> async with AsyncClient(...) as client:
        ...     urls = await asyncio.gather(
        ...         client.generate_presigned_url(
        ...             "get_object", {"Bucket": "b1", "Key": "k1"}
        ...         ),
        ...         client.generate_presigned_url(
        ...             "get_object", {"Bucket": "b2", "Key": "k2"}
        ...         ),
        ...         client.generate_presigned_url(
        ...             "get_object", {"Bucket": "b3", "Key": "k3"}
        ...         ),
        ...     )

    Note:
        This client uses async connection pooling via httpx.AsyncClient() for better
        performance in async applications. The same HTTP client instance is reused
        across multiple requests, reducing connection overhead and enabling HTTP/2 benefits.
        Always use the async context manager or call await close() to properly clean up
        connections.

    """

    def __init__(
        self,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        httpx_max_connections: int | None = None,
    ):
        super().__init__(
            endpoint_url=endpoint_url,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            httpx_max_connections=httpx_max_connections,
        )
        self._http_client = httpx.AsyncClient(limits=self._limits)

    async def _execute_request(
        self, method: str, url: str, headers: dict[str, str], body: bytes = b""
    ) -> httpx.Response:
        """Execute an HTTP request using async connection pooling.

        Args:
            method: HTTP method (GET, PUT, HEAD, DELETE, etc.)
            url: Full URL to request
            headers: Request headers
            body: Request body (for PUT/POST)

        Returns:
            httpx.Response object

        Raises:
            PresignError: If request execution fails

        """
        try:
            if method == "HEAD":
                return await self._http_client.head(url, headers=headers)
            elif method == "PUT":
                return await self._http_client.put(url, headers=headers, content=body)
            elif method == "GET":
                return await self._http_client.get(url, headers=headers)
            elif method == "DELETE":
                return await self._http_client.delete(url, headers=headers)
            elif method == "POST":
                return await self._http_client.post(
                    url, headers=headers, content=body
                )
            else:
                raise PresignError(f"Unsupported HTTP method: {method}")
        except httpx.HTTPError as e:
            raise PresignError(f"Failed to execute {method} request: {str(e)}") from e
        except Exception as e:
            raise PresignError(f"Failed to execute {method} request: {str(e)}") from e

    async def generate_presigned_post(
        self,
        Bucket: str,
        Key: str,
        Fields: dict[str, Any] | None = None,
        Conditions: list[Any] | None = None,
        ExpiresIn: int = 3600,
    ) -> dict[str, Any]:
        """Generate a presigned POST policy.

        This method generates a presigned POST policy for uploading objects
        directly to S3 from a browser or other client.

        Args:
            Bucket: S3 bucket name
            Key: Object key (path in bucket)
            Fields: Additional form fields to include (e.g., metadata, ACL)
            Conditions: Policy conditions for the upload
            ExpiresIn: Policy expiration time in seconds (max 604800 / 7 days)

        Returns:
            Dictionary with 'url' and 'fields' keys:
                - url: The S3 bucket URL to POST to
                - fields: Form fields to include in the POST request

        Raises:
            PresignError: If required parameters are missing or invalid

        Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_post.html

        Example:
            >>> post_data = await client.generate_presigned_post(
            ...     Bucket="mybucket",
            ...     Key="myfile.txt",
            ...     Fields={"acl": "public-read"},
            ...     Conditions=[["content-length-range", 0, 1048576]],
            ...     ExpiresIn=3600,
            ... )

        """
        return self._generate_presigned_post(
            Bucket=Bucket,
            Key=Key,
            Fields=Fields,
            Conditions=Conditions,
            ExpiresIn=ExpiresIn,
        )

    async def generate_presigned_url(
        self,
        ClientMethod: str,
        Params: Any,
        ExpiresIn: int = 3600,
        HttpMethod: str = "",
    ) -> str:
        """Generate a presigned URL for an S3 operation.

        This method is boto3-compatible and maps to the underlying fast presigner.

        Args:
            ClientMethod: S3 operation name (e.g., 'get_object', 'put_object')
            Params: Operation parameters (must include 'Bucket' and 'Key')
            ExpiresIn: URL expiration time in seconds (max 604800 / 7 days)
            HttpMethod: Optional HTTP method override (GET, PUT, DELETE, etc.)

        Returns:
            Presigned URL string

        Raises:
            PresignError: If required parameters are missing or invalid

        Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/generate_presigned_url.html

        Example:
            >>> url = await client.generate_presigned_url(
            ...     "get_object",
            ...     Params={"Bucket": "mybucket", "Key": "documents/report.pdf"},
            ...     ExpiresIn=3600,
            ... )

        """
        return self._generate_presigned_url(
            ClientMethod=ClientMethod,
            Params=Params,
            ExpiresIn=ExpiresIn,
            HttpMethod=HttpMethod,
        )

    async def __aenter__(self):
        """Async context manager entry point."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit point - close the HTTP client."""
        await self.close()
        return False

    async def close(self):
        """Ensure HTTP client is closed."""
        await self._http_client.aclose()

    async def head_bucket(self, Bucket: str, **kwargs) -> dict[str, Any]:
        """Check if a bucket exists and is accessible.

        Performs a HEAD request to the bucket to verify existence and access permissions.

        Args:
            Bucket: S3 bucket name
            **kwargs: Additional arguments (ExpectedBucketOwner, etc.)

        Returns:
            dict with response metadata containing:
                - BucketRegion: The region where the bucket is located

        Raises:
            NoSuchBucketError: If bucket does not exist or is not accessible
            PresignError: If request signing or execution fails

        Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_bucket.html

        Example:
            >>> response = await client.head_bucket(Bucket="mybucket")
            >>> print(response["BucketRegion"])
            'us-west-2'

        """
        url, signed_headers = self._prepare_head_bucket(Bucket, **kwargs)
        response = await self._execute_request("HEAD", url, signed_headers)
        return self._parse_head_bucket_response(response, Bucket)

    async def head_object(self, Bucket: str, Key: str, **kwargs) -> dict[str, Any]:
        """Check if an object exists and retrieve its metadata.

        Performs a HEAD request to retrieve object metadata without downloading the object.

        Args:
            Bucket: S3 bucket name
            Key: Object key (path in bucket)
            **kwargs: Additional arguments (VersionId, SSECustomerAlgorithm, etc.)

        Returns:
            dict with object metadata containing:
                - ContentLength: Size of the object in bytes
                - LastModified: Last modification time
                - ETag: Entity tag of the object
                - ResponseMetadata: Response metadata with HTTPStatusCode and HTTPHeaders

        Raises:
            NoSuchBucketError: If bucket does not exist
            PresignError: If request signing or execution fails

        Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/head_object.html

        Example:
            >>> response = await client.head_object(Bucket="mybucket", Key="myfile.txt")
            >>> print(response["ContentLength"])
            1024

        """
        url, signed_headers = self._prepare_head_object(Bucket, Key, **kwargs)
        response = await self._execute_request("HEAD", url, signed_headers)
        return self._parse_head_object_response(response, Bucket, Key)

    async def create_bucket(self, Bucket: str, **kwargs) -> dict[str, Any]:
        """Create a new S3 bucket.

        Performs a PUT request to create a new S3 bucket.

        Args:
            Bucket: S3 bucket name (required)
            **kwargs: Additional arguments including:
                - CreateBucketConfiguration: dict with 'LocationConstraint' (region)
                - ACL: Canned ACL to apply
                - ObjectLockEnabledForBucket: boolean

        Returns:
            dict with response metadata containing:
                - Location: The URI of the created bucket

        Raises:
            BucketAlreadyExistsError: If bucket already exists and is owned by someone else
            BucketAlreadyOwnedByYouError: If bucket already exists and is owned by you
            PresignError: If request signing or execution fails

        Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/create_bucket.html

        Example:
            >>> response = await client.create_bucket(Bucket="mybucket")
            >>> print(response["Location"])
            '/mybucket'

        """
        url, signed_headers, body = self._prepare_create_bucket(Bucket, **kwargs)
        response = await self._execute_request("PUT", url, signed_headers, body)
        return self._parse_create_bucket_response(response, Bucket)

    async def delete_objects(self, Bucket: str, Delete: dict[str, Any], **kwargs) -> dict[str, Any]:
        """Delete multiple objects from an S3 bucket in a single request.

        Performs a multi-object delete using a POST request with the ``?delete``
        query parameter and an XML body listing the objects to remove.

        Args:
            Bucket: S3 bucket name
            Delete: Dictionary with deletion specification containing:
                - Objects: List of dicts, each with 'Key' (required) and
                  optional 'VersionId'
                - Quiet: Optional bool. When True, only errors are returned
                  in the response (default: False)
            **kwargs: Additional arguments (currently unused)

        Returns:
            dict with deletion results containing:
                - Deleted: List of successfully deleted objects
                - Errors: List of objects that failed to delete
                - ResponseMetadata: Response metadata with HTTPStatusCode
                  and HTTPHeaders

        Raises:
            PresignError: If required parameters are missing or the request fails

        Reference:
            https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/delete_objects.html

        Example:
            >>> response = await client.delete_objects(
            ...     Bucket="mybucket",
            ...     Delete={
            ...         "Objects": [
            ...             {"Key": "file1.txt"},
            ...             {"Key": "file2.txt"},
            ...         ],
            ...         "Quiet": False,
            ...     },
            ... )
            >>> print(response["Deleted"])
            [{'Key': 'file1.txt'}, {'Key': 'file2.txt'}]

        """
        url, signed_headers, body = self._prepare_delete_objects(Bucket, Delete, **kwargs)
        response = await self._execute_request("POST", url, signed_headers, body)
        return self._parse_delete_objects_response(response, Bucket)
