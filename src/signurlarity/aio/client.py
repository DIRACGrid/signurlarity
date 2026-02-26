from __future__ import annotations

from typing import Any, Mapping, Optional
from urllib.parse import urlparse

import httpx

from ..exceptions import (
    BucketAlreadyExistsError,
    BucketAlreadyOwnedByYouError,
    NoSuchBucketError,
    PresignError,
)
from ..presigner import S3Presigner


class AsyncClient:
    """Async S3 client for generating presigned URLs.

    This is a lightweight, boto3-compatible async client that focuses on presigned URL
    generation without the boto3 dependency overhead. Uses connection pooling
    for better performance.

    Args:
        endpoint_url: S3 endpoint URL (e.g., 'https://s3.amazonaws.com' or
                      'https://s3.us-west-2.amazonaws.com')
        aws_access_key_id: AWS access key ID
        aws_secret_access_key: AWS secret access key

    Example:
        >>> # Basic async usage
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

        >>> # Using async context manager for automatic cleanup
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

    Note:
        This client uses connection pooling via httpx.AsyncClient() for better
        performance. The same HTTP client instance is reused across multiple
        requests, reducing connection overhead and enabling HTTP/2 benefits.

    """

    def __init__(
        self,
        endpoint_url: str,
        aws_access_key_id: str,
        aws_secret_access_key: str,
        httpx_max_connections: Optional[int] = None,
    ):
        self.endpoint_url = endpoint_url
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key

        # Extract region from endpoint URL
        self.region = self._extract_region(endpoint_url)

        # Initialize the presigner
        self._presigner = S3Presigner(
            access_key=aws_access_key_id,
            secret_key=aws_secret_access_key,
            region=self.region,
            endpoint_url=endpoint_url,
        )

        # Initialize HTTP client for connection pooling
        limits = httpx._config.DEFAULT_LIMITS
        if httpx_max_connections:
            limits.max_connections = httpx_max_connections
        self._http_client = httpx.AsyncClient(limits=limits)

    def _extract_region(self, endpoint_url: str) -> str:
        """Extract AWS region from endpoint URL.

        Args:
            endpoint_url: S3 endpoint URL

        Returns:
            AWS region string (defaults to 'us-east-1')

        """
        parsed = urlparse(endpoint_url)
        hostname = parsed.hostname or parsed.netloc

        # Parse patterns like:
        # - s3.amazonaws.com -> us-east-1
        # - s3.us-west-2.amazonaws.com -> us-west-2
        # - bucket.s3.eu-central-1.amazonaws.com -> eu-central-1
        parts = hostname.split(".")
        if "s3" in parts:
            idx = parts.index("s3")
            if idx + 1 < len(parts) and parts[idx + 1] != "amazonaws":
                return parts[idx + 1]

        return "us-east-1"

    def _build_request_url(
        self, bucket: str, key: str | None = None
    ) -> tuple[str, str, dict[str, str]]:
        """Build request URL and headers for S3 operations.

        Args:
            bucket: S3 bucket name
            key: Optional object key

        Returns:
            Tuple of (base_url, path, headers)

        """
        scheme = "https"
        if self.endpoint_url:
            parsed = urlparse(self.endpoint_url)
            scheme = parsed.scheme or "https"
            # For custom endpoints, use path-style URLs
            actual_host = parsed.netloc
            if key:
                path = f"/{bucket}/{key}"
            else:
                path = f"/{bucket}"
            base_url = f"{scheme}://{actual_host}"
            headers = {"host": actual_host}
        else:
            # For AWS endpoints, use virtual-hosted style
            actual_host = self._presigner._get_host(bucket)
            path = f"/{key}" if key else "/"
            base_url = f"{scheme}://{actual_host}"
            headers = {"host": actual_host}

        return base_url, path, headers

    def _build_query_string(self, query_params: dict[str, str]) -> str:
        """Build query string from parameters.

        Args:
            query_params: Dictionary of query parameters

        Returns:
            Query string (without leading '?')

        """
        return "&".join(f"{k}={v}" for k, v in query_params.items())

    async def _execute_request(
        self, method: str, url: str, headers: dict[str, str], body: bytes = b""
    ) -> httpx.Response:
        """Execute an HTTP request using connection pooling.

        Uses the instance's httpx.AsyncClient for connection pooling, reusing
        the same HTTP connection across multiple requests for better performance.

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
            >>> # Use post_data['url'] and post_data['fields'] for upload

        """
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")
        if not Key:
            raise PresignError("Missing required parameter 'Key'")

        try:
            return self._presigner.generate_presigned_post(
                bucket=Bucket,
                key=Key,
                fields=Fields,
                conditions=Conditions,
                expires=ExpiresIn,
            )
        except ValueError as e:
            raise PresignError(str(e)) from e

    async def generate_presigned_url(
        self,
        ClientMethod: str,
        Params: Mapping[str, Any],
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
            ...     Params={"Bucket": "mybucket", "Key": "myfile.txt"},
            ...     ExpiresIn=3600,
            ... )

        """
        if Params is None:
            Params = {}

        # Extract bucket and key from params
        bucket = Params.get("Bucket")
        key = Params.get("Key")

        if not bucket:
            raise PresignError("Missing required parameter 'Bucket' in Params")
        if not key:
            raise PresignError("Missing required parameter 'Key' in Params")

        # Determine HTTP method from ClientMethod if not explicitly provided
        if not HttpMethod:
            HttpMethod = self._client_method_to_http_method(ClientMethod)

        try:
            return self._presigner.generate_presigned_url(
                bucket=bucket, key=key, method=HttpMethod, expires=ExpiresIn
            )
        except ValueError as e:
            raise PresignError(str(e)) from e

    def _client_method_to_http_method(self, client_method: str) -> str:
        """Map boto3 client method name to HTTP method.

        Args:
            client_method: S3 client method name (e.g., 'get_object')

        Returns:
            HTTP method string (GET, PUT, DELETE, etc.)

        """
        method_map = {
            "get_object": "GET",
            "put_object": "PUT",
            "delete_object": "DELETE",
            "head_object": "HEAD",
            "list_objects": "GET",
            "list_objects_v2": "GET",
        }
        return method_map.get(client_method.lower(), "GET")

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

    async def head_bucket(self, Bucket: str, **kwargs):
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
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")

        # Build the request
        base_url, path, headers = self._build_request_url(Bucket)

        # Add optional parameters as query parameters if provided
        query_params = {}
        if "ExpectedBucketOwner" in kwargs:
            query_params["expected-bucket-owner"] = kwargs["ExpectedBucketOwner"]

        # Sign the request
        signed_headers = self._presigner.sign_request_headers(
            method="HEAD",
            path=path,
            headers=headers,
        )

        # Build the full URL
        url = base_url + path
        if query_params:
            url = f"{url}?{self._build_query_string(query_params)}"

        # Execute the request
        response = await self._execute_request("HEAD", url, signed_headers)

        # Handle error responses
        if response.status_code == 404:
            raise NoSuchBucketError(
                f"Bucket '{Bucket}' does not exist or is not accessible"
            )
        elif response.status_code == 403:
            raise NoSuchBucketError(
                f"Access denied to bucket '{Bucket}'. Check credentials and permissions."
            )
        elif response.status_code == 400:
            raise PresignError(f"Bad request to bucket '{Bucket}': {response.text}")
        elif response.status_code != 200:
            raise PresignError(
                f"HEAD request failed with status {response.status_code}: {response.text}"
            )

        # Extract metadata from response headers
        bucket_region = response.headers.get("x-amz-bucket-region", self.region)

        return {
            "BucketRegion": bucket_region,
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }

    async def head_object(self, Bucket: str, Key: str, **kwargs):
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
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")
        if not Key:
            raise PresignError("Missing required parameter 'Key'")

        # Build the request
        base_url, path, headers = self._build_request_url(Bucket, Key)

        # Add optional parameters as query parameters if provided
        query_params = {}
        if "VersionId" in kwargs:
            query_params["versionId"] = kwargs["VersionId"]

        # Sign the request
        signed_headers = self._presigner.sign_request_headers(
            method="HEAD",
            path=path,
            headers=headers,
        )

        # Build the full URL
        url = base_url + path
        if query_params:
            url = f"{url}?{self._build_query_string(query_params)}"

        # Execute the request
        response = await self._execute_request("HEAD", url, signed_headers)

        # Handle error responses
        if response.status_code == 404:
            raise PresignError(
                f"Object '{Key}' in bucket '{Bucket}' does not exist or is not accessible"
            )
        elif response.status_code == 403:
            raise PresignError(
                f"Access denied to object '{Key}' in bucket '{Bucket}'. Check credentials and permissions."
            )
        elif response.status_code == 400:
            raise PresignError(f"Bad request for object '{Key}': {response.text}")
        elif response.status_code != 200:
            raise PresignError(
                f"HEAD request failed with status {response.status_code}: {response.text}"
            )

        # Extract metadata from response headers
        result = {
            "ContentLength": int(response.headers.get("content-length", 0)),
            "LastModified": response.headers.get("last-modified"),
            "ETag": response.headers.get("etag"),
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }

        # Add optional metadata if present
        if "content-type" in response.headers:
            result["ContentType"] = response.headers.get("content-type")
        if "cache-control" in response.headers:
            result["CacheControl"] = response.headers.get("cache-control")
        if "content-encoding" in response.headers:
            result["ContentEncoding"] = response.headers.get("content-encoding")
        if "x-amz-version-id" in response.headers:
            result["VersionId"] = response.headers.get("x-amz-version-id")

        return result

    async def create_bucket(self, Bucket: str, **kwargs):
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
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")

        # Build the request
        base_url, path, headers = self._build_request_url(Bucket)

        # Build request body for CreateBucketConfiguration if provided
        body = b""
        if "CreateBucketConfiguration" in kwargs:
            config = kwargs["CreateBucketConfiguration"]
            if "LocationConstraint" in config:
                location = config["LocationConstraint"]
                # Only send body if location is not us-east-1 (default)
                if location and location != "us-east-1":
                    body = (
                        f"<CreateBucketConfiguration>"
                        f"<LocationConstraint>{location}</LocationConstraint>"
                        f"</CreateBucketConfiguration>"
                    ).encode("utf-8")
                    headers["Content-Type"] = "application/xml"

        # Sign the request with the body
        signed_headers = self._presigner.sign_request_headers(
            method="PUT",
            path=path,
            headers=headers,
            body=body,
        )

        # Build the full URL
        url = base_url + path

        # Execute the request
        response = await self._execute_request("PUT", url, signed_headers, body)

        # Handle error responses
        if response.status_code == 409:
            # Check if it's BucketAlreadyExistsError or BucketAlreadyOwnedByYouError
            if "BucketAlreadyOwnedByYouError" in response.text:
                raise BucketAlreadyOwnedByYouError(
                    f"Bucket '{Bucket}' already exists and is owned by you"
                )
            else:
                raise BucketAlreadyExistsError(f"Bucket '{Bucket}' already exists")
        elif response.status_code == 400:
            raise PresignError(
                f"Bad request to create bucket '{Bucket}': {response.text}"
            )
        elif response.status_code not in (200, 201):
            raise PresignError(
                f"PUT request failed with status {response.status_code}: {response.text}"
            )

        # Extract location from response headers or construct it
        location = response.headers.get("Location", f"/{Bucket}")

        return {
            "Location": location,
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }
