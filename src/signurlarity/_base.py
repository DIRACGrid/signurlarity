"""Shared base class for sync and async S3 clients."""

from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET
from base64 import b64encode
from typing import Any, Mapping, Optional
from urllib.parse import urlparse
from xml.sax.saxutils import escape as xml_escape

import httpx

from .exceptions import (
    BucketAlreadyExistsError,
    BucketAlreadyOwnedByYouError,
    NoSuchBucketError,
    PresignError,
)
from .presigner import S3Presigner


class _BaseClient:
    """Base class containing shared logic for sync and async S3 clients.

    This class implements all non-I/O operations: initialization, URL building,
    request preparation, response parsing, and presigned URL/POST generation.
    Subclasses only need to implement I/O (HTTP execution) and lifecycle methods.
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

        # Build connection pool limits for subclasses
        self._limits = httpx._config.DEFAULT_LIMITS
        if httpx_max_connections:
            self._limits.max_connections = httpx_max_connections

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

    def _generate_presigned_post(
        self,
        Bucket: str,
        Key: str,
        Fields: dict[str, Any] | None = None,
        Conditions: list[Any] | None = None,
        ExpiresIn: int = 3600,
    ) -> dict[str, Any]:
        """Generate a presigned POST policy (implementation).

        Subclasses expose this as a public method (sync or async).
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

    def _generate_presigned_url(
        self,
        ClientMethod: str,
        Params: Mapping[str, Any],
        ExpiresIn: int = 3600,
        HttpMethod: str = "",
    ) -> str:
        """Generate a presigned URL for an S3 operation (implementation).

        Subclasses expose this as a public method (sync or async).
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

    # -- Request preparation helpers (no I/O) --

    def _prepare_head_bucket(self, Bucket: str, **kwargs) -> tuple[str, dict[str, str]]:
        """Validate and build a signed HEAD bucket request.

        Returns:
            Tuple of (url, signed_headers)

        """
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")

        base_url, path, headers = self._build_request_url(Bucket)

        query_params = {}
        if "ExpectedBucketOwner" in kwargs:
            query_params["expected-bucket-owner"] = kwargs["ExpectedBucketOwner"]

        signed_headers = self._presigner.sign_request_headers(
            method="HEAD",
            path=path,
            headers=headers,
        )

        url = base_url + path
        if query_params:
            url = f"{url}?{self._build_query_string(query_params)}"

        return url, signed_headers

    def _parse_head_bucket_response(
        self, response: httpx.Response, Bucket: str
    ) -> dict[str, Any]:
        """Parse HEAD bucket response, raising on errors."""
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

        bucket_region = response.headers.get("x-amz-bucket-region", self.region)

        return {
            "BucketRegion": bucket_region,
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }

    def _prepare_head_object(
        self, Bucket: str, Key: str, **kwargs
    ) -> tuple[str, dict[str, str]]:
        """Validate and build a signed HEAD object request.

        Returns:
            Tuple of (url, signed_headers)

        """
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")
        if not Key:
            raise PresignError("Missing required parameter 'Key'")

        base_url, path, headers = self._build_request_url(Bucket, Key)

        query_params = {}
        if "VersionId" in kwargs:
            query_params["versionId"] = kwargs["VersionId"]

        signed_headers = self._presigner.sign_request_headers(
            method="HEAD",
            path=path,
            headers=headers,
        )

        url = base_url + path
        if query_params:
            url = f"{url}?{self._build_query_string(query_params)}"

        return url, signed_headers

    def _parse_head_object_response(
        self, response: httpx.Response, Bucket: str, Key: str
    ) -> dict[str, Any]:
        """Parse HEAD object response, raising on errors."""
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

        result: dict[str, Any] = {
            "ContentLength": int(response.headers.get("content-length", 0)),
            "LastModified": response.headers.get("last-modified"),
            "ETag": response.headers.get("etag"),
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }

        if "content-type" in response.headers:
            result["ContentType"] = response.headers.get("content-type")
        if "cache-control" in response.headers:
            result["CacheControl"] = response.headers.get("cache-control")
        if "content-encoding" in response.headers:
            result["ContentEncoding"] = response.headers.get("content-encoding")
        if "x-amz-version-id" in response.headers:
            result["VersionId"] = response.headers.get("x-amz-version-id")

        return result

    def _prepare_create_bucket(
        self, Bucket: str, **kwargs
    ) -> tuple[str, dict[str, str], bytes]:
        """Validate and build a signed PUT bucket request.

        Returns:
            Tuple of (url, signed_headers, body)

        """
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")

        base_url, path, headers = self._build_request_url(Bucket)

        body = b""
        if "CreateBucketConfiguration" in kwargs:
            config = kwargs["CreateBucketConfiguration"]
            if "LocationConstraint" in config:
                location = config["LocationConstraint"]
                if location and location != "us-east-1":
                    body = (
                        f"<CreateBucketConfiguration>"
                        f"<LocationConstraint>{location}</LocationConstraint>"
                        f"</CreateBucketConfiguration>"
                    ).encode("utf-8")
                    headers["Content-Type"] = "application/xml"

        signed_headers = self._presigner.sign_request_headers(
            method="PUT",
            path=path,
            headers=headers,
            body=body,
        )

        url = base_url + path

        return url, signed_headers, body

    def _parse_create_bucket_response(
        self, response: httpx.Response, Bucket: str
    ) -> dict[str, Any]:
        """Parse PUT bucket response, raising on errors."""
        if response.status_code == 409:
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

        location = response.headers.get("Location", f"/{Bucket}")

        return {
            "Location": location,
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }

    def _prepare_delete_objects(
        self, Bucket: str, Delete: dict[str, Any], **kwargs
    ) -> tuple[str, dict[str, str], bytes]:
        """Validate and build a signed multi-object delete request.

        Returns:
            Tuple of (url, signed_headers, body)

        """
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")
        if not Delete or "Objects" not in Delete:
            raise PresignError(
                "Missing required parameter 'Objects' in Delete"
            )

        objects = Delete["Objects"]
        if not objects:
            raise PresignError("Delete.Objects must not be empty")

        quiet = Delete.get("Quiet", False)

        # Build XML body
        body = self._build_delete_xml(objects, quiet)

        # Compute Content-MD5 (required by S3 for multi-object delete)
        content_md5 = b64encode(hashlib.md5(body).digest()).decode()  # noqa: S324

        # Build the request
        base_url, path, headers = self._build_request_url(Bucket)
        headers["Content-Type"] = "application/xml"
        headers["Content-MD5"] = content_md5

        # Sign the request with the query string included
        signed_headers = self._presigner.sign_request_headers(
            method="POST",
            path=path,
            headers=headers,
            body=body,
            query_string="delete=",
        )

        # Build the full URL with ?delete query parameter
        url = f"{base_url}{path}?delete"

        return url, signed_headers, body

    @staticmethod
    def _build_delete_xml(objects: list[dict[str, str]], quiet: bool) -> bytes:
        """Build the XML body for a multi-object delete request.

        Args:
            objects: List of dicts with 'Key' and optional 'VersionId'
            quiet: When True, only errors are returned

        Returns:
            Encoded XML body bytes

        """
        parts = ["<Delete>"]
        if quiet:
            parts.append("<Quiet>true</Quiet>")
        for obj in objects:
            parts.append("<Object>")
            parts.append(f"<Key>{xml_escape(obj['Key'])}</Key>")
            if "VersionId" in obj:
                parts.append(f"<VersionId>{xml_escape(obj['VersionId'])}</VersionId>")
            parts.append("</Object>")
        parts.append("</Delete>")
        return "".join(parts).encode("utf-8")

    def _parse_delete_objects_response(
        self, response: httpx.Response, Bucket: str
    ) -> dict[str, Any]:
        """Parse multi-object delete response, raising on errors."""
        if response.status_code == 404:
            raise NoSuchBucketError(
                f"Bucket '{Bucket}' does not exist or is not accessible"
            )
        elif response.status_code == 403:
            raise PresignError(
                f"Access denied to bucket '{Bucket}'. Check credentials and permissions."
            )
        elif response.status_code == 400:
            raise PresignError(
                f"Bad request for delete_objects on bucket '{Bucket}': {response.text}"
            )
        elif response.status_code != 200:
            raise PresignError(
                f"POST request failed with status {response.status_code}: {response.text}"
            )

        deleted: list[dict[str, Any]] = []
        errors: list[dict[str, Any]] = []

        root = ET.fromstring(response.text)  # noqa: S314
        # Handle namespace in the response XML
        ns = ""
        if root.tag.startswith("{"):
            ns = root.tag.split("}")[0] + "}"

        for elem in root.findall(f"{ns}Deleted"):
            item: dict[str, Any] = {}
            key = elem.find(f"{ns}Key")
            if key is not None and key.text:
                item["Key"] = key.text
            version_id = elem.find(f"{ns}VersionId")
            if version_id is not None and version_id.text:
                item["VersionId"] = version_id.text
            delete_marker = elem.find(f"{ns}DeleteMarker")
            if delete_marker is not None and delete_marker.text:
                item["DeleteMarker"] = delete_marker.text.lower() == "true"
            delete_marker_vid = elem.find(f"{ns}DeleteMarkerVersionId")
            if delete_marker_vid is not None and delete_marker_vid.text:
                item["DeleteMarkerVersionId"] = delete_marker_vid.text
            deleted.append(item)

        for elem in root.findall(f"{ns}Error"):
            item = {}
            key = elem.find(f"{ns}Key")
            if key is not None and key.text:
                item["Key"] = key.text
            version_id = elem.find(f"{ns}VersionId")
            if version_id is not None and version_id.text:
                item["VersionId"] = version_id.text
            code = elem.find(f"{ns}Code")
            if code is not None and code.text:
                item["Code"] = code.text
            message = elem.find(f"{ns}Message")
            if message is not None and message.text:
                item["Message"] = message.text
            errors.append(item)

        return {
            "Deleted": deleted,
            "Errors": errors,
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }
