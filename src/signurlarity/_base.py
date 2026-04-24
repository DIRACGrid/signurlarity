"""Shared base class for sync and async S3 clients."""

from __future__ import annotations

import base64
import hashlib
from typing import Any, Mapping, Optional
from urllib.parse import urlencode, urlparse
from xml.etree import ElementTree
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

        if "ACL" in kwargs and kwargs["ACL"] is not None:
            headers["x-amz-acl"] = str(kwargs["ACL"])

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

    def _prepare_copy_object(
        self, Bucket: str, Key: str, CopySource: str | dict[str, str], **kwargs
    ) -> tuple[str, dict[str, str]]:
        """Validate and build a signed PUT copy-object request.

        Returns:
            Tuple of (url, signed_headers)

        """
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")
        if not Key:
            raise PresignError("Missing required parameter 'Key'")
        if not CopySource:
            raise PresignError("Missing required parameter 'CopySource'")

        # Normalise CopySource to a "bucket/key" string
        if isinstance(CopySource, dict):
            src_bucket = CopySource.get("Bucket", "")
            src_key = CopySource.get("Key", "")
            if not src_bucket or not src_key:
                raise PresignError(
                    "CopySource dict must contain non-empty 'Bucket' and 'Key'"
                )
            copy_source_str = f"{src_bucket}/{src_key}"
            version_id = CopySource.get("VersionId")
            if version_id:
                copy_source_str = f"{copy_source_str}?versionId={version_id}"
        else:
            copy_source_str = CopySource

        base_url, path, headers = self._build_request_url(Bucket, Key)
        headers["x-amz-copy-source"] = copy_source_str

        if "MetadataDirective" in kwargs:
            headers["x-amz-metadata-directive"] = kwargs["MetadataDirective"]
        if "ContentType" in kwargs:
            headers["Content-Type"] = kwargs["ContentType"]
        if "ACL" in kwargs and kwargs["ACL"] is not None:
            headers["x-amz-acl"] = str(kwargs["ACL"])

        signed_headers = self._presigner.sign_request_headers(
            method="PUT",
            path=path,
            headers=headers,
        )

        url = base_url + path

        return url, signed_headers

    def _parse_copy_object_response(
        self, response: httpx.Response, Bucket: str, Key: str
    ) -> dict[str, Any]:
        """Parse copy-object response, raising on errors."""
        if response.status_code == 404:
            raise PresignError(
                f"Source or destination not found for copy to '{Bucket}/{Key}'"
            )
        elif response.status_code == 403:
            raise PresignError(
                f"Access denied for copy to '{Bucket}/{Key}'. Check credentials and permissions."
            )
        elif response.status_code == 400:
            raise PresignError(f"Bad request for copy_object '{Key}': {response.text}")
        elif response.status_code != 200:
            raise PresignError(
                f"PUT copy request failed with status {response.status_code}: {response.text}"
            )

        result: dict[str, Any] = {
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }

        if response.text:
            root = ElementTree.fromstring(response.text)  # noqa: S314
            ns = ""
            if root.tag.startswith("{"):
                ns = root.tag.split("}")[0] + "}"
            copy_result: dict[str, str] = {}
            etag = root.findtext(f"{ns}ETag")
            if etag:
                copy_result["ETag"] = etag
            last_modified = root.findtext(f"{ns}LastModified")
            if last_modified:
                copy_result["LastModified"] = last_modified
            if copy_result:
                result["CopyObjectResult"] = copy_result

        return result

    def _prepare_list_objects(
        self, Bucket: str, **kwargs
    ) -> tuple[str, dict[str, str]]:
        """Validate and build a signed GET list-objects request.

        Returns:
            Tuple of (url, signed_headers)

        """
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")

        base_url, path, headers = self._build_request_url(Bucket)

        query_params: dict[str, str] = {}
        if "Delimiter" in kwargs:
            query_params["delimiter"] = kwargs["Delimiter"]
        if "EncodingType" in kwargs:
            query_params["encoding-type"] = kwargs["EncodingType"]
        if "Marker" in kwargs:
            query_params["marker"] = kwargs["Marker"]
        if "MaxKeys" in kwargs:
            query_params["max-keys"] = str(kwargs["MaxKeys"])
        if "Prefix" in kwargs:
            query_params["prefix"] = kwargs["Prefix"]

        query_string = urlencode(query_params) if query_params else ""

        signed_headers = self._presigner.sign_request_headers(
            method="GET",
            path=path,
            headers=headers,
            query_string=query_string,
        )

        url = base_url + path
        if query_string:
            url = f"{url}?{query_string}"

        return url, signed_headers

    def _parse_list_objects_response(
        self, response: httpx.Response, Bucket: str
    ) -> dict[str, Any]:
        """Parse list-objects response, raising on errors."""
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
                f"Bad request for list_objects on bucket '{Bucket}': {response.text}"
            )
        elif response.status_code != 200:
            raise PresignError(
                f"GET request failed with status {response.status_code}: {response.text}"
            )

        result: dict[str, Any] = {
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }

        if response.text:
            root = ElementTree.fromstring(response.text)  # noqa: S314
            ns = ""
            if root.tag.startswith("{"):
                ns = root.tag.split("}")[0] + "}"

            result["Name"] = root.findtext(f"{ns}Name", Bucket)
            result["Prefix"] = root.findtext(f"{ns}Prefix", "")
            result["Delimiter"] = root.findtext(f"{ns}Delimiter", "")
            max_keys_text = root.findtext(f"{ns}MaxKeys", "1000")
            result["MaxKeys"] = int(max_keys_text) if max_keys_text else 1000
            result["IsTruncated"] = (
                root.findtext(f"{ns}IsTruncated", "false").lower() == "true"
            )
            next_marker = root.findtext(f"{ns}NextMarker")
            if next_marker:
                result["NextMarker"] = next_marker

            contents = []
            for obj in root.findall(f"{ns}Contents"):
                entry: dict[str, Any] = {
                    "Key": obj.findtext(f"{ns}Key", ""),
                    "ETag": obj.findtext(f"{ns}ETag", ""),
                    "Size": int(obj.findtext(f"{ns}Size", "0") or "0"),
                    "LastModified": obj.findtext(f"{ns}LastModified", ""),
                    "StorageClass": obj.findtext(f"{ns}StorageClass", ""),
                }
                owner_elem = obj.find(f"{ns}Owner")
                if owner_elem is not None:
                    entry["Owner"] = {
                        "DisplayName": owner_elem.findtext(f"{ns}DisplayName", ""),
                        "ID": owner_elem.findtext(f"{ns}ID", ""),
                    }
                contents.append(entry)
            result["Contents"] = contents

            common_prefixes = []
            for cp in root.findall(f"{ns}CommonPrefixes"):
                prefix_text = cp.findtext(f"{ns}Prefix", "")
                common_prefixes.append({"Prefix": prefix_text})
            if common_prefixes:
                result["CommonPrefixes"] = common_prefixes

        return result

    def _prepare_put_object(
        self, Bucket: str, Key: str, **kwargs
    ) -> tuple[str, dict[str, str], bytes]:
        """Validate and build a signed PUT object request.

        Returns:
            Tuple of (url, signed_headers, body)

        """
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")
        if not Key:
            raise PresignError("Missing required parameter 'Key'")

        base_url, path, headers = self._build_request_url(Bucket, Key)

        body = kwargs.get("Body", b"") or b""
        if isinstance(body, str):
            body = body.encode("utf-8")

        if "ContentType" in kwargs:
            headers["Content-Type"] = kwargs["ContentType"]
        if "ContentLength" in kwargs:
            headers["Content-Length"] = str(kwargs["ContentLength"])
        if "ACL" in kwargs and kwargs["ACL"] is not None:
            headers["x-amz-acl"] = str(kwargs["ACL"])
        if "Metadata" in kwargs:
            for meta_key, meta_value in kwargs["Metadata"].items():
                headers[f"x-amz-meta-{meta_key.lower()}"] = meta_value

        signed_headers = self._presigner.sign_request_headers(
            method="PUT",
            path=path,
            headers=headers,
            body=body,
        )

        url = base_url + path

        return url, signed_headers, body

    def _parse_put_object_response(
        self, response: httpx.Response, Bucket: str, Key: str
    ) -> dict[str, Any]:
        """Parse PUT object response, raising on errors."""
        if response.status_code == 404:
            raise PresignError(f"Bucket '{Bucket}' does not exist or is not accessible")
        elif response.status_code == 403:
            raise PresignError(
                f"Access denied to bucket '{Bucket}'. Check credentials and permissions."
            )
        elif response.status_code == 400:
            raise PresignError(f"Bad request for put_object '{Key}': {response.text}")
        elif response.status_code not in (200, 201):
            raise PresignError(
                f"PUT request failed with status {response.status_code}: {response.text}"
            )

        result: dict[str, Any] = {
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }

        if "etag" in response.headers:
            result["ETag"] = response.headers["etag"]

        return result

    def _prepare_delete_objects(
        self, Bucket: str, Delete: dict[str, Any], **kwargs
    ) -> tuple[str, dict[str, str], bytes]:
        """Validate and build a signed POST multi-object delete request.

        Returns:
            Tuple of (url, signed_headers, body)

        """
        if not Bucket:
            raise PresignError("Missing required parameter 'Bucket'")
        if not Delete or "Objects" not in Delete:
            raise PresignError(
                "Missing required parameter 'Delete' with 'Objects' list"
            )
        if not Delete["Objects"]:
            raise PresignError("'Delete.Objects' must contain at least one object")

        base_url, path, headers = self._build_request_url(Bucket)

        # Build XML body
        quiet = Delete.get("Quiet", False)
        xml_parts = ['<?xml version="1.0" encoding="UTF-8"?>', "<Delete>"]
        if quiet:
            xml_parts.append("<Quiet>true</Quiet>")
        for obj in Delete["Objects"]:
            xml_parts.append("<Object>")
            xml_parts.append(f"<Key>{xml_escape(obj['Key'])}</Key>")
            if "VersionId" in obj:
                xml_parts.append(
                    f"<VersionId>{xml_escape(obj['VersionId'])}</VersionId>"
                )
            xml_parts.append("</Object>")
        xml_parts.append("</Delete>")
        body = "".join(xml_parts).encode("utf-8")

        # Content-MD5 is required for delete_objects
        content_md5 = hashlib.md5(body).digest()  # noqa: S324
        headers["Content-MD5"] = base64.b64encode(content_md5).decode()
        headers["Content-Type"] = "application/xml"

        query_string = "delete="
        signed_headers = self._presigner.sign_request_headers(
            method="POST",
            path=path,
            headers=headers,
            body=body,
            query_string=query_string,
        )

        url = f"{base_url}{path}?{query_string}"

        return url, signed_headers, body

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
                f"Bad request to delete objects in bucket '{Bucket}': {response.text}"
            )
        elif response.status_code != 200:
            raise PresignError(
                f"POST delete request failed with status {response.status_code}: {response.text}"
            )

        result: dict[str, Any] = {
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }

        # Parse XML response
        if response.text:
            root = ElementTree.fromstring(response.text)  # noqa: S314
            # Handle namespace in the XML response
            ns = ""
            if root.tag.startswith("{"):
                ns = root.tag.split("}")[0] + "}"

            deleted = []
            for d in root.findall(f"{ns}Deleted"):
                entry: dict[str, str] = {"Key": d.findtext(f"{ns}Key", "")}
                version_id = d.findtext(f"{ns}VersionId")
                if version_id:
                    entry["VersionId"] = version_id
                deleted.append(entry)
            if deleted:
                result["Deleted"] = deleted

            errors = []
            for e in root.findall(f"{ns}Error"):
                entry_err: dict[str, str] = {
                    "Key": e.findtext(f"{ns}Key", ""),
                    "Code": e.findtext(f"{ns}Code", ""),
                    "Message": e.findtext(f"{ns}Message", ""),
                }
                version_id = e.findtext(f"{ns}VersionId")
                if version_id:
                    entry_err["VersionId"] = version_id
                errors.append(entry_err)
            if errors:
                result["Errors"] = errors

        return result

    def _prepare_delete_bucket(
        self, Bucket: str, **kwargs
    ) -> tuple[str, dict[str, str]]:
        """Validate and build a signed DELETE bucket request.

        Args:
            Bucket: S3 bucket name
            **kwargs: Additional arguments (e.g., ExpectedBucketOwner)

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
            method="DELETE",
            path=path,
            headers=headers,
        )

        url = base_url + path
        if query_params:
            url = f"{url}?{self._build_query_string(query_params)}"

        return url, signed_headers

    def _parse_delete_bucket_response(
        self,
        response: httpx.Response,
        Bucket: str,
    ) -> dict[str, Any]:
        """Parse delete bucket response, raising on errors."""
        if response.status_code == 404:
            raise NoSuchBucketError(
                f"Bucket '{Bucket}' does not exist or is not accessible"
            )
        elif response.status_code == 409:
            raise PresignError(f"Bucket '{Bucket}' is not empty.")
        elif response.status_code == 403:
            raise PresignError(
                f"Access denied to bucket '{Bucket}'. Check credentials and permissions."
            )
        elif response.status_code == 400:
            raise PresignError(
                f"Bad request for delete_bucket '{Bucket}': {response.text}"
            )
        elif response.status_code not in (200, 204):
            raise PresignError(
                f"DELETE request failed with status {response.status_code}: {response.text}"
            )

        result: dict[str, Any] = {
            "ResponseMetadata": {
                "HTTPStatusCode": response.status_code,
                "HTTPHeaders": dict(response.headers),
            },
        }

        return result
