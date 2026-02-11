"""AWS Signature V4 presigned URL generator - pure stdlib implementation."""

from __future__ import annotations

import base64
import hashlib
import hmac
from datetime import datetime, timedelta, timezone
from typing import Any, Optional
from urllib.parse import urlparse

import orjson


class S3Presigner:
    """Generate presigned URLs for S3 operations using AWS Signature Version 4.

    This implementation uses only Python stdlib (no boto3 dependency) for
    significantly faster URL generation.

    Args:
        access_key: AWS access key ID
        secret_key: AWS secret access key
        region: AWS region (default: us-east-1)

    """

    def __init__(
        self,
        access_key: str,
        secret_key: str,
        region: str = "us-east-1",
        endpoint_url: Optional[str] = None,
    ):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.endpoint_url = endpoint_url

        self._scheme = "https"

        self._parsed_host: Optional[str] = None
        if self.endpoint_url:
            parsed = urlparse(self.endpoint_url)
            # For custom endpoints like moto, use hostname:port directly
            if parsed.port:
                self._parsed_host = f"{parsed.hostname}:{parsed.port}"
            else:
                self._parsed_host = parsed.hostname or parsed.netloc
            self._scheme = parsed.scheme or "https"

    def _format_timestamps(
        self, timestamp: Optional[datetime] = None
    ) -> tuple[str, str]:
        """Format timestamp for AWS Signature V4.

        Args:
            timestamp: Optional fixed timestamp (for testing)

        Returns:
            Tuple of (amz_date, date_stamp) formatted strings

        """
        now = timestamp or datetime.now(timezone.utc)
        date_stamp = f"{now.year}{now.month:02d}{now.day:02d}"
        amz_date = f"{date_stamp}T{now.hour:02d}{now.minute:02d}{now.second:02d}Z"

        return amz_date, date_stamp

    def _get_credential_scope(self, date_stamp: str) -> str:
        """Build the credential scope for AWS Signature V4.

        Args:
            date_stamp: Date stamp in YYYYMMDD format

        Returns:
            Credential scope string

        """
        return f"{date_stamp}/{self.region}/s3/aws4_request"

    def generate_presigned_url(
        self,
        bucket: str,
        key: str,
        method: str = "GET",
        expires: int = 3600,
        timestamp: Optional[datetime] = None,
    ) -> str:
        """Generate a presigned URL for an S3 object.

        Args:
            bucket: S3 bucket name
            key: Object key (path in bucket)
            method: HTTP method (GET, PUT, DELETE, etc.)
            expires: URL expiration time in seconds (max 604800 / 7 days)
            timestamp: Optional fixed timestamp (for testing)

        Returns:
            Presigned URL string

        Raises:
            ValueError: If expires is out of valid range

        """
        if expires < 1 or expires > 604800:
            raise ValueError("Expires must be between 1 and 604800 seconds (7 days)")

        amz_date, date_stamp = self._format_timestamps(timestamp)
        credential_scope = self._get_credential_scope(date_stamp)

        # Build canonical request
        # For custom endpoints, use path-style: /bucket/key
        # For AWS endpoints, use virtual-hosted style: /key (bucket is in host)
        if self.endpoint_url:
            canonical_uri = f"/{bucket}/{self._uri_encode_path(key)}"
        else:
            canonical_uri = f"/{self._uri_encode_path(key)}"

        query_params = {
            "X-Amz-Algorithm": "AWS4-HMAC-SHA256",
            "X-Amz-Credential": f"{self.access_key}/{credential_scope}",
            "X-Amz-Date": amz_date,
            "X-Amz-Expires": str(expires),
            "X-Amz-SignedHeaders": "host",
        }

        canonical_querystring = "&".join(
            f"{self._uri_encode(k)}={self._uri_encode(v)}"
            for k, v in sorted(query_params.items())
        )

        host = self._get_host(bucket)
        canonical_headers = f"host:{host}\n"

        canonical_request = "\n".join(
            [
                method.upper(),
                canonical_uri,
                canonical_querystring,
                canonical_headers,
                "host",
                "UNSIGNED-PAYLOAD",
            ]
        )

        # String to sign
        canonical_hash = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        string_to_sign = "\n".join(
            ["AWS4-HMAC-SHA256", amz_date, credential_scope, canonical_hash]
        )

        # Calculate signature
        signing_key = self._get_signature_key(date_stamp)
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        # Build final URL

        return (
            f"{self._scheme}://{host}{canonical_uri}?"
            f"{canonical_querystring}&X-Amz-Signature={signature}"
        )

    def _get_host(self, bucket: str) -> str:
        """Get the S3 host for the given bucket and region."""
        # If custom endpoint is provided, extract hostname from it
        if self._parsed_host:
            return self._parsed_host

        # Standard AWS S3 endpoints
        if self.region == "us-east-1":
            return f"{bucket}.s3.amazonaws.com"
        return f"{bucket}.s3.{self.region}.amazonaws.com"

    def _get_signature_key(self, date_stamp: str) -> bytes:
        """Derive the signing key for AWS Signature Version 4."""
        k_date = self._sign(("AWS4" + self.secret_key).encode("utf-8"), date_stamp)
        k_region = self._sign(k_date, self.region)
        k_service = self._sign(k_region, "s3")
        k_signing = self._sign(k_service, "aws4_request")
        return k_signing

    def _sign(self, key: bytes, msg: str) -> bytes:
        """Sign a message with a key using HMAC-SHA256."""
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def _uri_encode(self, s: str) -> str:
        """URI encode a string following AWS requirements."""
        # AWS requires specific encoding: encode everything except unreserved chars
        result = []
        for char in s:
            if (
                char
                in "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"
            ):
                result.append(char)
            else:
                for byte in char.encode("utf-8"):
                    result.append(f"%{byte:02X}")
        return "".join(result)

    def _uri_encode_path(self, path: str) -> str:
        """URI encode a path, preserving forward slashes."""
        # For paths, we need to encode each segment separately
        segments = path.split("/")
        return "/".join(self._uri_encode(segment) for segment in segments)

    def sign_request_headers(
        self,
        method: str,
        path: str,
        headers: dict[str, str],
        timestamp: Optional[datetime] = None,
        body: bytes = b"",
    ) -> dict[str, str]:
        """Generate AWS Signature V4 Authorization header for an HTTP request.

        This method signs actual HTTP requests (not presigned URLs) by adding an
        Authorization header with the AWS Signature V4 signature.

        Args:
            method: HTTP method (GET, HEAD, PUT, DELETE, etc.)
            path: Request path (e.g., "/" for bucket, "/key" for object)
            headers: Request headers dict (must include 'host')
            timestamp: Optional fixed timestamp (for testing)
            body: Request body (default: empty bytes)

        Returns:
            Updated headers dict with Authorization and X-Amz-Date headers

        Raises:
            ValueError: If 'host' header is missing

        """
        if "host" not in headers:
            raise ValueError("'host' header is required for request signing")

        amz_date, date_stamp = self._format_timestamps(timestamp)
        credential_scope = self._get_credential_scope(date_stamp)

        # Canonical request components
        canonical_method = method.upper()
        canonical_uri = path
        canonical_querystring = ""  # No query string for HEAD requests

        # Build canonical headers
        canonical_headers_list = []
        signed_headers_list = []

        # Always include host
        host = headers["host"]
        canonical_headers_list.append(f"host:{host}")
        signed_headers_list.append("host")

        # Calculate and include x-amz-content-sha256 header
        payload_hash = hashlib.sha256(body).hexdigest()
        canonical_headers_list.append(f"x-amz-content-sha256:{payload_hash}")
        signed_headers_list.append("x-amz-content-sha256")

        # Add any other headers that should be signed (optional)
        canonical_headers = "\n".join(canonical_headers_list) + "\n"
        signed_headers = ";".join(signed_headers_list)

        canonical_request = "\n".join(
            [
                canonical_method,
                canonical_uri,
                canonical_querystring,
                canonical_headers,
                signed_headers,
                payload_hash,
            ]
        )

        # String to sign
        canonical_hash = hashlib.sha256(canonical_request.encode("utf-8")).hexdigest()
        string_to_sign = (
            f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{canonical_hash}"
        )

        # Calculate signature
        signing_key = self._get_signature_key(date_stamp)
        signature = hmac.new(
            signing_key, string_to_sign.encode("utf-8"), hashlib.sha256
        ).hexdigest()

        # Build Authorization header
        authorization_header = (
            f"AWS4-HMAC-SHA256 "
            f"Credential={self.access_key}/{credential_scope}, "
            f"SignedHeaders={signed_headers}, "
            f"Signature={signature}"
        )

        # Return updated headers
        result_headers = headers.copy()
        result_headers["Authorization"] = authorization_header
        result_headers["X-Amz-Date"] = amz_date
        result_headers["X-Amz-Content-Sha256"] = payload_hash

        return result_headers

    def generate_presigned_post(
        self,
        bucket: str,
        key: str,
        fields: dict[str, Any] | None = None,
        conditions: list[Any] | None = None,
        expires: int = 3600,
        timestamp: Optional[datetime] = None,
    ) -> dict[str, Any]:
        """Generate a presigned POST policy for S3 uploads.

        Args:
            bucket: S3 bucket name
            key: Object key (path in bucket)
            fields: Additional form fields to include (e.g., metadata, ACL)
            conditions: Policy conditions for the upload
            expires: Policy expiration time in seconds (max 604800 / 7 days)
            timestamp: Optional fixed timestamp (for testing)

        Returns:
            Dictionary with 'url' and 'fields' keys:
                - url: The S3 bucket URL to POST to
                - fields: Form fields to include in the POST request

        Raises:
            ValueError: If expires is out of valid range

        """
        if expires < 1 or expires > 604800:
            raise ValueError("Expires must be between 1 and 604800 seconds (7 days)")

        now = timestamp or datetime.now(timezone.utc)
        expiration = now + timedelta(seconds=expires)
        amz_date, date_stamp = self._format_timestamps(timestamp)
        credential_scope = self._get_credential_scope(date_stamp)
        credential = f"{self.access_key}/{credential_scope}"

        # Prepare fields
        post_fields = fields.copy() if fields else {}
        post_fields["key"] = key
        post_fields["x-amz-algorithm"] = "AWS4-HMAC-SHA256"
        post_fields["x-amz-credential"] = credential
        post_fields["x-amz-date"] = amz_date

        # Build policy conditions
        policy_conditions = conditions.copy() if conditions else []
        policy_conditions.extend(
            [
                {"bucket": bucket},
                {"key": key},
                {"x-amz-algorithm": "AWS4-HMAC-SHA256"},
                {"x-amz-credential": credential},
                {"x-amz-date": amz_date},
            ]
        )

        # Build policy document
        expiration_date = (
            f"{expiration.year}-{expiration.month:02d}-{expiration.day:02d}"
        )
        expiration_time = f"{expiration.hour:02d}:{expiration.minute:02d}:{expiration.second:02d}.000Z"
        policy_document = {
            "expiration": f"{expiration_date}T{expiration_time}",
            "conditions": policy_conditions,
        }

        # Encode policy
        policy_json = orjson.dumps(policy_document)
        policy_b64 = base64.b64encode(policy_json).decode("utf-8")
        post_fields["policy"] = policy_b64

        # Sign the policy
        signing_key = self._get_signature_key(date_stamp)
        signature = hmac.new(
            signing_key, policy_b64.encode("utf-8"), hashlib.sha256
        ).hexdigest()
        post_fields["x-amz-signature"] = signature

        # Build URL

        host = self._get_host(bucket)

        # For custom endpoints (like moto), use path-style: /bucket
        # For AWS, use virtual-hosted style (bucket is in the host)
        if self.endpoint_url:
            url = f"{self._scheme}://{host}/{bucket}"
        else:
            url = f"{self._scheme}://{host}/"

        return {
            "url": url,
            "fields": post_fields,
        }
