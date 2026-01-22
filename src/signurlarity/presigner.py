"""AWS Signature V4 presigned URL generator - pure stdlib implementation."""

import hashlib
import hmac
from datetime import datetime, timezone
from typing import Optional
from urllib.parse import quote


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
        region: str = 'us-east-1',
        endpoint_url: Optional[str] = None
    ):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region
        self.endpoint_url = endpoint_url

    def generate_presigned_url(
        self,
        bucket: str,
        key: str,
        method: str = 'GET',
        expires: int = 3600,
        timestamp: Optional[datetime] = None
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

        now = timestamp or datetime.now(timezone.utc)
        amz_date = now.strftime('%Y%m%dT%H%M%SZ')
        date_stamp = now.strftime('%Y%m%d')
        credential_scope = f'{date_stamp}/{self.region}/s3/aws4_request'

        # Build canonical request
        # For custom endpoints, use path-style: /bucket/key
        # For AWS endpoints, use virtual-hosted style: /key (bucket is in host)
        if self.endpoint_url:
            canonical_uri = f'/{bucket}/{self._uri_encode_path(key)}'
        else:
            canonical_uri = f'/{self._uri_encode_path(key)}'

        query_params = {
            'X-Amz-Algorithm': 'AWS4-HMAC-SHA256',
            'X-Amz-Credential': f'{self.access_key}/{credential_scope}',
            'X-Amz-Date': amz_date,
            'X-Amz-Expires': str(expires),
            'X-Amz-SignedHeaders': 'host'
        }

        canonical_querystring = '&'.join(
            f'{self._uri_encode(k)}={self._uri_encode(v)}'
            for k, v in sorted(query_params.items())
        )

        host = self._get_host(bucket)
        canonical_headers = f'host:{host}\n'

        canonical_request = '\n'.join([
            method.upper(),
            canonical_uri,
            canonical_querystring,
            canonical_headers,
            'host',
            'UNSIGNED-PAYLOAD'
        ])

        # String to sign
        canonical_hash = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
        string_to_sign = '\n'.join([
            'AWS4-HMAC-SHA256',
            amz_date,
            credential_scope,
            canonical_hash
        ])

        # Calculate signature
        signing_key = self._get_signature_key(date_stamp)
        signature = hmac.new(
            signing_key,
            string_to_sign.encode('utf-8'),
            hashlib.sha256
        ).hexdigest()

        # Build final URL
        # Use the scheme from endpoint_url if provided, otherwise default to https
        scheme = 'https'
        if self.endpoint_url:
            from urllib.parse import urlparse
            parsed = urlparse(self.endpoint_url)
            scheme = parsed.scheme or 'https'

        return (
            f'{scheme}://{host}{canonical_uri}?'
            f'{canonical_querystring}&X-Amz-Signature={signature}'
        )

    def _get_host(self, bucket: str) -> str:
        """Get the S3 host for the given bucket and region."""
        # If custom endpoint is provided, extract hostname from it
        if self.endpoint_url:
            from urllib.parse import urlparse
            parsed = urlparse(self.endpoint_url)
            # For custom endpoints like moto, use hostname:port directly
            if parsed.port:
                return f'{parsed.hostname}:{parsed.port}'
            return parsed.hostname or parsed.netloc

        # Standard AWS S3 endpoints
        if self.region == 'us-east-1':
            return f'{bucket}.s3.amazonaws.com'
        return f'{bucket}.s3.{self.region}.amazonaws.com'

    def _get_signature_key(self, date_stamp: str) -> bytes:
        """Derive the signing key for AWS Signature Version 4."""
        k_date = self._sign(('AWS4' + self.secret_key).encode('utf-8'), date_stamp)
        k_region = self._sign(k_date, self.region)
        k_service = self._sign(k_region, 's3')
        k_signing = self._sign(k_service, 'aws4_request')
        return k_signing

    def _sign(self, key: bytes, msg: str) -> bytes:
        """Sign a message with a key using HMAC-SHA256."""
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    def _uri_encode(self, s: str) -> str:
        """URI encode a string following AWS requirements."""
        # AWS requires specific encoding: encode everything except unreserved chars
        result = []
        for char in s:
            if char in 'ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~':
                result.append(char)
            else:
                for byte in char.encode('utf-8'):
                    result.append(f'%{byte:02X}')
        return ''.join(result)

    def _uri_encode_path(self, path: str) -> str:
        """URI encode a path, preserving forward slashes."""
        # For paths, we need to encode each segment separately
        segments = path.split('/')
        return '/'.join(self._uri_encode(segment) for segment in segments)
