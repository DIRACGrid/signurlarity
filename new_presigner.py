# new_presigner.py - NO boto3 import!
import hashlib
import hmac
from datetime import datetime


class S3PresignedURLGenerator:
    """Minimal presigned URL generator - NO boto3 dependency."""

    def __init__(self, access_key: str, secret_key: str, region: str = "us-east-1"):
        self.access_key = access_key
        self.secret_key = secret_key
        self.region = region

    def generate_presigned_url(
        self,
        bucket: str,
        key: str,
        method: str = "GET",
        expires: int = 3600,
        timestamp=None,
    ) -> str:
        """Generate presigned URL using only stdlib."""
        now = timestamp or datetime.utcnow()
        amz_date = now.strftime("%Y%m%dT%H%M%SZ")
        date_stamp = now.strftime("%Y%m%d")
        credential_scope = f"{date_stamp}/{self.region}/s3/aws4_request"

        # Build canonical request
        canonical_uri = f"/{key}"
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

        if self.region == "us-east-1":
            host = f"{bucket}.s3.amazonaws.com"
        else:
            host = f"{bucket}.s3.{self.region}.amazonaws.com"
        canonical_headers = f"host:{host}\n"

        canonical_request = "\n".join(
            [
                method,
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

        # Build URL
        return (
            f"https://{host}{canonical_uri}?"
            f"{canonical_querystring}&X-Amz-Signature={signature}"
        )

    def _get_signature_key(self, date_stamp: str) -> bytes:
        k_date = self._sign(("AWS4" + self.secret_key).encode("utf-8"), date_stamp)
        k_region = self._sign(k_date, self.region)
        k_service = self._sign(k_region, "s3")
        k_signing = self._sign(k_service, "aws4_request")
        return k_signing

    def _sign(self, key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    def _uri_encode(self, s: str) -> str:
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


if __name__ == "__main__":
    from datetime import datetime

    gen = S3PresignedURLGenerator(
        access_key="AKIAIOSFODNN7EXAMPLE",
        secret_key="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
        region="us-east-1",
    )

    # Use fixed timestamp from AWS example
    test_time = datetime.strptime("20130524T000000Z", "%Y%m%dT%H%M%SZ")
    url = gen.generate_presigned_url(
        "examplebucket", "test.txt", expires=86400, timestamp=test_time
    )

    print("Generated URL:")
    print(url)
    print()

    expected_sig = "aeeed9bbccd4d02ee5c0109b86d86835f995330da4c265957d157751f604d404"
    if expected_sig in url:
        print("✓ SUCCESS! Signature matches AWS example")
    else:
        print("✗ FAIL - signature doesn't match")
        print(f"Expected: {expected_sig}")
