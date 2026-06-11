"""Unit tests for request-path encoding (no live backend / Docker required).

These lock the SigV4 canonical-URI encoding so a regression is caught in CI even
when no signature-validating backend (minio/rustfs) is available. The signed
canonical URI and the wire URL are both built from the single ``path`` returned
by ``_build_request_url``; if that path is not percent-encoded the same way a
backend re-encodes it, head_object fails with SignatureDoesNotMatch.
"""

from __future__ import annotations

import pytest

from signurlarity import Client

from .conftest import BUCKET_NAME

ENDPOINT_URL = "http://localhost:9999"


@pytest.fixture()
def client():
    """Return a Client pointed at a non-existent endpoint (no request is made)."""
    c = Client(
        endpoint_url=ENDPOINT_URL,
        aws_access_key_id="x",
        aws_secret_access_key="y",  # noqa: S106
    )
    yield c
    c.close()


def test_build_request_url_encodes_special_chars(client):
    """Percent-encode chars that require it, keeping bucket and slashes intact."""
    key = "p/sha256:abc def+ghi"
    _base_url, path, _headers = client._build_request_url(BUCKET_NAME, key)

    # Bucket and the path separators are preserved.
    assert path == f"/{BUCKET_NAME}/p/sha256%3Aabc%20def%2Bghi"

    # No raw special characters survive in the path.
    assert ":" not in path
    assert " " not in path
    assert "+" not in path


def test_build_request_url_no_key_is_unchanged(client):
    """Bucket-only requests need no encoding."""
    _base_url, path, _headers = client._build_request_url(BUCKET_NAME)
    assert path == f"/{BUCKET_NAME}"


def test_build_request_url_simple_key_unchanged(client):
    """Keys with only unreserved chars are unaffected (encoded == raw)."""
    _base_url, path, _headers = client._build_request_url(BUCKET_NAME, "a/b/file.txt")
    assert path == f"/{BUCKET_NAME}/a/b/file.txt"


def test_header_and_presigned_paths_encode_identically(client):
    """The two signing paths must encode keys the same way so they cannot drift.

    ``generate_presigned_url`` (download path) and ``_build_request_url``
    (header-signed path) both feed the canonical URI, so the encoded key segment
    must appear identically in each.
    """
    key = "p/sha256:abc def+ghi"
    encoded_key = "p/sha256%3Aabc%20def%2Bghi"

    _base_url, path, _headers = client._build_request_url(BUCKET_NAME, key)
    presigned_url = client._presigner.generate_presigned_url(
        bucket=BUCKET_NAME, key=key
    )

    assert encoded_key in path
    assert encoded_key in presigned_url
