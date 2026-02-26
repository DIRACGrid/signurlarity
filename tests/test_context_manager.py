"""Tests for context manager functionality in Client and AsyncClient."""

from __future__ import annotations

import pytest

from signurlarity import Client
from signurlarity.aio import AsyncClient


def test_sync_client_context_manager():
    """Test that synchronous Client works as a context manager."""
    endpoint_url = "http://localhost:27132"
    aws_access_key_id = "testing"
    aws_secret_access_key = "testing"  # noqa: S105

    with Client(
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    ) as client:
        # Client should be usable within context
        assert client.endpoint_url == endpoint_url
        assert client.aws_access_key_id == aws_access_key_id
        assert client.aws_secret_access_key == aws_secret_access_key

        # HTTP client should be initialized
        assert hasattr(client, "_http_client")
        assert client._http_client is not None

    # After context exit, HTTP client should be closed
    # We can't directly check if it's closed, but we can verify no exceptions were raised


@pytest.mark.asyncio
async def test_async_client_context_manager():
    """Test that asynchronous AsyncClient works as a context manager."""
    endpoint_url = "http://localhost:28132"
    aws_access_key_id = "testing"
    aws_secret_access_key = "testing"  # noqa: S105

    async with AsyncClient(
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    ) as client:
        # Client should be usable within context
        assert client.endpoint_url == endpoint_url
        assert client.aws_access_key_id == aws_access_key_id
        assert client.aws_secret_access_key == aws_secret_access_key

        # HTTP client should be initialized
        assert hasattr(client, "_http_client")
        assert client._http_client is not None

    # After context exit, HTTP client should be closed
    # We can't directly check if it's closed, but we can verify no exceptions were raised


def test_sync_client_direct_usage():
    """Test that synchronous Client works without context manager."""
    endpoint_url = "http://localhost:27132"
    aws_access_key_id = "testing"
    aws_secret_access_key = "testing"  # noqa: S105

    # Create client without context manager
    client = Client(
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    # Client should be usable
    assert client.endpoint_url == endpoint_url
    assert client.aws_access_key_id == aws_access_key_id
    assert client.aws_secret_access_key == aws_secret_access_key

    # HTTP client should be initialized
    assert hasattr(client, "_http_client")
    assert client._http_client is not None

    # Clean up explicitly
    client._http_client.close()


@pytest.mark.asyncio
async def test_async_client_direct_usage():
    """Test that asynchronous AsyncClient works without context manager."""
    endpoint_url = "http://localhost:28132"
    aws_access_key_id = "testing"
    aws_secret_access_key = "testing"  # noqa: S105

    # Create client without context manager
    client = AsyncClient(
        endpoint_url=endpoint_url,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    # Client should be usable
    assert client.endpoint_url == endpoint_url
    assert client.aws_access_key_id == aws_access_key_id
    assert client.aws_secret_access_key == aws_secret_access_key

    # HTTP client should be initialized
    assert hasattr(client, "_http_client")
    assert client._http_client is not None

    # Clean up explicitly
    await client._http_client.aclose()
