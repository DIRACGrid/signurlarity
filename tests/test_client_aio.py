from __future__ import annotations

import logging
import tempfile

import httpx
import pytest
from botocore.errorfactory import ClientError

from signurlarity.exceptions import NoSuchBucketError

from .conftest import (
    BUCKET_NAME as BASE_BUCKET_NAME,
)
from .conftest import (
    CHECKSUM_ALGORITHM,
    b16_to_b64,
    random_file,
)
from .conftest import (
    MISSING_BUCKET_NAME as BASE_MISSING_BUCKET_NAME,
)
from .conftest import (
    OTHER_BUCKET_NAME as BASE_OTHER_BUCKET_NAME,
)

logging.basicConfig(
    format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


# Use different bucket names for async tests to avoid conflicts
BUCKET_NAME = f"{BASE_BUCKET_NAME}-aio"
OTHER_BUCKET_NAME = f"{BASE_OTHER_BUCKET_NAME}-aio"
MISSING_BUCKET_NAME = f"{BASE_MISSING_BUCKET_NAME}-aio"
INVALID_BUCKET_NAME = ".."


@pytest.mark.asyncio
async def test_create_bucket_aio(s3_clients_aio):
    boto_client, async_light_client = s3_clients_aio
    with pytest.raises(ClientError):
        await boto_client.head_bucket(Bucket=OTHER_BUCKET_NAME)
    await async_light_client.create_bucket(Bucket=OTHER_BUCKET_NAME)
    await boto_client.head_bucket(Bucket=OTHER_BUCKET_NAME)


@pytest.mark.asyncio
async def test_head_bucket_exists_aio(s3_clients_aio):
    """Test that head_bucket succeeds for an existing bucket."""
    _boto_client, async_light_client = s3_clients_aio
    response = await async_light_client.head_bucket(Bucket=BUCKET_NAME)

    # Verify response structure
    assert "BucketRegion" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


@pytest.mark.asyncio
async def test_head_bucket_not_found_aio(s3_clients_aio):
    """Test that head_bucket raises NoSuchBucketError for non-existent bucket."""
    _boto_client, async_light_client = s3_clients_aio
    with pytest.raises(NoSuchBucketError):
        await async_light_client.head_bucket(Bucket=MISSING_BUCKET_NAME)


@pytest.mark.asyncio
async def test_head_object_exists_aio(s3_clients_aio):
    """Test that head_object succeeds for an existing object."""
    boto_client, async_light_client = s3_clients_aio

    # First, create an object
    file_content = b"test content for head_object"
    key = "test_file.txt"
    await boto_client.put_object(
        Body=file_content, Bucket=BUCKET_NAME, Key=key, ContentType="text/plain"
    )

    # Now test head_object
    response = await async_light_client.head_object(Bucket=BUCKET_NAME, Key=key)

    # Verify response structure
    assert "ContentLength" in response
    assert "ETag" in response
    assert "LastModified" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response["ContentLength"] == len(file_content)
    assert response["ContentType"] == "text/plain"


@pytest.mark.asyncio
async def test_head_object_not_found_aio(s3_clients_aio):
    """Test that head_object raises PresignError for non-existent object."""
    _boto_client, async_light_client = s3_clients_aio
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        await async_light_client.head_object(
            Bucket=BUCKET_NAME, Key="nonexistent_key.txt"
        )


@pytest.mark.asyncio
async def test_head_object_missing_key_param_aio(s3_clients_aio):
    """Test that head_object raises PresignError when Key parameter is missing."""
    _boto_client, async_light_client = s3_clients_aio
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        await async_light_client.head_object(Bucket=BUCKET_NAME, Key="")


@pytest.mark.asyncio
async def test_head_object_missing_bucket_param_aio(s3_clients_aio):
    """Test that head_object raises PresignError when Bucket parameter is missing."""
    _boto_client, async_light_client = s3_clients_aio
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        await async_light_client.head_object(Bucket="", Key="some_key.txt")


@pytest.mark.asyncio
async def test_generate_presigned_post_aio(s3_clients_aio):
    """Upload files using post presigned URLs with async client.

    Get a pre-signed URL with our async client, upload with httpx
    check it exists with boto.
    """
    boto_client, async_light_client = s3_clients_aio

    file_content, checksum = random_file(128)
    key = f"{checksum}.dat"
    size = len(file_content)

    fields = {
        "x-amz-checksum-algorithm": CHECKSUM_ALGORITHM,
        f"x-amz-checksum-{CHECKSUM_ALGORITHM}": b16_to_b64(checksum),
    }
    conditions = [["content-length-range", size, size]] + [
        {k: v} for k, v in fields.items()
    ]

    upload_info = await async_light_client.generate_presigned_post(
        Bucket=BUCKET_NAME,
        Key=key,
        Fields=fields,
        Conditions=conditions,
        ExpiresIn=60,
    )

    with httpx.Client() as client:
        r = client.post(
            upload_info["url"],
            data=upload_info["fields"],
            files={"file": file_content},
        )

    assert r.status_code in (200, 204), r.text
    await boto_client.head_object(Bucket=BUCKET_NAME, Key=key)


@pytest.mark.asyncio
async def test_generate_presigned_url_aio(s3_clients_aio, caplog):
    """Get a pre-signed URL with our async client, upload with httpx, check it exists with boto."""
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")

    boto_client, async_light_client = s3_clients_aio

    file_content, checksum = random_file(128)
    key = f"{checksum}.dat"

    response = await boto_client.put_object(
        Body=file_content, Bucket=BUCKET_NAME, Key=key, Metadata={"Checksum": checksum}
    )

    presigned_url = await async_light_client.generate_presigned_url(
        ClientMethod="get_object",
        Params={"Bucket": BUCKET_NAME, "Key": key},
        ExpiresIn=3600,
    )

    with tempfile.TemporaryFile(mode="w+b") as fh:
        with httpx.Client() as http_client:
            response = http_client.get(presigned_url)
            response.raise_for_status()
            for chunk in response.iter_bytes():
                fh.write(chunk)


@pytest.mark.asyncio
async def test_delete_objects_aio(s3_clients_aio):
    """Delete multiple objects using async delete_objects and verify they are removed."""
    boto_client, async_light_client = s3_clients_aio

    # Create test objects
    keys = ["delete_test_1.txt", "delete_test_2.txt", "delete_test_3.txt"]
    for key in keys:
        await boto_client.put_object(Body=b"test content", Bucket=BUCKET_NAME, Key=key)

    # Verify objects exist
    for key in keys:
        await boto_client.head_object(Bucket=BUCKET_NAME, Key=key)

    # Delete using our async client
    response = await async_light_client.delete_objects(
        Bucket=BUCKET_NAME,
        Delete={"Objects": [{"Key": k} for k in keys]},
    )

    # Verify response structure
    assert "Deleted" in response
    assert "Errors" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    # Verify all objects were reported as deleted
    deleted_keys = {d["Key"] for d in response["Deleted"]}
    assert deleted_keys == set(keys)
    assert response["Errors"] == []

    # Verify objects are actually gone
    for key in keys:
        with pytest.raises(ClientError):
            await boto_client.head_object(Bucket=BUCKET_NAME, Key=key)


@pytest.mark.asyncio
async def test_delete_objects_quiet_aio(s3_clients_aio):
    """Delete objects with Quiet=True using async client."""
    boto_client, async_light_client = s3_clients_aio

    key = "delete_quiet_test.txt"
    await boto_client.put_object(Body=b"test content", Bucket=BUCKET_NAME, Key=key)

    response = await async_light_client.delete_objects(
        Bucket=BUCKET_NAME,
        Delete={"Objects": [{"Key": key}], "Quiet": True},
    )

    assert "Deleted" in response
    assert "Errors" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response["Errors"] == []

    # Verify object is gone
    with pytest.raises(ClientError):
        await boto_client.head_object(Bucket=BUCKET_NAME, Key=key)


@pytest.mark.asyncio
async def test_delete_objects_missing_params_aio(s3_clients_aio):
    """Test that async delete_objects raises PresignError for missing parameters."""
    _boto_client, async_light_client = s3_clients_aio
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        await async_light_client.delete_objects(
            Bucket="", Delete={"Objects": [{"Key": "k"}]}
        )

    with pytest.raises(PresignError):
        await async_light_client.delete_objects(Bucket=BUCKET_NAME, Delete={})

    with pytest.raises(PresignError):
        await async_light_client.delete_objects(
            Bucket=BUCKET_NAME, Delete={"Objects": []}
        )
