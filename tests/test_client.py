from __future__ import annotations

import logging
import tempfile

import botocore
import httpx
import pytest

from signurlarity.exceptions import NoSuchBucketError

from .conftest import (
    BUCKET_NAME,
    CHECKSUM_ALGORITHM,
    MISSING_BUCKET_NAME,
    OTHER_BUCKET_NAME,
    b16_to_b64,
    random_file,
)

logging.basicConfig(
    format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


def test_create_bucket(s3_clients):
    boto_client, light_client = s3_clients
    with pytest.raises(botocore.exceptions.ClientError):
        boto_client.head_bucket(Bucket=OTHER_BUCKET_NAME)
    light_client.create_bucket(Bucket=OTHER_BUCKET_NAME)
    boto_client.head_bucket(Bucket=OTHER_BUCKET_NAME)


def test_head_bucket_exists(s3_clients):
    """Test that head_bucket succeeds for an existing bucket."""
    _boto_client, light_client = s3_clients
    response = light_client.head_bucket(Bucket=BUCKET_NAME)

    # Verify response structure
    assert "BucketRegion" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200


def test_head_bucket_not_found(s3_clients):
    """Test that head_bucket raises NoSuchBucketError for non-existent bucket."""
    _boto_client, light_client = s3_clients
    with pytest.raises(NoSuchBucketError):
        light_client.head_bucket(Bucket=MISSING_BUCKET_NAME)


def test_head_object_exists(s3_clients):
    """Test that head_object succeeds for an existing object."""
    boto_client, light_client = s3_clients

    # First, create an object
    file_content = b"test content for head_object"
    key = "test_file.txt"
    boto_client.put_object(
        Body=file_content, Bucket=BUCKET_NAME, Key=key, ContentType="text/plain"
    )

    # Now test head_object
    response = light_client.head_object(Bucket=BUCKET_NAME, Key=key)

    # Verify response structure
    assert "ContentLength" in response
    assert "ETag" in response
    assert "LastModified" in response
    assert "ResponseMetadata" in response
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert response["ContentLength"] == len(file_content)
    assert response["ContentType"] == "text/plain"


def test_head_object_not_found(s3_clients):
    """Test that head_object raises PresignError for non-existent object."""
    _boto_client, light_client = s3_clients
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        light_client.head_object(Bucket=BUCKET_NAME, Key="nonexistent_key.txt")


def test_head_object_missing_key_param(s3_clients):
    """Test that head_object raises PresignError when Key parameter is missing."""
    _boto_client, light_client = s3_clients
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        light_client.head_object(Bucket=BUCKET_NAME, Key="")


def test_head_object_missing_bucket_param(s3_clients):
    """Test that head_object raises PresignError when Bucket parameter is missing."""
    _boto_client, light_client = s3_clients
    from signurlarity.exceptions import PresignError

    with pytest.raises(PresignError):
        light_client.head_object(Bucket="", Key="some_key.txt")


# def test_head_bucket_missing_bucket_param(s3_clients):
#     """Test that head_bucket raises error when Bucket parameter is missing."""
#     _boto_client, light_client = s3_clients
#     with pytest.raises(Exception):  # PresignError or similar
#         light_client.head_bucket(Bucket="")


def test_generate_presigned_post(s3_clients):
    """Upload files using post presigned URLs.

    Get a pre-signed URL with our client, upload with httpx
    check it exists with boto.
    """
    boto_client, light_client = s3_clients

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

    upload_info = light_client.generate_presigned_post(
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
    boto_client.head_object(Bucket=BUCKET_NAME, Key=key)


def test_generate_presigned_url(s3_clients, caplog):
    """Get a pre-signed URL with our client, upload with httpx, check it exists with boto."""
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")

    boto_client, light_client = s3_clients

    file_content, checksum = random_file(128)
    key = f"{checksum}.dat"

    response = boto_client.put_object(
        Body=file_content, Bucket=BUCKET_NAME, Key=key, Metadata={"Checksum": checksum}
    )

    presigned_url = light_client.generate_presigned_url(
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


# @pytest.fixture()
# def fix_1():
#     print("entering fix 1")
#     yield "fix 1"
#     print("finishing fix 1")


# @pytest.fixture()
# def fix_2(worker_id):
#     print(f"entering fix 2 {worker_id}")
#     yield "fix 2"
#     print("finishing fix 2")


# @pytest.fixture(params=["fix_1", "fix_2"])
# def fix(request):
#     server_fixture = request.param
#     srv_fixt = request.getfixturevalue(server_fixture)
#     print("entering fix")
#     client_param = server_fixture
#     # breakpoint()
#     print(f"GOT {client_param}")
#     yield client_param

#     print("finishing fix")


# def test_my_fix(fix):
#     print(f"testing {fix}")
