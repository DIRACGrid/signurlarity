from __future__ import annotations

import base64
import hashlib
import random
import tempfile

import httpx
import pytest
import botocore
import boto3
from signurlarity import Client


BUCKET_NAME = "test-bucket"
OTHER_BUCKET_NAME = "other-bucket"
MISSING_BUCKET_NAME = "missing_bucket"
INVALID_BUCKET_NAME = ".."

CHECKSUM_ALGORITHM = "sha256"

AWS_ACCESS_KEY_ID = "testing"
AWS_SECRET_ACCESS_KEY = "testing"


rng = random.Random(1234)


def _random_file(size_bytes: int):
    file_content = rng.randbytes(size_bytes)
    checksum = hashlib.sha256(file_content).hexdigest()
    return file_content, checksum


def b16_to_b64(hex_string: str) -> str:
    """Convert hexadecimal encoded data to base64 encoded data."""
    return base64.b64encode(base64.b16decode(hex_string.upper())).decode()


@pytest.fixture(scope="session")
def moto_server(worker_id):
    """Start the moto server in a separate thread and return the base URL.

    The mocking provided by moto doesn't play nicely with aiobotocore so we use
    the server directly. See https://github.com/aio-libs/aiobotocore/issues/755
    """
    from moto.server import ThreadedMotoServer

    port = 27132
    if worker_id != "master":
        port += int(worker_id.replace("gw", "")) + 1
    server = ThreadedMotoServer(port=port)
    server.start()
    yield {
        "endpoint_url": f"http://localhost:{port}",
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
    }
    server.stop()


@pytest.fixture(scope="function")
def s3_clients(moto_server):
    """Very basic moto-based S3 backend.

    This is a fixture that can be used to test S3 interactions using moto.
    Note that this is not a complete S3 backend, in particular authentication
    and validation of requests is not implemented.
    """
    boto_client = boto3.client("s3", **moto_server)
    light_client = Client(**moto_server)
    boto_client.create_bucket(Bucket=BUCKET_NAME)
    yield boto_client, light_client


def test_create_bucket(s3_clients):
    boto_client, light_client = s3_clients
    with pytest.raises(botocore.exceptions.ClientError):
        boto_client.head_bucket(Bucket=OTHER_BUCKET_NAME)
    light_client.create_bucket(Bucket=OTHER_BUCKET_NAME)
    boto_client.head_bucket(Bucket=OTHER_BUCKET_NAME)


def test_s3_bucket_exists(s3_clients):
    _boto_client, light_client = s3_clients
    # That should exist
    light_client.head_bucket(Bucket=BUCKET_NAME)
    # That should not exist
    light_client.head_bucket(Bucket=MISSING_BUCKET_NAME)


def test_generate_presigned_post(s3_clients):
    """
    Get a pre-signed URL with our client, upload with httpx
    check it exists with boto
    """
    boto_client, light_client = s3_clients

    file_content, checksum = _random_file(128)
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

    assert r.status_code == 204, r.text
    boto_client.head_object(Bucket=BUCKET_NAME, Key=key)


def test_generate_presigned_url(s3_clients):
    """
    Get a pre-signed URL with our client, upload with httpx
    check it exists with boto
    """
    boto_client, light_client = s3_clients

    file_content, checksum = _random_file(128)
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
