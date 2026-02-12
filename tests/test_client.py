from __future__ import annotations

import base64
import hashlib
import logging
import random
import tempfile

import boto3
import botocore
import httpx
import pytest
from botocore.client import Config

from signurlarity import Client
from signurlarity.exceptions import NoSuchBucketError

logging.basicConfig(
    format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


BUCKET_NAME = "test-bucket"
OTHER_BUCKET_NAME = "other-bucket"
MISSING_BUCKET_NAME = "missing-bucket"
INVALID_BUCKET_NAME = ".."

CHECKSUM_ALGORITHM = "sha256"


rng = random.Random(1234)  # noqa: S311


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
    AWS_ACCESS_KEY_ID = "testing"
    AWS_SECRET_ACCESS_KEY = "testing"  # noqa: S105

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


@pytest.fixture(scope="session")
def rustfs_server():
    """Run a rustfs server."""
    AWS_ACCESS_KEY_ID = "rustfsadmin"
    AWS_SECRET_ACCESS_KEY = "rustfsadmin"  # noqa: S105
    import subprocess

    cmd = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        "rustfs_local",
        "-p",
        "9000:9000",
        "-p",
        "9001:9001",
        "rustfs/rustfs:1.0.0-alpha.82",  # return to latest when https://github.com/rustfs/rustfs/issues/1773 is fixed
        "/data",
    ]
    # print(shlex.join(cmd))

    subprocess.run(cmd, check=True)  # noqa: S603
    yield {
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
    }
    cmd = ["docker", "stop", "rustfs_local"]
    subprocess.run(cmd, check=True)  # noqa: S603


@pytest.fixture(scope="session")
def minio_server():
    """Run a minio server."""
    AWS_ACCESS_KEY_ID = "minioadmin"
    AWS_SECRET_ACCESS_KEY = "minioadmin"  # noqa: S105
    import subprocess

    cmd = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--name",
        "minio_local",
        "-p",
        "9100:9000",
        "-p",
        "9101:9001",
        "-e",
        "MINIO_ROOT_USER=minioadmin",
        "-e",
        "MINIO_ROOT_PASSWORD=minioadmin",
        "minio/minio",
        "server",
        "/data",
    ]
    subprocess.run(cmd, check=True)  # noqa: S603
    yield {
        "endpoint_url": "http://localhost:9100",
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
    }
    cmd = ["docker", "stop", "minio_local"]

    subprocess.run(cmd, check=True)  # noqa: S603


# @pytest.fixture(
#     scope="session",
#     params=[
#         rustfs_server,
#     ],
# )
# def s3_server(
#     request,
# ):
#     breakpoint()
#     # getfixturevalue
#     yield from request.param()


@pytest.fixture(
    scope="function", params=["minio_server", "moto_server", "rustfs_server"]
)
def s3_clients(request):
    """Very basic moto-based S3 backend.

    This is a fixture that can be used to test S3 interactions using moto.
    Note that this is not a complete S3 backend, in particular authentication
    and validation of requests is not implemented.
    """
    s3_server_fixture = request.param
    s3_server = request.getfixturevalue(s3_server_fixture)
    boto_client = boto3.client(
        "s3", **s3_server, config=Config(signature_version="s3v4")
    )
    light_client = Client(**s3_server)

    try:
        boto_client.head_bucket(Bucket=BUCKET_NAME)
    except botocore.exceptions.ClientError as exx:
        if exx.response["Error"]["Code"] == "404":
            boto_client.create_bucket(Bucket=BUCKET_NAME)
    yield boto_client, light_client


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

    assert r.status_code in (200, 204), r.text
    boto_client.head_object(Bucket=BUCKET_NAME, Key=key)


def test_generate_presigned_url(s3_clients, caplog):
    """Get a pre-signed URL with our client, upload with httpx, check it exists with boto."""
    caplog.set_level(logging.DEBUG, logger="httpx")
    caplog.set_level(logging.DEBUG, logger="httpcore")

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
