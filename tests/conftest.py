"""Shared test fixtures for synchronous and asynchronous tests."""

from __future__ import annotations

import base64
import hashlib
import random

import boto3
import botocore
import pytest
from aiobotocore.session import get_session
from botocore.client import Config

from signurlarity import Client
from signurlarity.aio import AsyncClient

# logging.basicConfig(
#     format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
#     datefmt="%Y-%m-%d %H:%M:%S",
#     level=logging.DEBUG,
# )


# Constants
BUCKET_NAME = "test-bucket"
OTHER_BUCKET_NAME = "other-bucket"
MISSING_BUCKET_NAME = "missing-bucket"
INVALID_BUCKET_NAME = ".."

CHECKSUM_ALGORITHM = "sha256"

rng = random.Random(1234)  # noqa: S311


# Utility functions
def random_file(size_bytes: int):
    """Generate random file content and its SHA256 checksum."""
    file_content = rng.randbytes(size_bytes)
    checksum = hashlib.sha256(file_content).hexdigest()
    return file_content, checksum


def b16_to_b64(hex_string: str) -> str:
    """Convert hexadecimal encoded data to base64 encoded data."""
    return base64.b64encode(base64.b16decode(hex_string.upper())).decode()


# Server fixtures
@pytest.fixture(scope="module")
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


@pytest.fixture(scope="module")
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


@pytest.fixture(scope="module")
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


# Synchronous client fixtures
@pytest.fixture(
    scope="function", params=["minio_server", "moto_server", "rustfs_server"]
)
def s3_clients(request):
    """S3 clients for synchronous tests with multiple server backends.

    This fixture can be used to test S3 interactions using different
    backends (moto, minio, rustfs). Returns both boto3 and signurlarity clients.
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
    light_client.close()


# Asynchronous client fixtures
@pytest.fixture(
    scope="function", params=["minio_server", "moto_server", "rustfs_server"]
)
async def s3_clients_aio(request):
    """S3 clients for asynchronous tests with multiple server backends.

    This fixture can be used to test async S3 interactions using different
    backends (moto, minio, rustfs). Returns both aiobotocore and signurlarity async clients.
    """
    s3_server_fixture = request.param
    s3_server = request.getfixturevalue(s3_server_fixture)
    AIO_BUCKET_NAME = f"{BUCKET_NAME}-aio"

    session = get_session()
    async with session.create_client(
        "s3",
        endpoint_url=s3_server["endpoint_url"],
        aws_access_key_id=s3_server["aws_access_key_id"],
        aws_secret_access_key=s3_server["aws_secret_access_key"],
        config=Config(signature_version="s3v4"),
    ) as boto_client:
        async_light_client = AsyncClient(**s3_server)

        try:
            await boto_client.head_bucket(Bucket=AIO_BUCKET_NAME)
        except Exception as e:
            print(f"CHRIS {e!r}")
            await boto_client.create_bucket(Bucket=AIO_BUCKET_NAME)

        yield boto_client, async_light_client
        await async_light_client.close()
