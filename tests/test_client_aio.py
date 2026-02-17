from __future__ import annotations

import base64
import hashlib
import logging
import random
import tempfile

import httpx
import pytest
from aiobotocore.session import get_session
from botocore.client import Config
from botocore.errorfactory import ClientError

from signurlarity.aio import AsyncClient
from signurlarity.exceptions import NoSuchBucketError

logging.basicConfig(
    format="%(levelname)s [%(asctime)s] %(name)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.DEBUG,
)


BUCKET_NAME = "test-bucket-aio"
OTHER_BUCKET_NAME = "other-bucket-aio"
MISSING_BUCKET_NAME = "missing-bucket-aio"
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


@pytest.fixture(scope="module")
def moto_server(worker_id):
    """Start the moto server in a separate thread and return the base URL.

    The mocking provided by moto doesn't play nicely with aiobotocore so we use
    the server directly. See https://github.com/aio-libs/aiobotocore/issues/755
    """
    AWS_ACCESS_KEY_ID = "testing"
    AWS_SECRET_ACCESS_KEY = "testing"  # noqa: S105

    from moto.server import ThreadedMotoServer

    port = 28132
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


@pytest.fixture(
    scope="function", params=["minio_server", "moto_server", "rustfs_server"]
)
async def s3_clients_aio(request):
    """Very basic moto-based S3 backend for async tests.

    This is a fixture that can be used to test async S3 interactions using moto.
    Note that this is not a complete S3 backend, in particular authentication
    and validation of requests is not implemented.
    """
    s3_server_fixture = request.param
    s3_server = request.getfixturevalue(s3_server_fixture)

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
            await boto_client.head_bucket(Bucket=BUCKET_NAME)
        except Exception:
            await boto_client.create_bucket(Bucket=BUCKET_NAME)
        yield boto_client, async_light_client


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

    file_content, checksum = _random_file(128)
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
