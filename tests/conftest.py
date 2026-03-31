"""Shared test fixtures for synchronous and asynchronous tests."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import random
import signal
import subprocess
import time
from tempfile import TemporaryDirectory

import boto3
import botocore
import pytest
from aiobotocore.session import get_session
from botocore.client import Config

from signurlarity import Client
from signurlarity.aio import AsyncClient

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


@pytest.fixture(scope="module")
def seaweedfs_server():
    """Run a SeaweedFS server with S3 API enabled.

    Because it creates volumes on the fly, we have to upload a file
    and wait for the initialization to be over, otherwise all the tests
    fail.
    """
    AWS_ACCESS_KEY_ID = "admin"
    AWS_SECRET_ACCESS_KEY = "key"  # noqa: S105

    def check_volume_status(max_retries=10, retry_delay=5):
        cmd = ["weed", "shell"]
        # Use echo to send the command to weed shell
        input_cmd = "cluster.status\n"

        for attempt in range(1, max_retries + 1):
            try:
                process = subprocess.Popen(  # noqa: S603
                    cmd,
                    stdin=subprocess.PIPE,
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE,
                    text=True,
                )
                stdout, _stderr = process.communicate(input=input_cmd, timeout=15)

                # Check if "7 volume" is in the output
                if "7 volume" in stdout:
                    print("Found '7 volume' in output!")
                    return

                print(
                    f"'7 volume' not found (attempt {attempt}/{max_retries}), "
                    f"retrying in {retry_delay} seconds..."
                )
            except subprocess.TimeoutExpired:
                process.kill()
                stdout, _stderr = process.communicate()
                print(
                    f"weed shell timed out (attempt {attempt}/{max_retries}), "
                    f"retrying in {retry_delay} seconds..."
                )
            except Exception as exc:
                print(
                    f"Error checking volume status (attempt {attempt}/{max_retries}): {exc}"
                )

            if attempt < max_retries:
                time.sleep(retry_delay)

        raise RuntimeError(
            f"SeaweedFS did not report '7 volume' after {max_retries} attempts"
        )

    with TemporaryDirectory() as tmp_dir:
        os.mkdir(f"{tmp_dir}/seaweedfs")
        with open(f"{tmp_dir}/seaweedfs_s3.json", "wt") as f:
            json.dump(
                {
                    "identities": [
                        {
                            "name": "admin",
                            "credentials": [
                                {
                                    "accessKey": AWS_ACCESS_KEY_ID,
                                    "secretKey": AWS_SECRET_ACCESS_KEY,
                                }
                            ],
                            "actions": ["Admin", "Read", "Write", "List", "Tagging"],
                        }
                    ]
                },
                f,
            )
        cmd = [
            "weed",
            "mini",
            "-dir",
            f"{tmp_dir}/seaweedfs",
            "-s3.config",
            f"{tmp_dir}/seaweedfs_s3.json",
        ]
        with open(f"{tmp_dir}/seaweedfs.log", "w") as log_file:
            pid = None
            try:
                process = subprocess.Popen(  # noqa: S603
                    cmd,
                    stdout=log_file,
                    stderr=subprocess.STDOUT,  # Redirect stderr to stdout
                )

                pid = process.pid
                print(f"Process PID: {pid} Working Directory {tmp_dir}")
                upload_cmd = [
                    "weed",
                    "upload",
                    "-master",
                    "localhost:9333",
                    f"{tmp_dir}/seaweedfs.log",
                ]
                max_retries = 10
                retry_delay = 5

                for attempt in range(1, max_retries + 1):
                    try:
                        subprocess.run(  # noqa: S603
                            upload_cmd, check=True, capture_output=True, text=True
                        )
                        print("Upload successful!")
                        break
                    except subprocess.CalledProcessError as e:
                        if attempt >= max_retries:
                            raise RuntimeError(
                                f"Upload failed after {max_retries} attempts: {e.stderr}"
                            ) from e
                        print(
                            f"Upload failed (attempt {attempt}/{max_retries}), "
                            f"retrying in {retry_delay} seconds... (Error: {e.stderr})"
                        )
                        time.sleep(retry_delay)
                check_volume_status()

                yield {
                    "endpoint_url": "http://localhost:8333",
                    "aws_access_key_id": AWS_ACCESS_KEY_ID,
                    "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
                }
            except RuntimeError as e:
                print(e)
                log_file.flush()
                print("=== SeaweedFS log start ===")
                try:
                    with open(
                        f"{tmp_dir}/seaweedfs.log",
                        "rt",
                        encoding="utf-8",
                        errors="replace",
                    ) as read_log:
                        print(read_log.read())
                except OSError as log_error:
                    print(f"Failed to read SeaweedFS log file: {log_error}")
                print("=== SeaweedFS log end ===")
                raise
            finally:
                if pid:
                    os.kill(pid, signal.SIGKILL)


# Synchronous client fixtures
@pytest.fixture(
    scope="function",
    params=["minio_server", "moto_server", "rustfs_server", "seaweedfs_server"],
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
    scope="function",
    params=["minio_server", "moto_server", "rustfs_server", "seaweedfs_server"],
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
        except Exception:
            await boto_client.create_bucket(Bucket=AIO_BUCKET_NAME)

        yield boto_client, async_light_client
        await async_light_client.close()
