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
import urllib.error
import urllib.request
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

# Server backend fixtures the shared client fixtures fan out across.
S3_BACKENDS = ("minio_server", "moto_server", "rustfs_server", "seaweedfs_server")

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
        "rustfs/rustfs:1.0.0-alpha.82",
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


def _wait_for_seaweedfs_writable(
    master: str = "localhost:9333", timeout: float = 120, poll_interval: float = 1.0
) -> None:
    """Block until the SeaweedFS master can assign a writable volume.

    SeaweedFS creates volumes lazily, so the S3 gateway rejects the first writes
    until a volume has been grown. A successful ``/dir/assign`` (one that returns
    a ``fid``) both triggers and confirms volume readiness -- the same operation
    ``weed upload`` performs internally, but as a single cheap HTTP call with no
    dependency on the ``weed`` CLI or its output format.
    """
    url = f"http://{master}/dir/assign"
    deadline = time.monotonic() + timeout
    last_status = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(url, timeout=5) as response:  # noqa: S310
                payload = json.load(response)
        except urllib.error.HTTPError as exc:
            # Assignment errors (e.g. "No free volumes left!") arrive as non-2xx.
            try:
                payload = json.load(exc)
            except ValueError:
                payload = {"error": f"HTTP {exc.code}"}
        except (urllib.error.URLError, OSError) as exc:
            # Master not accepting connections yet.
            last_status = exc
            time.sleep(poll_interval)
            continue

        if payload.get("fid"):
            return
        last_status = payload.get("error", payload)
        time.sleep(poll_interval)

    raise RuntimeError(
        f"SeaweedFS master at {master} did not provide a writable volume "
        f"within {timeout:.0f}s (last response: {last_status})"
    )


@pytest.fixture(scope="module")
def seaweedfs_server():
    """Run a SeaweedFS server with S3 API enabled.

    Because it creates volumes on the fly, we wait until the master can assign a
    writable volume before yielding, otherwise the first writes fail.
    """
    AWS_ACCESS_KEY_ID = "admin"
    AWS_SECRET_ACCESS_KEY = "key"  # noqa: S105

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
            # explicitely disable deleting non empty bucket
            "-s3.allowDeleteBucketNotEmpty=false",
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

                _wait_for_seaweedfs_writable()

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
    params=list(S3_BACKENDS),
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
    params=list(S3_BACKENDS),
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


def _active_backend(request):
    """Return the backend server fixture name for the current parametrization.

    ``None`` if the test is not fanned out across the S3 backends.
    """
    callspec = getattr(request.node, "callspec", None)
    if callspec is None:
        return None
    for value in callspec.params.values():
        if value in S3_BACKENDS:
            return value
    return None


@pytest.fixture(autouse=True)
def _apply_backend_constraints(request):
    """Apply per-backend skip/xfail declared via markers.

    The shared ``s3_clients`` / ``s3_clients_aio`` fixtures fan every test out
    across all server backends. A few behaviours differ per backend, so tests
    declare the difference with a marker instead of re-parametrizing the fixture
    (which pytest forbids when the fixture already defines ``params``):

      * ``@pytest.mark.backend_only("moto_server")`` -- skip every other backend.
      * ``@pytest.mark.xfail_backend("seaweedfs_server", reason=...)`` -- strict
        xfail on the named backend(s).

    Being autouse, this runs before ``s3_clients`` during setup, so a skip avoids
    starting an unused server and an xfail marker lands before the call phase
    evaluates it. Works for sync and async tests alike.
    """
    backend = _active_backend(request)
    if backend is None:
        return

    only = request.node.get_closest_marker("backend_only")
    if only and backend not in only.args:
        pytest.skip(f"only runs against: {', '.join(only.args)}")

    for marker in request.node.iter_markers("xfail_backend"):
        if backend in marker.args:
            request.node.add_marker(
                pytest.mark.xfail(reason=marker.kwargs.get("reason"), strict=True)
            )
