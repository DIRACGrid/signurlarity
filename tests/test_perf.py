from __future__ import annotations

import time
import random
import pytest
import boto3

from signurlarity import Client


AWS_ACCESS_KEY_ID = "testing"
AWS_SECRET_ACCESS_KEY = "testing"


@pytest.fixture(scope="session")
def moto_server(worker_id):
    """Start the moto server in a separate thread and return the base URL.

    Using a real endpoint URL keeps URLs comparable to functional tests.
    """
    from moto.server import ThreadedMotoServer

    port = 27133
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


def _timeit(fn, iterations: int) -> float:
    start = time.perf_counter()
    fn(iterations)
    return time.perf_counter() - start


def test_generate_presigned_post_perf(moto_server):
    """Compare performance of boto3 vs signurlarity for presigned POST.

    This is a non-failing, informational test: it prints timings and skips
    if the signurlarity implementation is not available.
    """
    rng = random.Random(42)
    bucket = "perf-bucket"
    key = "object.txt"

    boto_client = boto3.client("s3", **moto_server)
    light_client = Client(**moto_server)

    # Bucket creation ensures produced URLs are fully valid for the endpoint
    # but is not part of the benchmark itself.
    # boto_client.create_bucket(Bucket=bucket)

    # Minimal fields/conditions for a fair apples-to-apples comparison
    fields = None
    conditions = None

    iterations = 2000

    # Warm-up to mitigate one-time costs (imports, JIT-like caches, etc.)
    for _ in range(50):
        boto_client.generate_presigned_post(
            Bucket=bucket, Key=key, Fields=fields, Conditions=conditions, ExpiresIn=60
        )
    try:
        for _ in range(50):
            light_client.generate_presigned_post(
                Bucket=bucket,
                Key=key,
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=60,
            )
    except NotImplementedError:
        pytest.skip(
            "signurlarity.Client.generate_presigned_post not implemented; skipping perf comparison"
        )

    def run_boto(n: int):
        for _ in range(n):
            # Vary key slightly to avoid any internal memoization across loops
            boto_client.generate_presigned_post(
                Bucket=bucket,
                Key=f"{key}-{rng.randint(0, 1_000_000)}",
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=60,
            )

    def run_light(n: int):
        for _ in range(n):
            light_client.generate_presigned_post(
                Bucket=bucket,
                Key=f"{key}-{rng.randint(0, 1_000_000)}",
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=60,
            )

    t_boto = _timeit(run_boto, iterations)
    t_light = _timeit(run_light, iterations)

    # Informational output; keep concise for CI logs
    print(
        f"boto3 generate_presigned_post: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"signurlarity generate_presigned_post: {t_light:.4f}s for {iterations} ops ({iterations / t_light:.0f} ops/s)"
    )
    if t_light > 0:
        print(f"relative speed (signurlarity vs boto3): {t_boto / t_light:.2f}x")
