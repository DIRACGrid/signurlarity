from __future__ import annotations

import json
import sys
import os
import time
import random
import pytest
import boto3
from pathlib import Path
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


def test_generate_presigned_url_perf(moto_server, perf_test_dir):
    """Compare performance of boto3 vs custom S3PresignedURLGenerator for presigned URL.

    This benchmark compares boto3's generate_presigned_url with the custom
    implementation that has zero boto3 dependencies.
    """
    py_vers = sys.version_info
    test_dir = perf_test_dir / Path("test_generate_presigned_url_perf")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")
    rng = random.Random(42)
    bucket = "perf-bucket"
    key = "object.txt"

    # Extract region from endpoint_url (moto uses us-east-1 by default)
    region = "us-east-1"

    boto_client = boto3.client("s3", region_name=region, **moto_server)
    light_client = Client(**moto_server)

    # custom_generator = S3PresignedURLGenerator(
    #     access_key=AWS_ACCESS_KEY_ID, secret_key=AWS_SECRET_ACCESS_KEY, region=region
    # )

    iterations = 5000

    # Warm-up to mitigate one-time costs
    for _ in range(50):
        boto_client.generate_presigned_url(
            "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=60
        )

    for _ in range(50):
        light_client.generate_presigned_url(
            bucket=bucket, key=key, method="GET", expires=60
        )

    def run_boto(n: int):
        for _ in range(n):
            # Vary key slightly to avoid any internal memoization
            boto_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": bucket, "Key": f"{key}-{rng.randint(0, 1_000_000)}"},
                ExpiresIn=60,
            )

    def run_custom(n: int):
        for _ in range(n):
            light_client.generate_presigned_url(
                bucket=bucket,
                key=f"{key}-{rng.randint(0, 1_000_000)}",
                method="GET",
                expires=60,
            )

    t_boto = _timeit(run_boto, iterations)
    t_custom = _timeit(run_custom, iterations)

    results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": "generate_presigned_url",
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
        "boto_ops": iterations / t_boto,
        "signurlarity_ops": iterations / t_custom,
        "speedup": t_boto / t_custom,
    }

    # Informational output
    print("\n" + "=" * 60)
    print("PRESIGNED URL (GET) BENCHMARK")
    print("=" * 60)
    print(
        f"boto3 generate_presigned_url: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"custom S3PresignedURLGenerator: {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
    )
    if t_custom > 0:
        speedup = t_boto / t_custom
        print(f"relative speed (custom vs boto3): {speedup:.2f}x")
        if speedup > 1:
            print(f"✓ Custom implementation is {speedup:.2f}x FASTER!")
        else:
            print(f"boto3 is {1 / speedup:.2f}x faster")

    result_file.write_text(json.dumps(results, indent=2))

    print("=" * 60)
