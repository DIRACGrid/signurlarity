from __future__ import annotations

import json
import os
import random
import sys
from pathlib import Path

import boto3
import pytest
from botocore.client import Config

from conftest import _timeit
from signurlarity import Client


def test_generate_presigned_post_perf_sync_cm(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity for presigned POST.

    This is a non-failing, informational test: it prints timings and skips
    if the signurlarity implementation is not available.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_generate_presigned_post_perf_sync_cm")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    rng = random.Random(42)  # noqa: S311
    bucket = "perf-bucket"
    key = "object.txt"

    boto_client = boto3.client("s3", **rustfs_server)
    with Client(**rustfs_server) as light_client:
        # Bucket creation ensures produced URLs are fully valid for the endpoint
        # but is not part of the benchmark itself.
        # boto_client.create_bucket(Bucket=bucket)

        # Minimal fields/conditions for a fair apples-to-apples comparison
        fields = None
        conditions = None

        iterations = 500

        # Warm-up to mitigate one-time costs (imports, JIT-like caches, etc.)
        for _ in range(50):
            boto_client.generate_presigned_post(
                Bucket=bucket,
                Key=key,
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=60,
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
        t_custom = _timeit(run_light, iterations)

        results = {
            "python_version": f"{py_vers.major}.{py_vers.minor}",
            "tested_method": "generate_presigned_post_sync_cm",
            "iterations": iterations,
            "boto_total": t_boto,
            "signurlarity_total": t_custom,
            "boto_ops": iterations / t_boto,
            "signurlarity_ops": iterations / t_custom,
            "speedup": t_boto / t_custom,
        }

        print(results)
        result_file.write_text(json.dumps(results, indent=2))


def test_generate_presigned_url_perf_sync_cm(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs custom S3PresignedURLGenerator for presigned URL.

    This benchmark compares boto3's generate_presigned_url with the custom
    implementation that has zero boto3 dependencies.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_generate_presigned_url_perf_sync_cm")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")
    rng = random.Random(42)  # noqa: S311
    bucket = "perf-bucket"
    key = "object.txt"

    # Extract region from endpoint_url
    region = "us-east-1"

    boto_client = boto3.client(
        "s3",
        region_name=region,
        **rustfs_server,
        config=Config(signature_version="s3v4"),
    )
    with Client(**rustfs_server) as light_client:
        # custom_generator = S3PresignedURLGenerator(
        #     access_key=AWS_ACCESS_KEY_ID, secret_key=AWS_SECRET_ACCESS_KEY, region=region
        # )

        iterations = 500

        # Warm-up to mitigate one-time costs
        for _ in range(50):
            boto_client.generate_presigned_url(
                "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=60
            )

        for _ in range(50):
            light_client.generate_presigned_url(
                "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=60
            )

        def run_boto(n: int):
            for _ in range(n):
                # Vary key slightly to avoid any internal memoization
                boto_client.generate_presigned_url(
                    "get_object",
                    Params={
                        "Bucket": bucket,
                        "Key": f"{key}-{rng.randint(0, 1_000_000)}",
                    },
                    ExpiresIn=60,
                )

        def run_custom(n: int):
            for _ in range(n):
                light_client.generate_presigned_url(
                    "get_object",
                    Params={
                        "Bucket": bucket,
                        "Key": f"{key}-{rng.randint(0, 1_000_000)}",
                    },
                    ExpiresIn=60,
                )

        t_boto = _timeit(run_boto, iterations)
        t_custom = _timeit(run_custom, iterations)

        results = {
            "python_version": f"{py_vers.major}.{py_vers.minor}",
            "tested_method": "generate_presigned_url_sync_cm",
            "iterations": iterations,
            "boto_total": t_boto,
            "signurlarity_total": t_custom,
            "boto_ops": iterations / t_boto,
            "signurlarity_ops": iterations / t_custom,
            "speedup": t_boto / t_custom,
        }

        print(results)
        result_file.write_text(json.dumps(results, indent=2))


def test_head_bucket_perf_sync_cm(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity for head_bucket.

    This benchmark compares boto3's head_bucket with the custom
    implementation that has zero boto3 dependencies.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_head_bucket_perf_sync_cm")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")
    bucket = "perf-bucket"

    # Extract region from endpoint_url
    region = "us-east-1"

    boto_client = boto3.client("s3", region_name=region, **rustfs_server)
    with Client(**rustfs_server) as light_client:
        # Create the bucket for testing
        boto_client.create_bucket(Bucket=bucket)

        iterations = 500

        # Warm-up to mitigate one-time costs
        for _ in range(10):
            boto_client.head_bucket(Bucket=bucket)

        for _ in range(10):
            light_client.head_bucket(Bucket=bucket)

        def run_boto(n: int):
            for _ in range(n):
                boto_client.head_bucket(Bucket=bucket)

        def run_custom(n: int):
            for _ in range(n):
                light_client.head_bucket(Bucket=bucket)

        t_boto = _timeit(run_boto, iterations)
        t_custom = _timeit(run_custom, iterations)

        results = {
            "python_version": f"{py_vers.major}.{py_vers.minor}",
            "tested_method": "head_bucket_sync_cm",
            "iterations": iterations,
            "boto_total": t_boto,
            "signurlarity_total": t_custom,
            "boto_ops": iterations / t_boto,
            "signurlarity_ops": iterations / t_custom,
            "speedup": t_boto / t_custom,
        }

        result_file.write_text(json.dumps(results, indent=2))


def test_head_object_perf_sync_cm(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity for head_object.

    This benchmark compares boto3's head_object with the custom
    implementation that has zero boto3 dependencies.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_head_object_perf_sync_cm")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")
    bucket = "perf-object"
    key = "perf-object.txt"

    # Extract region from endpoint_url
    region = "us-east-1"

    boto_client = boto3.client("s3", region_name=region, **rustfs_server)
    with Client(**rustfs_server) as light_client:
        # Create the bucket and object for testing
        boto_client.create_bucket(Bucket=bucket)
        boto_client.put_object(
            Bucket=bucket, Key=key, Body=b"test data for head_object perf test"
        )

        iterations = 500

        # Warm-up to mitigate one-time costs
        for _ in range(10):
            boto_client.head_object(Bucket=bucket, Key=key)

        for _ in range(10):
            light_client.head_object(Bucket=bucket, Key=key)

        def run_boto(n: int):
            for _ in range(n):
                boto_client.head_object(Bucket=bucket, Key=key)

        def run_custom(n: int):
            for _ in range(n):
                light_client.head_object(Bucket=bucket, Key=key)

        t_boto = _timeit(run_boto, iterations)
        t_custom = _timeit(run_custom, iterations)

        results = {
            "python_version": f"{py_vers.major}.{py_vers.minor}",
            "tested_method": "head_object_sync_cm",
            "iterations": iterations,
            "boto_total": t_boto,
            "signurlarity_total": t_custom,
            "boto_ops": iterations / t_boto,
            "signurlarity_ops": iterations / t_custom,
            "speedup": t_boto / t_custom,
        }

        # Informational output
        print("\n" + "=" * 60)
        print("HEAD OBJECT BENCHMARK")
        print("=" * 60)
        print(
            f"boto3 head_object: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
        )
        print(
            f"signurlarity head_object: {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
        )
        if t_custom > 0:
            speedup = t_boto / t_custom
            print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
            if speedup > 1:
                print(f"✓ Signurlarity implementation is {speedup:.2f}x FASTER!")
            else:
                print(f"boto3 is {1 / speedup:.2f}x faster")

        result_file.write_text(json.dumps(results, indent=2))

        print("=" * 60)


def test_create_bucket_perf_sync_cm(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity for create_bucket.

    This benchmark compares boto3's create_bucket with the signurlarity
    implementation that uses httpx with AWS Signature V4.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_create_bucket_perf_sync_cm")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    boto_client = boto3.client("s3", **rustfs_server)
    with Client(**rustfs_server) as light_client:
        iterations = 500
        bucket_prefix = "perf-bucket-create"

        # Warm-up to mitigate one-time costs
        for i in range(10):
            bucket = f"{bucket_prefix}-warmup-{i}"
            boto_client.create_bucket(Bucket=bucket)
            boto_client.delete_bucket(Bucket=bucket)

        for i in range(10):
            bucket = f"{bucket_prefix}-warmup-light-{i}"
            light_client.create_bucket(Bucket=bucket)
            boto_client.delete_bucket(Bucket=bucket)

        def run_boto(n: int):
            for i in range(n):
                bucket = f"{bucket_prefix}-boto-{i}"
                boto_client.create_bucket(Bucket=bucket)
                boto_client.delete_bucket(Bucket=bucket)

        def run_custom(n: int):
            for i in range(n):
                bucket = f"{bucket_prefix}-custom-{i}"
                light_client.create_bucket(Bucket=bucket)
                boto_client.delete_bucket(Bucket=bucket)

        t_boto = _timeit(run_boto, iterations)
        t_custom = _timeit(run_custom, iterations)

        results = {
            "python_version": f"{py_vers.major}.{py_vers.minor}",
            "tested_method": "create_bucket_sync_cm",
            "iterations": iterations,
            "boto_total": t_boto,
            "signurlarity_total": t_custom,
            "boto_ops": iterations / t_boto,
            "signurlarity_ops": iterations / t_custom,
            "speedup": t_boto / t_custom,
        }

        # Informational output
        print("\n" + "=" * 60)
        print("CREATE BUCKET BENCHMARK")
        print("=" * 60)
        print(
            f"boto3 create_bucket: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
        )
        print(
            f"signurlarity create_bucket: {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
        )
        if t_custom > 0:
            speedup = t_boto / t_custom
            print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
            if speedup > 1:
                print(f"✓ Signurlarity implementation is {speedup:.2f}x FASTER!")
            else:
                print(f"boto3 is {1 / speedup:.2f}x faster")

        result_file.write_text(json.dumps(results, indent=2))

        print("=" * 60)


def test_delete_objects_perf_sync_cm(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity for delete_objects.

    This benchmark compares boto3's delete_objects with the signurlarity
    implementation that uses httpx with AWS Signature V4.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_delete_objects_perf_sync_cm")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    bucket = "perf-delete-objects"
    num_keys = 10

    boto_client = boto3.client("s3", **rustfs_server)
    with Client(**rustfs_server) as light_client:
        # Create the bucket for testing
        boto_client.create_bucket(Bucket=bucket)

        iterations = 10

        def _populate(prefix: str):
            keys = [f"{prefix}-{i}.txt" for i in range(num_keys)]
            for k in keys:
                boto_client.put_object(Bucket=bucket, Key=k, Body=b"data")
            return keys

        # Warm-up
        for i in range(5):
            keys = _populate(f"warmup-boto-{i}")
            boto_client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
            )

        for i in range(5):
            keys = _populate(f"warmup-light-{i}")
            light_client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
            )

        def run_boto(n: int):
            for i in range(n):
                keys = _populate(f"bench-boto-{i}")
                boto_client.delete_objects(
                    Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
                )

        def run_custom(n: int):
            for i in range(n):
                keys = _populate(f"bench-light-{i}")
                light_client.delete_objects(
                    Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
                )

        t_boto = _timeit(run_boto, iterations)
        t_custom = _timeit(run_custom, iterations)

        results = {
            "python_version": f"{py_vers.major}.{py_vers.minor}",
            "tested_method": "delete_objects_sync_cm",
            "iterations": iterations,
            "boto_total": t_boto,
            "signurlarity_total": t_custom,
            "boto_ops": iterations / t_boto,
            "signurlarity_ops": iterations / t_custom,
            "speedup": t_boto / t_custom,
        }

        print("\n" + "=" * 60)
        print("DELETE OBJECTS BENCHMARK")
        print("=" * 60)
        print(
            f"boto3 delete_objects: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
        )
        print(
            f"signurlarity delete_objects: {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
        )
        if t_custom > 0:
            speedup = t_boto / t_custom
            print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
            if speedup > 1:
                print(f"✓ Signurlarity implementation is {speedup:.2f}x FASTER!")
            else:
                print(f"boto3 is {1 / speedup:.2f}x faster")

        result_file.write_text(json.dumps(results, indent=2))

        print("=" * 60)
