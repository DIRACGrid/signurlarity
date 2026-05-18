from __future__ import annotations

import json
import os
import random
import sys
import tempfile
from pathlib import Path
from typing import Any

import boto3
import pytest
from aiobotocore.session import get_session
from botocore.client import Config

from conftest import _timeit, _timeit_async_helper
from signurlarity import Client
from signurlarity.aio import AsyncClient

# =============================================================================
# OPERATION CONFIGURATION
# =============================================================================

BUCKET = "perf-bucket"
KEY = "object.txt"
SRC_KEY = "source.txt"
BODY_1KB = b"x" * 1024
BODY_SRC = b"source content"
NUM_KEYS = 10
PREFIX = "bench/"
REGION = "us-east-1"
RNG = random.Random(42)  # noqa: S311


# Operation-specific configurations
# Each config defines: iterations, warmup, setup requirements, and call templates
OPERATIONS = [
    "generate_presigned_post",
    "generate_presigned_url",
    "head_bucket",
    "head_object",
    "create_bucket",
    "delete_objects",
    "put_object",
    "list_objects",
    "copy_object",
    "upload_file",
]


def _make_key(base: str, rng: random.Random) -> str:
    """Generate a unique key with random suffix to avoid memoization."""
    return f"{base}-{rng.randint(0, 1_000_000)}"


# =============================================================================
# SYNC BENCHMARK RUNNERS
# =============================================================================


def _run_generate_presigned_post_sync(
    boto_client: boto3.client,
    light_client: Client,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark generate_presigned_post for sync clients."""
    iterations = 200
    warmup = 50

    # Warmup
    for _ in range(warmup):
        boto_client.generate_presigned_post(
            Bucket=BUCKET, Key=KEY, Fields=None, Conditions=None, ExpiresIn=60
        )
    try:
        for _ in range(warmup):
            light_client.generate_presigned_post(
                Bucket=BUCKET, Key=KEY, Fields=None, Conditions=None, ExpiresIn=60
            )
    except NotImplementedError:
        if use_cm:
            light_client.close()
        pytest.skip("signurlarity.Client.generate_presigned_post not implemented")

    def run_boto(n: int):
        for _ in range(n):
            boto_client.generate_presigned_post(
                Bucket=BUCKET,
                Key=_make_key(KEY, RNG),
                Fields=None,
                Conditions=None,
                ExpiresIn=60,
            )

    def run_custom(n: int):
        for _ in range(n):
            light_client.generate_presigned_post(
                Bucket=BUCKET,
                Key=_make_key(KEY, RNG),
                Fields=None,
                Conditions=None,
                ExpiresIn=60,
            )

    t_boto = _timeit(run_boto, iterations)
    t_custom = _timeit(run_custom, iterations)

    if not use_cm:
        light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


def _run_generate_presigned_url_sync(
    boto_client: boto3.client,
    light_client: Client,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark generate_presigned_url for sync clients."""
    iterations = 500
    warmup = 50

    # Warmup
    for _ in range(warmup):
        boto_client.generate_presigned_url(
            "get_object", Params={"Bucket": BUCKET, "Key": KEY}, ExpiresIn=60
        )
    for _ in range(warmup):
        light_client.generate_presigned_url(
            "get_object", Params={"Bucket": BUCKET, "Key": KEY}, ExpiresIn=60
        )

    def run_boto(n: int):
        for _ in range(n):
            boto_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET, "Key": _make_key(KEY, RNG)},
                ExpiresIn=60,
            )

    def run_custom(n: int):
        for _ in range(n):
            light_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET, "Key": _make_key(KEY, RNG)},
                ExpiresIn=60,
            )

    t_boto = _timeit(run_boto, iterations)
    t_custom = _timeit(run_custom, iterations)

    if not use_cm:
        light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


def _run_head_bucket_sync(
    boto_client: boto3.client,
    light_client: Client,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark head_bucket for sync clients."""
    iterations = 10
    warmup = 10

    # Setup: create bucket
    boto_client.create_bucket(Bucket=BUCKET)

    # Warmup
    for _ in range(warmup):
        boto_client.head_bucket(Bucket=BUCKET)
    for _ in range(warmup):
        light_client.head_bucket(Bucket=BUCKET)

    def run_boto(n: int):
        for _ in range(n):
            boto_client.head_bucket(Bucket=BUCKET)

    def run_custom(n: int):
        for _ in range(n):
            light_client.head_bucket(Bucket=BUCKET)

    t_boto = _timeit(run_boto, iterations)
    t_custom = _timeit(run_custom, iterations)

    if not use_cm:
        light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


def _run_head_object_sync(
    boto_client: boto3.client,
    light_client: Client,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark head_object for sync clients."""
    iterations = 10
    warmup = 10

    # Setup: create bucket and object
    boto_client.create_bucket(Bucket=BUCKET)
    boto_client.put_object(
        Bucket=BUCKET, Key=KEY, Body=b"test data for head_object perf test"
    )

    # Warmup
    for _ in range(warmup):
        boto_client.head_object(Bucket=BUCKET, Key=KEY)
    for _ in range(warmup):
        light_client.head_object(Bucket=BUCKET, Key=KEY)

    def run_boto(n: int):
        for _ in range(n):
            boto_client.head_object(Bucket=BUCKET, Key=KEY)

    def run_custom(n: int):
        for _ in range(n):
            light_client.head_object(Bucket=BUCKET, Key=KEY)

    t_boto = _timeit(run_boto, iterations)
    t_custom = _timeit(run_custom, iterations)

    if not use_cm:
        light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


def _run_create_bucket_sync(
    boto_client: boto3.client,
    light_client: Client,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark create_bucket for sync clients."""
    iterations = 10
    warmup = 10
    bucket_prefix = "perf-bucket-create"

    # Warmup
    for i in range(warmup):
        bucket = f"{bucket_prefix}-warmup-{i}"
        boto_client.create_bucket(Bucket=bucket)
        boto_client.delete_bucket(Bucket=bucket)

    for i in range(warmup):
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

    if not use_cm:
        light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


def _run_delete_objects_sync(
    boto_client: boto3.client,
    light_client: Client,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark delete_objects for sync clients."""
    iterations = 10
    warmup = 5
    bucket = "perf-delete-objects"

    # Setup: create bucket
    boto_client.create_bucket(Bucket=bucket)

    def _populate(prefix: str):
        keys = [f"{prefix}-{i}.txt" for i in range(NUM_KEYS)]
        for k in keys:
            boto_client.put_object(Bucket=bucket, Key=k, Body=b"data")
        return keys

    # Warmup
    for i in range(warmup):
        keys = _populate(f"warmup-boto-{i}")
        boto_client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
        )

    for i in range(warmup):
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

    if not use_cm:
        light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


def _run_put_object_sync(
    boto_client: boto3.client,
    light_client: Client,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark put_object for sync clients."""
    iterations = 10
    warmup = 10
    bucket = "perf-put-object"

    # Setup: create bucket
    boto_client.create_bucket(Bucket=bucket)

    # Warmup
    for i in range(warmup):
        boto_client.put_object(Bucket=bucket, Key=f"warmup-boto-{i}.txt", Body=BODY_1KB)
    for i in range(warmup):
        light_client.put_object(
            Bucket=bucket, Key=f"warmup-light-{i}.txt", Body=BODY_1KB
        )

    def run_boto(n: int):
        for _ in range(n):
            boto_client.put_object(
                Bucket=bucket,
                Key=f"bench-boto-{RNG.randint(0, 1_000_000)}.txt",
                Body=BODY_1KB,
            )

    def run_custom(n: int):
        for _ in range(n):
            light_client.put_object(
                Bucket=bucket,
                Key=f"bench-light-{RNG.randint(0, 1_000_000)}.txt",
                Body=BODY_1KB,
            )

    t_boto = _timeit(run_boto, iterations)
    t_custom = _timeit(run_custom, iterations)

    if not use_cm:
        light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


def _run_list_objects_sync(
    boto_client: boto3.client,
    light_client: Client,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark list_objects for sync clients."""
    iterations = 10
    warmup = 10
    bucket = "perf-list-objects"

    # Setup: create bucket and objects
    boto_client.create_bucket(Bucket=bucket)
    for i in range(10):
        boto_client.put_object(Bucket=bucket, Key=f"{PREFIX}obj-{i}.txt", Body=b"data")

    # Warmup
    for _ in range(warmup):
        boto_client.list_objects(Bucket=bucket, Prefix=PREFIX)
    for _ in range(warmup):
        light_client.list_objects(Bucket=bucket, Prefix=PREFIX)

    def run_boto(n: int):
        for _ in range(n):
            boto_client.list_objects(Bucket=bucket, Prefix=PREFIX)

    def run_custom(n: int):
        for _ in range(n):
            light_client.list_objects(Bucket=bucket, Prefix=PREFIX)

    t_boto = _timeit(run_boto, iterations)
    t_custom = _timeit(run_custom, iterations)

    # Output
    print("\n" + "=" * 60)
    print("LIST OBJECTS BENCHMARK")
    print("=" * 60)
    print(
        f"boto3 list_objects: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"signurlarity list_objects: {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
    )
    if t_custom > 0:
        speedup = t_boto / t_custom
        print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
        if speedup > 1:
            print(f"✓ Signurlarity implementation is {speedup:.2f}x FASTER!")
        else:
            print(f"boto3 is {1 / speedup:.2f}x faster")
    print("=" * 60)

    if not use_cm:
        light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


def _run_copy_object_sync(
    boto_client: boto3.client,
    light_client: Client,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark copy_object for sync clients."""
    iterations = 10
    warmup = 10
    bucket = "perf-copy-object"

    # Setup: create bucket and source object
    boto_client.create_bucket(Bucket=bucket)
    boto_client.put_object(Bucket=bucket, Key=SRC_KEY, Body=BODY_SRC)

    # Warmup
    for i in range(warmup):
        boto_client.copy_object(
            Bucket=bucket,
            Key=f"warmup-boto-{i}.txt",
            CopySource={"Bucket": bucket, "Key": SRC_KEY},
        )
    for i in range(warmup):
        light_client.copy_object(
            Bucket=bucket,
            Key=f"warmup-light-{i}.txt",
            CopySource=f"{bucket}/{SRC_KEY}",
        )

    def run_boto(n: int):
        for _ in range(n):
            boto_client.copy_object(
                Bucket=bucket,
                Key=f"bench-boto-{RNG.randint(0, 1_000_000)}.txt",
                CopySource={"Bucket": bucket, "Key": SRC_KEY},
            )

    def run_custom(n: int):
        for _ in range(n):
            light_client.copy_object(
                Bucket=bucket,
                Key=f"bench-light-{RNG.randint(0, 1_000_000)}.txt",
                CopySource=f"{bucket}/{SRC_KEY}",
            )

    t_boto = _timeit(run_boto, iterations)
    t_custom = _timeit(run_custom, iterations)

    # Output
    print("\n" + "=" * 60)
    print("COPY OBJECT BENCHMARK")
    print("=" * 60)
    print(
        f"boto3 copy_object: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"signurlarity copy_object: {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
    )
    if t_custom > 0:
        speedup = t_boto / t_custom
        print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
        if speedup > 1:
            print(f"✓ Signurlarity implementation is {speedup:.2f}x FASTER!")
        else:
            print(f"boto3 is {1 / speedup:.2f}x faster")
    print("=" * 60)

    if not use_cm:
        light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


def _run_upload_file_sync(
    boto_client: boto3.client,
    light_client: Client,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark upload_file for sync clients."""
    iterations = 10
    warmup = 10
    bucket = "perf-upload-file"

    # Setup: create bucket and temp file
    boto_client.create_bucket(Bucket=bucket)

    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as tmp:
        tmp.write(BODY_1KB)
        tmp_path = tmp.name

    try:
        # Warmup
        for i in range(warmup):
            boto_client.upload_file(
                Filename=tmp_path, Bucket=bucket, Key=f"warmup-boto-{i}.bin"
            )
        for i in range(warmup):
            light_client.upload_file(
                Filename=tmp_path, Bucket=bucket, Key=f"warmup-light-{i}.bin"
            )

        def run_boto(n: int):
            for _ in range(n):
                boto_client.upload_file(
                    Filename=tmp_path,
                    Bucket=bucket,
                    Key=f"bench-boto-{RNG.randint(0, 1_000_000)}.bin",
                )

        def run_custom(n: int):
            for _ in range(n):
                light_client.upload_file(
                    Filename=tmp_path,
                    Bucket=bucket,
                    Key=f"bench-light-{RNG.randint(0, 1_000_000)}.bin",
                )

        t_boto = _timeit(run_boto, iterations)
        t_custom = _timeit(run_custom, iterations)
    finally:
        os.unlink(tmp_path)

    # Output
    print("\n" + "=" * 60)
    print("UPLOAD FILE BENCHMARK")
    print("=" * 60)
    print(
        f"boto3 upload_file: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"signurlarity upload_file: {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
    )
    if t_custom > 0:
        speedup = t_boto / t_custom
        print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
        if speedup > 1:
            print(f"✓ Signurlarity implementation is {speedup:.2f}x FASTER!")
        else:
            print(f"boto3 is {1 / speedup:.2f}x faster")
    print("=" * 60)

    if not use_cm:
        light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


# =============================================================================
# ASYNC BENCHMARK RUNNERS
# =============================================================================


async def _run_generate_presigned_post_async(
    boto_client,
    async_light_client: AsyncClient,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark generate_presigned_post for async clients."""
    from uuid import uuid4

    iterations = 500
    warmup = 50

    # Warmup
    for _ in range(warmup):
        await boto_client.generate_presigned_post(
            Bucket=BUCKET,
            Key=KEY + str(uuid4()),
            Fields=None,
            Conditions=None,
            ExpiresIn=60,
        )
    try:
        for _ in range(warmup):
            await async_light_client.generate_presigned_post(
                Bucket=BUCKET,
                Key=KEY + str(uuid4()),
                Fields=None,
                Conditions=None,
                ExpiresIn=60,
            )
    except NotImplementedError:
        if use_cm:
            await async_light_client.close()
        pytest.skip("signurlarity.AsyncClient.generate_presigned_post not implemented")

    async def run_boto(n: int):
        for _ in range(n):
            await boto_client.generate_presigned_post(
                Bucket=BUCKET,
                Key=f"{KEY}-{RNG.randint(0, 1_000_000)}",
                Fields=None,
                Conditions=None,
                ExpiresIn=60,
            )

    async def run_custom(n: int):
        for _ in range(n):
            await async_light_client.generate_presigned_post(
                Bucket=BUCKET,
                Key=f"{KEY}-{RNG.randint(0, 1_000_000)}",
                Fields=None,
                Conditions=None,
                ExpiresIn=60,
            )

    t_boto = await _timeit_async_helper(run_boto, iterations)
    t_custom = await _timeit_async_helper(run_custom, iterations)

    if not use_cm:
        await async_light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


async def _run_generate_presigned_url_async(
    boto_client,
    async_light_client: AsyncClient,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark generate_presigned_url for async clients."""
    iterations = 500
    warmup = 50

    # Warmup
    for _ in range(warmup):
        await boto_client.generate_presigned_url(
            "get_object", Params={"Bucket": BUCKET, "Key": KEY}, ExpiresIn=60
        )
    for _ in range(warmup):
        await async_light_client.generate_presigned_url(
            "get_object", Params={"Bucket": BUCKET, "Key": KEY}, ExpiresIn=60
        )

    async def run_boto(n: int):
        for _ in range(n):
            await boto_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET, "Key": f"{KEY}-{RNG.randint(0, 1_000_000)}"},
                ExpiresIn=60,
            )

    async def run_custom(n: int):
        for _ in range(n):
            await async_light_client.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET, "Key": f"{KEY}-{RNG.randint(0, 1_000_000)}"},
                ExpiresIn=60,
            )

    t_boto = await _timeit_async_helper(run_boto, iterations)
    t_custom = await _timeit_async_helper(run_custom, iterations)

    if not use_cm:
        await async_light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


async def _run_head_bucket_async(
    boto_client,
    async_light_client: AsyncClient,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark head_bucket for async clients."""
    iterations = 500
    warmup = 10

    # Setup: create bucket
    await async_light_client.create_bucket(Bucket=BUCKET)

    # Warmup
    for _ in range(warmup):
        await async_light_client.head_bucket(Bucket=BUCKET)
    for _ in range(warmup):
        await boto_client.head_bucket(Bucket=BUCKET)

    async def run_boto(n: int):
        for _ in range(n):
            await boto_client.head_bucket(Bucket=BUCKET)

    async def run_custom(n: int):
        for _ in range(n):
            await async_light_client.head_bucket(Bucket=BUCKET)

    t_boto = await _timeit_async_helper(run_boto, iterations)
    t_custom = await _timeit_async_helper(run_custom, iterations)

    if not use_cm:
        await async_light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


async def _run_head_object_async(
    boto_client,
    async_light_client: AsyncClient,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark head_object for async clients."""
    iterations = 500
    warmup = 10
    key = "perf-object.txt"

    # Setup: create bucket and object
    await async_light_client.create_bucket(Bucket=BUCKET)
    await boto_client.put_object(
        Bucket=BUCKET, Key=key, Body=b"test data for head_object perf test"
    )

    # Warmup
    for _ in range(warmup):
        await boto_client.head_object(Bucket=BUCKET, Key=key)
    for _ in range(warmup):
        await async_light_client.head_object(Bucket=BUCKET, Key=key)

    async def run_boto(n: int):
        for _ in range(n):
            await boto_client.head_object(Bucket=BUCKET, Key=key)

    async def run_custom(n: int):
        for _ in range(n):
            await async_light_client.head_object(Bucket=BUCKET, Key=key)

    t_boto = await _timeit_async_helper(run_boto, iterations)
    t_custom = await _timeit_async_helper(run_custom, iterations)

    # Output
    print("\n" + "=" * 60)
    print("HEAD OBJECT BENCHMARK (ASYNC)")
    print("=" * 60)
    print(
        f"boto3 head_object: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"signurlarity head_object (async): {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
    )
    if t_custom > 0:
        speedup = t_boto / t_custom
        print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
        if speedup > 1:
            print(f"✓ Signurlarity async implementation is {speedup:.2f}x FASTER!")
        else:
            print(f"boto3 is {1 / speedup:.2f}x faster")
    print("=" * 60)

    if not use_cm:
        await async_light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


async def _run_create_bucket_async(
    boto_client,
    async_light_client: AsyncClient,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark create_bucket for async clients."""
    iterations = 500
    warmup = 10
    bucket_prefix = "perf-bucket-create"

    # Warmup
    for i in range(warmup):
        bucket = f"{bucket_prefix}-warmup-{i}"
        await boto_client.create_bucket(Bucket=bucket)

    for i in range(warmup):
        bucket = f"{bucket_prefix}-warmup-light-{i}"
        await async_light_client.create_bucket(Bucket=bucket)

    async def run_boto(n: int):
        for i in range(n):
            bucket = f"{bucket_prefix}-boto-{i}"
            await boto_client.create_bucket(Bucket=bucket)

    async def run_custom(n: int):
        for i in range(n):
            bucket = f"{bucket_prefix}-custom-{i}"
            await async_light_client.create_bucket(Bucket=bucket)

    t_boto = await _timeit_async_helper(run_boto, iterations)
    t_custom = await _timeit_async_helper(run_custom, iterations)

    # Output
    print("\n" + "=" * 60)
    print("CREATE BUCKET BENCHMARK (ASYNC)")
    print("=" * 60)
    print(
        f"boto3 create_bucket: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"signurlarity create_bucket (async): {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
    )
    if t_custom > 0:
        speedup = t_boto / t_custom
        print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
        if speedup > 1:
            print(f"✓ Signurlarity async implementation is {speedup:.2f}x FASTER!")
        else:
            print(f"boto3 is {1 / speedup:.2f}x faster")
    print("=" * 60)

    if not use_cm:
        await async_light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


async def _run_delete_objects_async(
    boto_client,
    async_light_client: AsyncClient,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark delete_objects for async clients."""
    iterations = 10
    warmup = 5
    bucket = "perf-delete-objects"

    # Setup: create bucket
    await async_light_client.create_bucket(Bucket=bucket)

    async def _populate(prefix: str):
        keys = [f"{prefix}-{i}.txt" for i in range(NUM_KEYS)]
        for k in keys:
            await boto_client.put_object(Bucket=bucket, Key=k, Body=b"data")
        return keys

    # Warmup
    for i in range(warmup):
        keys = await _populate(f"warmup-boto-{i}")
        await boto_client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
        )

    for i in range(warmup):
        keys = await _populate(f"warmup-light-{i}")
        await async_light_client.delete_objects(
            Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
        )

    async def run_boto(n: int):
        for i in range(n):
            keys = await _populate(f"bench-boto-{i}")
            await boto_client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
            )

    async def run_custom(n: int):
        for i in range(n):
            keys = await _populate(f"bench-light-{i}")
            await async_light_client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
            )

    t_boto = await _timeit_async_helper(run_boto, iterations)
    t_custom = await _timeit_async_helper(run_custom, iterations)

    # Output
    print("\n" + "=" * 60)
    print("DELETE OBJECTS BENCHMARK (ASYNC)")
    print("=" * 60)
    print(
        f"boto3 delete_objects: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"signurlarity delete_objects (async): {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
    )
    if t_custom > 0:
        speedup = t_boto / t_custom
        print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
        if speedup > 1:
            print(f"✓ Signurlarity async implementation is {speedup:.2f}x FASTER!")
        else:
            print(f"boto3 is {1 / speedup:.2f}x faster")
    print("=" * 60)

    if not use_cm:
        await async_light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


async def _run_put_object_async(
    boto_client,
    async_light_client: AsyncClient,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark put_object for async clients."""
    iterations = 10
    warmup = 10
    bucket = "perf-put-object-aio"

    # Setup: create bucket
    await boto_client.create_bucket(Bucket=bucket)

    # Warmup
    for i in range(warmup):
        await boto_client.put_object(
            Bucket=bucket, Key=f"warmup-boto-{i}.txt", Body=BODY_1KB
        )
    for i in range(warmup):
        await async_light_client.put_object(
            Bucket=bucket, Key=f"warmup-light-{i}.txt", Body=BODY_1KB
        )

    async def run_boto(n: int):
        for _ in range(n):
            await boto_client.put_object(
                Bucket=bucket,
                Key=f"bench-boto-{RNG.randint(0, 1_000_000)}.txt",
                Body=BODY_1KB,
            )

    async def run_custom(n: int):
        for _ in range(n):
            await async_light_client.put_object(
                Bucket=bucket,
                Key=f"bench-light-{RNG.randint(0, 1_000_000)}.txt",
                Body=BODY_1KB,
            )

    t_boto = await _timeit_async_helper(run_boto, iterations)
    t_custom = await _timeit_async_helper(run_custom, iterations)

    # Output
    print("\n" + "=" * 60)
    print("PUT OBJECT BENCHMARK (ASYNC)")
    print("=" * 60)
    print(
        f"boto3 put_object: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"signurlarity put_object (async): {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
    )
    if t_custom > 0:
        speedup = t_boto / t_custom
        print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
        if speedup > 1:
            print(f"✓ Signurlarity async implementation is {speedup:.2f}x FASTER!")
        else:
            print(f"boto3 is {1 / speedup:.2f}x faster")
    print("=" * 60)

    if not use_cm:
        await async_light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


async def _run_list_objects_async(
    boto_client,
    async_light_client: AsyncClient,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark list_objects for async clients."""
    iterations = 10
    warmup = 10
    bucket = "perf-list-objects-aio"

    # Setup: create bucket and objects
    await boto_client.create_bucket(Bucket=bucket)
    for i in range(10):
        await boto_client.put_object(
            Bucket=bucket, Key=f"{PREFIX}obj-{i}.txt", Body=b"data"
        )

    # Warmup
    for _ in range(warmup):
        await boto_client.list_objects(Bucket=bucket, Prefix=PREFIX)
    for _ in range(warmup):
        await async_light_client.list_objects(Bucket=bucket, Prefix=PREFIX)

    async def run_boto(n: int):
        for _ in range(n):
            await boto_client.list_objects(Bucket=bucket, Prefix=PREFIX)

    async def run_custom(n: int):
        for _ in range(n):
            await async_light_client.list_objects(Bucket=bucket, Prefix=PREFIX)

    t_boto = await _timeit_async_helper(run_boto, iterations)
    t_custom = await _timeit_async_helper(run_custom, iterations)

    # Output
    print("\n" + "=" * 60)
    print("LIST OBJECTS BENCHMARK (ASYNC)")
    print("=" * 60)
    print(
        f"boto3 list_objects: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"signurlarity list_objects (async): {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
    )
    if t_custom > 0:
        speedup = t_boto / t_custom
        print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
        if speedup > 1:
            print(f"✓ Signurlarity async implementation is {speedup:.2f}x FASTER!")
        else:
            print(f"boto3 is {1 / speedup:.2f}x faster")
    print("=" * 60)

    if not use_cm:
        await async_light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


async def _run_copy_object_async(
    boto_client,
    async_light_client: AsyncClient,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark copy_object for async clients."""
    iterations = 10
    warmup = 10
    bucket = "perf-copy-object-aio"

    # Setup: create bucket and source object
    await boto_client.create_bucket(Bucket=bucket)
    await boto_client.put_object(Bucket=bucket, Key=SRC_KEY, Body=BODY_SRC)

    # Warmup
    for i in range(warmup):
        await boto_client.copy_object(
            Bucket=bucket,
            Key=f"warmup-boto-{i}.txt",
            CopySource={"Bucket": bucket, "Key": SRC_KEY},
        )
    for i in range(warmup):
        await async_light_client.copy_object(
            Bucket=bucket,
            Key=f"warmup-light-{i}.txt",
            CopySource=f"{bucket}/{SRC_KEY}",
        )

    async def run_boto(n: int):
        for _ in range(n):
            await boto_client.copy_object(
                Bucket=bucket,
                Key=f"bench-boto-{RNG.randint(0, 1_000_000)}.txt",
                CopySource={"Bucket": bucket, "Key": SRC_KEY},
            )

    async def run_custom(n: int):
        for _ in range(n):
            await async_light_client.copy_object(
                Bucket=bucket,
                Key=f"bench-light-{RNG.randint(0, 1_000_000)}.txt",
                CopySource=f"{bucket}/{SRC_KEY}",
            )

    t_boto = await _timeit_async_helper(run_boto, iterations)
    t_custom = await _timeit_async_helper(run_custom, iterations)

    # Output
    print("\n" + "=" * 60)
    print("COPY OBJECT BENCHMARK (ASYNC)")
    print("=" * 60)
    print(
        f"boto3 copy_object: {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"signurlarity copy_object (async): {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
    )
    if t_custom > 0:
        speedup = t_boto / t_custom
        print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
        if speedup > 1:
            print(f"✓ Signurlarity async implementation is {speedup:.2f}x FASTER!")
        else:
            print(f"boto3 is {1 / speedup:.2f}x faster")
    print("=" * 60)

    if not use_cm:
        await async_light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


async def _run_upload_file_async(
    boto_client,
    async_light_client: AsyncClient,
    test_dir: Path,
    use_cm: bool,
) -> dict[str, Any]:
    """Benchmark upload_file for async clients."""
    iterations = 10
    warmup = 10
    bucket = "perf-upload-file-aio"

    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as tmp:
        tmp.write(BODY_1KB)
        tmp_path = tmp.name

    try:
        # Setup: create bucket
        await boto_client.create_bucket(Bucket=bucket)

        # Warmup (aiobotocore has no upload_file; use put_object with file read)
        for i in range(warmup):
            with open(tmp_path, "rb") as fh:
                await boto_client.put_object(
                    Bucket=bucket, Key=f"warmup-boto-{i}.bin", Body=fh.read()
                )
        for i in range(warmup):
            await async_light_client.upload_file(
                Filename=tmp_path, Bucket=bucket, Key=f"warmup-light-{i}.bin"
            )

        async def run_boto(n: int):
            for _ in range(n):
                with open(tmp_path, "rb") as fh:
                    await boto_client.put_object(
                        Bucket=bucket,
                        Key=f"bench-boto-{RNG.randint(0, 1_000_000)}.bin",
                        Body=fh.read(),
                    )

        async def run_custom(n: int):
            for _ in range(n):
                await async_light_client.upload_file(
                    Filename=tmp_path,
                    Bucket=bucket,
                    Key=f"bench-light-{RNG.randint(0, 1_000_000)}.bin",
                )

        t_boto = await _timeit_async_helper(run_boto, iterations)
        t_custom = await _timeit_async_helper(run_custom, iterations)
    finally:
        os.unlink(tmp_path)

    # Output
    print("\n" + "=" * 60)
    print("UPLOAD FILE BENCHMARK (ASYNC)")
    print("=" * 60)
    print(
        f"aiobotocore put_object (file): {t_boto:.4f}s for {iterations} ops ({iterations / t_boto:.0f} ops/s)"
    )
    print(
        f"signurlarity upload_file (async): {t_custom:.4f}s for {iterations} ops ({iterations / t_custom:.0f} ops/s)"
    )
    if t_custom > 0:
        speedup = t_boto / t_custom
        print(f"relative speed (signurlarity vs boto3): {speedup:.2f}x")
        if speedup > 1:
            print(f"✓ Signurlarity async implementation is {speedup:.2f}x FASTER!")
        else:
            print(f"boto3 is {1 / speedup:.2f}x faster")
    print("=" * 60)

    if not use_cm:
        await async_light_client.close()

    return {
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
    }


# =============================================================================
# MAIN PARAMETRIZED TEST FUNCTIONS
# =============================================================================

# Mapping of operation names to their runner functions
SYNC_RUNNERS = {
    "generate_presigned_post": _run_generate_presigned_post_sync,
    "generate_presigned_url": _run_generate_presigned_url_sync,
    "head_bucket": _run_head_bucket_sync,
    "head_object": _run_head_object_sync,
    "create_bucket": _run_create_bucket_sync,
    "delete_objects": _run_delete_objects_sync,
    "put_object": _run_put_object_sync,
    "list_objects": _run_list_objects_sync,
    "copy_object": _run_copy_object_sync,
    "upload_file": _run_upload_file_sync,
}

ASYNC_RUNNERS = {
    "generate_presigned_post": _run_generate_presigned_post_async,
    "generate_presigned_url": _run_generate_presigned_url_async,
    "head_bucket": _run_head_bucket_async,
    "head_object": _run_head_object_async,
    "create_bucket": _run_create_bucket_async,
    "delete_objects": _run_delete_objects_async,
    "put_object": _run_put_object_async,
    "list_objects": _run_list_objects_async,
    "copy_object": _run_copy_object_async,
    "upload_file": _run_upload_file_async,
}


# --- Sync benchmarks ---


@pytest.mark.parametrize("operation", OPERATIONS)
@pytest.mark.parametrize("use_cm", [False, True])
def test_benchmark_sync(
    operation: str, use_cm: bool, rustfs_server: dict, test_results_dir: Path
):
    """Parametrized sync benchmarks: 10 operations × 2 client patterns = 20 tests.

    This replaces test_benchmark.py and test_benchmark_cm.py (1298 lines total).
    """
    runner = SYNC_RUNNERS[operation]
    py_vers = sys.version_info

    # Create test directory
    test_name = f"test_{operation}_perf_sync_{'cm' if use_cm else 'plain'}"
    test_dir = test_results_dir / Path(test_name)
    os.makedirs(test_dir, exist_ok=True)
    result_file = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    # Create clients based on pattern
    if use_cm:
        boto_client = boto3.client("s3", **rustfs_server)
        with Client(**rustfs_server) as light_client:
            results = runner(boto_client, light_client, test_dir, use_cm=True)
    else:
        boto_client = boto3.client("s3", **rustfs_server)
        light_client = Client(**rustfs_server)
        results = runner(boto_client, light_client, test_dir, use_cm=False)

    # Build final results
    final_results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": f"{operation}_sync_{'cm' if use_cm else 'plain'}",
        **results,
        "boto_ops": results["iterations"] / results["boto_total"],
        "signurlarity_ops": results["iterations"] / results["signurlarity_total"],
        "speedup": results["boto_total"] / results["signurlarity_total"],
    }

    print(final_results)
    result_file.write_text(json.dumps(final_results, indent=2))


# --- Async benchmarks ---


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", OPERATIONS)
@pytest.mark.parametrize("use_cm", [False, True])
async def test_benchmark_async(
    operation: str, use_cm: bool, rustfs_server: dict, test_results_dir: Path
):
    """Parametrized async benchmarks: 10 operations × 2 client patterns = 20 tests.

    This replaces test_benchmark_aio.py and test_benchmark_aio_cm.py (1313 lines total).
    """
    runner = ASYNC_RUNNERS[operation]
    py_vers = sys.version_info

    # Create test directory
    test_name = f"test_{operation}_perf_aio_{'cm' if use_cm else 'plain'}"
    test_dir = test_results_dir / Path(test_name)
    os.makedirs(test_dir, exist_ok=True)
    result_file = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    # Create clients based on pattern
    session = get_session()

    if use_cm:
        async with session.create_client(
            "s3", **rustfs_server, config=Config(signature_version="s3v4")
        ) as boto_client:
            async with AsyncClient(**rustfs_server) as async_light_client:
                results = await runner(
                    boto_client, async_light_client, test_dir, use_cm=True
                )
    else:
        async with session.create_client(
            "s3", **rustfs_server, config=Config(signature_version="s3v4")
        ) as boto_client:
            async_light_client = AsyncClient(**rustfs_server)
            results = await runner(
                boto_client, async_light_client, test_dir, use_cm=False
            )

    # Build final results
    final_results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": f"{operation}_aio_{'cm' if use_cm else 'plain'}",
        **results,
        "boto_ops": results["iterations"] / results["boto_total"],
        "signurlarity_ops": results["iterations"] / results["signurlarity_total"],
        "speedup": results["boto_total"] / results["signurlarity_total"],
    }

    print(final_results)
    result_file.write_text(json.dumps(final_results, indent=2))
