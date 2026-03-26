from __future__ import annotations

import json
import os
import random
import sys
import tempfile
from pathlib import Path
from uuid import uuid4

import pytest
from aiobotocore.session import get_session
from botocore.client import Config

from conftest import _timeit_async_helper
from signurlarity.aio import AsyncClient


@pytest.mark.asyncio
async def test_generate_presigned_post_perf_aio(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity for presigned POST (async).

    This is a non-failing, informational test: it prints timings and skips
    if the signurlarity implementation is not available.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_generate_presigned_post_perf_aio")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    rng = random.Random(42)  # noqa: S311
    bucket = "perf-bucket"
    key = "object.txt"

    session = get_session()
    async with session.create_client(
        "s3", **rustfs_server, config=Config(signature_version="s3v4")
    ) as boto_client:
        async_light_client = AsyncClient(
            **rustfs_server,
        )

        # Minimal fields/conditions for a fair apples-to-apples comparison
        fields = None
        conditions = None

        iterations = 500

        # Warm-up to mitigate one-time costs (imports, JIT-like caches, etc.)
        for _ in range(50):
            await boto_client.generate_presigned_post(
                Bucket=bucket,
                Key=key + str(uuid4()),
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=60,
            )
        for _ in range(50):
            await async_light_client.generate_presigned_post(
                Bucket=bucket,
                Key=key + str(uuid4()),
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=60,
            )

        async def run_boto(n: int):
            for _ in range(n):
                await boto_client.generate_presigned_post(
                    Bucket=bucket,
                    Key=f"{key}-{rng.randint(0, 1_000_000)}",
                    Fields=fields,
                    Conditions=conditions,
                    ExpiresIn=60,
                )

        async def run_light(n: int):
            for _ in range(n):
                await async_light_client.generate_presigned_post(
                    Bucket=bucket,
                    Key=f"{key}-{rng.randint(0, 1_000_000)}",
                    Fields=fields,
                    Conditions=conditions,
                    ExpiresIn=60,
                )

        t_boto = await _timeit_async_helper(run_boto, iterations)
        t_custom = await _timeit_async_helper(run_light, iterations)

        await async_light_client.close()
        results = {
            "python_version": f"{py_vers.major}.{py_vers.minor}",
            "tested_method": "generate_presigned_post_aio",
            "iterations": iterations,
            "boto_total": t_boto,
            "signurlarity_total": t_custom,
            "boto_ops": iterations / t_boto,
            "signurlarity_ops": iterations / t_custom,
            "speedup": t_boto / t_custom,
        }

        print(results)
        result_file.write_text(json.dumps(results, indent=2))


@pytest.mark.asyncio
async def test_generate_presigned_url_perf_aio(rustfs_server, test_results_dir):
    """Compare performance of signurlarity async for presigned URL (async).

    This benchmark tests the async implementation's presigned URL generation.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_generate_presigned_url_perf_aio")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")
    rng = random.Random(42)  # noqa: S311
    bucket = "perf-bucket"
    key = "object.txt"

    async_light_client = AsyncClient(**rustfs_server)
    session = get_session()
    async with session.create_client(
        "s3", **rustfs_server, config=Config(signature_version="s3v4")
    ) as boto_client:
        iterations = 500

        # Warm-up to mitigate one-time costs
        for _ in range(50):
            await boto_client.generate_presigned_url(
                "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=60
            )

        for _ in range(50):
            await async_light_client.generate_presigned_url(
                "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=60
            )

        async def run_boto(n: int):
            for _ in range(n):
                await boto_client.generate_presigned_url(
                    "get_object",
                    Params={
                        "Bucket": bucket,
                        "Key": f"{key}-{rng.randint(0, 1_000_000)}",
                    },
                    ExpiresIn=60,
                )

        async def run_custom(n: int):
            for _ in range(n):
                await async_light_client.generate_presigned_url(
                    "get_object",
                    Params={
                        "Bucket": bucket,
                        "Key": f"{key}-{rng.randint(0, 1_000_000)}",
                    },
                    ExpiresIn=60,
                )

        t_boto = await _timeit_async_helper(run_boto, iterations)
        t_custom = await _timeit_async_helper(run_custom, iterations)

        await async_light_client.close()
        results = {
            "python_version": f"{py_vers.major}.{py_vers.minor}",
            "tested_method": "generate_presigned_url_aio",
            "iterations": iterations,
            "boto_total": t_boto,
            "signurlarity_total": t_custom,
            "boto_ops": iterations / t_boto,
            "signurlarity_ops": iterations / t_custom,
            "speedup": t_boto / t_custom,
        }

    print(results)
    result_file.write_text(json.dumps(results, indent=2))


@pytest.mark.asyncio
async def test_head_bucket_perf_aio(rustfs_server, test_results_dir):
    """Compare performance of signurlarity async for head_bucket.

    This benchmark tests the async implementation's head_bucket functionality.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_head_bucket_perf_aio")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")
    bucket = "perf-bucket"

    async_light_client = AsyncClient(**rustfs_server)
    session = get_session()
    async with session.create_client(
        "s3", **rustfs_server, config=Config(signature_version="s3v4")
    ) as boto_client:
        # Create the bucket for testing
        await async_light_client.create_bucket(Bucket=bucket)

        iterations = 500

        # Warm-up to mitigate one-time costs
        for _ in range(10):
            await async_light_client.head_bucket(Bucket=bucket)

        # Warm-up to mitigate one-time costs
        for _ in range(10):
            await boto_client.head_bucket(Bucket=bucket)

        async def run_boto(n: int):
            for _ in range(n):
                await boto_client.head_bucket(Bucket=bucket)

        async def run_custom(n: int):
            for _ in range(n):
                await async_light_client.head_bucket(Bucket=bucket)

        t_boto = await _timeit_async_helper(run_custom, iterations)
        t_custom = await _timeit_async_helper(run_custom, iterations)

    await async_light_client.close()
    results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": "head_bucket_aio",
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
        "boto_ops": iterations / t_boto,
        "signurlarity_ops": iterations / t_custom,
        "speedup": t_boto / t_custom,
    }

    result_file.write_text(json.dumps(results, indent=2))


@pytest.mark.asyncio
async def test_head_object_perf_aio(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity async for head_object.

    This benchmark tests the async implementation's head_object functionality.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_head_object_perf_aio")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")
    bucket = "perf-object"
    key = "perf-object.txt"

    async_light_client = AsyncClient(**rustfs_server)
    session = get_session()
    async with session.create_client(
        "s3", **rustfs_server, config=Config(signature_version="s3v4")
    ) as boto_client:
        # Create the bucket and object for testing
        await async_light_client.create_bucket(Bucket=bucket)
        await boto_client.put_object(
            Bucket=bucket, Key=key, Body=b"test data for head_object perf test"
        )

        iterations = 500
        # Warm-up to mitigate one-time costs
        for _ in range(10):
            await boto_client.head_object(Bucket=bucket, Key=key)

        for _ in range(10):
            await async_light_client.head_object(Bucket=bucket, Key=key)

        async def run_boto(n: int):
            for _ in range(n):
                await boto_client.head_object(Bucket=bucket, Key=key)

        async def run_custom(n: int):
            for _ in range(n):
                await async_light_client.head_object(Bucket=bucket, Key=key)

        t_boto = await _timeit_async_helper(run_boto, iterations)
        t_custom = await _timeit_async_helper(run_custom, iterations)

    await async_light_client.close()
    results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": "head_object_aio",
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
        "boto_ops": iterations / t_boto,
        "signurlarity_ops": iterations / t_custom,
        "speedup": t_boto / t_custom,
    }

    # Informational output
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

    result_file.write_text(json.dumps(results, indent=2))

    print("=" * 60)


@pytest.mark.asyncio
async def test_create_bucket_perf_aio(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity async for create_bucket.

    This benchmark tests the async implementation's create_bucket functionality.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_create_bucket_perf_aio")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    async_light_client = AsyncClient(**rustfs_server)
    session = get_session()
    async with session.create_client(
        "s3", **rustfs_server, config=Config(signature_version="s3v4")
    ) as boto_client:
        iterations = 500
        bucket_prefix = "perf-bucket-create"

        # Warm-up to mitigate one-time costs
        for i in range(10):
            bucket = f"{bucket_prefix}-warmup-{i}"

            await boto_client.create_bucket(Bucket=bucket)

        for i in range(10):
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

    await async_light_client.close()
    results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": "create_bucket_aio",
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
        "boto_ops": iterations / t_boto,
        "signurlarity_ops": iterations / t_custom,
        "speedup": t_boto / t_custom,
    }

    # Informational output
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

    result_file.write_text(json.dumps(results, indent=2))

    print("=" * 60)


@pytest.mark.asyncio
async def test_delete_objects_perf_aio(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity async for delete_objects.

    This benchmark tests the async implementation's delete_objects functionality.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_delete_objects_perf_aio")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    bucket = "perf-delete-objects"
    num_keys = 10

    async_light_client = AsyncClient(**rustfs_server)
    session = get_session()
    async with session.create_client(
        "s3", **rustfs_server, config=Config(signature_version="s3v4")
    ) as boto_client:
        # Create the bucket for testing
        await async_light_client.create_bucket(Bucket=bucket)

        iterations = 10

        async def _populate(prefix: str):
            keys = [f"{prefix}-{i}.txt" for i in range(num_keys)]
            for k in keys:
                await boto_client.put_object(Bucket=bucket, Key=k, Body=b"data")
            return keys

        # Warm-up
        for i in range(5):
            keys = await _populate(f"warmup-boto-{i}")
            await boto_client.delete_objects(
                Bucket=bucket, Delete={"Objects": [{"Key": k} for k in keys]}
            )

        for i in range(5):
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

    await async_light_client.close()
    results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": "delete_objects_aio",
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
        "boto_ops": iterations / t_boto,
        "signurlarity_ops": iterations / t_custom,
        "speedup": t_boto / t_custom,
    }

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

    result_file.write_text(json.dumps(results, indent=2))

    print("=" * 60)


@pytest.mark.asyncio
async def test_put_object_perf_aio(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity async for put_object.

    Uploads a 1 KB object per iteration to a unique key.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_put_object_perf_aio")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    rng = random.Random(42)  # noqa: S311
    bucket = "perf-put-object-aio"
    body = b"x" * 1024  # 1 KB

    async_light_client = AsyncClient(**rustfs_server)
    session = get_session()
    async with session.create_client(
        "s3", **rustfs_server, config=Config(signature_version="s3v4")
    ) as boto_client:
        await boto_client.create_bucket(Bucket=bucket)

        iterations = 10

        # Warm-up
        for i in range(10):
            await boto_client.put_object(
                Bucket=bucket, Key=f"warmup-boto-{i}.txt", Body=body
            )
        for i in range(10):
            await async_light_client.put_object(
                Bucket=bucket, Key=f"warmup-light-{i}.txt", Body=body
            )

        async def run_boto(n: int):
            for _ in range(n):
                await boto_client.put_object(
                    Bucket=bucket,
                    Key=f"bench-boto-{rng.randint(0, 1_000_000)}.txt",
                    Body=body,
                )

        async def run_custom(n: int):
            for _ in range(n):
                await async_light_client.put_object(
                    Bucket=bucket,
                    Key=f"bench-light-{rng.randint(0, 1_000_000)}.txt",
                    Body=body,
                )

        t_boto = await _timeit_async_helper(run_boto, iterations)
        t_custom = await _timeit_async_helper(run_custom, iterations)

    await async_light_client.close()
    results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": "put_object_aio",
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
        "boto_ops": iterations / t_boto,
        "signurlarity_ops": iterations / t_custom,
        "speedup": t_boto / t_custom,
    }

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

    result_file.write_text(json.dumps(results, indent=2))
    print("=" * 60)


@pytest.mark.asyncio
async def test_list_objects_perf_aio(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity async for list_objects.

    Pre-populates 10 objects and benchmarks listing them with a prefix filter.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_list_objects_perf_aio")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    bucket = "perf-list-objects-aio"
    prefix = "bench/"

    async_light_client = AsyncClient(**rustfs_server)
    session = get_session()
    async with session.create_client(
        "s3", **rustfs_server, config=Config(signature_version="s3v4")
    ) as boto_client:
        await boto_client.create_bucket(Bucket=bucket)
        for i in range(10):
            await boto_client.put_object(
                Bucket=bucket, Key=f"{prefix}obj-{i}.txt", Body=b"data"
            )

        iterations = 10

        # Warm-up
        for _ in range(10):
            await boto_client.list_objects(Bucket=bucket, Prefix=prefix)
        for _ in range(10):
            await async_light_client.list_objects(Bucket=bucket, Prefix=prefix)

        async def run_boto(n: int):
            for _ in range(n):
                await boto_client.list_objects(Bucket=bucket, Prefix=prefix)

        async def run_custom(n: int):
            for _ in range(n):
                await async_light_client.list_objects(Bucket=bucket, Prefix=prefix)

        t_boto = await _timeit_async_helper(run_boto, iterations)
        t_custom = await _timeit_async_helper(run_custom, iterations)

    await async_light_client.close()
    results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": "list_objects_aio",
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
        "boto_ops": iterations / t_boto,
        "signurlarity_ops": iterations / t_custom,
        "speedup": t_boto / t_custom,
    }

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

    result_file.write_text(json.dumps(results, indent=2))
    print("=" * 60)


@pytest.mark.asyncio
async def test_copy_object_perf_aio(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity async for copy_object.

    Pre-uploads a source object and benchmarks copying it to unique destination keys.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_copy_object_perf_aio")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    rng = random.Random(42)  # noqa: S311
    bucket = "perf-copy-object-aio"
    src_key = "source.txt"

    async_light_client = AsyncClient(**rustfs_server)
    session = get_session()
    async with session.create_client(
        "s3", **rustfs_server, config=Config(signature_version="s3v4")
    ) as boto_client:
        await boto_client.create_bucket(Bucket=bucket)
        await boto_client.put_object(Bucket=bucket, Key=src_key, Body=b"source content")

        iterations = 10

        # Warm-up
        for i in range(10):
            await boto_client.copy_object(
                Bucket=bucket,
                Key=f"warmup-boto-{i}.txt",
                CopySource={"Bucket": bucket, "Key": src_key},
            )
        for i in range(10):
            await async_light_client.copy_object(
                Bucket=bucket,
                Key=f"warmup-light-{i}.txt",
                CopySource=f"{bucket}/{src_key}",
            )

        async def run_boto(n: int):
            for _ in range(n):
                await boto_client.copy_object(
                    Bucket=bucket,
                    Key=f"bench-boto-{rng.randint(0, 1_000_000)}.txt",
                    CopySource={"Bucket": bucket, "Key": src_key},
                )

        async def run_custom(n: int):
            for _ in range(n):
                await async_light_client.copy_object(
                    Bucket=bucket,
                    Key=f"bench-light-{rng.randint(0, 1_000_000)}.txt",
                    CopySource=f"{bucket}/{src_key}",
                )

        t_boto = await _timeit_async_helper(run_boto, iterations)
        t_custom = await _timeit_async_helper(run_custom, iterations)

    await async_light_client.close()
    results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": "copy_object_aio",
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
        "boto_ops": iterations / t_boto,
        "signurlarity_ops": iterations / t_custom,
        "speedup": t_boto / t_custom,
    }

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

    result_file.write_text(json.dumps(results, indent=2))
    print("=" * 60)


@pytest.mark.asyncio
async def test_upload_file_perf_aio(rustfs_server, test_results_dir):
    """Compare performance of boto3 vs signurlarity async for upload_file.

    Uses a 1 KB temporary file and uploads to unique keys per iteration.
    aiobotocore has no upload_file; boto reference uses put_object with file read.
    """
    py_vers = sys.version_info
    test_dir = test_results_dir / Path("test_upload_file_perf_aio")
    os.makedirs(test_dir, exist_ok=True)
    result_file: Path = test_dir / Path(f"run_{py_vers.major}.{py_vers.minor}.json")

    rng = random.Random(42)  # noqa: S311
    bucket = "perf-upload-file-aio"

    async_light_client = AsyncClient(**rustfs_server)
    session = get_session()

    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as tmp:
        tmp.write(b"y" * 1024)  # 1 KB
        tmp_path = tmp.name

    try:
        async with session.create_client(
            "s3", **rustfs_server, config=Config(signature_version="s3v4")
        ) as boto_client:
            await boto_client.create_bucket(Bucket=bucket)

            iterations = 10

            # Warm-up (aiobotocore has no upload_file; use put_object with file read)
            for i in range(10):
                with open(tmp_path, "rb") as fh:  # noqa: PTH123
                    await boto_client.put_object(
                        Bucket=bucket, Key=f"warmup-boto-{i}.bin", Body=fh.read()
                    )
            for i in range(10):
                await async_light_client.upload_file(
                    Filename=tmp_path, Bucket=bucket, Key=f"warmup-light-{i}.bin"
                )

            async def run_boto(n: int):
                for _ in range(n):
                    with open(tmp_path, "rb") as fh:  # noqa: PTH123
                        await boto_client.put_object(
                            Bucket=bucket,
                            Key=f"bench-boto-{rng.randint(0, 1_000_000)}.bin",
                            Body=fh.read(),
                        )

            async def run_custom(n: int):
                for _ in range(n):
                    await async_light_client.upload_file(
                        Filename=tmp_path,
                        Bucket=bucket,
                        Key=f"bench-light-{rng.randint(0, 1_000_000)}.bin",
                    )

            t_boto = await _timeit_async_helper(run_boto, iterations)
            t_custom = await _timeit_async_helper(run_custom, iterations)
    finally:
        os.unlink(tmp_path)

    await async_light_client.close()
    results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": "upload_file_aio",
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
        "boto_ops": iterations / t_boto,
        "signurlarity_ops": iterations / t_custom,
        "speedup": t_boto / t_custom,
    }

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

    result_file.write_text(json.dumps(results, indent=2))
    print("=" * 60)
