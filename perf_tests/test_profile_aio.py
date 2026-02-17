from __future__ import annotations

import asyncio
import cProfile
import os
import pstats
import random
from pathlib import Path
from pstats import SortKey

from signurlarity.aio import AsyncClient

ITERATIONS = 100_000


def test_profile_generate_presigned_post_aio(perf_test_dir):
    """Compare performance of signurlarity async for presigned POST.

    This is a non-failing, informational test: it prints profiling info.
    """
    test_dir = perf_test_dir / Path("profile_generate_presigned_post_aio")
    os.makedirs(test_dir, exist_ok=True)

    rng = random.Random(42)  # noqa: S311
    bucket = "perf-bucket"
    key = "object_" * 10

    async_light_client = AsyncClient(
        **{
            "endpoint_url": "http://localhost:9000",
            "aws_access_key_id": "AWS_ACCESS_KEY_ID",
            "aws_secret_access_key": "AWS_SECRET_ACCESS_KEY",
        }
    )

    # Minimal fields/conditions for a fair apples-to-apples comparison
    fields = None
    conditions = None

    async def profile_coro():
        for _ in range(ITERATIONS):
            await async_light_client.generate_presigned_post(
                Bucket=bucket,
                Key=f"{key}-{rng.randint(0, 1_000_000)}",
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=60,
            )

    with cProfile.Profile() as pr:
        asyncio.run(profile_coro())
        stats = pstats.Stats(pr).strip_dirs()
        stats.dump_stats(test_dir / Path("presigned_post_aio.prof"))
        print(stats.sort_stats(SortKey.TIME).print_stats(10))
