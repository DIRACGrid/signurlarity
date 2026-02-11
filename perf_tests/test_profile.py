from __future__ import annotations

import cProfile
import os
import pstats
import random
from pathlib import Path
from pstats import SortKey

from signurlarity import Client


def test_profile_generate_presigned_post(perf_test_dir):
    """Compare performance of boto3 vs signurlarity for presigned POST.

    This is a non-failing, informational test: it prints timings and skips
    if the signurlarity implementation is not available.
    """
    test_dir = perf_test_dir / Path("profile_generate_presigned_post")
    os.makedirs(test_dir, exist_ok=True)

    rng = random.Random(42)  # noqa: S311
    bucket = "perf-bucket"
    key = "object.txt"

    light_client = Client(
        **{
            "endpoint_url": "http://localhost:9000",
            "aws_access_key_id": "AWS_ACCESS_KEY_ID",
            "aws_secret_access_key": "AWS_SECRET_ACCESS_KEY",
        }
    )

    # Minimal fields/conditions for a fair apples-to-apples comparison
    fields = None
    conditions = None

    iterations = 2000

    with cProfile.Profile() as pr:
        for _ in range(iterations):
            light_client.generate_presigned_post(
                Bucket=bucket,
                Key=f"{key}-{rng.randint(0, 1_000_000)}",
                Fields=fields,
                Conditions=conditions,
                ExpiresIn=60,
            )
        stats = pstats.Stats(pr).strip_dirs()
        stats.dump_stats(test_dir / Path("presigned_post.prof"))
        print(stats.sort_stats(SortKey.TIME).print_stats(10))
