from __future__ import annotations

import json
import time
from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--test-results-dir",
        type=Path,
        default=None,
        help="Path to store the perf test results",
    )


@pytest.fixture(scope="module")
def test_results_dir(request) -> Path:
    test_results_dir = request.config.getoption("--test-results-dir")
    if test_results_dir is None:
        pytest.skip(
            "Requires a directory to store the test results with --test-results-dir"
        )
    test_results_dir = test_results_dir.resolve()
    yield test_results_dir


@pytest.fixture(scope="module")
def rustfs_server():
    """Spawn a test rustfs image for benchmarking."""
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
        "rustfs/rustfs:latest",
        "/data",
    ]
    subprocess.run(cmd, check=True)  # noqa: S603
    time.sleep(1)  # Wait for server to start
    yield {
        "endpoint_url": "http://localhost:9000",
        "aws_access_key_id": AWS_ACCESS_KEY_ID,
        "aws_secret_access_key": AWS_SECRET_ACCESS_KEY,
    }
    cmd = ["docker", "stop", "rustfs_local"]
    subprocess.run(cmd, check=True)  # noqa: S603


def _timeit(fn, iterations: int) -> float:
    """Measure execution time of a function."""
    import time

    start = time.perf_counter()
    fn(iterations)
    return time.perf_counter() - start


def _timeit_async(fn, iterations: int) -> float:
    """Measure execution time of an async function."""
    import asyncio
    import time

    start = time.perf_counter()
    asyncio.run(fn(iterations))
    return time.perf_counter() - start


async def _timeit_async_helper(fn, iterations: int) -> float:
    """Measure the execution time of an async function within async context."""
    import time

    start = time.perf_counter()
    await fn(iterations)
    return time.perf_counter() - start


def _write_benchmark_results(
    result_file: Path,
    py_vers,
    tested_method: str,
    iterations: int,
    t_boto: float,
    t_custom: float,
) -> dict[str, float | int | str]:
    """Build and persist common benchmark result payloads."""
    results = {
        "python_version": f"{py_vers.major}.{py_vers.minor}",
        "tested_method": tested_method,
        "iterations": iterations,
        "boto_total": t_boto,
        "signurlarity_total": t_custom,
        "boto_ops": iterations / t_boto,
        "signurlarity_ops": iterations / t_custom,
        "speedup": t_boto / t_custom,
    }
    result_file.write_text(json.dumps(results, indent=2))
    return results
