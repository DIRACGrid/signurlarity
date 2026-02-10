from __future__ import annotations

from pathlib import Path

import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--perf-test-dir",
        type=Path,
        default=None,
        help="Path to store the perf test results",
    )


@pytest.fixture(scope="session")
def perf_test_dir(request) -> Path:
    perf_test_dir = request.config.getoption("--perf-test-dir")
    if perf_test_dir is None:
        pytest.skip("Requires a directory to store the test results")
    perf_test_dir = perf_test_dir.resolve()
    yield perf_test_dir
