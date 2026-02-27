from __future__ import annotations

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
