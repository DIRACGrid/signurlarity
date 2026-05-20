"""Test fixtures for the benchmark_tests directory.

This file re-exports constants, utilities, and timing functions from the root conftest.py.
Pytest fixtures defined in root conftest.py are automatically available.
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

# Add project root to path so we can import root_conftest_module
_project_root = str(Path(__file__).parent.parent)
if _project_root not in sys.path:
    sys.path.insert(0, _project_root)

# Import root conftest as a module with a unique name
_root_conftest_path = str(Path(_project_root) / "conftest.py")
spec = importlib.util.spec_from_file_location(
    "root_conftest_module", _root_conftest_path
)
root_conftest_module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(root_conftest_module)

# Re-export constants, utilities, and timing functions
# (fixtures are automatically available via pytest, but we need these for direct imports)
BUCKET_NAME = root_conftest_module.BUCKET_NAME
CHECKSUM_ALGORITHM = root_conftest_module.CHECKSUM_ALGORITHM
MISSING_BUCKET_NAME = root_conftest_module.MISSING_BUCKET_NAME
OTHER_BUCKET_NAME = root_conftest_module.OTHER_BUCKET_NAME
_timeit = root_conftest_module._timeit
_timeit_async = root_conftest_module._timeit_async
_timeit_async_helper = root_conftest_module._timeit_async_helper
b16_to_b64 = root_conftest_module.b16_to_b64
random_file = root_conftest_module.random_file
