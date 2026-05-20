"""Test fixtures for the profiling_tests directory.

This file re-exports fixtures from the root conftest.py and can contain
profiling-specific fixtures that are not needed elsewhere.

NOTE: pytest_addoption is NOT re-exported here because it should only be
registered once (in the root conftest.py).
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

# Re-export fixtures (but NOT pytest_addoption hook which should only be registered once)
test_results_dir = root_conftest_module.test_results_dir
