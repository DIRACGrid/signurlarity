from __future__ import annotations

import argparse
import glob
import json
from pathlib import Path
from collections import defaultdict
from rich.console import Console

from rich.table import Table


def compare_results(result_dir: Path):
    global_results = defaultdict(dict)

    for test_path in glob.glob(f"{result_dir}/test*/run_*.json"):
        test_result = json.loads(Path(test_path).read_text())
        global_results[test_result["tested_method"]][test_result["python_version"]] = (
            test_result["speedup"]
        )

    # Collect all unique Python versions
    versions = set()
    for test in global_results.values():
        versions.update(test.keys())
    versions = sorted(versions, key=lambda x: float(x))

    # Create the table
    table = Table(title="Test Results by Python Version")
    table.add_column("Test", style="cyan", no_wrap=True)
    for version in versions:
        table.add_column(version, style="magenta")

    # Add rows
    for test_name, test_data in global_results.items():
        row = [test_name]
        for version in versions:
            row.append(str(test_data.get(version, "?")))
        table.add_row(*row)

    # Display the table
    console = Console()
    console.print(table)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--perf-test-dir", required=True, help="Path to the perf tests results."
    )
    args = parser.parse_args()
    compare_results(Path(args.perf_test_dir))


if __name__ == "__main__":
    parse_args()
