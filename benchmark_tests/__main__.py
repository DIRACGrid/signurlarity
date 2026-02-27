from __future__ import annotations

import argparse
import glob
import json
from collections import defaultdict
from pathlib import Path
from typing import Optional

from rich.console import Console
from rich.table import Table


def rich_table_to_markdown(table):
    # Extract headers
    headers = [col.header for col in table.columns]
    markdown = "| " + " | ".join(headers) + " |\n"
    markdown += "| " + " | ".join(["---"] * len(headers)) + " |\n"
    nb_cells = max([len(col._cells) for col in table.columns])
    # Extract rows
    for row_id in range(nb_cells):
        row = [col._cells[row_id] for col in table.columns]
        markdown += "| " + " | ".join(row) + " |\n"
    return markdown


def compare_results(result_dir: Path, md_output: Optional[Path] = None):
    if not result_dir.exists():
        raise ValueError(f"The directory {result_dir} does not exist !")
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
    for test_name, test_data in sorted(global_results.items()):
        row = [test_name]
        for version in versions:
            if version in test_data:
                val = f"{test_data[version]:.3f}"
            else:
                val = "?"
            row.append(val)
        table.add_row(*row)

    # Display the table
    console = Console()
    console.print(table)

    if md_output:
        md_output.write_text(rich_table_to_markdown(table))


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--test-results-dir", required=True, help="Path to the perf tests results."
    )
    parser.add_argument(
        "--md-output", required=False, help="Path to store the markdown output."
    )
    args = parser.parse_args()
    md_output = None
    if args.md_output:
        md_output = Path(args.md_output).resolve()
    compare_results(Path(args.test_results_dir).resolve(), md_output)


if __name__ == "__main__":
    parse_args()
