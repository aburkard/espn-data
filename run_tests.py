#!/usr/bin/env python3
"""
Script to run all ESPN data tests.
"""

import sys
import subprocess
import argparse


def main():
    parser = argparse.ArgumentParser(description="Run ESPN data tests")
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable verbose output")
    parser.add_argument("-k", "--keyword", type=str, default=None, help="Keyword to filter tests")
    parser.add_argument("-m", "--mark", type=str, default=None, help="Run tests with specific marks")
    parser.add_argument("--collect-only", action="store_true", help="Only collect tests, don't run them")

    args = parser.parse_args()

    print(f"Running ESPN data tests...")

    # Build the pytest command
    cmd = ["pytest", "-xvs", "tests/"]

    if args.verbose:
        cmd.append("-v")

    if args.keyword:
        cmd.append(f"-k '{args.keyword}'")

    if args.mark:
        cmd.append(f"-m {args.mark}")

    if args.collect_only:
        cmd.append("--collect-only")

    # Run the tests
    try:
        result = subprocess.run(" ".join(cmd), shell=True)
        return result.returncode
    except Exception as e:
        print(f"Error running tests: {str(e)}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
